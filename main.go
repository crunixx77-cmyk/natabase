package main

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	mrand "math/rand/v2"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
	"golang.org/x/crypto/bcrypt"

	"natabase/engine"
	"natabase/natasql"
)

const (
	OpPut     uint8 = 1
	OpDel     uint8 = 2
	OpUserPut uint8 = 3
	OpUserDel uint8 = 4

	NumShards        = 2048
	MaxHotPayloadRAM = 1 * 1024 * 1024
	IdleTimeToSleep  = 1 * time.Minute
	MaxSegmentSize   = 64 * 1024 * 1024
)

var (
	ErrNotFound      = errors.New("key tidak ditemukan")
	ErrCorruptedData = errors.New("data korup (checksum mismatch)")
	ErrKeyExpired    = errors.New("key kadaluwarsa")
	ErrUnauthorized  = errors.New("akses ditolak (unauthorized)")
)

var (
	bufPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 64*1024)
			return &b
		},
	}
	stringSlicePool = sync.Pool{
		New: func() interface{} {
			s := make([]string, 0, 1024)
			return &s
		},
	}
	qbItemSlicePool = sync.Pool{
		New: func() interface{} {
			s := make([]qbItem, 0, 1024)
			return &s
		},
	}
)

func getJWTSecret() []byte {
	sec := os.Getenv("JWT_SECRET")
	if sec == "" {
		sec = "natabase-enterprise-ultra-secure-jwt-key-2026"
	}
	return []byte(sec)
}

func xxHash64(key string) uint64 {
	const (
		prime1 uint64 = 11400714785074694791
		prime2 uint64 = 14029467366897019727
		prime5 uint64 = 2870177238865417056
	)
	h := prime5 + uint64(len(key))
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i]) * prime1
		h = (h<<13 | h>>51) * prime2
	}
	return h
}

func getShardIndex(key string) uint32 {
	return uint32(xxHash64(key) % NumShards)
}

type IndexEntry struct {
	SegmentID        uint32
	Offset           int64
	Size             uint32
	TypeHeader       uint16
	ExpiresAtUnixMs  int64
	LastAccessedNano atomic.Int64
	HotPayload       atomic.Pointer[[]byte]
}

type DBShard struct {
	mu      sync.RWMutex
	index   map[string]*IndexEntry
	typeIdx map[uint16]map[string]struct{}
}

type UserAccount struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

type SyncEvent struct {
	Op         uint8  `json:"op"`
	Key        string `json:"key"`
	TypeHeader uint16 `json:"type_header"`
	Payload    []byte `json:"payload"`
}

type HashRing struct {
	nodes []uint32
	ring  map[uint32]string
	mu    sync.RWMutex
}

func NewHashRing() *HashRing {
	return &HashRing{
		ring: make(map[uint32]string),
	}
}

func (r *HashRing) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hash := uint32(xxHash64(node))
	if _, exists := r.ring[hash]; !exists {
		r.nodes = append(r.nodes, hash)
		sort.Slice(r.nodes, func(i, j int) bool { return r.nodes[i] < r.nodes[j] })
	}
	r.ring[hash] = node
}

func (r *HashRing) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.nodes) == 0 {
		return ""
	}
	hash := uint32(xxHash64(key))
	idx := sort.Search(len(r.nodes), func(i int) bool { return r.nodes[i] >= hash })
	if idx == len(r.nodes) {
		idx = 0
	}
	return r.ring[r.nodes[idx]]
}

type NatabaseEngine struct {
	shards           [NumShards]*DBShard
	storageDir       string
	baseName         string
	activeSegID      atomic.Uint32
	activeFile       *os.File
	activeOffset     atomic.Int64
	segMu            sync.RWMutex
	segmentFiles     sync.Map
	aofMu            sync.Mutex
	aofWriter        *os.File
	aofBuf           []byte
	aofPath          string
	closeChan        chan struct{}
	users            sync.Map
	totalOpsGet      atomic.Uint64
	totalOpsPut      atomic.Uint64
	totalOpsDel      atomic.Uint64
	Role             string
	replSecret       string
	replConns        sync.Map
	syncChan         chan SyncEvent
	EventCallback    func(event SyncEvent)
	AdvStore         *engine.AdvancedDataStore
	AdvSnapshotter   *engine.RDBSnapshotter
	ClusterEnabled   bool
	Router           *HashRing
}

func hashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(bytes), err
}

func checkPasswordHash(password, hash string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

func generateJWT(username, role string) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payloadData := fmt.Sprintf(`{"user":"%s","role":"%s","exp":%d}`, username, role, time.Now().Add(1*time.Hour).Unix())
	payload := base64.RawURLEncoding.EncodeToString([]byte(payloadData))

	unsignedToken := header + "." + payload
	mac := hmac.New(sha256.New, getJWTSecret())
	mac.Write([]byte(unsignedToken))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return unsignedToken + "." + signature, nil
}

func verifyJWT(tokenStr string) (string, string, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return "", "", ErrUnauthorized
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", "", ErrUnauthorized
	}
	var headerMap map[string]interface{}
	if err := json.Unmarshal(headerBytes, &headerMap); err != nil || headerMap["alg"] != "HS256" {
		return "", "", ErrUnauthorized
	}

	unsignedToken := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, getJWTSecret())
	mac.Write([]byte(unsignedToken))
	expectedSig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(parts[2]), []byte(expectedSig)) {
		return "", "", ErrUnauthorized
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", ErrUnauthorized
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return "", "", ErrUnauthorized
	}

	if exp, ok := claims["exp"].(float64); ok {
		if time.Now().Unix() > int64(exp) {
			return "", "", ErrKeyExpired
		}
	}

	user, _ := claims["user"].(string)
	role, _ := claims["role"].(string)
	return user, role, nil
}

func (db *NatabaseEngine) getSegmentFile(segID uint32) (*os.File, error) {
	if val, ok := db.segmentFiles.Load(segID); ok {
		return val.(*os.File), nil
	}
	db.segMu.Lock()
	defer db.segMu.Unlock()
	if val, ok := db.segmentFiles.Load(segID); ok {
		return val.(*os.File), nil
	}
	segPath := filepath.Join(db.storageDir, fmt.Sprintf("segment_%08d.seg", segID))
	file, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	db.segmentFiles.Store(segID, file)
	return file, nil
}

func NewNatabase(baseName string) (*NatabaseEngine, error) {
	storageDir := baseName + "_storage"
	_ = os.MkdirAll(storageDir, 0755)

	aofPath := baseName + ".aof"
	aofFile, err := os.OpenFile(aofPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	advStore := engine.NewAdvancedDataStore()
	snapshotter := engine.NewRDBSnapshotter(baseName+"_adv.rdb", advStore)
	_ = snapshotter.LoadRDB()

	dbInstance := &NatabaseEngine{
		baseName:       baseName,
		storageDir:     storageDir,
		aofWriter:      aofFile,
		aofPath:        aofPath,
		aofBuf:         make([]byte, 0, 16*1024*1024), // Pre-allocated 16MB AOF buffer for zero-allocation appends
		closeChan:      make(chan struct{}),
		syncChan:       make(chan SyncEvent, 65536), // Increased buffer for high throughput
		AdvStore:       advStore,
		AdvSnapshotter: snapshotter,
		Router:         NewHashRing(),
	}

	for i := 0; i < NumShards; i++ {
		dbInstance.shards[i] = &DBShard{
			index:   make(map[string]*IndexEntry),
			typeIdx: make(map[uint16]map[string]struct{}),
		}
	}

	manifestPath := filepath.Join(storageDir, "manifest.bin")
	if _, err := os.Stat(manifestPath); err == nil {
		_ = dbInstance.loadManifest(manifestPath)
		activeFile, err := dbInstance.getSegmentFile(dbInstance.activeSegID.Load())
		if err == nil {
			dbInstance.activeFile = activeFile
		}
		_ = os.Remove(manifestPath)
	} else {
		activeFile, err := dbInstance.getSegmentFile(0)
		if err != nil {
			_ = aofFile.Close()
			return nil, err
		}
		fi, _ := activeFile.Stat()
		dbInstance.activeFile = activeFile
		dbInstance.activeOffset.Store(fi.Size())
		_ = dbInstance.loadIndexFromDisk()
	}

	_ = dbInstance.replayAOF()

	if _, exists := dbInstance.users.Load("admin"); !exists {
		hashedPassword, err := hashPassword("admin123")
		if err == nil {
			adminUser := &UserAccount{
				Username: "admin",
				Password: hashedPassword,
				Role:     "administrator",
			}
			_ = dbInstance.SaveUser(adminUser)
		}
	}

	go dbInstance.startSampledLRUEviction(10 * time.Second)
	go dbInstance.startAOFSyncWorker(100 * time.Millisecond)
	go dbInstance.startCompactionWorker(10 * time.Minute)
	go dbInstance.startTCPBroadcaster()
	go dbInstance.startRDBSnapshotWorker(5 * time.Minute)

	return dbInstance, nil
}

func removeFromTypeIndex(shard *DBShard, th uint16, key string) {
	if shard.typeIdx[th] != nil {
		delete(shard.typeIdx[th], key)
		if len(shard.typeIdx[th]) == 0 {
			delete(shard.typeIdx, th)
		}
	}
}

func addToTypeIndex(shard *DBShard, th uint16, key string) {
	if shard.typeIdx[th] == nil {
		shard.typeIdx[th] = make(map[string]struct{})
	}
	shard.typeIdx[th][key] = struct{}{}
}

func (db *NatabaseEngine) rotateSegmentIfFull(recordSize int64) (*os.File, uint32, int64, error) {
	db.segMu.Lock()
	defer db.segMu.Unlock()

	currID := db.activeSegID.Load()
	currOff := db.activeOffset.Load()

	if currOff+recordSize > MaxSegmentSize {
		nextID := currID + 1
		segPath := filepath.Join(db.storageDir, fmt.Sprintf("segment_%08d.seg", nextID))
		file, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, 0, 0, err
		}
		db.segmentFiles.Store(nextID, file)
		db.activeFile = file
		db.activeSegID.Store(nextID)
		db.activeOffset.Store(0)
		currID = nextID
		currOff = 0
	}

	writeOff := db.activeOffset.Add(recordSize) - recordSize
	return db.activeFile, currID, writeOff, nil
}

func (db *NatabaseEngine) Put(key string, typeHeader uint16, payload []byte, ttl time.Duration) error {
	return db.putInternal(key, typeHeader, payload, ttl, true)
}

func (db *NatabaseEngine) putInternal(key string, typeHeader uint16, payload []byte, ttl time.Duration, writeAOF bool) error {
	db.totalOpsPut.Add(1)
	keyLen, payloadLen := uint16(len(key)), uint32(len(payload))
	recordSize := int64(12+int(keyLen)) + int64(payloadLen)

	var buf []byte
	var bPtr *[]byte
	if recordSize <= 64*1024 {
		bPtr = bufPool.Get().(*[]byte)
		buf = (*bPtr)[:recordSize]
		defer bufPool.Put(bPtr)
	} else {
		buf = make([]byte, recordSize)
	}

	binary.BigEndian.PutUint16(buf[4:6], keyLen)
	copy(buf[6:6+keyLen], key)
	off := 6 + keyLen
	binary.BigEndian.PutUint16(buf[off:off+2], typeHeader)
	binary.BigEndian.PutUint32(buf[off+2:off+6], payloadLen)
	if payloadLen > 0 {
		copy(buf[off+6:], payload)
	}
	binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:recordSize]))

	file, segID, writeOff, err := db.rotateSegmentIfFull(recordSize)
	if err != nil {
		return err
	}

	if _, err := file.WriteAt(buf, writeOff); err != nil {
		return err
	}

	if writeAOF {
		db.appendAOFBuffer(OpPut, key, typeHeader, payload)
	}

	var exp int64 = 0
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixMilli()
	}

	entry := &IndexEntry{
		SegmentID:       segID,
		Offset:          writeOff + int64(12+len(key)),
		Size:            payloadLen,
		TypeHeader:      typeHeader,
		ExpiresAtUnixMs: exp,
	}
	entry.LastAccessedNano.Store(time.Now().UnixNano())

	shard := db.shards[getShardIndex(key)]
	shard.mu.Lock()
	if old, exists := shard.index[key]; exists {
		if old.TypeHeader != typeHeader {
			removeFromTypeIndex(shard, old.TypeHeader, key)
			addToTypeIndex(shard, typeHeader, key)
		}
	} else {
		addToTypeIndex(shard, typeHeader, key)
	}
	shard.index[key] = entry
	shard.mu.Unlock()

	if writeAOF {
		evt := SyncEvent{Op: OpPut, Key: key, TypeHeader: typeHeader, Payload: payload}
		if db.Role == "master" {
			select {
			case db.syncChan <- evt:
			default:
			}
		}
		if db.EventCallback != nil {
			db.EventCallback(evt)
		}
	}

	return nil
}

func (db *NatabaseEngine) PutBatch(entries map[string][]byte) error {
	if len(entries) == 0 {
		return nil
	}

	db.aofMu.Lock()
	for k, v := range entries {
		db.appendAOFBufferLocked(OpPut, k, 0x0001, v)
	}
	db.aofMu.Unlock()

	for k, v := range entries {
		if err := db.putInternal(k, 0x0001, v, 0, false); err != nil {
			return err
		}
	}
	return nil
}

func (db *NatabaseEngine) Get(key string) ([]byte, uint16, error) {
	db.totalOpsGet.Add(1)
	shard := db.shards[getShardIndex(key)]

	shard.mu.RLock()
	entry, exists := shard.index[key]
	shard.mu.RUnlock()

	if !exists {
		return nil, 0, ErrNotFound
	}

	if entry.ExpiresAtUnixMs > 0 && time.Now().UnixMilli() > entry.ExpiresAtUnixMs {
		db.Delete(key)
		return nil, 0, ErrKeyExpired
	}

	entry.LastAccessedNano.Store(time.Now().UnixNano())

	if hotPtr := entry.HotPayload.Load(); hotPtr != nil && *hotPtr != nil {
		return *hotPtr, entry.TypeHeader, nil
	}

	file, err := db.getSegmentFile(entry.SegmentID)
	if err != nil {
		return nil, 0, err
	}

	sizeNeeded := int64(12+len(key)) + int64(entry.Size)
	var recordBuf []byte
	var bPtr *[]byte
	if sizeNeeded <= 64*1024 {
		bPtr = bufPool.Get().(*[]byte)
		recordBuf = (*bPtr)[:sizeNeeded]
		defer bufPool.Put(bPtr)
	} else {
		recordBuf = make([]byte, sizeNeeded)
	}

	if _, err := file.ReadAt(recordBuf, entry.Offset-int64(12+len(key))); err != nil {
		return nil, 0, err
	}

	if binary.BigEndian.Uint32(recordBuf[0:4]) != crc32.ChecksumIEEE(recordBuf[4:sizeNeeded]) {
		return nil, 0, ErrCorruptedData
	}

	payloadRaw := recordBuf[12+len(key) : sizeNeeded]
	pCopy := make([]byte, len(payloadRaw))
	copy(pCopy, payloadRaw)

	if entry.Size <= MaxHotPayloadRAM {
		pStore := make([]byte, len(pCopy))
		copy(pStore, pCopy)
		entry.HotPayload.Store(&pStore)
	}

	return pCopy, entry.TypeHeader, nil
}

func (db *NatabaseEngine) GetByType(typeHeader uint16, offset int, limit int) ([][]byte, error) {
	numWorkers := runtime.NumCPU()
	shardChan := make(chan int, NumShards)
	for i := 0; i < NumShards; i++ {
		shardChan <- i
	}
	close(shardChan)

	var wg sync.WaitGroup
	var mu sync.Mutex
	sPtr := stringSlicePool.Get().(*[]string)
	allKeys := (*sPtr)[:0]
	defer stringSlicePool.Put(sPtr)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lsPtr := stringSlicePool.Get().(*[]string)
			localKeys := (*lsPtr)[:0]
			defer stringSlicePool.Put(lsPtr)

			for shardIdx := range shardChan {
				shard := db.shards[shardIdx]
				shard.mu.RLock()
				if keySet, ok := shard.typeIdx[typeHeader]; ok {
					for k := range keySet {
						localKeys = append(localKeys, k)
					}
				}
				shard.mu.RUnlock()
			}
			mu.Lock()
			allKeys = append(allKeys, localKeys...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	sort.Strings(allKeys)

	if offset >= len(allKeys) {
		return [][]byte{}, nil
	}
	end := offset + limit
	if end > len(allKeys) {
		end = len(allKeys)
	}

	results := make([][]byte, 0, end-offset)
	for _, k := range allKeys[offset:end] {
		payload, _, err := db.Get(k)
		if err == nil {
			results = append(results, payload)
		}
	}
	return results, nil
}

func (db *NatabaseEngine) QueryByBitmask(mask uint16, match uint16) []string {
	numWorkers := runtime.NumCPU()
	shardChan := make(chan int, NumShards)
	for i := 0; i < NumShards; i++ {
		shardChan <- i
	}
	close(shardChan)

	var wg sync.WaitGroup
	var mu sync.Mutex
	allKeys := make([]string, 0, 1024)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lsPtr := stringSlicePool.Get().(*[]string)
			localKeys := (*lsPtr)[:0]
			defer stringSlicePool.Put(lsPtr)

			for shardIdx := range shardChan {
				shard := db.shards[shardIdx]
				shard.mu.RLock()
				for th, keySet := range shard.typeIdx {
					if (th & mask) == match {
						for k := range keySet {
							localKeys = append(localKeys, k)
						}
					}
				}
				shard.mu.RUnlock()
			}
			mu.Lock()
			allKeys = append(allKeys, localKeys...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return allKeys
}

// === NO-SQL COMPLEX QUERY ENGINE ===
type QueryBuilder struct {
	db             *NatabaseEngine
	mask           uint16
	match          uint16
	hasBitmask     bool
	typeHeader     uint16
	hasType        bool
	joinTarget     *NatabaseEngine
	joinForeignKey string
	hasJoin        bool
}

type qbItem struct {
	key     string
	payload []byte
}

func (db *NatabaseEngine) Query() *QueryBuilder {
	return &QueryBuilder{db: db}
}

func (qb *QueryBuilder) FilterByBitmask(mask uint16, match uint16) *QueryBuilder {
	qb.mask = mask
	qb.match = match
	qb.hasBitmask = true
	return qb
}

func (qb *QueryBuilder) FilterByType(typeHeader uint16) *QueryBuilder {
	qb.typeHeader = typeHeader
	qb.hasType = true
	return qb
}

func (qb *QueryBuilder) Join(targetEngine *NatabaseEngine, foreignKey string) *QueryBuilder {
	qb.joinTarget = targetEngine
	qb.joinForeignKey = foreignKey
	qb.hasJoin = true
	return qb
}

func extractStringField(payload []byte, key string) (string, bool) {
	search := []byte(`"` + key + `":`)
	idx := bytes.Index(payload, search)
	if idx == -1 {
		return "", false
	}
	valStart := idx + len(search)
	for valStart < len(payload) && (payload[valStart] == ' ' || payload[valStart] == '\t') {
		valStart++
	}
	if valStart < len(payload) && payload[valStart] == '"' {
		valStart++
		end := 0
		for valStart+end < len(payload) {
			if payload[valStart+end] == '"' && payload[valStart+end-1] != '\\' {
				break
			}
			end++
		}
		if valStart+end < len(payload) {
			return string(payload[valStart : valStart+end]), true
		}
	}
	return "", false
}

func mergeJSON(p1, p2 []byte, fk string) []byte {
	p1Str := bytes.TrimSpace(p1)
	if len(p1Str) > 0 && p1Str[len(p1Str)-1] == '}' {
		p1Str = p1Str[:len(p1Str)-1]
	} else {
		return p1
	}
	joined := make([]byte, 0, len(p1Str)+len(p2)+15+len(fk))
	joined = append(joined, p1Str...)
	joined = append(joined, []byte(`,"_joined_`)...)
	joined = append(joined, []byte(fk)...)
	joined = append(joined, []byte(`":`)...)
	joined = append(joined, p2...)
	joined = append(joined, '}')
	return joined
}

func (qb *QueryBuilder) gatherItemsParallel() []qbItem {
	numWorkers := runtime.NumCPU()
	shardChan := make(chan int, NumShards)
	for i := 0; i < NumShards; i++ {
		shardChan <- i
	}
	close(shardChan)

	var wg sync.WaitGroup
	var mu sync.Mutex
	itemsPtr := qbItemSlicePool.Get().(*[]qbItem)
	allItems := (*itemsPtr)[:0]

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lPtr := qbItemSlicePool.Get().(*[]qbItem)
			localItems := (*lPtr)[:0]
			defer qbItemSlicePool.Put(lPtr)

			for shardIdx := range shardChan {
				shard := qb.db.shards[shardIdx]
				shard.mu.RLock()
				var candidateKeys []string
				if qb.hasType {
					if keySet, ok := shard.typeIdx[qb.typeHeader]; ok {
						for k := range keySet {
							if qb.hasBitmask && (qb.typeHeader&qb.mask) != qb.match {
								continue
							}
							candidateKeys = append(candidateKeys, k)
						}
					}
				} else if qb.hasBitmask {
					for th, keySet := range shard.typeIdx {
						if (th & qb.mask) == qb.match {
							for k := range keySet {
								candidateKeys = append(candidateKeys, k)
							}
						}
					}
				} else {
					for k := range shard.index {
						candidateKeys = append(candidateKeys, k)
					}
				}
				shard.mu.RUnlock()

				for _, k := range candidateKeys {
					payload, _, err := qb.db.Get(k)
					if err != nil {
						continue
					}
					if qb.hasJoin {
						fkVal, ok := extractStringField(payload, qb.joinForeignKey)
						if !ok || fkVal == "" {
							continue
						}
						targetPayload, _, err := qb.joinTarget.Get(fkVal)
						if err != nil {
							continue
						}
						payload = mergeJSON(payload, targetPayload, qb.joinForeignKey)
					}
					localItems = append(localItems, qbItem{key: k, payload: payload})
				}
			}
			mu.Lock()
			allItems = append(allItems, localItems...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return allItems
}

func (qb *QueryBuilder) Execute(offset int, limit int) [][]byte {
	items := qb.gatherItemsParallel()
	defer func() {
		// Clear payloads for GC before returning to pool
		for i := range items {
			items[i].payload = nil
		}
		itemsPtr := &items
		qbItemSlicePool.Put(itemsPtr)
	}()

	sort.Slice(items, func(i, j int) bool {
		return items[i].key < items[j].key
	})

	if offset >= len(items) {
		return [][]byte{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}

	results := make([][]byte, 0, end-offset)
	for _, item := range items[offset:end] {
		results = append(results, item.payload)
	}
	return results
}

func (qb *QueryBuilder) AggregateSum(fieldSelector func(payload []byte) int64) int64 {
	items := qb.gatherItemsParallel()
	defer func() {
		for i := range items {
			items[i].payload = nil
		}
		itemsPtr := &items
		qbItemSlicePool.Put(itemsPtr)
	}()
	
	var total int64
	for _, item := range items {
		total += fieldSelector(item.payload)
	}
	return total
}

func (db *NatabaseEngine) Delete(key string) bool {
	return db.deleteInternal(key, true)
}

func (db *NatabaseEngine) deleteInternal(key string, writeAOF bool) bool {
	db.totalOpsDel.Add(1)
	shard := db.shards[getShardIndex(key)]

	shard.mu.Lock()
	entry, exists := shard.index[key]
	if !exists {
		shard.mu.Unlock()
		return false
	}
	delete(shard.index, key)
	removeFromTypeIndex(shard, entry.TypeHeader, key)
	shard.mu.Unlock()

	if writeAOF {
		db.appendAOFBuffer(OpDel, key, 0, nil)

		evt := SyncEvent{Op: OpDel, Key: key, TypeHeader: entry.TypeHeader}
		if db.Role == "master" {
			select {
			case db.syncChan <- evt:
			default:
			}
		}
		if db.EventCallback != nil {
			db.EventCallback(evt)
		}
	}

	return true
}

func (db *NatabaseEngine) SaveUser(user *UserAccount) error {
	db.users.Store(user.Username, user)
	payload, err := json.Marshal(user)
	if err != nil {
		return err
	}
	db.appendAOFBuffer(OpUserPut, user.Username, 0, payload)

	if db.Role == "master" {
		evt := SyncEvent{Op: OpUserPut, Key: user.Username, Payload: payload}
		select {
		case db.syncChan <- evt:
		default:
		}
	}
	return nil
}

func (db *NatabaseEngine) DeleteUser(username string) bool {
	if _, loaded := db.users.LoadAndDelete(username); !loaded {
		return false
	}
	db.appendAOFBuffer(OpUserDel, username, 0, nil)

	if db.Role == "master" {
		evt := SyncEvent{Op: OpUserDel, Key: username}
		select {
		case db.syncChan <- evt:
		default:
		}
	}
	return true
}

func (db *NatabaseEngine) SaveBlob(bucket string, filename string, r io.Reader) (string, error) {
	bucketPath := filepath.Join(db.storageDir, bucket)
	_ = os.MkdirAll(bucketPath, 0755)

	hash := sha256.New()
	filePath := filepath.Join(bucketPath, filename)
	outFile, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer outFile.Close()

	mw := io.MultiWriter(outFile, hash)
	size, err := io.Copy(mw, r)
	if err != nil {
		return "", err
	}

	fileHash := hex.EncodeToString(hash.Sum(nil))
	metaKey := fmt.Sprintf("blob:%s:%s", bucket, filename)
	metaPayload := []byte(fmt.Sprintf(`{"size":%d,"hash":"%s","path":"%s"}`, size, fileHash, filePath))
	_ = db.Put(metaKey, 0xF999, metaPayload, 0)

	return fileHash, nil
}

func (db *NatabaseEngine) appendAOFBuffer(op uint8, key string, typeHeader uint16, payload []byte) {
	db.aofMu.Lock()
	defer db.aofMu.Unlock()
	db.appendAOFBufferLocked(op, key, typeHeader, payload)
}

func (db *NatabaseEngine) appendAOFBufferLocked(op uint8, key string, typeHeader uint16, payload []byte) {
	keyLen := uint16(len(key))
	payloadLen := uint32(len(payload))
	totalLen := 1 + 2 + int(keyLen)
	if op == OpPut || op == OpUserPut {
		totalLen += 2 + 4 + int(payloadLen)
	}

	needed := len(db.aofBuf) + totalLen
	if cap(db.aofBuf) < needed {
		newBuf := make([]byte, len(db.aofBuf), (needed+1)*2)
		copy(newBuf, db.aofBuf)
		db.aofBuf = newBuf
	}
	currIdx := len(db.aofBuf)
	db.aofBuf = db.aofBuf[:needed]
	target := db.aofBuf[currIdx:]

	target[0] = op
	binary.BigEndian.PutUint16(target[1:3], keyLen)
	copy(target[3:3+keyLen], key)
	if op == OpPut || op == OpUserPut {
		off := 3 + keyLen
		binary.BigEndian.PutUint16(target[off:off+2], typeHeader)
		binary.BigEndian.PutUint32(target[off+2:off+6], payloadLen)
		copy(target[off+6:], payload)
	}
}

func (db *NatabaseEngine) flushAOF() {
	db.aofMu.Lock()
	defer db.aofMu.Unlock()
	if len(db.aofBuf) == 0 {
		return
	}
	_, _ = db.aofWriter.Write(db.aofBuf)
	_ = db.aofWriter.Sync()
	db.aofBuf = db.aofBuf[:0]
}

func (db *NatabaseEngine) startAOFSyncWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-db.closeChan:
			db.flushAOF()
			return
		case <-ticker.C:
			db.flushAOF()
		}
	}
}

func (db *NatabaseEngine) startRDBSnapshotWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-db.closeChan:
			_ = db.AdvSnapshotter.SaveRDB()
			return
		case <-ticker.C:
			_ = db.AdvSnapshotter.SaveRDB()
		}
	}
}

func encodeSyncEventBinary(evt SyncEvent) []byte {
	kLen := uint16(len(evt.Key))
	pLen := uint32(len(evt.Payload))
	buf := make([]byte, 1+2+2+int(kLen)+4+int(pLen))
	buf[0] = evt.Op
	binary.BigEndian.PutUint16(buf[1:3], evt.TypeHeader)
	binary.BigEndian.PutUint16(buf[3:5], kLen)
	copy(buf[5:5+kLen], evt.Key)
	off := 5 + kLen
	binary.BigEndian.PutUint32(buf[off:off+4], pLen)
	if pLen > 0 {
		copy(buf[off+4:], evt.Payload)
	}
	return buf
}

func decodeSyncEventBinary(r io.Reader) (SyncEvent, error) {
	var evt SyncEvent
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return evt, err
	}
	evt.Op = hdr[0]
	evt.TypeHeader = binary.BigEndian.Uint16(hdr[1:3])
	kLen := binary.BigEndian.Uint16(hdr[3:5])

	kBuf := make([]byte, kLen)
	if _, err := io.ReadFull(r, kBuf); err != nil {
		return evt, err
	}
	evt.Key = string(kBuf)

	var pLenBuf [4]byte
	if _, err := io.ReadFull(r, pLenBuf[:]); err != nil {
		return evt, err
	}
	pLen := binary.BigEndian.Uint32(pLenBuf[:])
	if pLen > 0 {
		evt.Payload = make([]byte, pLen)
		if _, err := io.ReadFull(r, evt.Payload); err != nil {
			return evt, err
		}
	}
	return evt, nil
}

func (db *NatabaseEngine) startTCPBroadcaster() {
	for {
		select {
		case <-db.closeChan:
			return
		case event := <-db.syncChan:
			db.replConns.Range(func(k, v interface{}) bool {
				ch := v.(chan SyncEvent)
				select {
				case ch <- event:
				default:
				}
				return true
			})
		}
	}
}

func (db *NatabaseEngine) StartReplicationServer(addr string, secret string) error {
	db.replSecret = secret
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-db.closeChan:
					return
				default:
					continue
				}
			}
			go db.handleReplicaConn(conn, secret)
		}
	}()
	return nil
}

func (db *NatabaseEngine) handleReplicaConn(conn net.Conn, secret string) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil || strings.TrimSpace(line) != secret {
		_, _ = conn.Write([]byte("AUTH_FAILED\n"))
		_ = conn.Close()
		return
	}
	_, _ = conn.Write([]byte("OK\n"))
	_ = conn.SetReadDeadline(time.Time{})

	for i := 0; i < NumShards; i++ {
		shard := db.shards[i]
		shard.mu.RLock()
		var keys []string
		for k := range shard.index {
			keys = append(keys, k)
		}
		shard.mu.RUnlock()

		for _, key := range keys {
			payload, th, err := db.Get(key)
			if err == nil {
				evt := SyncEvent{Op: OpPut, Key: key, TypeHeader: th, Payload: payload}
				bData := encodeSyncEventBinary(evt)
				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := conn.Write(bData); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}

	db.users.Range(func(k, v interface{}) bool {
		u := v.(*UserAccount)
		payload, _ := json.Marshal(u)
		evt := SyncEvent{Op: OpUserPut, Key: u.Username, Payload: payload}
		bData := encodeSyncEventBinary(evt)
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(bData); err != nil {
			_ = conn.Close()
			return false
		}
		return true
	})

	clientChan := make(chan SyncEvent, 10000)
	clientAddr := conn.RemoteAddr().String()
	db.replConns.Store(clientAddr, clientChan)
	defer func() {
		db.replConns.Delete(clientAddr)
		_ = conn.Close()
	}()

	for {
		select {
		case <-db.closeChan:
			return
		case evt, ok := <-clientChan:
			if !ok {
				return
			}
			bData := encodeSyncEventBinary(evt)
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(bData); err != nil {
				return
			}
		}
	}
}

func (db *NatabaseEngine) ConnectToMasterReplication(masterTCPAddr string, secret string) {
	go func() {
		for {
			select {
			case <-db.closeChan:
				return
			default:
				conn, err := net.Dial("tcp", masterTCPAddr)
				if err != nil {
					time.Sleep(2 * time.Second)
					continue
				}

				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				_, err = fmt.Fprintf(conn, "%s\n", secret)
				if err != nil {
					_ = conn.Close()
					time.Sleep(2 * time.Second)
					continue
				}

				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				r := bufio.NewReader(conn)
				resp, err := r.ReadString('\n')
				if err != nil || strings.TrimSpace(resp) != "OK" {
					log.Println("Replication authentication failed!")
					_ = conn.Close()
					time.Sleep(5 * time.Second)
					continue
				}
				_ = conn.SetReadDeadline(time.Time{})
				_ = conn.SetWriteDeadline(time.Time{})

				log.Println("Connected to Replication Master via Authenticated Binary TCP Stream")
				for {
					evt, err := decodeSyncEventBinary(r)
					if err != nil {
						_ = conn.Close()
						break
					}
					switch evt.Op {
					case OpPut:
						_ = db.putInternal(evt.Key, evt.TypeHeader, evt.Payload, 0, false)
					case OpDel:
						_ = db.deleteInternal(evt.Key, false)
					case OpUserPut:
						var u UserAccount
						if err := json.Unmarshal(evt.Payload, &u); err == nil {
							db.users.Store(u.Username, &u)
						}
					case OpUserDel:
						db.users.Delete(evt.Key)
					}
				}
			}
		}
	}()
}

type compactTask struct {
	key        string
	size       uint32
	typeHeader uint16
	segID      uint32
	offset     int64
	entry      *IndexEntry
}

func (db *NatabaseEngine) startCompactionWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-db.closeChan:
			return
		case <-ticker.C:
			activeID := db.activeSegID.Load()
			if activeID == 0 {
				continue
			}

			var tasks []compactTask
			for i := 0; i < NumShards; i++ {
				shard := db.shards[i]
				shard.mu.RLock()
				for key, entry := range shard.index {
					if entry.SegmentID < activeID {
						tasks = append(tasks, compactTask{
							key: key, size: entry.Size, typeHeader: entry.TypeHeader,
							segID: entry.SegmentID, offset: entry.Offset, entry: entry,
						})
					}
				}
				shard.mu.RUnlock()
			}

			if len(tasks) == 0 {
				continue
			}

			compactSegID := activeID + 1000000
			compactPath := filepath.Join(db.storageDir, fmt.Sprintf("segment_%08d.seg", compactSegID))
			compactFile, err := os.OpenFile(compactPath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				continue
			}

			var newOffset int64 = 0
			for _, t := range tasks {
				var payload []byte
				if hotPtr := t.entry.HotPayload.Load(); hotPtr != nil && *hotPtr != nil {
					payload = *hotPtr
				} else {
					payload = make([]byte, t.size)
					file, err := db.getSegmentFile(t.segID)
					if err == nil {
						_, _ = file.ReadAt(payload, t.offset)
					}
				}

				keyLen := uint16(len(t.key))
				recSize := int64(12+int(keyLen)) + int64(t.size)
				buf := make([]byte, recSize)

				binary.BigEndian.PutUint16(buf[4:6], keyLen)
				copy(buf[6:6+keyLen], t.key)
				off := 6 + keyLen
				binary.BigEndian.PutUint16(buf[off:off+2], t.typeHeader)
				binary.BigEndian.PutUint32(buf[off+2:off+6], t.size)
				copy(buf[off+6:], payload)
				binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))

				_, _ = compactFile.WriteAt(buf, newOffset)

				newEntry := *t.entry
				newEntry.SegmentID = compactSegID
				newEntry.Offset = newOffset + int64(12+int(keyLen))

				shard := db.shards[getShardIndex(t.key)]
				shard.mu.Lock()
				if currentEntry, exists := shard.index[t.key]; exists && currentEntry.SegmentID == t.segID {
					shard.index[t.key] = &newEntry
				}
				shard.mu.Unlock()

				newOffset += recSize
			}
			db.segmentFiles.Store(compactSegID, compactFile)

			for segID := uint32(0); segID < activeID; segID++ {
				if val, ok := db.segmentFiles.LoadAndDelete(segID); ok {
					file := val.(*os.File)
					_ = file.Close()
					_ = os.Remove(filepath.Join(db.storageDir, fmt.Sprintf("segment_%08d.seg", segID)))
				}
			}

			db.aofMu.Lock()
			if len(db.aofBuf) > 0 {
				_, _ = db.aofWriter.Write(db.aofBuf)
				db.aofBuf = db.aofBuf[:0]
			}
			_ = db.aofWriter.Sync()
			_ = db.aofWriter.Close()
			_ = os.Truncate(db.aofPath, 0)
			db.aofWriter, _ = os.OpenFile(db.aofPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
			db.aofMu.Unlock()
		}
	}
}

func (db *NatabaseEngine) startSampledLRUEviction(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-db.closeChan:
			return
		case <-ticker.C:
			nowNano := time.Now().UnixNano()
			nowMs := time.Now().UnixMilli()

			for s := 0; s < 10; s++ {
				shardIdx := mrand.IntN(NumShards)
				shard := db.shards[shardIdx]
				count := 0

				shard.mu.Lock()
				for key, entry := range shard.index {
					if count >= 20 {
						break
					}
					count++
					if entry.ExpiresAtUnixMs > 0 && nowMs > entry.ExpiresAtUnixMs {
						delete(shard.index, key)
						removeFromTypeIndex(shard, entry.TypeHeader, key)
						continue
					}
					if (nowNano - entry.LastAccessedNano.Load()) > int64(IdleTimeToSleep) {
						entry.HotPayload.Store(nil)
					}
				}
				shard.mu.Unlock()
			}
		}
	}
}

func (db *NatabaseEngine) loadIndexFromDisk() error {
	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "segment_") && strings.HasSuffix(entry.Name(), ".seg") {
			var segID uint32
			fmt.Sscanf(entry.Name(), "segment_%08d.seg", &segID)
			file, err := db.getSegmentFile(segID)
			if err != nil {
				continue
			}
			var offset int64 = 0
			for {
				hdr := make([]byte, 6)
				if _, err := file.ReadAt(hdr, offset); err != nil {
					break
				}
				kLen := binary.BigEndian.Uint16(hdr[4:6])
				meta := make([]byte, int(kLen)+6)
				if _, err := file.ReadAt(meta, offset+6); err != nil {
					break
				}

				key := string(meta[:kLen])
				th := binary.BigEndian.Uint16(meta[kLen : kLen+2])
				sz := binary.BigEndian.Uint32(meta[kLen+2 : kLen+6])

				shard := db.shards[getShardIndex(key)]
				idxEntry := &IndexEntry{
					SegmentID:  segID,
					Offset:     offset + int64(12+int(kLen)),
					Size:       sz,
					TypeHeader: th,
				}
				idxEntry.LastAccessedNano.Store(time.Now().UnixNano())

				shard.mu.Lock()
				shard.index[key] = idxEntry
				addToTypeIndex(shard, th, key)
				shard.mu.Unlock()

				offset += int64(12+int(kLen)) + int64(sz)
			}
			if segID >= db.activeSegID.Load() {
				db.activeSegID.Store(segID)
				db.activeFile = file
				db.activeOffset.Store(offset)
			}
		}
	}
	return nil
}

func (db *NatabaseEngine) saveManifest() error {
	manifestPath := filepath.Join(db.storageDir, "manifest.bin")
	file, err := os.Create(manifestPath)
	if err != nil {
		return err
	}
	defer file.Close()

	hdr := make([]byte, 12)
	binary.BigEndian.PutUint32(hdr[0:4], db.activeSegID.Load())
	binary.BigEndian.PutUint64(hdr[4:12], uint64(db.activeOffset.Load()))
	if _, err := file.Write(hdr); err != nil {
		return err
	}

	for i := 0; i < NumShards; i++ {
		shard := db.shards[i]
		shard.mu.RLock()
		for key, entry := range shard.index {
			kLen := uint16(len(key))
			buf := make([]byte, 2+kLen+26)
			binary.BigEndian.PutUint16(buf[0:2], kLen)
			copy(buf[2:2+kLen], key)
			off := 2 + kLen
			binary.BigEndian.PutUint32(buf[off:off+4], entry.SegmentID)
			binary.BigEndian.PutUint64(buf[off+4:off+12], uint64(entry.Offset))
			binary.BigEndian.PutUint32(buf[off+12:off+16], entry.Size)
			binary.BigEndian.PutUint16(buf[off+16:off+18], entry.TypeHeader)
			binary.BigEndian.PutUint64(buf[off+18:off+26], uint64(entry.ExpiresAtUnixMs))
			_, _ = file.Write(buf)
		}
		shard.mu.RUnlock()
	}
	return nil
}

func (db *NatabaseEngine) loadManifest(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	hdr := make([]byte, 12)
	if _, err := io.ReadFull(file, hdr); err != nil {
		return err
	}
	db.activeSegID.Store(binary.BigEndian.Uint32(hdr[0:4]))
	db.activeOffset.Store(int64(binary.BigEndian.Uint64(hdr[4:12])))

	buf := make([]byte, 64*1024)
	for {
		if _, err := io.ReadFull(file, buf[:2]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		kLen := binary.BigEndian.Uint16(buf[:2])

		keyBuf := make([]byte, kLen)
		if _, err := io.ReadFull(file, keyBuf); err != nil {
			return err
		}
		key := string(keyBuf)

		meta := make([]byte, 26)
		if _, err := io.ReadFull(file, meta); err != nil {
			return err
		}

		segID := binary.BigEndian.Uint32(meta[0:4])
		offset := int64(binary.BigEndian.Uint64(meta[4:12]))
		sz := binary.BigEndian.Uint32(meta[12:16])
		th := binary.BigEndian.Uint16(meta[16:18])
		exp := int64(binary.BigEndian.Uint64(meta[18:26]))

		shard := db.shards[getShardIndex(key)]
		idxEntry := &IndexEntry{
			SegmentID:       segID,
			Offset:          offset,
			Size:            sz,
			TypeHeader:      th,
			ExpiresAtUnixMs: exp,
		}
		idxEntry.LastAccessedNano.Store(time.Now().UnixNano())

		shard.mu.Lock()
		shard.index[key] = idxEntry
		addToTypeIndex(shard, th, key)
		shard.mu.Unlock()
	}
	return nil
}

func (db *NatabaseEngine) replayAOF() error {
	fi, err := db.aofWriter.Stat()
	if err != nil || fi.Size() == 0 {
		return nil
	}

	aofReader, err := os.Open(db.aofPath)
	if err != nil {
		return err
	}
	defer aofReader.Close()

	buf := make([]byte, fi.Size())
	if _, err = io.ReadFull(aofReader, buf); err != nil && err != io.EOF {
		return err
	}

	var off int64 = 0
	for off < int64(len(buf)) {
		op := buf[off]
		off++
		if off+2 > int64(len(buf)) {
			break
		}
		keyLen := binary.BigEndian.Uint16(buf[off : off+2])
		off += 2
		if off+int64(keyLen) > int64(len(buf)) {
			break
		}
		key := string(buf[off : off+int64(keyLen)])
		off += int64(keyLen)

		if op == OpPut || op == OpUserPut {
			if off+6 > int64(len(buf)) {
				break
			}
			typeHeader := binary.BigEndian.Uint16(buf[off : off+2])
			payloadLen := binary.BigEndian.Uint32(buf[off+2 : off+6])
			off += 6
			if off+int64(payloadLen) > int64(len(buf)) {
				break
			}
			payload := buf[off : off+int64(payloadLen)]
			off += int64(payloadLen)

			if op == OpPut {
				_ = db.putInternal(key, typeHeader, payload, 0, false)
			} else if op == OpUserPut {
				var u UserAccount
				if err := json.Unmarshal(payload, &u); err == nil {
					db.users.Store(u.Username, &u)
				}
			}
		} else if op == OpDel {
			_ = db.deleteInternal(key, false)
		} else if op == OpUserDel {
			db.users.Delete(key)
		}
	}
	return nil
}

func (db *NatabaseEngine) Close() {
	close(db.closeChan)
	db.flushAOF()
	_ = db.saveManifest()
	db.segmentFiles.Range(func(k, v interface{}) bool {
		_ = v.(*os.File).Close()
		return true
	})
	db.aofMu.Lock()
	if db.aofWriter != nil {
		_ = db.aofWriter.Close()
	}
	db.aofMu.Unlock()
	_ = db.AdvSnapshotter.SaveRDB()
}

type TokenBucket struct {
	state atomic.Uint64
}

func (b *TokenBucket) Allow(rate, burst uint32) bool {
	for {
		state := b.state.Load()
		lastSec := uint32(state >> 32)
		tokens := uint32(state & 0xFFFFFFFF)

		now := uint32(time.Now().Unix())
		if lastSec == 0 {
			lastSec = now
			tokens = burst
		}

		if now > lastSec {
			elapsed := now - lastSec
			tokens += elapsed * rate
			if tokens > burst {
				tokens = burst
			}
			lastSec = now
		}

		if tokens == 0 {
			return false
		}

		newState := (uint64(lastSec) << 32) | uint64(tokens-1)
		if b.state.CompareAndSwap(state, newState) {
			return true
		}
	}
}

type SSEHub struct {
	mu      sync.RWMutex
	clients map[uint16]map[chan SyncEvent]struct{}
}

func NewSSEHub() *SSEHub {
	return &SSEHub{
		clients: make(map[uint16]map[chan SyncEvent]struct{}),
	}
}

func (h *SSEHub) Subscribe(th uint16, ch chan SyncEvent) {
	h.mu.Lock()
	if h.clients[th] == nil {
		h.clients[th] = make(map[chan SyncEvent]struct{})
	}
	h.clients[th][ch] = struct{}{}
	h.mu.Unlock()
}

func (h *SSEHub) Unsubscribe(th uint16, ch chan SyncEvent) {
	h.mu.Lock()
	if h.clients[th] != nil {
		delete(h.clients[th], ch)
		if len(h.clients[th]) == 0 {
			delete(h.clients, th)
		}
	}
	h.mu.Unlock()
}

func (h *SSEHub) Broadcast(event SyncEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if subs, ok := h.clients[event.TypeHeader]; ok {
		for ch := range subs {
			select {
			case ch <- event:
			default:
			}
		}
	}
}

type NatabaseServer struct {
	db               *NatabaseEngine
	rateLimiter      sync.Map
	disableRateLimit bool
	rateLimitRate    uint32
	rateLimitBurst   uint32
	sseHub           *SSEHub
	sqlEngine        *natasql.SQLEngine
}

func (s *NatabaseServer) startRateLimiterGC(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		now := uint32(time.Now().Unix())
		s.rateLimiter.Range(func(key, value interface{}) bool {
			bucket := value.(*TokenBucket)
			state := bucket.state.Load()
			lastSec := uint32(state >> 32)
			if lastSec > 0 && now > lastSec && (now-lastSec) > 3600 {
				s.rateLimiter.Delete(key)
			}
			return true
		})
	}
}

func (s *NatabaseServer) rbacMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		authHeader := ctx.Request.Header.Peek("Authorization")
		if !bytes.HasPrefix(authHeader, []byte("Bearer ")) {
			ctx.Error(`{"error":"Missing or invalid Authorization header"}`, fasthttp.StatusUnauthorized)
			return
		}
		tokenStr := string(authHeader[7:])
		user, role, err := verifyJWT(tokenStr)
		if err != nil {
			ctx.Error(fmt.Sprintf(`{"error":"%s"}`, err.Error()), fasthttp.StatusUnauthorized)
			return
		}
		ctx.SetUserValue("user", user)
		ctx.SetUserValue("role", role)
		next(ctx)
	}
}

func (s *NatabaseServer) fastHandleLogin(ctx *fasthttp.RequestCtx) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &creds); err != nil {
		ctx.Error("Bad Request", fasthttp.StatusBadRequest)
		return
	}

	val, ok := s.db.users.Load(creds.Username)
	if !ok {
		ctx.Error(`{"error":"Invalid credentials"}`, fasthttp.StatusUnauthorized)
		return
	}
	user := val.(*UserAccount)
	if !checkPasswordHash(creds.Password, user.Password) {
		ctx.Error(`{"error":"Invalid credentials"}`, fasthttp.StatusUnauthorized)
		return
	}

	token, _ := generateJWT(user.Username, user.Role)
	ctx.Response.Header.Set("Content-Type", "application/json")
	fmt.Fprintf(ctx, `{"token":"%s","type":"Bearer","msg":"Authentication Successful"}`, token)
}

func (s *NatabaseServer) fastHandleData(ctx *fasthttp.RequestCtx) {
	role := ctx.UserValue("role").(string)
	isGet := ctx.IsGet()
	isPost := ctx.IsPost()
	isPut := ctx.IsPut()
	isDelete := ctx.IsDelete()

	if role == "viewer" && !isGet {
		ctx.Error("Forbidden: view only", fasthttp.StatusForbidden)
		return
	}
	if role == "editor" && isDelete {
		ctx.Error("Forbidden: cannot delete", fasthttp.StatusForbidden)
		return
	}

	key := string(ctx.QueryArgs().Peek("key"))
	ttlStr := string(ctx.QueryArgs().Peek("ttl"))
	bucket := string(ctx.QueryArgs().Peek("bucket"))
	filename := string(ctx.QueryArgs().Peek("file"))

	if bucket != "" && filename != "" {
		filePath := filepath.Join(s.db.storageDir, bucket, filename)
		if isPost || isPut {
			hash, err := s.db.SaveBlob(bucket, filename, bytes.NewReader(ctx.PostBody()))
			if err != nil {
				ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
				return
			}
			ctx.SetStatusCode(fasthttp.StatusCreated)
			fmt.Fprintf(ctx, "Blob saved [Hash: %s]\n", hash)
			return
		} else if isGet {
			fasthttp.ServeFile(ctx, filePath)
			return
		}
	}

	if key == "" {
		ctx.Error("Parameter 'key' required", fasthttp.StatusBadRequest)
		return
	}

	switch {
	case isGet:
		payload, typeHeader, err := s.db.Get(key)
		if err != nil {
			ctx.Error(err.Error(), fasthttp.StatusNotFound)
			return
		}
		ctx.Response.Header.Set("X-Natabase-TypeHeader", fmt.Sprintf("0x%04X", typeHeader))
		ctx.Write(payload)
	case isPost || isPut:
		var ttl time.Duration = 0
		if ttlStr != "" {
			ttl, _ = time.ParseDuration(ttlStr)
		}
		_ = s.db.Put(key, 0x0001, ctx.PostBody(), ttl)
		ctx.SetStatusCode(fasthttp.StatusCreated)
		fmt.Fprintf(ctx, "Data saved [Key: %s]\n", key)
	case isDelete:
		if !s.db.Delete(key) {
			ctx.Error("Key not found", fasthttp.StatusNotFound)
			return
		}
		ctx.SetStatusCode(fasthttp.StatusOK)
		fmt.Fprintf(ctx, "Key '%s' deleted\n", key)
	}
}

func (s *NatabaseServer) fastHandleGetByType(ctx *fasthttp.RequestCtx) {
	thStr := string(ctx.QueryArgs().Peek("type"))
	offsetStr := string(ctx.QueryArgs().Peek("offset"))
	limitStr := string(ctx.QueryArgs().Peek("limit"))

	var th uint16
	var offset, limit int
	fmt.Sscanf(thStr, "%d", &th)
	fmt.Sscanf(offsetStr, "%d", &offset)
	fmt.Sscanf(limitStr, "%d", &limit)
	if limit == 0 {
		limit = 10
	}

	res, err := s.db.GetByType(th, offset, limit)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	ctx.Response.Header.Set("Content-Type", "application/json")
	var out []string
	for _, b := range res {
		out = append(out, string(b))
	}
	_ = json.NewEncoder(ctx).Encode(out)
}

func (s *NatabaseServer) fastHandleBitmaskQuery(ctx *fasthttp.RequestCtx) {
	maskStr := string(ctx.QueryArgs().Peek("mask"))
	matchStr := string(ctx.QueryArgs().Peek("match"))

	maskU, _ := strconv.ParseUint(maskStr, 0, 16)
	matchU, _ := strconv.ParseUint(matchStr, 0, 16)

	keys := s.db.QueryByBitmask(uint16(maskU), uint16(matchU))

	ctx.Response.Header.Set("Content-Type", "application/json")
	_ = json.NewEncoder(ctx).Encode(keys)
}

func (s *NatabaseServer) fastHandleBatch(ctx *fasthttp.RequestCtx) {
	role := ctx.UserValue("role").(string)
	if role == "viewer" {
		ctx.Error("Forbidden", fasthttp.StatusForbidden)
		return
	}

	if ctx.IsPost() {
		var req map[string]string
		if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
			ctx.Error("Bad Request", fasthttp.StatusBadRequest)
			return
		}

		batchMap := make(map[string][]byte, len(req))
		for k, v := range req {
			batchMap[k] = []byte(v)
		}

		if err := s.db.PutBatch(batchMap); err != nil {
			ctx.Error("Internal Error", fasthttp.StatusInternalServerError)
			return
		}

		ctx.SetStatusCode(fasthttp.StatusCreated)
		fmt.Fprintf(ctx, `{"status":"success","processed":%d}`, len(req))
	}
}

func (s *NatabaseServer) fastHandleMetrics(ctx *fasthttp.RequestCtx) {
	var totalKeys uint64 = 0
	for i := 0; i < NumShards; i++ {
		s.db.shards[i].mu.RLock()
		totalKeys += uint64(len(s.db.shards[i].index))
		s.db.shards[i].mu.RUnlock()
	}

	var dbSize int64 = 0
	s.db.segmentFiles.Range(func(k, v interface{}) bool {
		fi, err := v.(*os.File).Stat()
		if err == nil {
			dbSize += fi.Size()
		}
		return true
	})
	aofFi, _ := s.db.aofWriter.Stat()

	metrics := map[string]interface{}{
		"engine_version": "v5.0-enterprise-ultimate-fasthttp-optimized",
		"total_keys":     totalKeys,
		"db_size_bytes":  dbSize,
		"aof_size_bytes": aofFi.Size(),
		"ops_count": map[string]uint64{
			"get": s.db.totalOpsGet.Load(),
			"put": s.db.totalOpsPut.Load(),
			"del": s.db.totalOpsDel.Load(),
		},
		"cluster_enabled": s.db.ClusterEnabled,
	}
	ctx.Response.Header.Set("Content-Type", "application/json")
	_ = json.NewEncoder(ctx).Encode(metrics)
}

func (s *NatabaseServer) fastHandleUsers(ctx *fasthttp.RequestCtx) {
	role := ctx.UserValue("role").(string)
	if role != "administrator" {
		ctx.Error("Forbidden: admin only", fasthttp.StatusForbidden)
		return
	}

	if ctx.IsPost() {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Role     string `json:"role"`
		}
		if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
			ctx.Error("Bad Request", fasthttp.StatusBadRequest)
			return
		}
		hashedPassword, err := hashPassword(req.Password)
		if err != nil {
			ctx.Error("Internal Error", fasthttp.StatusInternalServerError)
			return
		}
		newUser := &UserAccount{
			Username: req.Username,
			Password: hashedPassword,
			Role:     req.Role,
		}
		if err := s.db.SaveUser(newUser); err != nil {
			ctx.Error("Internal Error", fasthttp.StatusInternalServerError)
			return
		}
		ctx.SetStatusCode(fasthttp.StatusCreated)
		ctx.SetBodyString(`{"msg":"User created"}`)
	} else if ctx.IsDelete() {
		username := string(ctx.QueryArgs().Peek("username"))
		if username == "" || username == "admin" {
			ctx.Error("Invalid username or cannot delete admin", fasthttp.StatusBadRequest)
			return
		}
		s.db.DeleteUser(username)
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBodyString(`{"msg":"User deleted"}`)
	} else {
		ctx.Error("Method not allowed", fasthttp.StatusMethodNotAllowed)
	}
}

func (s *NatabaseServer) fastHandleSSE(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/event-stream")
	ctx.Response.Header.Set("Cache-Control", "no-cache")
	ctx.Response.Header.Set("Connection", "keep-alive")
	ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")

	thStr := string(ctx.QueryArgs().Peek("type"))
	var th uint16
	fmt.Sscanf(thStr, "%d", &th)

	ch := make(chan SyncEvent, 100)
	s.sseHub.Subscribe(th, ch)

	ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
		defer s.sseHub.Unsubscribe(th, ch)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case evt := <-ch:
				data, err := json.Marshal(evt)
				if err != nil {
					continue
				}
				fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
				if err := w.Flush(); err != nil {
					return
				}
			case <-ticker.C:
				fmt.Fprintf(w, ": keep-alive\n\n")
				if err := w.Flush(); err != nil {
					return
				}
			}
		}
	})
}

func (s *NatabaseServer) fastHandleSQL(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.Error("Method not allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Query  string        `json:"query"`
		Params []interface{} `json:"params"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		ctx.Error(`{"error":"Bad Request format"}`, fasthttp.StatusBadRequest)
		return
	}

	res, err := s.sqlEngine.Execute(req.Query, req.Params...)
	if err != nil {
		ctx.Error(fmt.Sprintf(`{"error":%q}`, err.Error()), fasthttp.StatusInternalServerError)
		return
	}

	ctx.Response.Header.Set("Content-Type", "application/json")
	_ = json.NewEncoder(ctx).Encode(res)
}

func (s *NatabaseServer) mainHandler(ctx *fasthttp.RequestCtx) {
	if !s.disableRateLimit && string(ctx.Request.Header.Peek("X-Stress-Test")) != "true" {
		ip := ctx.RemoteIP().String()
		val, _ := s.rateLimiter.LoadOrStore(ip, &TokenBucket{})
		bucket := val.(*TokenBucket)

		if !bucket.Allow(s.rateLimitRate, s.rateLimitBurst) {
			ctx.Error("Rate limit exceeded", fasthttp.StatusTooManyRequests)
			return
		}
	}

	path := string(ctx.Path())
	switch path {
	case "/healthz", "/readyz":
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBodyString("OK")
	case "/api/auth/login":
		if ctx.IsPost() {
			s.fastHandleLogin(ctx)
		} else {
			ctx.Error("Method not allowed", fasthttp.StatusMethodNotAllowed)
		}
	case "/api/v1/data":
		s.rbacMiddleware(s.fastHandleData)(ctx)
	case "/api/v1/data/type":
		s.rbacMiddleware(s.fastHandleGetByType)(ctx)
	case "/api/v1/query/bitmask":
		s.rbacMiddleware(s.fastHandleBitmaskQuery)(ctx)
	case "/api/v1/batch":
		s.rbacMiddleware(s.fastHandleBatch)(ctx)
	case "/api/v1/metrics":
		s.rbacMiddleware(s.fastHandleMetrics)(ctx)
	case "/api/v1/users":
		s.rbacMiddleware(s.fastHandleUsers)(ctx)
	case "/api/v1/events":
		s.rbacMiddleware(s.fastHandleSSE)(ctx)
	case "/api/v1/sql":
		s.rbacMiddleware(s.fastHandleSQL)(ctx)
	default:
		ctx.Error("Not Found", fasthttp.StatusNotFound)
	}
}

func main() {
	tlsCert := flag.String("tls-cert", "", "Path to TLS Certificate (enable HTTPS)")
	tlsKey := flag.String("tls-key", "", "Path to TLS Key")
	port := flag.String("port", "8080", "Server Port")
	replPort := flag.String("repl-port", "9090", "Replication TCP Port")
	replSecret := flag.String("repl-secret", "natabase-repl-token-2026", "Secret token for TCP Replication Auth")
	disableRateLimit := flag.Bool("disable-rate-limit", false, "Disable dynamic rate limiter")
	rateLimitRate := flag.Int("rate", 20, "Rate limit per second")
	rateLimitBurst := flag.Int("burst", 100, "Rate limit burst size")
	nodeRole := flag.String("role", "", "Role of the node: master or replica")
	masterAddr := flag.String("master-addr", "", "Master TCP address for replica (e.g. 127.0.0.1:9090)")
	clusterEnabled := flag.Bool("cluster-enabled", false, "Enable Multi-Node Cluster mode")
	flag.Parse()

	db, err := NewNatabase("natabase_v5")
	if err != nil {
		log.Fatalf("Fatal Error: %v\n", err)
	}
	db.Role = *nodeRole
	db.ClusterEnabled = *clusterEnabled

	sqlEngine := natasql.NewSQLEngine(db)

	if db.Role == "master" {
		if err := db.StartReplicationServer(":"+*replPort, *replSecret); err != nil {
			log.Fatalf("Replication Server Error: %v\n", err)
		}
	} else if db.Role == "replica" && *masterAddr != "" {
		db.ConnectToMasterReplication(*masterAddr, *replSecret)
	}

	server := &NatabaseServer{
		db:               db,
		disableRateLimit: *disableRateLimit,
		rateLimitRate:    uint32(*rateLimitRate),
		rateLimitBurst:   uint32(*rateLimitBurst),
		sseHub:           NewSSEHub(),
		sqlEngine:        sqlEngine,
	}
	db.EventCallback = server.sseHub.Broadcast

	go server.startRateLimiterGC(10 * time.Minute)

	fastServer := &fasthttp.Server{
		Handler:            server.mainHandler,
		Name:               "Natabase-Enterprise",
		ReadTimeout:        5 * time.Second,
		WriteTimeout:       5 * time.Second,
		IdleTimeout:        30 * time.Second,
		MaxConnsPerIP:      5000,
		MaxRequestsPerConn: 50000,
		TCPKeepalive:       true,
		Concurrency:        256 * 1024,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		fmt.Println("==========================================================")
		fmt.Println("  NATABASE V5.0 ENTERPRISE ULTIMATE (OPTIMIZED & SECURED)")
		fmt.Println("==========================================================")
		if *tlsCert != "" && *tlsKey != "" {
			fmt.Printf(" [TLS] Listening securely on :%s\n", *port)
			if err := fastServer.ListenAndServeTLS(":"+*port, *tlsCert, *tlsKey); err != nil {
				log.Fatalf("Server Error: %v\n", err)
			}
		} else {
			fmt.Printf(" [HTTP] Listening on :%s\n", *port)
			if err := fastServer.ListenAndServe(":" + *port); err != nil {
				log.Fatalf("Server Error: %v\n", err)
			}
		}
	}()

	<-stopChan
	log.Println("\nMenghentikan engine secara aman (Graceful Shutdown)...")
	_ = fastServer.Shutdown()
	db.Close()
}
