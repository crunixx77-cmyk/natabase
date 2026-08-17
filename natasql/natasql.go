package natasql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// ============================================================================
// UTILITIES & POOLS
// ============================================================================

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

var rngState uint32 = uint32(time.Now().UnixNano())

func fastrand() uint32 {
	for {
		old := atomic.LoadUint32(&rngState)
		next := old*1664525 + 1013904223
		if atomic.CompareAndSwapUint32(&rngState, old, next) {
			return next
		}
	}
}

func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func unsafeBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func valToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", val)
	}
}

// ============================================================================
// 1. STORAGE ENGINE ABSTRACTION LAYER
// ============================================================================

type StorageEngine interface {
	Get(key string) ([]byte, bool)
	Put(key string, val []byte) error
	Delete(key string) error
	Scan(fn func(key string, val []byte) bool)
	GetByType(typeHeader uint16) [][]byte
	QueryByBitmask(mask uint64, val uint64) [][]byte
}

type NatabaseAdapter struct {
	rawDb          interface{}
	getFn          func(key string) ([]byte, bool)
	getByTypeFn    func(typeHeader uint16) [][]byte
	queryBitmaskFn func(mask uint64, val uint64) [][]byte
	scanFn         func(fn func(key string, val []byte) bool)
	putFn          func(key string, val []byte) error
	deleteFn       func(key string) error
}

func NewNatabaseAdapter(db interface{}) *NatabaseAdapter {
	adapter := &NatabaseAdapter{rawDb: db}

	if g, ok := db.(interface{ Get(string) ([]byte, bool) }); ok {
		adapter.getFn = g.Get
	} else if g2, ok := db.(interface{ Get(string) ([]byte, uint16, error) }); ok {
		adapter.getFn = func(k string) ([]byte, bool) {
			b, _, err := g2.Get(k)
			return b, err == nil
		}
	} else {
		v := reflect.ValueOf(db)
		if getMethod := v.MethodByName("Get"); getMethod.IsValid() {
			adapter.getFn = func(k string) ([]byte, bool) {
				res := getMethod.Call([]reflect.Value{reflect.ValueOf(k)})
				if len(res) >= 2 {
					var val []byte
					if !res[0].IsNil() {
						val = res[0].Bytes()
					}
					return val, res[1].Bool()
				}
				return nil, false
			}
		}
	}

	if gt, ok := db.(interface{ GetByType(uint16) [][]byte }); ok {
		adapter.getByTypeFn = gt.GetByType
	} else if gt2, ok := db.(interface{ GetByType(uint16, int, int) ([][]byte, error) }); ok {
		adapter.getByTypeFn = func(th uint16) [][]byte {
			res, err := gt2.GetByType(th, 0, 1000000)
			if err != nil {
				return nil
			}
			return res
		}
	}

	if qb, ok := db.(interface{ QueryByBitmask(uint64, uint64) [][]byte }); ok {
		adapter.queryBitmaskFn = qb.QueryByBitmask
	}

	if sc, ok := db.(interface{ Scan(func(string, []byte) bool) }); ok {
		adapter.scanFn = sc.Scan
	}

	if p, ok := db.(interface{ Put(string, []byte) error }); ok {
		adapter.putFn = p.Put
	} else if p2, ok := db.(interface {
		Put(string, uint16, []byte, time.Duration) error
	}); ok {
		adapter.putFn = func(k string, val []byte) error {
			return p2.Put(k, 0x0001, val, 0)
		}
	}

	if d, ok := db.(interface{ Delete(string) error }); ok {
		adapter.deleteFn = d.Delete
	} else if d2, ok := db.(interface{ Delete(string) bool }); ok {
		adapter.deleteFn = func(k string) error {
			if !d2.Delete(k) {
				return errors.New("key not found or delete failed")
			}
			return nil
		}
	}

	return adapter
}

func (a *NatabaseAdapter) Get(key string) ([]byte, bool) {
	if a.getFn != nil {
		return a.getFn(key)
	}
	return nil, false
}

func (a *NatabaseAdapter) Put(key string, val []byte) error {
	if a.putFn != nil {
		return a.putFn(key, val)
	}
	return errors.New("put unsupported by storage adapter")
}

func (a *NatabaseAdapter) Delete(key string) error {
	if a.deleteFn != nil {
		return a.deleteFn(key)
	}
	return errors.New("delete unsupported by storage adapter")
}

func (a *NatabaseAdapter) Scan(fn func(key string, val []byte) bool) {
	if a.scanFn != nil {
		a.scanFn(fn)
	}
}

func (a *NatabaseAdapter) GetByType(typeHeader uint16) [][]byte {
	if a.getByTypeFn != nil {
		return a.getByTypeFn(typeHeader)
	}
	return nil
}

func (a *NatabaseAdapter) QueryByBitmask(mask uint64, val uint64) [][]byte {
	if a.queryBitmaskFn != nil {
		return a.queryBitmaskFn(mask, val)
	}
	return nil
}

// ============================================================================
// 2. SECONDARY INDEX MANAGER & SKIPLIST (O(log N) SEARCH)
// ============================================================================

const MaxSkipListLevel = 16
const SkipListP = 0.5

type SkipListNode struct {
	key    interface{}
	docIDs map[string]struct{}
	next   []*SkipListNode
}

type SkipList struct {
	mu     sync.RWMutex
	head   *SkipListNode
	level  int
	length int64
}

func newSkipList() *SkipList {
	return &SkipList{
		head:  &SkipListNode{next: make([]*SkipListNode, MaxSkipListLevel)},
		level: 1,
	}
}

func (sl *SkipList) randomLevel() int {
	lvl := 1
	r := fastrand()
	for (r&0xFFFF) < uint32(SkipListP*65536) && lvl < MaxSkipListLevel {
		lvl++
		r = r*1664525 + 1013904223
	}
	return lvl
}

func (sl *SkipList) Insert(key interface{}, docID string) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	var update [MaxSkipListLevel]*SkipListNode
	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && compareValues(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	curr = curr.next[0]
	if curr != nil && compareValues(curr.key, key) == 0 {
		if curr.docIDs == nil {
			curr.docIDs = make(map[string]struct{})
		}
		curr.docIDs[docID] = struct{}{}
		return
	}

	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
		}
		sl.level = lvl
	}

	newNode := &SkipListNode{
		key:    key,
		docIDs: map[string]struct{}{docID: {}},
		next:   make([]*SkipListNode, lvl),
	}
	for i := 0; i < lvl; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}
	sl.length++
}

func (sl *SkipList) Search(key interface{}) []string {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && compareValues(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]
	if curr != nil && compareValues(curr.key, key) == 0 {
		res := make([]string, 0, len(curr.docIDs))
		for id := range curr.docIDs {
			res = append(res, id)
		}
		return res
	}
	return nil
}

func (sl *SkipList) RangeSearch(minVal, maxVal interface{}, includeMin, includeMax bool) []string {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var results []string
	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil {
			cmp := compareValues(curr.next[i].key, minVal)
			if cmp < 0 || (!includeMin && cmp == 0) {
				curr = curr.next[i]
			} else {
				break
			}
		}
	}

	curr = curr.next[0]
	for curr != nil {
		cmpMax := compareValues(curr.key, maxVal)
		if cmpMax > 0 || (!includeMax && cmpMax == 0) {
			break
		}
		for docID := range curr.docIDs {
			results = append(results, docID)
		}
		curr = curr.next[0]
	}
	return results
}

func (sl *SkipList) Delete(key interface{}, docID string) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	var update [MaxSkipListLevel]*SkipListNode
	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && compareValues(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	curr = curr.next[0]
	if curr != nil && compareValues(curr.key, key) == 0 {
		delete(curr.docIDs, docID)
		if len(curr.docIDs) == 0 {
			for i := 0; i < sl.level; i++ {
				if update[i].next[i] != curr {
					break
				}
				update[i].next[i] = curr.next[i]
			}
			for sl.level > 1 && sl.head.next[sl.level-1] == nil {
				sl.level--
			}
			sl.length--
		}
	}
}

type IndexManager struct {
	mu      sync.RWMutex
	indexes map[string]map[string]*SkipList
}

func newIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]map[string]*SkipList),
	}
}

func (im *IndexManager) CreateIndex(table, column string) {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[table]; !exists {
		im.indexes[table] = make(map[string]*SkipList)
	}
	if _, exists := im.indexes[table][column]; !exists {
		im.indexes[table][column] = newSkipList()
	}
}

func (im *IndexManager) GetIndex(table, column string) (*SkipList, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	if cols, exists := im.indexes[table]; exists {
		idx, ok := cols[column]
		return idx, ok
	}
	return nil, false
}

func (im *IndexManager) IndexRecord(table, docID string, payload []byte) {
	im.mu.RLock()
	colsMap, exists := im.indexes[table]
	im.mu.RUnlock()

	if !exists {
		return
	}
	for col, sl := range colsMap {
		val := extractJSONField(payload, col)
		if val != nil {
			sl.Insert(val, docID)
		}
	}
}

func (im *IndexManager) UnindexRecord(table, docID string, payload []byte) {
	im.mu.RLock()
	colsMap, exists := im.indexes[table]
	im.mu.RUnlock()

	if !exists {
		return
	}
	for col, sl := range colsMap {
		val := extractJSONField(payload, col)
		if val != nil {
			sl.Delete(val, docID)
		}
	}
}

// ============================================================================
// 3. MVCC / WAL TRANSACTION MANAGER
// ============================================================================

type TxState int

const (
	TxActive TxState = iota
	TxCommitted
	TxRolledBack
)

type LogType byte

const (
	LogPut LogType = iota
	LogDelete
	LogCommit
	LogRollback
)

type WALRecord struct {
	TxID      uint64
	Type      LogType
	Key       string
	Value     []byte
	OldValue  []byte
	Timestamp int64
}

type WAL struct {
	mu      sync.Mutex
	records []WALRecord
}

func newWAL() *WAL {
	return &WAL{records: make([]WALRecord, 0, 1024)}
}

func (w *WAL) Append(rec WALRecord) {
	w.mu.Lock()
	w.records = append(w.records, rec)
	w.mu.Unlock()
}

type Transaction struct {
	TxID      uint64
	StartTime time.Time
	State     TxState
	writes    map[string][]byte
	deletes   map[string]struct{}
	undoMap   map[string][]byte
	tm        *TxManager
	mu        sync.Mutex
}

type TxManager struct {
	mu        sync.RWMutex
	nextTxID  uint64
	activeTxs map[uint64]*Transaction
	wal       *WAL
	storage   StorageEngine
}

func newTxManager(storage StorageEngine) *TxManager {
	return &TxManager{
		nextTxID:  1,
		activeTxs: make(map[uint64]*Transaction),
		wal:       newWAL(),
		storage:   storage,
	}
}

func (tm *TxManager) Begin() *Transaction {
	txID := atomic.AddUint64(&tm.nextTxID, 1)
	tx := &Transaction{
		TxID:      txID,
		StartTime: time.Now(),
		State:     TxActive,
		writes:    make(map[string][]byte),
		deletes:   make(map[string]struct{}),
		undoMap:   make(map[string][]byte),
		tm:        tm,
	}

	tm.mu.Lock()
	tm.activeTxs[txID] = tx
	tm.mu.Unlock()

	return tx
}

func (tx *Transaction) Put(key string, val []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.State != TxActive {
		return errors.New("transaction is not active")
	}

	if _, exists := tx.undoMap[key]; !exists {
		oldVal, _ := tx.tm.storage.Get(key)
		tx.undoMap[key] = oldVal
	}

	delete(tx.deletes, key)
	tx.writes[key] = val

	tx.tm.wal.Append(WALRecord{
		TxID:      tx.TxID,
		Type:      LogPut,
		Key:       key,
		Value:     val,
		OldValue:  tx.undoMap[key],
		Timestamp: time.Now().UnixNano(),
	})
	return nil
}

func (tx *Transaction) Delete(key string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.State != TxActive {
		return errors.New("transaction is not active")
	}

	if _, exists := tx.undoMap[key]; !exists {
		oldVal, _ := tx.tm.storage.Get(key)
		tx.undoMap[key] = oldVal
	}

	delete(tx.writes, key)
	tx.deletes[key] = struct{}{}

	tx.tm.wal.Append(WALRecord{
		TxID:      tx.TxID,
		Type:      LogDelete,
		Key:       key,
		OldValue:  tx.undoMap[key],
		Timestamp: time.Now().UnixNano(),
	})
	return nil
}

func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.State != TxActive {
		return errors.New("transaction is not active")
	}

	for k, v := range tx.writes {
		if err := tx.tm.storage.Put(k, v); err != nil {
			_ = tx.rollbackInternal()
			return fmt.Errorf("commit failed on key %s: %w", k, err)
		}
	}
	for k := range tx.deletes {
		if err := tx.tm.storage.Delete(k); err != nil {
			_ = tx.rollbackInternal()
			return fmt.Errorf("commit delete failed on key %s: %w", k, err)
		}
	}

	tx.State = TxCommitted
	tx.tm.wal.Append(WALRecord{
		TxID:      tx.TxID,
		Type:      LogCommit,
		Timestamp: time.Now().UnixNano(),
	})

	tx.tm.mu.Lock()
	delete(tx.tm.activeTxs, tx.TxID)
	tx.tm.mu.Unlock()

	return nil
}

func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.rollbackInternal()
}

func (tx *Transaction) rollbackInternal() error {
	if tx.State != TxActive {
		return errors.New("transaction is not active")
	}

	for k, oldVal := range tx.undoMap {
		if oldVal != nil {
			_ = tx.tm.storage.Put(k, oldVal)
		} else {
			_ = tx.tm.storage.Delete(k)
		}
	}

	tx.State = TxRolledBack
	tx.writes = nil
	tx.deletes = nil

	tx.tm.wal.Append(WALRecord{
		TxID:      tx.TxID,
		Type:      LogRollback,
		Timestamp: time.Now().UnixNano(),
	})

	tx.tm.mu.Lock()
	delete(tx.tm.activeTxs, tx.TxID)
	tx.tm.mu.Unlock()

	return nil
}

// ============================================================================
// 4. MEMORY-EFFICIENT AST & EXPRESSION POOLING
// ============================================================================

var binaryOpPool = sync.Pool{
	New: func() interface{} { return &BinaryOpExpr{} },
}

var literalPool = sync.Pool{
	New: func() interface{} { return &LiteralExpr{} },
}

var columnRefPool = sync.Pool{
	New: func() interface{} { return &ColumnRefExpr{} },
}

func getBinaryOpExpr(left Expression, op string, right Expression) *BinaryOpExpr {
	e := binaryOpPool.Get().(*BinaryOpExpr)
	e.Left = left
	e.Op = op
	e.Right = right
	return e
}

func getLiteralExpr(val interface{}) *LiteralExpr {
	e := literalPool.Get().(*LiteralExpr)
	e.Val = val
	return e
}

func getColumnRefExpr(col string) *ColumnRefExpr {
	e := columnRefPool.Get().(*ColumnRefExpr)
	e.Column = col
	return e
}

type QueryResult struct {
	Columns       []string
	Rows          [][]interface{}
	AffectedRows  int64
	ExecutionTime time.Duration
	FastPathUsed  bool
}

type Option func(*SQLEngine)

func WithCacheSize(size int) Option {
	return func(e *SQLEngine) {
		if size > 0 {
			e.planCache = newLRUPlanCache(size)
		}
	}
}

func WithFastPath(enabled bool) Option {
	return func(e *SQLEngine) {
		e.enableFastPath = enabled
	}
}

type SQLEngine struct {
	storage        StorageEngine
	planCache      *lruPlanCache
	enableFastPath bool
	txManager      *TxManager
	indexManager   *IndexManager
	ctxPool        sync.Pool
	rowPool        sync.Pool
}

func NewSQLEngine(db interface{}, opts ...Option) *SQLEngine {
	var se StorageEngine
	if adapter, ok := db.(StorageEngine); ok {
		se = adapter
	} else {
		se = NewNatabaseAdapter(db)
	}

	tm := newTxManager(se)
	im := newIndexManager()

	e := &SQLEngine{
		storage:        se,
		txManager:      tm,
		indexManager:   im,
		planCache:      newLRUPlanCache(1024),
		enableFastPath: true,
		ctxPool: sync.Pool{
			New: func() interface{} {
				return &ExecutionContext{
					Vars: make(map[string]interface{}, 16),
				}
			},
		},
		rowPool: sync.Pool{
			New: func() interface{} {
				s := make([]interface{}, 0, 16)
				return &s
			},
		},
	}

	for _, opt := range opts {
		opt(e)
	}
	return e
}

func (e *SQLEngine) CreateIndex(table, column string) {
	e.indexManager.CreateIndex(table, column)
	if e.storage != nil {
		e.storage.Scan(func(k string, v []byte) bool {
			e.indexManager.IndexRecord(table, k, v)
			return true
		})
	}
}

func (e *SQLEngine) BeginTx() *Transaction {
	return e.txManager.Begin()
}

func (e *SQLEngine) Execute(sql string, params ...interface{}) (*QueryResult, error) {
	return e.ExecuteTx(nil, sql, params...)
}

func (e *SQLEngine) ExecuteTx(tx *Transaction, sql string, params ...interface{}) (*QueryResult, error) {
	startTime := time.Now()
	sqlTrimmed := strings.TrimSpace(sql)
	if len(sqlTrimmed) == 0 {
		return nil, errors.New("empty SQL statement")
	}

	hash := xxHash64String(sqlTrimmed)
	stmt, cached := e.planCache.Get(hash)
	if !cached {
		var err error
		stmt, err = e.compile(sqlTrimmed)
		if err != nil {
			return nil, fmt.Errorf("sql compile error: %w", err)
		}
		e.planCache.Put(hash, stmt)
	}

	if len(params) > 0 {
		stmt = stmt.bindParams(params)
	}

	if e.enableFastPath && stmt.FastPath != nil && tx == nil {
		res, ok, err := e.executeFastPath(stmt.FastPath)
		if ok {
			if err != nil {
				return nil, err
			}
			res.ExecutionTime = time.Since(startTime)
			res.FastPathUsed = true
			return res, nil
		}
	}

	res, err := e.executeGeneralTx(tx, stmt)
	if err != nil {
		return nil, err
	}
	res.ExecutionTime = time.Since(startTime)
	res.FastPathUsed = false
	return res, nil
}

// ============================================================================
// 5. XXHASH64 IMPL & LRU PLAN CACHE
// ============================================================================

const (
	prime64_1 uint64 = 11400714785074694791
	prime64_2 uint64 = 14029467367941562011
	prime64_3 uint64 = 1609587929392839161
	prime64_4 uint64 = 9650029242287828579
	prime64_5 uint64 = 2870177450012600261
)

func xxHash64String(s string) uint64 {
	if len(s) == 0 {
		return xxHash64Bytes(nil)
	}
	return xxHash64Bytes(unsafe.Slice(unsafe.StringData(s), len(s)))
}

func xxHash64Bytes(input []byte) uint64 {
	n := len(input)
	var h64 uint64

	if n >= 32 {
		v1 := uint64(11400714857880709142)
		v2 := prime64_2
		v3 := uint64(0)
		v4 := uint64(11400714857880709142)

		p := 0
		for p <= n-32 {
			v1 = round64(v1, readU64(input[p:]))
			p += 8
			v2 = round64(v2, readU64(input[p:]))
			p += 8
			v3 = round64(v3, readU64(input[p:]))
			p += 8
			v4 = round64(v4, readU64(input[p:]))
			p += 8
		}

		h64 = rol64(v1, 1) + rol64(v2, 7) + rol64(v3, 12) + rol64(v4, 18)
		h64 = mergeRound64(h64, v1)
		h64 = mergeRound64(h64, v2)
		h64 = mergeRound64(h64, v3)
		h64 = mergeRound64(h64, v4)
	} else {
		h64 = prime64_5
	}

	h64 += uint64(n)
	p := n &^ 31
	for p+8 <= n {
		k1 := round64(0, readU64(input[p:]))
		h64 ^= k1
		h64 = rol64(h64, 27)*prime64_1 + prime64_4
		p += 8
	}

	if p+4 <= n {
		h64 ^= uint64(readU32(input[p:])) * prime64_1
		h64 = rol64(h64, 23)*prime64_2 + prime64_3
		p += 4
	}

	for p < n {
		h64 ^= uint64(input[p]) * prime64_5
		h64 = rol64(h64, 11) * prime64_1
		p++
	}

	h64 ^= h64 >> 33
	h64 *= prime64_2
	h64 ^= h64 >> 29
	h64 *= prime64_3
	h64 ^= h64 >> 32
	return h64
}

func round64(acc, input uint64) uint64 {
	acc += input * prime64_2
	acc = rol64(acc, 31)
	acc *= prime64_1
	return acc
}

func mergeRound64(acc, val uint64) uint64 {
	val = round64(0, val)
	acc ^= val
	acc = acc*prime64_1 + prime64_4
	return acc
}

func rol64(x uint64, r uint) uint64 {
	return (x << r) | (x >> (64 - r))
}

func readU64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func readU32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

type lruNode struct {
	key  uint64
	stmt *SQLStatement
	prev *lruNode
	next *lruNode
}

type lruPlanCache struct {
	capacity int
	mu       sync.RWMutex
	items    map[uint64]*lruNode
	head     *lruNode
	tail     *lruNode
}

func newLRUPlanCache(capacity int) *lruPlanCache {
	c := &lruPlanCache{
		capacity: capacity,
		items:    make(map[uint64]*lruNode, capacity),
		head:     &lruNode{},
		tail:     &lruNode{},
	}
	c.head.next = c.tail
	c.tail.prev = c.head
	return c
}

func (c *lruPlanCache) Get(key uint64) (*SQLStatement, bool) {
	c.mu.RLock()
	node, exists := c.items[key]
	c.mu.RUnlock()

	if exists {
		c.mu.Lock()
		c.moveToFront(node)
		c.mu.Unlock()
		return node.stmt, true
	}
	return nil, false
}

func (c *lruPlanCache) Put(key uint64, stmt *SQLStatement) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, exists := c.items[key]; exists {
		node.stmt = stmt
		c.moveToFront(node)
		return
	}

	if len(c.items) >= c.capacity {
		c.removeOldest()
	}

	node := &lruNode{key: key, stmt: stmt}
	c.items[key] = node
	c.addNode(node)
}

func (c *lruPlanCache) addNode(node *lruNode) {
	node.next = c.head.next
	node.prev = c.head
	c.head.next.prev = node
	c.head.next = node
}

func (c *lruPlanCache) removeNode(node *lruNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (c *lruPlanCache) moveToFront(node *lruNode) {
	c.removeNode(node)
	c.addNode(node)
}

func (c *lruPlanCache) removeOldest() {
	oldest := c.tail.prev
	if oldest != c.head {
		c.removeNode(oldest)
		delete(c.items, oldest.key)
	}
}

// ============================================================================
// 6. LEXER & TOKENIZER
// ============================================================================

type TokenType int

const (
	TokEOF TokenType = iota
	TokIdentifier
	TokString
	TokNumber
	TokSymbol
	TokSelect
	TokInsert
	TokUpdate
	TokDelete
	TokFrom
	TokInto
	TokValues
	TokSet
	TokWhere
	TokJoin
	TokInner
	TokLeft
	TokOn
	TokGroup
	TokBy
	TokHaving
	TokOrder
	TokAsc
	TokDesc
	TokLimit
	TokOffset
	TokAnd
	TokOr
	TokLike
	TokIn
	TokAs
	TokCount
	TokSum
	TokAvg
	TokMin
	TokMax
	TokParam
)

type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

type Lexer struct {
	input string
	pos   int
	read  int
	ch    byte
}

func newLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.read >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.read]
	}
	l.pos = l.read
	l.read++
}

func (l *Lexer) peekChar() byte {
	if l.read >= len(l.input) {
		return 0
	}
	return l.input[l.read]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	if l.ch == 0 {
		return Token{Type: TokEOF, Pos: l.pos}
	}

	startPos := l.pos
	switch l.ch {
	case '(', ')', ',', ';', '*', '=', '+', '-', '/', '&', '|', '^':
		ch := l.ch
		l.readChar()
		return Token{Type: TokSymbol, Value: string(ch), Pos: startPos}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokSymbol, Value: "!=", Pos: startPos}
		}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokSymbol, Value: "<=", Pos: startPos}
		} else if l.peekChar() == '>' {
			l.readChar()
			l.readChar()
			return Token{Type: TokSymbol, Value: "<>", Pos: startPos}
		}
		l.readChar()
		return Token{Type: TokSymbol, Value: "<", Pos: startPos}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokSymbol, Value: ">=", Pos: startPos}
		}
		l.readChar()
		return Token{Type: TokSymbol, Value: ">", Pos: startPos}
	case '\'', '"':
		quote := l.ch
		l.readChar()
		strStart := l.pos
		for l.ch != 0 && l.ch != quote {
			if l.ch == '\\' {
				l.readChar()
			}
			l.readChar()
		}
		val := l.input[strStart:l.pos]
		if l.ch == quote {
			l.readChar()
		}
		return Token{Type: TokString, Value: val, Pos: startPos}
	case '?':
		l.readChar()
		return Token{Type: TokParam, Value: "?", Pos: startPos}
	}

	if isDigit(l.ch) {
		for isDigit(l.ch) || l.ch == '.' {
			l.readChar()
		}
		return Token{Type: TokNumber, Value: l.input[startPos:l.pos], Pos: startPos}
	}

	if isAlpha(l.ch) {
		for isAlpha(l.ch) || isDigit(l.ch) || l.ch == '_' || l.ch == '.' {
			l.readChar()
		}
		val := l.input[startPos:l.pos]
		ttype := matchKeyword(val)
		return Token{Type: ttype, Value: val, Pos: startPos}
	}

	ch := l.ch
	l.readChar()
	return Token{Type: TokSymbol, Value: string(ch), Pos: startPos}
}

func matchKeyword(val string) TokenType {
	if len(val) < 2 || len(val) > 6 {
		return TokIdentifier
	}
	var b [8]byte
	for i := 0; i < len(val); i++ {
		c := val[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		b[i] = c
	}
	s := unsafeString(b[:len(val)])
	switch s {
	case "SELECT":
		return TokSelect
	case "INSERT":
		return TokInsert
	case "UPDATE":
		return TokUpdate
	case "DELETE":
		return TokDelete
	case "FROM":
		return TokFrom
	case "INTO":
		return TokInto
	case "VALUES":
		return TokValues
	case "SET":
		return TokSet
	case "WHERE":
		return TokWhere
	case "JOIN":
		return TokJoin
	case "INNER":
		return TokInner
	case "LEFT":
		return TokLeft
	case "ON":
		return TokOn
	case "GROUP":
		return TokGroup
	case "BY":
		return TokBy
	case "HAVING":
		return TokHaving
	case "ORDER":
		return TokOrder
	case "ASC":
		return TokAsc
	case "DESC":
		return TokDesc
	case "LIMIT":
		return TokLimit
	case "OFFSET":
		return TokOffset
	case "AND":
		return TokAnd
	case "OR":
		return TokOr
	case "LIKE":
		return TokLike
	case "IN":
		return TokIn
	case "AS":
		return TokAs
	case "COUNT":
		return TokCount
	case "SUM":
		return TokSum
	case "AVG":
		return TokAvg
	case "MIN":
		return TokMin
	case "MAX":
		return TokMax
	}
	return TokIdentifier
}

func isAlpha(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// ============================================================================
// 7. AST DEFINITIONS & COMPILER / PARSER
// ============================================================================

type StatementType int

const (
	StmtSelect StatementType = iota
	StmtInsert
	StmtUpdate
	StmtDelete
)

type Expression interface {
	Eval(ctx *ExecutionContext) interface{}
}

type FieldExpr struct {
	Name  string
	Alias string
}

type AggregateType int

const (
	AggNone AggregateType = iota
	AggCount
	AggSum
	AggAvg
	AggMin
	AggMax
)

type AggregateExpr struct {
	Type  AggregateType
	Field string
	Alias string
}

type BinaryOpExpr struct {
	Left  Expression
	Op    string
	Right Expression
}

type LiteralExpr struct {
	Val interface{}
}

type ColumnRefExpr struct {
	Column string
}

type InExpr struct {
	Left    Expression
	List    []Expression
	NotExpr bool
}

type JoinClause struct {
	Type       string
	Table      string
	OnLeftCol  string
	OnRightCol string
}

type FastPathType int

const (
	FastPathGet FastPathType = iota
	FastPathGetByType
	FastPathQueryBitmask
)

type FastPathPlan struct {
	Type          FastPathType
	Key           string
	TypeHeaderVal uint16
	BitmaskMask   uint64
	BitmaskVal    uint64
}

type SQLStatement struct {
	Type        StatementType
	Table       string
	Projections []FieldExpr
	Aggregates  []AggregateExpr
	Where       Expression
	Joins       []JoinClause
	GroupBy     []string
	Having      Expression
	OrderBy     string
	OrderDesc   bool
	Limit       int
	Offset      int
	InsertKeys  []string
	InsertVals  []Expression
	UpdatePairs map[string]Expression
	FastPath    *FastPathPlan
}

func (stmt *SQLStatement) bindParams(params []interface{}) *SQLStatement {
	paramIdx := 0
	cloned := *stmt
	if stmt.Where != nil {
		cloned.Where = bindExpressionParams(stmt.Where, params, &paramIdx)
	}
	if len(stmt.InsertVals) > 0 {
		newVals := make([]Expression, len(stmt.InsertVals))
		for i, v := range stmt.InsertVals {
			newVals[i] = bindExpressionParams(v, params, &paramIdx)
		}
		cloned.InsertVals = newVals
	}
	if len(stmt.UpdatePairs) > 0 {
		newPairs := make(map[string]Expression, len(stmt.UpdatePairs))
		for k, v := range stmt.UpdatePairs {
			newPairs[k] = bindExpressionParams(v, params, &paramIdx)
		}
		cloned.UpdatePairs = newPairs
	}
	if stmt.Having != nil {
		cloned.Having = bindExpressionParams(stmt.Having, params, &paramIdx)
	}
	cloned.FastPath = detectFastPathPlan(&cloned)
	return &cloned
}

func bindExpressionParams(expr Expression, params []interface{}, idx *int) Expression {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *LiteralExpr:
		if s, ok := e.Val.(string); ok && s == "?" {
			if *idx < len(params) {
				val := params[*idx]
				*idx++
				return getLiteralExpr(val)
			}
		}
	case *BinaryOpExpr:
		return getBinaryOpExpr(
			bindExpressionParams(e.Left, params, idx),
			e.Op,
			bindExpressionParams(e.Right, params, idx),
		)
	case *InExpr:
		newList := make([]Expression, len(e.List))
		for i, item := range e.List {
			newList[i] = bindExpressionParams(item, params, idx)
		}
		return &InExpr{
			Left:    bindExpressionParams(e.Left, params, idx),
			List:    newList,
			NotExpr: e.NotExpr,
		}
	}
	return expr
}

type Parser struct {
	lexer     *Lexer
	curToken  Token
	peekToken Token
}

func newParser(l *Lexer) *Parser {
	p := &Parser{lexer: l}
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

func (e *SQLEngine) compile(sql string) (*SQLStatement, error) {
	lexer := newLexer(sql)
	parser := newParser(lexer)
	stmt, err := parser.ParseStatement()
	if err != nil {
		return nil, err
	}
	stmt.FastPath = detectFastPathPlan(stmt)
	return stmt, nil
}

func (p *Parser) ParseStatement() (*SQLStatement, error) {
	stmt := &SQLStatement{Limit: -1}
	switch p.curToken.Type {
	case TokSelect:
		stmt.Type = StmtSelect
		if err := p.parseSelect(stmt); err != nil {
			return nil, err
		}
	case TokInsert:
		stmt.Type = StmtInsert
		if err := p.parseInsert(stmt); err != nil {
			return nil, err
		}
	case TokUpdate:
		stmt.Type = StmtUpdate
		if err := p.parseUpdate(stmt); err != nil {
			return nil, err
		}
	case TokDelete:
		stmt.Type = StmtDelete
		if err := p.parseDelete(stmt); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported SQL command at position %d", p.curToken.Pos)
	}
	return stmt, nil
}

func (p *Parser) parseSelect(stmt *SQLStatement) error {
	p.nextToken()
	for {
		if p.curToken.Type == TokCount || p.curToken.Type == TokSum ||
			p.curToken.Type == TokAvg || p.curToken.Type == TokMin || p.curToken.Type == TokMax {
			aggType := AggCount
			switch p.curToken.Type {
			case TokSum:
				aggType = AggSum
			case TokAvg:
				aggType = AggAvg
			case TokMin:
				aggType = AggMin
			case TokMax:
				aggType = AggMax
			}
			p.nextToken()
			if p.curToken.Value != "(" {
				return errors.New("expected '(' after aggregate function")
			}
			p.nextToken()
			field := p.curToken.Value
			p.nextToken()
			if p.curToken.Value != ")" {
				return errors.New("expected ')' after aggregate column")
			}
			p.nextToken()
			alias := field
			if p.curToken.Type == TokAs {
				p.nextToken()
				alias = p.curToken.Value
				p.nextToken()
			} else if p.curToken.Type == TokIdentifier {
				alias = p.curToken.Value
				p.nextToken()
			}
			stmt.Aggregates = append(stmt.Aggregates, AggregateExpr{
				Type:  aggType,
				Field: field,
				Alias: alias,
			})
		} else {
			fName := p.curToken.Value
			p.nextToken()
			alias := fName
			if p.curToken.Type == TokAs {
				p.nextToken()
				alias = p.curToken.Value
				p.nextToken()
			}
			stmt.Projections = append(stmt.Projections, FieldExpr{Name: fName, Alias: alias})
		}
		if p.curToken.Value == "," {
			p.nextToken()
			continue
		}
		break
	}

	if p.curToken.Type != TokFrom {
		return errors.New("expected FROM clause")
	}
	p.nextToken()
	stmt.Table = p.curToken.Value
	p.nextToken()

	for p.curToken.Type == TokJoin || p.curToken.Type == TokInner || p.curToken.Type == TokLeft {
		jType := "INNER"
		if p.curToken.Type == TokLeft {
			jType = "LEFT"
			p.nextToken()
		} else if p.curToken.Type == TokInner {
			p.nextToken()
		}
		if p.curToken.Type == TokJoin {
			p.nextToken()
		}
		joinTable := p.curToken.Value
		p.nextToken()

		if p.curToken.Type != TokOn {
			return errors.New("expected ON keyword after JOIN")
		}
		p.nextToken()
		leftCol := p.curToken.Value
		p.nextToken()
		if p.curToken.Value != "=" {
			return errors.New("expected '=' in JOIN ON condition")
		}
		p.nextToken()
		rightCol := p.curToken.Value
		p.nextToken()

		stmt.Joins = append(stmt.Joins, JoinClause{
			Type:       jType,
			Table:      joinTable,
			OnLeftCol:  leftCol,
			OnRightCol: rightCol,
		})
	}

	if p.curToken.Type == TokWhere {
		p.nextToken()
		whereExpr, err := p.parseExpression(0)
		if err != nil {
			return err
		}
		stmt.Where = whereExpr
	}

	if p.curToken.Type == TokGroup {
		p.nextToken()
		if p.curToken.Type == TokBy {
			p.nextToken()
		}
		for {
			stmt.GroupBy = append(stmt.GroupBy, p.curToken.Value)
			p.nextToken()
			if p.curToken.Value == "," {
				p.nextToken()
				continue
			}
			break
		}
	}

	if p.curToken.Type == TokHaving {
		p.nextToken()
		havingExpr, err := p.parseExpression(0)
		if err != nil {
			return err
		}
		stmt.Having = havingExpr
	}

	if p.curToken.Type == TokOrder {
		p.nextToken()
		if p.curToken.Type == TokBy {
			p.nextToken()
		}
		stmt.OrderBy = p.curToken.Value
		p.nextToken()
		if p.curToken.Type == TokDesc {
			stmt.OrderDesc = true
			p.nextToken()
		} else if p.curToken.Type == TokAsc {
			p.nextToken()
		}
	}

	if p.curToken.Type == TokLimit {
		p.nextToken()
		limitVal, _ := strconv.Atoi(p.curToken.Value)
		stmt.Limit = limitVal
		p.nextToken()
	}

	if p.curToken.Type == TokOffset {
		p.nextToken()
		offsetVal, _ := strconv.Atoi(p.curToken.Value)
		stmt.Offset = offsetVal
		p.nextToken()
	}

	return nil
}

func (p *Parser) parseInsert(stmt *SQLStatement) error {
	p.nextToken()
	if p.curToken.Type == TokInto {
		p.nextToken()
	}
	stmt.Table = p.curToken.Value
	p.nextToken()

	if p.curToken.Value == "(" {
		p.nextToken()
		for p.curToken.Value != ")" && p.curToken.Type != TokEOF {
			stmt.InsertKeys = append(stmt.InsertKeys, p.curToken.Value)
			p.nextToken()
			if p.curToken.Value == "," {
				p.nextToken()
			}
		}
		p.nextToken()
	}

	if p.curToken.Type != TokValues {
		return errors.New("expected VALUES clause")
	}
	p.nextToken()

	if p.curToken.Value == "(" {
		p.nextToken()
		for p.curToken.Value != ")" && p.curToken.Type != TokEOF {
			expr, err := p.parseExpression(0)
			if err != nil {
				return err
			}
			stmt.InsertVals = append(stmt.InsertVals, expr)
			if p.curToken.Value == "," {
				p.nextToken()
			}
		}
		p.nextToken()
	}

	return nil
}

func (p *Parser) parseUpdate(stmt *SQLStatement) error {
	p.nextToken()
	stmt.Table = p.curToken.Value
	p.nextToken()

	if p.curToken.Type != TokSet {
		return errors.New("expected SET clause in UPDATE")
	}
	p.nextToken()

	stmt.UpdatePairs = make(map[string]Expression)
	for {
		col := p.curToken.Value
		p.nextToken()
		if p.curToken.Value != "=" {
			return errors.New("expected '=' in SET assignments")
		}
		p.nextToken()
		expr, err := p.parseExpression(0)
		if err != nil {
			return err
		}
		stmt.UpdatePairs[col] = expr
		if p.curToken.Value == "," {
			p.nextToken()
			continue
		}
		break
	}

	if p.curToken.Type == TokWhere {
		p.nextToken()
		whereExpr, err := p.parseExpression(0)
		if err != nil {
			return err
		}
		stmt.Where = whereExpr
	}

	return nil
}

func (p *Parser) parseDelete(stmt *SQLStatement) error {
	p.nextToken()
	if p.curToken.Type == TokFrom {
		p.nextToken()
	}
	stmt.Table = p.curToken.Value
	p.nextToken()

	if p.curToken.Type == TokWhere {
		p.nextToken()
		whereExpr, err := p.parseExpression(0)
		if err != nil {
			return err
		}
		stmt.Where = whereExpr
	}

	return nil
}

func (p *Parser) parseExpression(precedence int) (Expression, error) {
	left, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}

	for {
		op := p.curToken.Value
		tokType := p.curToken.Type
		if tokType == TokAnd || tokType == TokOr || tokType == TokLike || tokType == TokIn || tokType == TokSymbol {
			curPrec := getOpPrecedence(op, tokType)
			if curPrec <= precedence {
				break
			}
			p.nextToken()

			if tokType == TokIn {
				if p.curToken.Value != "(" {
					return nil, errors.New("expected '(' after IN operator")
				}
				p.nextToken()
				var list []Expression
				for p.curToken.Value != ")" && p.curToken.Type != TokEOF {
					item, err := p.parseExpression(0)
					if err != nil {
						return nil, err
					}
					list = append(list, item)
					if p.curToken.Value == "," {
						p.nextToken()
					}
				}
				p.nextToken()
				left = &InExpr{Left: left, List: list}
			} else {
				right, err := p.parseExpression(curPrec)
				if err != nil {
					return nil, err
				}
				left = getBinaryOpExpr(left, op, right)
			}
		} else {
			break
		}
	}
	return left, nil
}

func (p *Parser) parsePrimaryExpression() (Expression, error) {
	switch p.curToken.Type {
	case TokIdentifier:
		col := p.curToken.Value
		p.nextToken()
		return getColumnRefExpr(col), nil
	case TokString:
		val := p.curToken.Value
		p.nextToken()
		return getLiteralExpr(val), nil
	case TokNumber:
		valStr := p.curToken.Value
		p.nextToken()
		if strings.Contains(valStr, ".") {
			f, _ := strconv.ParseFloat(valStr, 64)
			return getLiteralExpr(f), nil
		}
		i, _ := strconv.ParseInt(valStr, 10, 64)
		return getLiteralExpr(i), nil
	case TokParam:
		p.nextToken()
		return getLiteralExpr("?"), nil
	case TokSymbol:
		if p.curToken.Value == "(" {
			p.nextToken()
			expr, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			if p.curToken.Value != ")" {
				return nil, errors.New("expected closing ')'")
			}
			p.nextToken()
			return expr, nil
		}
	}
	return nil, fmt.Errorf("syntax error near '%s'", p.curToken.Value)
}

func getOpPrecedence(op string, ttype TokenType) int {
	if ttype == TokOr {
		return 1
	}
	if ttype == TokAnd {
		return 2
	}
	switch op {
	case "=", "!=", "<>", "LIKE", "IN":
		return 3
	case "<", "<=", ">", ">=":
		return 4
	case "&", "|", "^":
		return 5
	case "+", "-":
		return 6
	case "*", "/":
		return 7
	}
	return 0
}

// ============================================================================
// 8. PREDICATE PUSHDOWN & FAST PATH DETECTION
// ============================================================================

func isStringQuestion(v interface{}) bool {
	s, ok := v.(string)
	return ok && s == "?"
}

func detectFastPathPlan(stmt *SQLStatement) *FastPathPlan {
	if stmt.Type != StmtSelect || len(stmt.Joins) > 0 || len(stmt.GroupBy) > 0 || len(stmt.Aggregates) > 0 {
		return nil
	}

	binOp, ok := stmt.Where.(*BinaryOpExpr)
	if !ok {
		return nil
	}

	if binOp.Op == "=" {
		if col, isCol := binOp.Left.(*ColumnRefExpr); isCol && (strings.EqualFold(col.Column, "key") || strings.EqualFold(col.Column, "id")) {
			if lit, isLit := binOp.Right.(*LiteralExpr); isLit {
				if isStringQuestion(lit.Val) {
					return nil
				}
				return &FastPathPlan{
					Type: FastPathGet,
					Key:  valToString(lit.Val),
				}
			}
		}
	}

	if binOp.Op == "=" {
		if col, isCol := binOp.Left.(*ColumnRefExpr); isCol && strings.EqualFold(col.Column, "type_header") {
			if lit, isLit := binOp.Right.(*LiteralExpr); isLit {
				if isStringQuestion(lit.Val) {
					return nil
				}
				valInt := toInt64(lit.Val)
				return &FastPathPlan{
					Type:          FastPathGetByType,
					TypeHeaderVal: uint16(valInt),
				}
			}
		}
	}

	if binOp.Op == "=" {
		if bitwiseOp, isBitwise := binOp.Left.(*BinaryOpExpr); isBitwise && bitwiseOp.Op == "&" {
			if col, isCol := bitwiseOp.Left.(*ColumnRefExpr); isCol && strings.EqualFold(col.Column, "bitmask") {
				if maskLit, isMaskLit := bitwiseOp.Right.(*LiteralExpr); isMaskLit {
					if valLit, isValLit := binOp.Right.(*LiteralExpr); isValLit {
						if isStringQuestion(maskLit.Val) || isStringQuestion(valLit.Val) {
							return nil
						}
						return &FastPathPlan{
							Type:        FastPathQueryBitmask,
							BitmaskMask: uint64(toInt64(maskLit.Val)),
							BitmaskVal:  uint64(toInt64(valLit.Val)),
						}
					}
				}
			}
		}
	}

	return nil
}

func (e *SQLEngine) executeFastPath(plan *FastPathPlan) (*QueryResult, bool, error) {
	var rawRecords [][]byte

	switch plan.Type {
	case FastPathGet:
		val, ok := e.storage.Get(plan.Key)
		if !ok {
			return &QueryResult{Columns: []string{"key", "value"}, Rows: [][]interface{}{}}, true, nil
		}
		rawRecords = [][]byte{val}
	case FastPathGetByType:
		rawRecords = e.storage.GetByType(plan.TypeHeaderVal)
	case FastPathQueryBitmask:
		rawRecords = e.storage.QueryByBitmask(plan.BitmaskMask, plan.BitmaskVal)
	}

	rows := make([][]interface{}, 0, len(rawRecords))
	for _, rec := range rawRecords {
		row := []interface{}{unsafeString(rec)}
		rows = append(rows, row)
	}

	return &QueryResult{
		Columns: []string{"payload"},
		Rows:    rows,
	}, true, nil
}

// ============================================================================
// 9. ZERO-ALLOCATION FAST JSON KEY EXTRACTOR
// ============================================================================

func extractJSONField(data []byte, key string) interface{} {
	if len(data) == 0 {
		return nil
	}

	keyLen := len(key)
	keyB := unsafeBytes(key)

	for i := 0; i <= len(data)-keyLen-3; i++ {
		if data[i] == '"' && bytes.HasPrefix(data[i+1:], keyB) && data[i+1+keyLen] == '"' {
			p := i + 2 + keyLen
			for p < len(data) && (data[p] == ' ' || data[p] == '\t' || data[p] == '\r' || data[p] == '\n') {
				p++
			}
			if p < len(data) && data[p] == ':' {
				start := p + 1
				for start < len(data) && (data[start] == ' ' || data[start] == '\t' || data[start] == '\r' || data[start] == '\n') {
					start++
				}
				if start >= len(data) {
					return nil
				}
				if data[start] == '"' {
					start++
					end := start
					for end < len(data) {
						if data[end] == '"' && data[end-1] != '\\' {
							break
						}
						end++
					}
					return string(data[start:end])
				}
				end := start
				for end < len(data) && data[end] != ',' && data[end] != '}' && data[end] != ']' && data[end] != ' ' && data[end] != '\n' && data[end] != '\r' {
					end++
				}
				token := unsafeString(data[start:end])
				if token == "true" {
					return true
				}
				if token == "false" {
					return false
				}
				if token == "null" {
					return nil
				}
				if strings.Contains(token, ".") {
					if f, err := strconv.ParseFloat(token, 64); err == nil {
						return f
					}
				}
				if iVal, err := strconv.ParseInt(token, 10, 64); err == nil {
					return iVal
				}
				return token
			}
		}
	}

	return nil
}

// ============================================================================
// 10. EXECUTION CONTEXT & EVALUATOR
// ============================================================================

type ExecutionContext struct {
	Vars map[string]interface{}
}

func (e *ColumnRefExpr) Eval(ctx *ExecutionContext) interface{} {
	if val, ok := ctx.Vars[e.Column]; ok {
		return val
	}
	return nil
}

func (e *LiteralExpr) Eval(ctx *ExecutionContext) interface{} {
	return e.Val
}

func (e *BinaryOpExpr) Eval(ctx *ExecutionContext) interface{} {
	leftVal := e.Left.Eval(ctx)
	rightVal := e.Right.Eval(ctx)

	switch e.Op {
	case "=":
		return compareValues(leftVal, rightVal) == 0
	case "!=", "<>":
		return compareValues(leftVal, rightVal) != 0
	case ">":
		return compareValues(leftVal, rightVal) > 0
	case "<":
		return compareValues(leftVal, rightVal) < 0
	case ">=":
		return compareValues(leftVal, rightVal) >= 0
	case "<=":
		return compareValues(leftVal, rightVal) <= 0
	case "AND":
		return toBool(leftVal) && toBool(rightVal)
	case "OR":
		return toBool(leftVal) || toBool(rightVal)
	case "+":
		return toFloat64(leftVal) + toFloat64(rightVal)
	case "-":
		return toFloat64(leftVal) - toFloat64(rightVal)
	case "*":
		return toFloat64(leftVal) * toFloat64(rightVal)
	case "/":
		r := toFloat64(rightVal)
		if r == 0 {
			return 0.0
		}
		return toFloat64(leftVal) / r
	case "&":
		return toInt64(leftVal) & toInt64(rightVal)
	case "|":
		return toInt64(leftVal) | toInt64(rightVal)
	case "^":
		return toInt64(leftVal) ^ toInt64(rightVal)
	case "LIKE":
		str := valToString(leftVal)
		pattern := valToString(rightVal)
		return matchLike(str, pattern)
	}
	return false
}

func (e *InExpr) Eval(ctx *ExecutionContext) interface{} {
	leftVal := e.Left.Eval(ctx)
	for _, item := range e.List {
		if compareValues(leftVal, item.Eval(ctx)) == 0 {
			return !e.NotExpr
		}
	}
	return e.NotExpr
}

func compareValues(a, b interface{}) int {
	if a == nil || b == nil {
		if a == b {
			return 0
		}
		if a == nil {
			return -1
		}
		return 1
	}

	if isNumeric(a) && isNumeric(b) {
		fa := toFloat64(a)
		fb := toFloat64(b)
		if fa < fb {
			return -1
		} else if fa > fb {
			return 1
		}
		return 0
	}

	sa := valToString(a)
	sb := valToString(b)
	return strings.Compare(sa, sb)
}

func isNumeric(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	}
	return false
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	}
	return 0.0
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		i, _ := strconv.ParseInt(val, 10, 64)
		return i
	}
	return 0
}

func toBool(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	if isNumeric(v) {
		return toFloat64(v) != 0
	}
	return v != nil
}

func matchLike(str, pattern string) bool {
	if pattern == "%" {
		return true
	}
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		return strings.Contains(str, pattern[1:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "%") {
		return strings.HasSuffix(str, pattern[1:])
	}
	if strings.HasSuffix(pattern, "%") {
		return strings.HasPrefix(str, pattern[:len(pattern)-1])
	}
	return str == pattern
}

// ============================================================================
// 11. GENERAL EXECUTION ENGINE
// ============================================================================

func (e *SQLEngine) executeGeneralTx(tx *Transaction, stmt *SQLStatement) (*QueryResult, error) {
	switch stmt.Type {
	case StmtSelect:
		return e.executeSelect(stmt)
	case StmtInsert:
		return e.executeInsertTx(tx, stmt)
	case StmtUpdate:
		return e.executeUpdateTx(tx, stmt)
	case StmtDelete:
		return e.executeDeleteTx(tx, stmt)
	}
	return nil, errors.New("unsupported statement type")
}

func (e *SQLEngine) executeSelect(stmt *SQLStatement) (*QueryResult, error) {
	var dataset []map[string]interface{}
	var usedIndex bool

	dataset, usedIndex = e.tryIndexScan(stmt)

	if !usedIndex && e.storage != nil {
		e.storage.Scan(func(k string, v []byte) bool {
			row := make(map[string]interface{})
			row["key"] = k
			row["payload"] = unsafeString(v)
			if len(v) > 0 && v[0] == '{' {
				var jsonMap map[string]interface{}
				if json.Unmarshal(v, &jsonMap) == nil {
					for rk, rv := range jsonMap {
						row[rk] = rv
					}
				}
			}
			for _, proj := range stmt.Projections {
				if proj.Alias != "" && proj.Alias != proj.Name {
					if val, ok := row[proj.Name]; ok {
						row[proj.Alias] = val
					}
				}
			}
			dataset = append(dataset, row)
			return true
		})
	}

	if len(stmt.Joins) > 0 {
		dataset = e.executeJoins(dataset, stmt.Joins)
	}

	ctx := e.ctxPool.Get().(*ExecutionContext)
	defer e.ctxPool.Put(ctx)

	var filtered []map[string]interface{}
	for _, row := range dataset {
		ctx.Vars = row
		if stmt.Where == nil || toBool(stmt.Where.Eval(ctx)) {
			filtered = append(filtered, row)
		}
	}

	if len(stmt.GroupBy) > 0 || len(stmt.Aggregates) > 0 {
		filtered = e.executeGroupByAndAggregates(filtered, stmt)
	}

	if stmt.Having != nil {
		var havingFiltered []map[string]interface{}
		for _, row := range filtered {
			ctx.Vars = row
			if toBool(stmt.Having.Eval(ctx)) {
				havingFiltered = append(havingFiltered, row)
			}
		}
		filtered = havingFiltered
	}

	if stmt.OrderBy != "" {
		sort.Slice(filtered, func(i, j int) bool {
			vi := filtered[i][stmt.OrderBy]
			vj := filtered[j][stmt.OrderBy]
			res := compareValues(vi, vj)
			if stmt.OrderDesc {
				return res > 0
			}
			return res < 0
		})
	}

	if stmt.Offset > 0 {
		if stmt.Offset >= len(filtered) {
			filtered = nil
		} else {
			filtered = filtered[stmt.Offset:]
		}
	}

	if stmt.Limit >= 0 && len(filtered) > stmt.Limit {
		filtered = filtered[:stmt.Limit]
	}

	hasStar := false
	for _, p := range stmt.Projections {
		if p.Name == "*" {
			hasStar = true
			break
		}
	}

	var cols []string
	if hasStar {
		seen := make(map[string]bool)
		for _, record := range filtered {
			for k := range record {
				if !seen[k] {
					seen[k] = true
					cols = append(cols, k)
				}
			}
		}
		sort.Strings(cols)
	} else {
		cols = make([]string, 0, len(stmt.Projections)+len(stmt.Aggregates))
		for _, p := range stmt.Projections {
			cols = append(cols, p.Alias)
		}
	}
	for _, a := range stmt.Aggregates {
		cols = append(cols, a.Alias)
	}

	rows := make([][]interface{}, len(filtered))
	for i, record := range filtered {
		row := make([]interface{}, len(cols))
		for j, colName := range cols {
			row[j] = record[colName]
		}
		rows[i] = row
	}

	return &QueryResult{
		Columns: cols,
		Rows:    rows,
	}, nil
}

func (e *SQLEngine) tryIndexScan(stmt *SQLStatement) ([]map[string]interface{}, bool) {
	if stmt.Where == nil {
		return nil, false
	}
	binOp, ok := stmt.Where.(*BinaryOpExpr)
	if !ok {
		return nil, false
	}

	colExpr, isCol := binOp.Left.(*ColumnRefExpr)
	litExpr, isLit := binOp.Right.(*LiteralExpr)
	if !isCol || !isLit {
		return nil, false
	}

	sl, hasIndex := e.indexManager.GetIndex(stmt.Table, colExpr.Column)
	if !hasIndex {
		return nil, false
	}

	var docIDs []string
	switch binOp.Op {
	case "=":
		docIDs = sl.Search(litExpr.Val)
	case ">":
		docIDs = sl.RangeSearch(litExpr.Val, math.MaxFloat64, false, true)
	case ">=":
		docIDs = sl.RangeSearch(litExpr.Val, math.MaxFloat64, true, true)
	case "<":
		docIDs = sl.RangeSearch(-math.MaxFloat64, litExpr.Val, true, false)
	case "<=":
		docIDs = sl.RangeSearch(-math.MaxFloat64, litExpr.Val, true, true)
	default:
		return nil, false
	}

	dataset := make([]map[string]interface{}, 0, len(docIDs))
	for _, id := range docIDs {
		val, found := e.storage.Get(id)
		if !found {
			continue
		}
		row := make(map[string]interface{})
		row["key"] = id
		row["payload"] = unsafeString(val)
		if len(val) > 0 && val[0] == '{' {
			var jsonMap map[string]interface{}
			if json.Unmarshal(val, &jsonMap) == nil {
				for rk, rv := range jsonMap {
					row[rk] = rv
				}
			}
		}
		for _, proj := range stmt.Projections {
			if proj.Alias != "" && proj.Alias != proj.Name {
				if v, ok := row[proj.Name]; ok {
					row[proj.Alias] = v
				}
			}
		}
		dataset = append(dataset, row)
	}
	return dataset, true
}

func (e *SQLEngine) executeJoins(leftSet []map[string]interface{}, joins []JoinClause) []map[string]interface{} {
	current := leftSet
	for _, join := range joins {
		var joined []map[string]interface{}
		for _, lRow := range current {
			lVal := lRow[join.OnLeftCol]
			matched := false
			if e.storage != nil {
				e.storage.Scan(func(rk string, rv []byte) bool {
					rVal := extractJSONField(rv, join.OnRightCol)
					if compareValues(lVal, rVal) == 0 {
						matched = true
						merged := make(map[string]interface{}, len(lRow)+2)
						for k, v := range lRow {
							merged[k] = v
						}
						merged[join.Table+"_key"] = rk
						merged[join.OnRightCol] = rVal
						joined = append(joined, merged)
					}
					return true
				})
			}
			if !matched && join.Type == "LEFT" {
				joined = append(joined, lRow)
			}
		}
		current = joined
	}
	return current
}

func (e *SQLEngine) executeGroupByAndAggregates(dataset []map[string]interface{}, stmt *SQLStatement) []map[string]interface{} {
	groups := make(map[string][]map[string]interface{})
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)

	for _, row := range dataset {
		buf.Reset()
		for _, gCol := range stmt.GroupBy {
			buf.WriteString(valToString(row[gCol]))
			buf.WriteByte('|')
		}
		gKey := buf.String()
		groups[gKey] = append(groups[gKey], row)
	}

	var results []map[string]interface{}
	for _, groupRows := range groups {
		resRow := make(map[string]interface{})
		if len(groupRows) > 0 {
			for _, gCol := range stmt.GroupBy {
				resRow[gCol] = groupRows[0][gCol]
			}
		}
		for _, agg := range stmt.Aggregates {
			switch agg.Type {
			case AggCount:
				resRow[agg.Alias] = int64(len(groupRows))
			case AggSum:
				var sum float64
				for _, r := range groupRows {
					sum += toFloat64(r[agg.Field])
				}
				resRow[agg.Alias] = sum
			case AggAvg:
				var sum float64
				for _, r := range groupRows {
					sum += toFloat64(r[agg.Field])
				}
				if len(groupRows) > 0 {
					resRow[agg.Alias] = sum / float64(len(groupRows))
				} else {
					resRow[agg.Alias] = 0.0
				}
			case AggMin:
				minVal := math.MaxFloat64
				for _, r := range groupRows {
					v := toFloat64(r[agg.Field])
					if v < minVal {
						minVal = v
					}
				}
				resRow[agg.Alias] = minVal
			case AggMax:
				maxVal := -math.MaxFloat64
				for _, r := range groupRows {
					v := toFloat64(r[agg.Field])
					if v > maxVal {
						maxVal = v
					}
				}
				resRow[agg.Alias] = maxVal
			}
		}
		results = append(results, resRow)
	}
	return results
}

func (e *SQLEngine) executeInsertTx(tx *Transaction, stmt *SQLStatement) (*QueryResult, error) {
	autoCommit := false
	if tx == nil {
		tx = e.txManager.Begin()
		autoCommit = true
	}

	ctx := e.ctxPool.Get().(*ExecutionContext)
	defer e.ctxPool.Put(ctx)

	var key string
	payloadMap := make(map[string]interface{})
	for i, k := range stmt.InsertKeys {
		if i < len(stmt.InsertVals) {
			val := stmt.InsertVals[i].Eval(ctx)
			payloadMap[k] = val
			if strings.EqualFold(k, "key") || strings.EqualFold(k, "id") {
				key = valToString(val)
			}
		}
	}

	if key == "" {
		key = fmt.Sprintf("auto_%d", time.Now().UnixNano())
	}

	payloadBytes, err := json.Marshal(payloadMap)
	if err != nil {
		if autoCommit {
			_ = tx.Rollback()
		}
		return nil, err
	}

	if err := tx.Put(key, payloadBytes); err != nil {
		if autoCommit {
			_ = tx.Rollback()
		}
		return nil, err
	}

	if autoCommit {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	e.indexManager.IndexRecord(stmt.Table, key, payloadBytes)
	return &QueryResult{AffectedRows: 1}, nil
}

func (e *SQLEngine) executeUpdateTx(tx *Transaction, stmt *SQLStatement) (*QueryResult, error) {
	if e.storage == nil {
		return nil, errors.New("underlying StorageEngine is nil")
	}

	autoCommit := false
	if tx == nil {
		tx = e.txManager.Begin()
		autoCommit = true
	}

	ctx := e.ctxPool.Get().(*ExecutionContext)
	defer e.ctxPool.Put(ctx)

	var affected int64
	e.storage.Scan(func(k string, v []byte) bool {
		row := map[string]interface{}{"key": k, "payload": unsafeString(v)}
		updatedMap := make(map[string]interface{})
		if len(v) > 0 && v[0] == '{' {
			if json.Unmarshal(v, &updatedMap) == nil {
				for rk, rv := range updatedMap {
					row[rk] = rv
				}
			}
		}
		ctx.Vars = row
		if stmt.Where == nil || toBool(stmt.Where.Eval(ctx)) {
			for col, expr := range stmt.UpdatePairs {
				updatedMap[col] = expr.Eval(ctx)
			}
			newPayload, err := json.Marshal(updatedMap)
			if err != nil {
				return true
			}

			e.indexManager.UnindexRecord(stmt.Table, k, v)
			if err := tx.Put(k, newPayload); err == nil {
				e.indexManager.IndexRecord(stmt.Table, k, newPayload)
				affected++
			}
		}
		return true
	})

	if autoCommit {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return &QueryResult{AffectedRows: affected}, nil
}

func (e *SQLEngine) executeDeleteTx(tx *Transaction, stmt *SQLStatement) (*QueryResult, error) {
	if e.storage == nil {
		return nil, errors.New("underlying StorageEngine is nil")
	}

	autoCommit := false
	if tx == nil {
		tx = e.txManager.Begin()
		autoCommit = true
	}

	ctx := e.ctxPool.Get().(*ExecutionContext)
	defer e.ctxPool.Put(ctx)

	type deleteCandidate struct {
		key     string
		payload []byte
	}

	var targets []deleteCandidate
	e.storage.Scan(func(k string, v []byte) bool {
		row := map[string]interface{}{"key": k, "payload": unsafeString(v)}
		if len(v) > 0 && v[0] == '{' {
			var jsonMap map[string]interface{}
			if json.Unmarshal(v, &jsonMap) == nil {
				for rk, rv := range jsonMap {
					row[rk] = rv
				}
			}
		}
		ctx.Vars = row
		if stmt.Where == nil || toBool(stmt.Where.Eval(ctx)) {
			targets = append(targets, deleteCandidate{key: k, payload: v})
		}
		return true
	})

	var affected int64
	for _, item := range targets {
		if err := tx.Delete(item.key); err == nil {
			e.indexManager.UnindexRecord(stmt.Table, item.key, item.payload)
			affected++
		}
	}

	if autoCommit {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return &QueryResult{AffectedRows: affected}, nil
}
