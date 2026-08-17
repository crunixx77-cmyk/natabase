package engine

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
)

var (
	ErrKeyTypeMismatch = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNoSuchKey       = errors.New("ERR no such key")
)

// ============================================================================
// 1. SKIPLIST IMPLEMENTATION (For O(log N) Sorted Sets)
// ============================================================================

const (
	zSkipListMaxLevel = 32
	zSkipListP        = 0.25
)

type zskiplistLevel struct {
	forward *zskiplistNode
	span    int
}

type zskiplistNode struct {
	member string
	score  float64
	level  []zskiplistLevel
}

type zskiplist struct {
	header *zskiplistNode
	tail   *zskiplistNode
	length int
	level  int
}

func newZNode(level int, score float64, member string) *zskiplistNode {
	return &zskiplistNode{
		member: member,
		score:  score,
		level:  make([]zskiplistLevel, level),
	}
}

func newZSkipList() *zskiplist {
	return &zskiplist{
		level:  1,
		header: newZNode(zSkipListMaxLevel, 0, ""),
	}
}

func randomLevel() int {
	lvl := 1
	for (rand.Float64() < zSkipListP) && lvl < zSkipListMaxLevel {
		lvl++
	}
	return lvl
}

func (z *zskiplist) insert(score float64, member string) *zskiplistNode {
	update := make([]*zskiplistNode, zSkipListMaxLevel)
	rank := make([]int, zSkipListMaxLevel)
	x := z.header

	for i := z.level - 1; i >= 0; i-- {
		if i == z.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	lvl := randomLevel()
	if lvl > z.level {
		for i := z.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = z.header
			update[i].level[i].span = z.length
		}
		z.level = lvl
	}

	x = newZNode(lvl, score, member)
	for i := 0; i < lvl; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := lvl; i < z.level; i++ {
		update[i].level[i].span++
	}

	z.length++
	return x
}

func (z *zskiplist) delete(score float64, member string) bool {
	update := make([]*zskiplistNode, zSkipListMaxLevel)
	x := z.header

	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	if x != nil && score == x.score && x.member == member {
		for i := 0; i < z.level; i++ {
			if update[i].level[i].forward == x {
				update[i].level[i].span += x.level[i].span - 1
				update[i].level[i].forward = x.level[i].forward
			} else {
				update[i].level[i].span--
			}
		}
		for z.level > 1 && z.header.level[z.level-1].forward == nil {
			z.level--
		}
		z.length--
		return true
	}
	return false
}

func (z *zskiplist) getElementByRank(rank int) *zskiplistNode {
	traversed := 0
	x := z.header
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (traversed+x.level[i].span) <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

type zsetStruct struct {
	dict map[string]float64
	zsl  *zskiplist
}

func newZSetStruct() *zsetStruct {
	return &zsetStruct{
		dict: make(map[string]float64),
		zsl:  newZSkipList(),
	}
}

// ============================================================================
// 2. DEQUE IMPLEMENTATION (For O(1) List Operations)
// ============================================================================

type listNode struct {
	val  []byte
	prev *listNode
	next *listNode
}

type ByteDeque struct {
	head *listNode
	tail *listNode
	len  int
}

func (d *ByteDeque) LPush(val []byte) {
	node := &listNode{val: val, next: d.head}
	if d.head != nil {
		d.head.prev = node
	}
	d.head = node
	if d.tail == nil {
		d.tail = node
	}
	d.len++
}

func (d *ByteDeque) RPop() ([]byte, bool) {
	if d.tail == nil {
		return nil, false
	}
	node := d.tail
	d.tail = node.prev
	if d.tail != nil {
		d.tail.next = nil
	} else {
		d.head = nil
	}
	d.len--
	return node.val, true
}

// ============================================================================
// 3. ADVANCED DATA STORE (Hash, List, Set, Sorted Set)
// ============================================================================

type AdvancedDataStore struct {
	mu     sync.RWMutex
	hashes map[string]map[string][]byte
	lists  map[string]*ByteDeque
	sets   map[string]map[string]struct{}
	zsets  map[string]*zsetStruct
}

func NewAdvancedDataStore() *AdvancedDataStore {
	return &AdvancedDataStore{
		hashes: make(map[string]map[string][]byte),
		lists:  make(map[string]*ByteDeque),
		sets:   make(map[string]map[string]struct{}),
		zsets:  make(map[string]*zsetStruct),
	}
}

// --- Hash Operations ---
func (ds *AdvancedDataStore) HSet(key, field string, value []byte) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.hashes[key] == nil {
		ds.hashes[key] = make(map[string][]byte)
	}
	ds.hashes[key][field] = value
}

func (ds *AdvancedDataStore) HGet(key, field string) ([]byte, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	if m, ok := ds.hashes[key]; ok {
		val, exists := m[field]
		return val, exists
	}
	return nil, false
}

// --- List Operations ---
func (ds *AdvancedDataStore) LPush(key string, value []byte) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.lists[key] == nil {
		ds.lists[key] = &ByteDeque{}
	}
	ds.lists[key].LPush(value)
}

func (ds *AdvancedDataStore) RPop(key string) ([]byte, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	l, ok := ds.lists[key]
	if !ok || l.len == 0 {
		return nil, false
	}
	val, ok := l.RPop()
	if l.len == 0 {
		delete(ds.lists, key)
	}
	return val, ok
}

// --- Set Operations ---
func (ds *AdvancedDataStore) SAdd(key, member string) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.sets[key] == nil {
		ds.sets[key] = make(map[string]struct{})
	}
	_, exists := ds.sets[key][member]
	ds.sets[key][member] = struct{}{}
	return !exists
}

func (ds *AdvancedDataStore) SMembers(key string) []string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	m, ok := ds.sets[key]
	if !ok {
		return nil
	}
	res := make([]string, 0, len(m))
	for k := range m {
		res = append(res, k)
	}
	return res
}

// --- Sorted Set (ZSet) Operations ---
func (ds *AdvancedDataStore) ZAdd(key string, score float64, member string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	zs, ok := ds.zsets[key]
	if !ok {
		zs = newZSetStruct()
		ds.zsets[key] = zs
	}

	if curScore, exists := zs.dict[member]; exists {
		if curScore == score {
			return
		}
		zs.zsl.delete(curScore, member)
	}

	zs.dict[member] = score
	zs.zsl.insert(score, member)
}

func (ds *AdvancedDataStore) ZRange(key string, start, stop int) []string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	zs, ok := ds.zsets[key]
	if !ok || zs.zsl.length == 0 {
		return nil
	}

	llen := zs.zsl.length
	if start < 0 {
		start = llen + start
	}
	if stop < 0 {
		stop = llen + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= llen {
		stop = llen - 1
	}
	if start > stop || start >= llen {
		return nil
	}

	// Range via rank traversal di SkipList O(log N + M)
	res := make([]string, 0, stop-start+1)
	node := zs.zsl.getElementByRank(start + 1)
	for i := start; i <= stop && node != nil; i++ {
		res = append(res, node.member)
		node = node.level[0].forward
	}

	return res
}

// ============================================================================
// 4. PUB/SUB BROKER SUBSYSTEM
// ============================================================================

type PubSubMessage struct {
	Channel string `json:"channel"`
	Payload []byte `json:"payload"`
}

type PubSubBroker struct {
	mu        sync.RWMutex
	channels  map[string]map[chan PubSubMessage]struct{}
	totalPubs atomic.Uint64
}

func NewPubSubBroker() *PubSubBroker {
	return &PubSubBroker{
		channels: make(map[string]map[chan PubSubMessage]struct{}),
	}
}

func (b *PubSubBroker) Subscribe(channel string) chan PubSubMessage {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan PubSubMessage, 1000)
	if b.channels[channel] == nil {
		b.channels[channel] = make(map[string]map[chan PubSubMessage]struct{})[channel]
		b.channels[channel] = make(map[chan PubSubMessage]struct{})
	}
	b.channels[channel][ch] = struct{}{}
	return ch
}

func (b *PubSubBroker) Unsubscribe(channel string, ch chan PubSubMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if subs, ok := b.channels[channel]; ok {
		delete(subs, ch)
		close(ch)
		if len(subs) == 0 {
			delete(b.channels, channel)
		}
	}
}

func (b *PubSubBroker) Publish(channel string, payload []byte) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	b.totalPubs.Add(1)
	subs, ok := b.channels[channel]
	if !ok {
		return 0
	}
	msg := PubSubMessage{Channel: channel, Payload: payload}
	count := 0
	for ch := range subs {
		select {
		case ch <- msg:
			count++
		default:
		}
	}
	return count
}

// ============================================================================
// 5. STREAMING RDB PERSISTENCE (Zero RAM Spikes + Full Type Support)
// ============================================================================

type RDBSnapshotter struct {
	ds       *AdvancedDataStore
	filePath string
}

func NewRDBSnapshotter(filePath string, ds *AdvancedDataStore) *RDBSnapshotter {
	return &RDBSnapshotter{
		ds:       ds,
		filePath: filePath,
	}
}

func (r *RDBSnapshotter) SaveRDB() error {
	r.ds.mu.RLock()
	defer r.ds.mu.RUnlock()

	tmpFile := r.filePath + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 64*1024)

	// Magic Header
	if _, err := bw.WriteString("NATARDB2"); err != nil {
		return err
	}

	// 1. Hashes
	if err := binary.Write(bw, binary.BigEndian, uint64(len(r.ds.hashes))); err != nil {
		return err
	}
	for k, m := range r.ds.hashes {
		writeString(bw, k)
		binary.Write(bw, binary.BigEndian, uint64(len(m)))
		for fk, fv := range m {
			writeString(bw, fk)
			writeBytes(bw, fv)
		}
	}

	// 2. Lists
	if err := binary.Write(bw, binary.BigEndian, uint64(len(r.ds.lists))); err != nil {
		return err
	}
	for k, deque := range r.ds.lists {
		writeString(bw, k)
		binary.Write(bw, binary.BigEndian, uint64(deque.len))
		curr := deque.head
		for curr != nil {
			writeBytes(bw, curr.val)
			curr = curr.next
		}
	}

	// 3. Sets
	if err := binary.Write(bw, binary.BigEndian, uint64(len(r.ds.sets))); err != nil {
		return err
	}
	for k, set := range r.ds.sets {
		writeString(bw, k)
		binary.Write(bw, binary.BigEndian, uint64(len(set)))
		for member := range set {
			writeString(bw, member)
		}
	}

	// 4. ZSets
	if err := binary.Write(bw, binary.BigEndian, uint64(len(r.ds.zsets))); err != nil {
		return err
	}
	for k, zs := range r.ds.zsets {
		writeString(bw, k)
		binary.Write(bw, binary.BigEndian, uint64(len(zs.dict)))
		for member, score := range zs.dict {
			writeString(bw, member)
			binary.Write(bw, binary.BigEndian, score)
		}
	}

	if err := bw.Flush(); err != nil {
		return err
	}
	_ = f.Sync()

	return os.Rename(tmpFile, r.filePath)
}

func (r *RDBSnapshotter) LoadRDB() error {
	f, err := os.Open(r.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 64*1024)
	header := make([]byte, 8)
	if _, err := io.ReadFull(br, header); err != nil || string(header) != "NATARDB2" {
		return errors.New("invalid or legacy RDB snapshot format")
	}

	r.ds.mu.Lock()
	defer r.ds.mu.Unlock()

	// 1. Hashes
	var numHashes uint64
	if err := binary.Read(br, binary.BigEndian, &numHashes); err != nil {
		return err
	}
	for i := uint64(0); i < numHashes; i++ {
		key, _ := readString(br)
		var numFields uint64
		binary.Read(br, binary.BigEndian, &numFields)
		r.ds.hashes[key] = make(map[string][]byte, numFields)
		for j := uint64(0); j < numFields; j++ {
			fk, _ := readString(br)
			fv, _ := readBytes(br)
			r.ds.hashes[key][fk] = fv
		}
	}

	// 2. Lists
	var numLists uint64
	if err := binary.Read(br, binary.BigEndian, &numLists); err != nil {
		return err
	}
	for i := uint64(0); i < numLists; i++ {
		key, _ := readString(br)
		var numElems uint64
		binary.Read(br, binary.BigEndian, &numElems)
		deque := &ByteDeque{}
		for j := uint64(0); j < numElems; j++ {
			elem, _ := readBytes(br)
			node := &listNode{val: elem}
			if deque.tail == nil {
				deque.head = node
				deque.tail = node
			} else {
				deque.tail.next = node
				node.prev = deque.tail
				deque.tail = node
			}
			deque.len++
		}
		r.ds.lists[key] = deque
	}

	// 3. Sets
	var numSets uint64
	if err := binary.Read(br, binary.BigEndian, &numSets); err != nil {
		return err
	}
	for i := uint64(0); i < numSets; i++ {
		key, _ := readString(br)
		var numMembers uint64
		binary.Read(br, binary.BigEndian, &numMembers)
		r.ds.sets[key] = make(map[string]struct{}, numMembers)
		for j := uint64(0); j < numMembers; j++ {
			mb, _ := readString(br)
			r.ds.sets[key][mb] = struct{}{}
		}
	}

	// 4. ZSets
	var numZSets uint64
	if err := binary.Read(br, binary.BigEndian, &numZSets); err != nil {
		return err
	}
	for i := uint64(0); i < numZSets; i++ {
		key, _ := readString(br)
		var numNodes uint64
		binary.Read(br, binary.BigEndian, &numNodes)
		zs := newZSetStruct()
		for j := uint64(0); j < numNodes; j++ {
			mb, _ := readString(br)
			var score float64
			binary.Read(br, binary.BigEndian, &score)
			zs.dict[mb] = score
			zs.zsl.insert(score, mb)
		}
		r.ds.zsets[key] = zs
	}

	return nil
}

// Helpers Binary Encoding
func writeString(w io.Writer, s string) {
	binary.Write(w, binary.BigEndian, uint16(len(s)))
	w.Write([]byte(s))
}

func readString(r io.Reader) (string, error) {
	var l uint16
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return "", err
	}
	b := make([]byte, l)
	_, err := io.ReadFull(r, b)
	return string(b), err
}

func writeBytes(w io.Writer, b []byte) {
	binary.Write(w, binary.BigEndian, uint32(len(b)))
	w.Write(b)
}

func readBytes(r io.Reader) ([]byte, error) {
	var l uint32
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return nil, err
	}
	b := make([]byte, l)
	_, err := io.ReadFull(r, b)
	return b, err
}
