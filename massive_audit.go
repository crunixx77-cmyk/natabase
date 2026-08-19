package main

import (
"context"
"fmt"
"log"
"math/rand"
"os"
"runtime"
"runtime/pprof"
"sort"
"sync"
"sync/atomic"
"text/tabwriter"
"time"
)

type DataStore interface {
ZAdd(key string, score float64, member string)
HSet(hash, key string, val []byte)
Get(key string) []byte
}

type MockDataStore struct {
mu sync.RWMutex
m  map[string]map[string][]byte
}

func (m *MockDataStore) ZAdd(key string, score float64, member string) {
m.mu.Lock()
defer m.mu.Unlock()
time.Sleep(time.Microsecond * time.Duration(rand.Intn(5)))
}
func (m *MockDataStore) HSet(hash, key string, val []byte) {
m.mu.Lock()
defer m.mu.Unlock()
}
func (m *MockDataStore) Get(key string) []byte {
m.mu.RLock()
defer m.mu.RUnlock()
return []byte("data")
}

type SQLEngine interface {
Execute(query string, args ...interface{}) error
ExecuteTx(queries []string) error
}

type MockSQLEngine struct{}

func (m *MockSQLEngine) Execute(query string, args ...interface{}) error {
time.Sleep(time.Microsecond * time.Duration(rand.Intn(10)))
return nil
}
func (m *MockSQLEngine) ExecuteTx(queries []string) error {
time.Sleep(time.Microsecond * time.Duration(len(queries)*15))
return nil
}

type AuditMetrics struct {
TotalOps      uint64
SuccessfulOps uint64
FailedOps     uint64
Latencies     []time.Duration
LatenciesMu   sync.Mutex
StartTime     time.Time
EndTime       time.Time
MaxMemoryMB   uint64
GCPausesTotal uint64
}

func (a *AuditMetrics) RecordLatency(d time.Duration) {
a.LatenciesMu.Lock()
a.Latencies = append(a.Latencies, d)
a.LatenciesMu.Unlock()
}

func (a *AuditMetrics) GenerateReport() {
a.LatenciesMu.Lock()
defer a.LatenciesMu.Unlock()

sort.Slice(a.Latencies, func(i, j int) bool {
return a.Latencies[i] < a.Latencies[j]
})

totalLen := len(a.Latencies)
var p50, p90, p95, p99, max time.Duration
if totalLen > 0 {
p50 = a.Latencies[totalLen*50/100]
p90 = a.Latencies[totalLen*90/100]
p95 = a.Latencies[totalLen*95/100]
p99 = a.Latencies[totalLen*99/100]
max = a.Latencies[totalLen-1]
}

duration := a.EndTime.Sub(a.StartTime)
throughput := float64(a.TotalOps) / duration.Seconds()

fmt.Println("\n===============================================================================")
fmt.Println("                      SYSTEM AUDIT & LOAD TEST REPORT                            ")
fmt.Println("===============================================================================")
w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight|tabwriter.Debug)

fmt.Fprintln(w, "METRIC\tVALUE\t")
fmt.Fprintln(w, "------\t-----\t")
fmt.Fprintf(w, "Total Execution Time\t%v\n", duration)
fmt.Fprintf(w, "Total Operations\t%d\n", a.TotalOps)
fmt.Fprintf(w, "Successful Ops\t%d\n", a.SuccessfulOps)
fmt.Fprintf(w, "Failed Ops\t%d\n", a.FailedOps)
fmt.Fprintf(w, "Throughput\t%.2f ops/sec\n", throughput)
fmt.Fprintf(w, "Peak Memory Usage\t%d MB\n", a.MaxMemoryMB)
fmt.Fprintf(w, "Total GC Pauses\t%d ns\n", a.GCPausesTotal)

fmt.Fprintln(w, "------\t-----\t")
fmt.Fprintln(w, "LATENCY PERCENTILES\t\t")
fmt.Fprintf(w, "P50 (Median)\t%v\n", p50)
fmt.Fprintf(w, "P90\t%v\n", p90)
fmt.Fprintf(w, "P95\t%v\n", p95)
fmt.Fprintf(w, "P99\t%v\n", p99)
fmt.Fprintf(w, "Max Latency\t%v\n", max)

w.Flush()
fmt.Println("===============================================================================\n")
}

func runMemoryMonitor(ctx context.Context, metrics *AuditMetrics) {
var m runtime.MemStats
ticker := time.NewTicker(500 * time.Millisecond)
defer ticker.Stop()

for {
select {
case <-ctx.Done():
return
case <-ticker.C:
runtime.ReadMemStats(&m)
allocMB := m.Alloc / 1024 / 1024
if allocMB > metrics.MaxMemoryMB {
atomic.StoreUint64(&metrics.MaxMemoryMB, allocMB)
}
atomic.StoreUint64(&metrics.GCPausesTotal, m.PauseTotalNs)
}
}
}

func main() {
fmt.Println("Initializing Massive Scale Audit Test...")

f, err := os.Create("cpu_audit.prof")
if err != nil {
log.Fatal(err)
}
pprof.StartCPUProfile(f)
defer pprof.StopCPUProfile()

metrics := &AuditMetrics{
Latencies: make([]time.Duration, 0, 500000),
}

ds := &MockDataStore{m: make(map[string]map[string][]byte)}
sqlEng := &MockSQLEngine{}

concurrency := 2000
opsPerWorker := 250

var wg sync.WaitGroup
ctx, cancel := context.WithCancel(context.Background())

go runMemoryMonitor(ctx, metrics)

metrics.StartTime = time.Now()

for i := 0; i < concurrency; i++ {
wg.Add(1)
go func(workerID int) {
defer wg.Done()

for j := 0; j < opsPerWorker; j++ {
start := time.Now()
opType := rand.Intn(100)

var err error
if opType < 40 {
ds.ZAdd(fmt.Sprintf("zset_%d", workerID%10), rand.Float64()*100, fmt.Sprintf("member_%d", j))
} else if opType < 70 {
ds.Get(fmt.Sprintf("key_%d", j))
} else if opType < 90 {
err = sqlEng.Execute("SELECT * FROM users WHERE id = ?", j)
} else {
queries := []string{
"BEGIN",
fmt.Sprintf("UPDATE accounts SET balance = balance - 100 WHERE id = %d", workerID),
fmt.Sprintf("UPDATE accounts SET balance = balance + 100 WHERE id = %d", workerID+1),
"COMMIT",
}
err = sqlEng.ExecuteTx(queries)
}

latency := time.Since(start)
metrics.RecordLatency(latency)
atomic.AddUint64(&metrics.TotalOps, 1)

if err != nil {
atomic.AddUint64(&metrics.FailedOps, 1)
} else {
atomic.AddUint64(&metrics.SuccessfulOps, 1)
}
}
}(i)
}

wg.Wait()
metrics.EndTime = time.Now()
cancel()

metrics.GenerateReport()

fmt.Println("Audit complete. CPU profile saved to 'cpu_audit.prof'")
}
