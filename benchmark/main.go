package main

import (
"bytes"
"encoding/json"
"fmt"
"log"
"math/rand"
"sort"
"sync"
"sync/atomic"
"time"

"github.com/valyala/fasthttp"
)

const (
BaseURL  = "http://localhost:8080"
AdminUsr = "admin"
AdminPwd = "admin123"
)

var (
client = &fasthttp.Client{
MaxConnsPerHost: 50000,
ReadTimeout:     10 * time.Second,
WriteTimeout:    10 * time.Second,
}
jwtToken string
)

type TestResult struct {
Name        string
TotalReqs   int
Success     int
Failed      int
Duration    time.Duration
TPS         float64
Latencies   []float64
}

func main() {
fmt.Println("==================================================")
fmt.Println("🚀 NATABASE V5.0 ULTRA-BENCHMARK SUITE")
fmt.Println("==================================================")

authenticate()

fmt.Println("\n📊 [METRIK AWAL SERVER]")
printServerMetrics()

fmt.Println("\n🔥 [PHASE 1] Basic KV PUT/GET - Low Concurrency (100 Workers)")
res1 := runLoadTest("Phase 1 (100x1000)", 100, 1000, "kv")
printReport(res1)

fmt.Println("\n🔥 [PHASE 2] Basic KV PUT/GET - High Concurrency (1000 Workers)")
res2 := runLoadTest("Phase 2 (1000x1000)", 1000, 1000, "kv")
printReport(res2)

fmt.Println("\n🔥 [PHASE 3] Batch Operations - Extreme Payload")
res3 := runLoadTest("Phase 3 (Batch Inserts)", 50, 100, "batch")
printReport(res3)

fmt.Println("\n📊 [METRIK AKHIR SERVER (Batas Performa)]")
printServerMetrics()
}

func authenticate() {
req := fasthttp.AcquireRequest()
res := fasthttp.AcquireResponse()
defer fasthttp.ReleaseRequest(req)
defer fasthttp.ReleaseResponse(res)

req.SetRequestURI(BaseURL + "/api/auth/login")
req.Header.SetMethod("POST")
req.Header.SetContentType("application/json")
req.SetBodyString(fmt.Sprintf(`{"username":"%s","password":"%s"}`, AdminUsr, AdminPwd))

if err := client.Do(req, res); err != nil {
log.Fatalf("Gagal terhubung ke server: %v", err)
}

var data map[string]interface{}
if err := json.Unmarshal(res.Body(), &data); err != nil {
log.Fatalf("Gagal parse auth response: %v", err)
}

if token, ok := data["token"].(string); ok {
jwtToken = "Bearer " + token
fmt.Println("✅ Autentikasi Berhasil. Token JWT didapatkan.")
} else {
log.Fatalf("Gagal mendapatkan token: %s", string(res.Body()))
}
}

func runLoadTest(name string, concurrency int, reqsPerWorker int, mode string) TestResult {
var wg sync.WaitGroup
var success, failed atomic.Int32

latencies := make(chan float64, concurrency*reqsPerWorker)

start := time.Now()

for i := 0; i < concurrency; i++ {
wg.Add(1)
go func(workerID int) {
defer wg.Done()
req := fasthttp.AcquireRequest()
res := fasthttp.AcquireResponse()
defer fasthttp.ReleaseRequest(req)
defer fasthttp.ReleaseResponse(res)

for j := 0; j < reqsPerWorker; j++ {
req.Header.Set("Authorization", jwtToken)
req.Header.Set("X-Stress-Test", "true")

var isSuccess bool
reqStart := time.Now()

if mode == "kv" {
key := fmt.Sprintf("bench_key_%d_%d", workerID, rand.Intn(10000))
req.SetRequestURI(BaseURL + "/api/v1/data?key=" + key)

if rand.Intn(100) < 30 {
req.Header.SetMethod("POST")
req.SetBodyString(`{"data": "tes_performa_tinggi_1234567890"}`)
} else {
req.Header.SetMethod("GET")
}
} else if mode == "batch" {
req.SetRequestURI(BaseURL + "/api/v1/batch")
req.Header.SetMethod("POST")
req.SetBodyString(fmt.Sprintf(`{"batch_key_%d_1":"val1", "batch_key_%d_2":"val2", "batch_key_%d_3":"val3"}`, workerID, workerID, workerID))
}

if err := client.Do(req, res); err == nil {
status := res.StatusCode()
if status == 200 || status == 201 || status == 404 {
success.Add(1)
isSuccess = true
} else {
failed.Add(1)
}
} else {
failed.Add(1)
}

if isSuccess {
latencies <- time.Since(reqStart).Seconds() * 1000
}
}
}(i)
}

wg.Wait()
close(latencies)
duration := time.Since(start)

var latArr []float64
for l := range latencies {
latArr = append(latArr, l)
}

return TestResult{
Name:      name,
TotalReqs: concurrency * reqsPerWorker,
Success:   int(success.Load()),
Failed:    int(failed.Load()),
Duration:  duration,
TPS:       float64(success.Load()) / duration.Seconds(),
Latencies: latArr,
}
}

func printReport(res TestResult) {
fmt.Printf("--- Hasil: %s ---\n", res.Name)
fmt.Printf("Total Requests : %d\n", res.TotalReqs)
fmt.Printf("Sukses         : %d\n", res.Success)
fmt.Printf("Gagal          : %d\n", res.Failed)
fmt.Printf("Durasi         : %v\n", res.Duration)
fmt.Printf("Throughput     : %.2f TPS (Trans/sec)\n", res.TPS)

if len(res.Latencies) > 0 {
sort.Float64s(res.Latencies)
p50 := res.Latencies[len(res.Latencies)/2]
p90 := res.Latencies[int(float64(len(res.Latencies))*0.90)]
p99 := res.Latencies[int(float64(len(res.Latencies))*0.99)]

fmt.Printf("Latensi p50    : %.2f ms\n", p50)
fmt.Printf("Latensi p90    : %.2f ms\n", p90)
fmt.Printf("Latensi p99    : %.2f ms\n", p99)
}
fmt.Println("--------------------------------------------------")
}

func printServerMetrics() {
req := fasthttp.AcquireRequest()
res := fasthttp.AcquireResponse()
defer fasthttp.ReleaseRequest(req)
defer fasthttp.ReleaseResponse(res)

req.SetRequestURI(BaseURL + "/api/v1/metrics")
req.Header.SetMethod("GET")
req.Header.Set("Authorization", jwtToken)

if err := client.Do(req, res); err != nil {
fmt.Printf("Gagal mengambil metrik server: %v\n", err)
return
}

var metrics map[string]interface{}
if err := json.Unmarshal(res.Body(), &metrics); err != nil {
fmt.Println("Gagal parse metrik")
return
}

b, _ := json.MarshalIndent(metrics, "", "  ")
fmt.Println(string(b))
}
