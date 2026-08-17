package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/valyala/fasthttp"
)

const (
	BaseURL    = "http://localhost:8080"
	AdminUsr   = "admin"
	AdminPwd   = "admin123"
	KeySpace   = 100000 // Total 100.000 unique keys
	ZipfTheta = 0.8    // Parameter kemiringan distribusi Zipf (80/20 rule)
)

var (
	client = &fasthttp.Client{
		MaxConnsPerHost: 100000,
		ReadTimeout:     15 * time.Second,
		WriteTimeout:    15 * time.Second,
	}
	jwtToken string
	zipfGen  *rand.Zipf
)

type TestResult struct {
	Name      string
	TotalReqs int
	Success   int
	Failed    int
	Duration  time.Duration
	TPS       float64
	Histogram *hdrhistogram.Histogram
}

func main() {
	// Inisialisasi Zipf Generator untuk simulasi Hot Keys (80/20 Rule)
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	zipfGen = rand.NewZipf(r, 1.1, 1.0, KeySpace-1)

	fmt.Println("==================================================")
	fmt.Println("🚀 NATABASE V5.0 ADVANCED BENCHMARK SUITE")
	fmt.Println("==================================================")

	// 1. Autentikasi JWT
	authenticate()

	// 2. Metrik Awal Server
	fmt.Println("\n📊 [METRIK AWAL SERVER]")
	printServerMetrics()

	// 3. WARM-UP PHASE (Pemanasan Connection Pool & JIT Optimization)
	fmt.Println("\n☕ [PHASE 0] Warm-Up Phase (10.000 Requests)")
	res0 := runLoadTest("Phase 0 (Warm-up)", 50, 200, "kv_uniform")
	printReport(res0)

	// 4. DATA SEEDING PHASE (Mengisi 50.000 data awal agar GET tidak 404)
	fmt.Println("\n🌱 [PHASE 1] Data Seeding Phase (Populating 50.000 Keys)")
	seedData(50000)

	// 5. ZIPFIAN DISTRIBUTED KV TEST (Mengecek efisiensi cache & lock contention)
	fmt.Println("\n🔥 [PHASE 2] Realistic KV Traffic (Zipfian 80/20 Hot Keys - 500 Workers)")
	res2 := runLoadTest("Phase 2 (Zipfian KV)", 500, 2000, "kv_zipfian")
	printReport(res2)

	// 6. READ AFTER WRITE / INTEGRITY TEST (Validasi konsistensi data)
	fmt.Println("\n🧪 [PHASE 3] Read-After-Write Consistency Verification")
	verifyReadAfterWrite(1000)

	// 7. HIGH CONCURRENCY STRESS TEST (Beban Lebih Berat: 2.000 Workers)
	fmt.Println("\n💥 [PHASE 4] Extreme Stress Test (2.000 Concurrency x 1.000 Reqs)")
	res4 := runLoadTest("Phase 4 (Extreme Load)", 2000, 1000, "kv_zipfian")
	printReport(res4)

	// 8. EXTREME BATCH INSERTS (Beban Payload Berat)
	fmt.Println("\n📦 [PHASE 5] Heavy Batch Operations (200 Workers x 100 Batch Reqs)")
	res5 := runLoadTest("Phase 5 (Heavy Batch)", 200, 100, "batch")
	printReport(res5)

	// 9. Metrik Akhir Server
	fmt.Println("\n📊 [METRIK AKHIR SERVER]")
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

func seedData(totalKeys int) {
	var wg sync.WaitGroup
	workers := 100
	keysPerWorker := totalKeys / workers
	start := time.Now()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			req := fasthttp.AcquireRequest()
			res := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
 defer fasthttp.ReleaseResponse(res)

			for j := 0; j < keysPerWorker; j++ {
				keyID := workerID*keysPerWorker + j
				req.Header.Set("Authorization", jwtToken)
				req.Header.Set("X-Stress-Test", "true")
				req.SetRequestURI(fmt.Sprintf("%s/api/v1/data?key=bench_key_%d", BaseURL, keyID))
				req.Header.SetMethod("POST")
				req.SetBodyString(`{"data": "seeded_initial_value_for_testing"}`)
				_ = client.Do(req, res)
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("✅ Seeding selesai. %d keys ditambahkan dalam %v\n", totalKeys, time.Since(start))
}

func verifyReadAfterWrite(samples int) {
	successCount := 0
	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)

	for i := 0; i < samples; i++ {
		key := fmt.Sprintf("raw_key_%d", i)
		val := fmt.Sprintf("value_payload_%d_%d", i, rand.Intn(99999))

		// 1. Write
		req.Header.Set("Authorization", jwtToken)
		req.Header.Set("X-Stress-Test", "true")
		req.SetRequestURI(BaseURL + "/api/v1/data?key=" + key)
		req.Header.SetMethod("POST")
		req.SetBodyString(fmt.Sprintf(`{"data": "%s"}`, val))
		_ = client.Do(req, res)

		// 2. Immediate Read
		req.Header.SetMethod("GET")
		if err := client.Do(req, res); err == nil && res.StatusCode() == 200 {
			var resp map[string]interface{}
			if err := json.Unmarshal(res.Body(), &resp); err == nil {
				if resp["data"] == val {
					successCount++
				}
			}
		}
	}
	fmt.Printf("✅ Verification Complete: %d/%d data konsisten (Read-after-Write Rate: %.2f%%)\n",
		successCount, samples, float64(successCount)/float64(samples)*100)
}

func runLoadTest(name string, concurrency int, reqsPerWorker int, mode string) TestResult {
	var wg sync.WaitGroup
	var success, failed atomic.Int32

	// HDR Histogram: Mengukur latensi 1 mikrodetik hingga 1 menit tanpa membengkakkan memori
	hist := hdrhistogram.New(1, 60000000, 3) 
	var histLock sync.Mutex

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			req := fasthttp.AcquireRequest()
			res := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(res)

			localHist := hdrhistogram.New(1, 60000000, 3)

			for j := 0; j < reqsPerWorker; j++ {
				req.Header.Set("Authorization", jwtToken)
				req.Header.Set("X-Stress-Test", "true")

				var isSuccess bool
				reqStart := time.Now()

				switch mode {
				case "kv_uniform":
					key := fmt.Sprintf("bench_key_%d", rand.Intn(KeySpace))
					req.SetRequestURI(BaseURL + "/api/v1/data?key=" + key)
					if rand.Intn(100) < 30 {
						req.Header.SetMethod("POST")
						req.SetBodyString(`{"data": "uniform_payload_value"}`)
					} else {
						req.Header.SetMethod("GET")
					}
				case "kv_zipfian":
					keyID := zipfGen.Uint64()
					key := fmt.Sprintf("bench_key_%d", keyID)
					req.SetRequestURI(BaseURL + "/api/v1/data?key=" + key)
					if rand.Intn(100) < 20 { // 20% Write, 80% Read (Real-world OLTP workload)
						req.Header.SetMethod("POST")
						req.SetBodyString(`{"data": "zipfian_payload_heavy_data"}`)
					} else {
						req.Header.SetMethod("GET")
					}
				case "batch":
					req.SetRequestURI(BaseURL + "/api/v1/batch")
					req.Header.SetMethod("POST")
					req.SetBodyString(fmt.Sprintf(`{
						"b_key_%d_1":"val1_extreme_heavy_payload_string",
						"b_key_%d_2":"val2_extreme_heavy_payload_string",
						"b_key_%d_3":"val3_extreme_heavy_payload_string",
						"b_key_%d_4":"val4_extreme_heavy_payload_string"
					}`, workerID, workerID, workerID, workerID))
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
					latMicros := time.Since(reqStart).Microseconds()
					_ = localHist.RecordValue(latMicros)
				}
			}

			histLock.Lock()
			hist.Merge(localHist)
			histLock.Unlock()
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	return TestResult{
		Name:      name,
		TotalReqs: concurrency * reqsPerWorker,
		Success:   int(success.Load()),
		Failed:    int(failed.Load()),
		Duration:  duration,
		TPS:       float64(success.Load()) / duration.Seconds(),
		Histogram: hist,
	}
}

func printReport(res TestResult) {
	fmt.Printf("--- Hasil: %s ---\n", res.Name)
	fmt.Printf("Total Requests : %d\n", res.TotalReqs)
	fmt.Printf("Sukses         : %d\n", res.Success)
	fmt.Printf("Gagal          : %d\n", res.Failed)
	fmt.Printf("Durasi         : %v\n", res.Duration)
	fmt.Printf("Throughput     : %.2f TPS (Trans/sec)\n", res.TPS)

	if res.Histogram.TotalCount() > 0 {
		p50 := float64(res.Histogram.ValueAtQuantile(50.0)) / 1000.0
		p90 := float64(res.Histogram.ValueAtQuantile(90.0)) / 1000.0
		p99 := float64(res.Histogram.ValueAtQuantile(99.0)) / 1000.0

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
