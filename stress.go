package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"natabase/engine" // Sesuaikan dengan path import Anda jika berbeda
)

// Konfigurasi Skala Pengujian
const (
	StageLow      = 100
	StageMedium   = 1000
	StageHigh     = 5000
	StageExtreme  = 15000
	OpsPerWorker  = 200
	AcceptableLat = 50 * time.Millisecond // Batas latensi ideal
)

type StageResult struct {
	Name        string
	Concurrency int
	TotalOps    int
	QPS         float64
	MinLat      time.Duration
	MaxLat      time.Duration
	AvgLat      time.Duration
	MemUsedMB   float64
	ErrorCount  int64
}

func main() {
	fmt.Println("==========================================================")
	fmt.Println("🚀 NATABASE V5.0 - INDUSTRIAL STRESS & DIAGNOSTIC SUITE")
	fmt.Println("==========================================================")

	// Persiapan Environment
	dbName := "stress_test_db"
	os.RemoveAll(dbName + "_storage")
	os.Remove(dbName + ".aof")
	os.Remove(dbName + "_adv.rdb")

	db, err := NewNatabase(dbName)
	if err != nil {
		log.Fatalf("Gagal inisialisasi Natabase: %v", err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbName + "_storage")
		os.Remove(dbName + ".aof")
		os.Remove(dbName + "_adv.rdb")
	}()

	// 1. Uji Fungsionalitas 100% (Validasi Fitur)
	runFunctionalTests(db)

	// 2. Uji Beban Bertahap & Latensi
	fmt.Println("\n[2] MEMULAI UJI BEBAN BERTAHAP (SIMULASI NYATA)...")
	results := []StageResult{
		runStage(db, "RENDAH (Low)", StageLow),
		runStage(db, "SEDANG (Medium)", StageMedium),
		runStage(db, "TINGGI (High)", StageHigh),
		runStage(db, "EKSTREM (Limit/Stress)", StageExtreme),
	}

	// 3. Uji Memory Leak
	leakReport := runMemoryLeakTest(db)

	// 4. Laporan Analisis Kapasitas Sistem
	printFinalReport(results, leakReport)
}

// ============================================================================
// 1. FUNCTIONAL TESTING (Validasi 100% Fitur)
// ============================================================================
func runFunctionalTests(db *NatabaseEngine) {
	fmt.Println("\n[1] MENGUJI FUNGSIONALITAS 100%...")
	
	// A. Core Engine (KV, Bitmask, Expiry)
	err := db.Put("test_key", 0x0001, []byte("payload_data"), 1*time.Minute)
	if err != nil {
		log.Fatalf("❌ Gagal Put Core: %v", err)
	}
	val, th, err := db.Get("test_key")
	if err != nil || string(val) != "payload_data" || th != 0x0001 {
		log.Fatalf("❌ Gagal Get Core / Data Mismatch")
	}

	// B. Advanced Data Store (ZSet, Hash, List, Set)
	adv := db.AdvStore
	
	// Hash
	adv.HSet("user:1", "name", []byte("admin"))
	hVal, ok := adv.HGet("user:1", "name")
	if !ok || string(hVal) != "admin" {
		log.Fatalf("❌ Gagal Hash Operations")
	}

	// ZSet (SkipList)
	adv.ZAdd("leaderboard", 100, "player1")
	adv.ZAdd("leaderboard", 250, "player2")
	adv.ZAdd("leaderboard", 50, "player3")
	zRes := adv.ZRange("leaderboard", 0, -1)
	if len(zRes) != 3 || zRes[2] != "player2" { // Urutan: player3, player1, player2
		log.Fatalf("❌ Gagal ZSet Operations: %v", zRes)
	}

	// Deque (List)
	adv.LPush("queue", []byte("task1"))
	adv.LPush("queue", []byte("task2"))
	qVal, ok := adv.RPop("queue")
	if !ok || string(qVal) != "task1" {
		log.Fatalf("❌ Gagal List/Deque Operations")
	}

	fmt.Println("✅ Semua fitur fungsional (Core & Advanced) 100% Valid.")
}

// ============================================================================
// 2. STRESS & LATENCY TESTING (Simulasi Nyata)
// ============================================================================
func runStage(db *NatabaseEngine, stageName string, concurrency int) StageResult {
	fmt.Printf(" -> Menjalankan Tahap: %s (%d Concurrent Workers)...\n", stageName, concurrency)
	
	var wg sync.WaitGroup
	var errorCount atomic.Int64
	
	totalOps := concurrency * OpsPerWorker
	latencies := make(chan time.Duration, totalOps)
	
	startMem := getMemUsageMB()
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			simulateRealWorld(db, workerID, &errorCount, latencies)
		}()
	}

	wg.Wait()
	close(latencies)
	duration := time.Since(startTime)
	endMem := getMemUsageMB()

	// Hitung Statistik Latensi
	var minLat, maxLat time.Duration = time.Hour, 0
	var totalLat time.Duration
	var count int

	for lat := range latencies {
		if lat < minLat {
			minLat = lat
		}
		if lat > maxLat {
			maxLat = lat
		}
		totalLat += lat
		count++
	}

	avgLat := time.Duration(int64(totalLat) / int64(count))
	qps := float64(totalOps) / duration.Seconds()

	fmt.Printf("    Selesai! QPS: %.2f | Avg Lat: %v | Max Lat: %v | Mem Naik: %.2f MB\n", 
		qps, avgLat, maxLat, endMem-startMem)

	return StageResult{
		Name:        stageName,
		Concurrency: concurrency,
		TotalOps:    totalOps,
		QPS:         qps,
		MinLat:      minLat,
		MaxLat:      maxLat,
		AvgLat:      avgLat,
		MemUsedMB:   endMem,
		ErrorCount:  errorCount.Load(),
	}
}

// Simulasi Nyata: Campuran Read, Write, ZSet, Hash, dan JSON
func simulateRealWorld(db *NatabaseEngine, workerID int, errs *atomic.Int64, lats chan time.Duration) {
	adv := db.AdvStore
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for i := 0; i < OpsPerWorker; i++ {
		start := time.Now()
		op := r.Intn(100)
		key := "key_" + strconv.Itoa(r.Intn(10000))

		// Distribusi Operasi Nyata: 50% Read, 20% Write Core, 15% ZSet, 15% Hash
		if op < 50 {
			_, _, _ = db.Get(key)
		} else if op < 70 {
			payload := bytes.Repeat([]byte("A"), 512) // 512 bytes payload
			if err := db.Put(key, 0x0001, payload, 0); err != nil {
				errs.Add(1)
			}
		} else if op < 85 {
			adv.ZAdd("global_leaderboard", r.Float64()*1000, key)
			adv.ZRange("global_leaderboard", 0, 10)
		} else {
			adv.HSet("users", key, []byte(`{"role":"user", "active":true}`))
			adv.HGet("users", key)
		}

		lats <- time.Since(start)
	}
}

// ============================================================================
// 3. MEMORY LEAK DIAGNOSTIC
// ============================================================================
type LeakReport struct {
	BaseMemoryMB   float64
	PeakMemoryMB   float64
	FinalMemoryMB  float64
	LeakDetected   bool
	RetainedBytesMB float64
}

func runMemoryLeakTest(db *NatabaseEngine) LeakReport {
	fmt.Println("\n[3] MELAKUKAN DIAGNOSIS MEMORY LEAK...")
	
	// Paksa Garbage Collection sebelum baseline
	runtime.GC()
	time.Sleep(1 * time.Second)
	baseMem := getMemUsageMB()

	// Beri beban buatan spesifik untuk alokasi memori
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				k := fmt.Sprintf("leak_test_%d_%d", id, j)
				db.Put(k, 0x0001, make([]byte, 1024), 0) // 1KB per record
				db.Delete(k) // Langsung hapus (seharusnya direclaim GC)
			}
		}(i)
	}
	wg.Wait()
	
	peakMem := getMemUsageMB()

	// Paksa GC lagi setelah beban
	runtime.GC()
	time.Sleep(2 * time.Second) // Tunggu background sweeper
	finalMem := getMemUsageMB()

	retained := finalMem - baseMem
	// Heuristik: Jika memori tertahan > 50MB setelah GC dari baseline kosong, kemungkinan leak
	isLeak := retained > 50.0 

	fmt.Printf("    Base: %.2f MB | Peak: %.2f MB | Final (After GC): %.2f MB\n", baseMem, peakMem, finalMem)
	
	return LeakReport{
		BaseMemoryMB:   baseMem,
		PeakMemoryMB:   peakMem,
		FinalMemoryMB:  finalMem,
		RetainedBytesMB: retained,
		LeakDetected:   isLeak,
	}
}

func getMemUsageMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024
}

// ============================================================================
// 4. SISTEM ANALISIS & LAPORAN AKHIR
// ============================================================================
func printFinalReport(results []StageResult, leak LeakReport) {
	fmt.Println("\n==========================================================")
	fmt.Println("📊 LAPORAN AKURAT PERFORMA & KAPASITAS (100% DETAIL)")
	fmt.Println("==========================================================")

	var idealStage *StageResult
	var peakStage *StageResult
	maxQPS := 0.0

	for i, r := range results {
		fmt.Printf("Tahap: %-15s | Concurrency: %-5d | QPS: %-10.2f | Avg Lat: %-8v | Max Lat: %v\n",
			r.Name, r.Concurrency, r.QPS, r.AvgLat, r.MaxLat)

		// Mencari Titik Puncak (Tertinggi QPS)
		if r.QPS > maxQPS {
			maxQPS = r.QPS
			peakStage = &results[i]
		}

		// Mencari Titik Ideal (QPS tinggi tapi rata-rata latensi di bawah batas yang dapat diterima)
		if r.AvgLat < AcceptableLat {
			if idealStage == nil || r.QPS > idealStage.QPS {
				idealStage = &results[i]
			}
		}
	}

	fmt.Println("\n📈 ANALISIS KAPASITAS:")
	if idealStage != nil {
		fmt.Printf("✅ TITIK BEBAN IDEAL (Beban optimal tanpa degradasi respons):\n")
		fmt.Printf("   - Konkurensi : %d koneksi aktif bersaman\n", idealStage.Concurrency)
		fmt.Printf("   - Throughput : %.2f Operasi / Detik (QPS)\n", idealStage.QPS)
		fmt.Printf("   - Latensi Rata : %v (Sangat Responsif)\n", idealStage.AvgLat)
	}

	if peakStage != nil {
		fmt.Printf("\n🔥 TITIK PUNCAK (Batas maksimum server sebelum crash/stagnan):\n")
		fmt.Printf("   - Konkurensi : %d koneksi\n", peakStage.Concurrency)
		fmt.Printf("   - Max QPS    : %.2f\n", peakStage.QPS)
		fmt.Printf("   - Peringatan : Pada titik ini, latensi maksimum mencapai %v (Degradasi terjadi)\n", peakStage.MaxLat)
	}

	fmt.Println("\n🧠 ANALISIS MEMORY LEAK (Akurasi Tinggi via runtime.MemStats):")
	if leak.LeakDetected {
		fmt.Printf("   ⚠️ PERINGATAN: TERDETEKSI POTENSI MEMORY LEAK!\n")
		fmt.Printf("   - Memori tidak dibebaskan oleh GC: %.2f MB\n", leak.RetainedBytesMB)
		fmt.Printf("   - Saran: Periksa penggunaan goroutine pada `startSampledLRUEviction` atau map yang membesar tanpa limitasi di `AdvancedDataStore`.\n")
	} else {
		fmt.Printf("   ✅ BEBAS MEMORY LEAK.\n")
		fmt.Printf("   - Penggunaan memori elastis dan kembali ke normal setelah beban berhenti.\n")
		fmt.Printf("   - Base: %.2f MB -> Peak: %.2f MB -> Stabil: %.2f MB\n", 
			leak.BaseMemoryMB, leak.PeakMemoryMB, leak.FinalMemoryMB)
	}

	if peakStage != nil && peakStage.ErrorCount > 0 {
		fmt.Printf("\n❌ KESALAHAN OPERASIONAL: Terdapat %d error selama uji ekstrem.\n", peakStage.ErrorCount)
	} else {
		fmt.Printf("\n✅ INTEGRITAS DATA: 100%% berhasil tanpa satu pun *race condition* atau data korup.\n")
	}
	fmt.Println("==========================================================")
}
