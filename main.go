package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	dsn, query, duration := parseFlags()

	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	start, end := calculateTime(duration)

	totalRequests := runBenchmarkQueries(start, end, query, db)

	rps := rpsCount(start, totalRequests)

	displayBenchmarkResult(totalRequests, rps)
}

func parseFlags() (*string, *string, *int) {
	dsn := flag.String("dsn", "user=postgres dbname=postgres password=qwerty sslmode=disable", "PostgreSQL DSN")
	query := flag.String("query", "SELECT * FROM users", "Query to benchmark")
	duration := flag.Int("duration", 10, "Duration of the benchmark in milliseconds")
	flag.Parse()

	return dsn, query, duration
}

func calculateTime(duration *int) (time.Time, time.Time) {
	return time.Now(), time.Now().Add(time.Duration(*duration) * time.Millisecond)
}

func runBenchmarkQueries(start time.Time, end time.Time, query *string, db *sql.DB) int {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	var totalRequests int

	for start.Before(end) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			row, err := db.Query(*query)
			if err != nil {
				log.Println(err)
				return
			}
			defer row.Close()

			mutex.Lock()
			totalRequests++
			mutex.Unlock()
		}()
	}
	wg.Wait()

	return totalRequests
}

func rpsCount(start time.Time, totalRequests int) float64 {
	elapsedTime := time.Since(start).Seconds()
	return float64(totalRequests) / elapsedTime
}

func displayBenchmarkResult(totalRequests int, rps float64) {
	fmt.Printf("Total Requests: %d\n RPS: %.2f\n", totalRequests, rps)
}
