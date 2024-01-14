package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ParserCount int = 1
var AggregatorCount int = 1

type Aggregation struct {
	data map[string]*Measurement
	mu   sync.RWMutex
}

// intended for later use for improving performance
type AtomicFloat64 struct {
	bits uint64
}

func NewAtomicFloat64(initialValue float64) *AtomicFloat64 {
	return &AtomicFloat64{bits: math.Float64bits(initialValue)}
}

func (af *AtomicFloat64) Load() float64 {
	return math.Float64frombits(atomic.LoadUint64(&af.bits))
}

func (af *AtomicFloat64) Store(newValue float64) {
	atomic.StoreUint64(&af.bits, math.Float64bits(newValue))
}

func (af *AtomicFloat64) Add(delta float64) float64 {
	for {
		oldBits := atomic.LoadUint64(&af.bits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + delta)
		if atomic.CompareAndSwapUint64(&af.bits, oldBits, newBits) {
			return math.Float64frombits(newBits)
		}
	}
}

type Measurement struct {
	Min   float64
	Sum   float64
	Count int
	Max   float64

	mu sync.Mutex
}

type ParsedInfo struct {
	City        string
	Temperature float64
}

func main() {
	filename := os.Args[1]
	f, err := os.Open(filename)

	if len(os.Args) > 2 {
		ParserCount, _ = strconv.Atoi(os.Args[2])
	}

	if len(os.Args) > 3 {
		AggregatorCount, _ = strconv.Atoi(os.Args[3])
	}

	if err != nil {
		log.Panicln(err)
	}

	aggr := &Aggregation{
		data: make(map[string]*Measurement),
	}

	parserPipe := make(chan string, 10_000_000)
	aggPipe := make(chan ParsedInfo, 10_000_000)

	// go func(parserPipe chan string, aggPipe chan ParsedInfo) {
	// 	for {
	// 		log.Println(len(parserPipe), len(aggPipe))
	// 		time.Sleep(time.Millisecond * 500)
	// 	}

	// }(parserPipe, aggPipe)

	scanner := bufio.NewScanner(f)

	parserWg := &sync.WaitGroup{}
	for i := 0; i < ParserCount; i++ {
		parserWg.Add(1)
		go Parser(parserPipe, aggPipe, parserWg)
	}

	aggrWg := &sync.WaitGroup{}
	for i := 0; i < AggregatorCount; i++ {
		aggrWg.Add(1)
		go aggr.Aggregator(aggPipe, aggrWg)
	}

	readTime := time.Now()
	for scanner.Scan() {
		text := scanner.Text()
		parserPipe <- text
	}

	log.Println("reading file finished in", time.Since(readTime))

	//close parses and wait until parsers finished
	close(parserPipe)
	parserWg.Wait()

	//close aggrs and wait until aggregators finished
	close(aggPipe)
	aggrWg.Wait()

	log.Println("aggregated printing out data")

	out, err := os.Create("out.txt")
	if err != nil {
		log.Fatalln(err)
	}

	for k, v := range aggr.data {
		out.WriteString(fmt.Sprintf("%s:%f,%f,%d,%f\n", k, v.Min, v.Max, v.Count, v.Sum/float64(v.Count)))
	}
}

func (a *Aggregation) Aggregator(aggPipe chan ParsedInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	sum := float64(0)
	count := 0
	now := time.Now()

	for v := range aggPipe {
		now := time.Now()
		a.mu.RLock()
		measure, ok := a.data[v.City]
		a.mu.RUnlock()

		if ok {
			measure.mu.Lock()
			measure.Count += 1
			measure.Sum += v.Temperature

			if v.Temperature < measure.Min {
				measure.Min = v.Temperature
			}

			if v.Temperature > measure.Max {
				measure.Max = v.Temperature
			}
			measure.mu.Unlock()
		} else {
			a.mu.Lock()
			a.data[v.City] = &Measurement{
				Count: 1,
				Sum:   v.Temperature,
				Min:   v.Temperature,
				Max:   v.Temperature,
				mu:    sync.Mutex{},
			}
			a.mu.Unlock()
		}

		count++
		sum += time.Since(now).Seconds()
	}

	log.Println("avg duration for aggregator goroutine", sum/float64(count))
	log.Println("total execution duration for aggregator", time.Since(now))
}

func Parser(parserPipe chan string, aggrPipe chan ParsedInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	sum := float64(0)
	count := 0
	now := time.Now()

	for str := range parserPipe {
		now := time.Now()
		parts := strings.Split(str, ";")

		num, err := strconv.ParseFloat(parts[1], 64)

		if err != nil {
			log.Fatalln("unable to parse input data", str)
		}

		info := ParsedInfo{
			City:        parts[0],
			Temperature: num,
		}

		aggrPipe <- info

		count += 1
		sum += time.Since(now).Seconds()
	}

	log.Println("avg duration for parser goroutine", sum/float64(count))
	log.Println("total execution duration for parser", time.Since(now))
}
