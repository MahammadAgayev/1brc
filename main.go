package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

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
	Min   int
	Sum   int
	Count int
	Max   int

	mu sync.Mutex
}

type ParsedInfo struct {
	City        string
	Temperature int
}

func main() {
	filename := os.Args[1]
	f, err := os.Open(filename)

	if err != nil {
		log.Panicln(err)
	}

	aggr := &Aggregation{
		data: make(map[string]*Measurement),
	}

	aggPipe := make(chan *ParsedInfo, 10_000_000)

	// go func(parserPipe chan string, aggPipe chan ParsedInfo) {
	// 	for {
	// 		log.Println(len(parserPipe), len(aggPipe))
	// 		time.Sleep(time.Millisecond * 500)
	// 	}
	// }(parserPipe, aggPipe)

	reader := bufio.NewReader(f)

	now := time.Now()
	parserWg := &sync.WaitGroup{}
	parserWg.Add(1)
	go AnotherParser(reader, aggPipe, parserWg)

	aggrWg := &sync.WaitGroup{}
	aggrWg.Add(1)
	go aggr.Aggregator(aggPipe, aggrWg)

	parserWg.Wait()
	log.Println("parser finished", time.Since(now))

	close(aggPipe)
	aggrWg.Wait()

	out, err := os.Create("out.txt")
	if err != nil {
		log.Fatalln(err)
	}

	for k, v := range aggr.data {
		out.WriteString(fmt.Sprintf("%s:%f,%f,%d,%f\n", k, float64(v.Min/10.0), float64(v.Max/10.0), v.Count, float64(v.Sum/v.Count)/10.0))
	}
}

func (a *Aggregation) Aggregator(aggPipe chan *ParsedInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	now := time.Now()

	for v := range aggPipe {
		measure, ok := a.data[v.City]

		if ok {
			measure.Count += 1
			measure.Sum += v.Temperature

			if v.Temperature < measure.Min {
				measure.Min = v.Temperature
			}

			if v.Temperature > measure.Max {
				measure.Max = v.Temperature
			}
		} else {
			a.data[v.City] = &Measurement{
				Count: 1,
				Sum:   v.Temperature,
				Min:   v.Temperature,
				Max:   v.Temperature,
				mu:    sync.Mutex{},
			}
		}
	}

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

		num, err := strconv.Atoi(parts[1])

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

func AnotherParser(reader *bufio.Reader, aggrPipe chan *ParsedInfo, wg *sync.WaitGroup) error {
	defer wg.Done()

	for {
		slice, err := reader.ReadSlice('\n')

		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		idxSemicolon := bytes.Index(slice, []byte{0x3B})

		info := &ParsedInfo{
			City:        string(slice[:idxSemicolon]),
			Temperature: ParseInt(slice[idxSemicolon:]),
		}

		aggrPipe <- info
	}
}

func ParseInt(slice []byte) int {
	var sign int

	if slice[0] == '-' {
		slice = slice[1:]
		sign = -1
	} else {
		sign = 1
	}

	if slice[1] == '.' {
		return (int(slice[0])*10 + int(slice[2]) - int('0')*11) * sign
	}

	return (int(slice[0])*100 + int(slice[1])*10 + int(slice[3]) - int('0')*111) * sign
}
