package timeseries

import (
	"container/ring"
	"sort"
	"sync/atomic"
	"time"
)

type timeSeries struct {
	resolution time.Duration
	values     *ring.Ring
	ticker     *time.Ticker
}

type TimeSeriesMetric struct {
	timeSeries
}

type TimeSeriesTopK struct {
	timeSeries
}

type Tuple struct {
	Key   string
	Value int64
}

type Tuples []Tuple

func (tps Tuples) Len() int {
	return len(tps)
}

func (tps Tuples) Swap(i, j int) {
	tps[i], tps[j] = tps[j], tps[i]
}

func (tps Tuples) Less(i, j int) bool {
	return tps[i].Value < tps[j].Value
}

func (tsm *timeSeries) Stop() {
	tsm.ticker.Stop()
}

func NewTimeSeriesMetric(resolution time.Duration, points int) *TimeSeriesMetric {
	tsm := &TimeSeriesMetric{}
	tsm.values = ring.New(points + 1) // add one that can act as a buffer so we don't have a race condition
	tsm.ticker = time.NewTicker(resolution)
	tsm.values.Value = new(int64)
	go func() {
		for _ = range tsm.ticker.C {
			tsm.tick()
		}
	}()
	return tsm
}

func (tsm *TimeSeriesMetric) tick() {
	switch v := tsm.values.Next().Value.(type) {
	case *int64:
		*v = 0
	default:
		tsm.values.Next().Value = new(int64)
	}
	tsm.values = tsm.values.Next()
}

func (tsm *TimeSeriesMetric) AddInt64(delta int64) {
	atomic.AddInt64(tsm.values.Value.(*int64), delta)
}

func (tsm *TimeSeriesMetric) PastN(points int) []int64 {
	if points > (tsm.values.Len() - 1) {
		panic("Requested more points than available.")
	}
	r := make([]int64, points)
	for i, n := 0, tsm.values; i < points; i, n = i+1, n.Prev() {
		switch v := n.Value.(type) {
		case *int64:
			r[i] = *v
		default:
			r[i] = int64(0)
		}
	}
	return r
}

func (tsm *TimeSeriesMetric) SumPastN(points int) int64 {
	if points > (tsm.values.Len() - 1) {
		panic("Requested more points than available.")
	}
	r := int64(0)
	for i, n := 0, tsm.values; i < points; i, n = i+1, n.Prev() {
		switch v := n.Value.(type) {
		case *int64:
			r += *v
		}
	}
	return r
}

func NewTimeSeriesTopK(resolution time.Duration, points int) *TimeSeriesTopK {
	tsm := &TimeSeriesTopK{}
	tsm.values = ring.New(points + 1) // add one that can act as a buffer so we don't have a race condition
	tsm.ticker = time.NewTicker(resolution)
	tsm.values.Value = make(map[string]Tuple)
	go func() {
		for _ = range tsm.ticker.C {
			tsm.tick()
		}
	}()
	return tsm
}

func (tsm *TimeSeriesTopK) tick() {
	tsm.values.Next().Value = make(map[string]Tuple)
	tsm.values = tsm.values.Next()
}

func (tsm *TimeSeriesTopK) GetCurr(key string) int64 {
	m := tsm.values.Value.(map[string]Tuple)
	return m[key].Value
}

func (tsm *TimeSeriesTopK) AddInt64(key string, delta int64) {
	m := tsm.values.Value.(map[string]Tuple)
	if v, ok := m[key]; ok {
		m[key] = Tuple{Key: key, Value: v.Value + delta}
	} else {
		m[key] = Tuple{Key: key, Value: delta}
	}
}

func (tsm *TimeSeriesTopK) PastN(points int) [][]Tuple {
	if points > (tsm.values.Len() - 1) {
		panic("Requested more points than available.")
	}
	r := make([][]Tuple, points)
	for i, n := 0, tsm.values; i < points; i, n = i+1, n.Prev() {
		switch v := n.Value.(type) {
		case map[string]Tuple:
			tuples := make([]Tuple, len(v))
			j := 0
			for _, val := range v {
				tuples[j] = val
				j++
			}
			sort.Sort(sort.Reverse(Tuples(tuples)))
			r[i] = tuples
		default:
			r[i] = []Tuple{}
		}
	}
	return r
}

func (tsm *TimeSeriesTopK) SumPastN(points int) []Tuple {
	if points > (tsm.values.Len() - 1) {
		panic("Requested more points than available.")
	}
	master := make(map[string]Tuple)
	for i, n := 0, tsm.values; i < points; i, n = i+1, n.Prev() {
		switch rv := n.Value.(type) {
		case map[string]Tuple:
			for k, v := range rv {
				if _, ok := master[k]; ok {
					v.Value += master[k].Value
					master[k] = v
				} else {
					master[k] = v
				}
			}
		}
	}
	r := make([]Tuple, len(master))
	i := 0
	for _, val := range master {
		r[i] = val
		i++
	}
	sort.Sort(sort.Reverse(Tuples(r)))
	return r
}
