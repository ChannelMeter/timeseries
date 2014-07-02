package timeseries

import (
	"testing"
	"time"
)

func TestTimeSeriesMetric(t *testing.T) {
	tsm := NewTimeSeriesMetric(100*time.Millisecond, 16)
	for i := 0; i < 16; i++ {
		tsm.AddInt64(int64(i + 1))
		time.Sleep(100 * time.Millisecond)
	}
	tsm.Stop()
	data := tsm.PastN(16)
	for i, n := range data {
		if n != (17-int64(i))%17 {
			t.Errorf("expected %d, got %d.", (17-int64(i))%17, n)
		}
	}
}

func TestTimeSeriesTopK(t *testing.T) {
	tsm := NewTimeSeriesTopK(100*time.Millisecond, 16)
	names := []string{"Mark", "Anderson", "Jamie", "Paul", "Remy", "Eren", "Annie", "Sasha", "Zoe"}
	for i := 0; i < 16; i++ {
		for _, n := range names {
			tsm.AddInt64(n, int64(len(n)))
		}
		time.Sleep(100 * time.Millisecond)
	}
	tsm.Stop()
	data := tsm.SumPastN(16)
	if data[0].Key != "Anderson" {
		t.Errorf("expected %s, got %s.", "Anderson", data[0].Key)
	}
}
