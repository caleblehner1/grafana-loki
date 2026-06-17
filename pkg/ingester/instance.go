package ingester

import (
	"sync"
)

type Ingester struct {
	mu              sync.RWMutex
	walDir          string
	lastFlushedTime int64
	Series          map[uint64]*Series
}

func NewIngester(walDir string, lastFlushedTime int64) *Ingester {
	return &Ingester{
		walDir:          walDir,
		lastFlushedTime: lastFlushedTime,
		Series:          make(map[uint64]*Series),
	}
}

func (ing *Ingester) Recover() error {
	ing.mu.Lock()
	defer ing.mu.Unlock()

	series, err := ReplayWAL(ing.walDir, ing.lastFlushedTime)
	if err != nil {
		return err
	}
	ing.Series = series
	return nil
}
