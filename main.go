package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/caleblehner1/grafana-loki/pkg/ingester"
)

func main() {
	fmt.Println("Starting WAL Replay verification...")

	tmpDir, err := os.MkdirTemp("", "wal-main-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walFilePath := filepath.Join(tmpDir, "000001.wal")
	f, err := os.Create(walFilePath)
	if err != nil {
		log.Fatalf("failed to create WAL file: %v", err)
	}

	writer := ingester.NewWALWriter(f)
	seriesRec := ingester.WALRecord{
		Type:        ingester.RecordSeries,
		Fingerprint: 999,
	}
	seriesBytes, _ := json.Marshal(seriesRec)
	writer.WriteRecord(seriesBytes)

	chunk := ingester.Chunk{
		ID:   "chunk-main",
		From: 100,
		To:   200,
		Data: []byte("main log line"),
	}
	chunkRec := ingester.WALRecord{
		Type:        ingester.RecordChunk,
		Fingerprint: 999,
		Chunk:       &chunk,
	}
	chunkBytes, _ := json.Marshal(chunkRec)
	writer.WriteRecord(chunkBytes)
	f.Close()

	seriesMap, err := ingester.ReplayWAL(tmpDir, 0)
	if err != nil {
		log.Fatalf("failed to replay WAL: %v", err)
	}

	if s, exists := seriesMap[999]; exists && len(s.Chunks) == 1 && s.Chunks[0].ID == "chunk-main" {
		fmt.Println("WAL Replay verification successful!")
	} else {
		log.Fatalf("verification failed: seriesMap=%+v", seriesMap)
	}
}
