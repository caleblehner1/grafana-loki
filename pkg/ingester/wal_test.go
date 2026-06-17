package ingester

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWALReplay_CorruptionHandling(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		tot.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	walFilePath := filepath.Join(tmpDir, "000001.wal")
	f, err := os.Create(walFilePath)
	if err != nil {
		t.Fatal(err)
	}

	writer := NewWALWriter(f)

	seriesRec := WALRecord{
		Type:        RecordSeries,
		Fingerprint: 12345,
	}
	seriesBytes, _ := json.Marshal(seriesRec)
	if err := writer.WriteRecord(seriesBytes); err != nil {
		t.Fatal(err)
	}

	chunk1 := Chunk{
		ID:   "chunk-1",
		From: 100,
		To:   200,
		Data: []byte("log line 1\n"),
	}
	chunkRec1 := WALRecord{
		Type:        RecordChunk,
		Fingerprint: 12345,
		Chunk:       &chunk1,
	}
	chunkBytes1, _ := json.Marshal(chunkRec1)
	if err := writer.WriteRecord(chunkBytes1); err != nil {
		t.Fatal(err)
	}

	chunk2 := Chunk{
		ID:   "chunk-2",
		From: 201,
		To:   300,
		Data: []byte("log line 2\n"),
	}
	chunkRec2 := WALRecord{
		Type:        RecordChunk,
		Fingerprint: 12345,
		Chunk:       &chunk2,
	}
	chunkBytes2, _ := json.Marshal(chunkRec2)
	if err := writer.WriteRecord(chunkBytes2); err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0x00, 0x00, 0x00, 0x10}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	seriesMap, err := ReplayWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	s, exists := seriesMap[12345]
	if !exists {
		t.Fatal("expected series 12345 to be recovered")
	}

	if len(s.Chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(s.Chunks))
	}

	if s.Chunks[0].ID != "chunk-1" || s.Chunks[1].ID != "chunk-2" {
		t.Errorf("unexpected chunks recovered: %+v", s.Chunks)
	}
}

func TestWALReplay_CheckpointBoundary(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	walFilePath := filepath.Join(tmpDir, "000001.wal")
	f, err := os.Create(walFilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	writer := NewWALWriter(f)
	lastFlushedTime := int64(150)

	chunk1 := Chunk{
		ID:   "chunk-1",
		From: 50,
		To:   150,
		Data: []byte("flushed"),
	}
	rec1 := WALRecord{Type: RecordChunk, Fingerprint: 12345, Chunk: &chunk1}
	b1, _ := json.Marshal(rec1)
	writer.WriteRecord(b1)

	chunk2 := Chunk{
		ID:   "chunk-2",
		From: 100,
		To:   200,
		Data: []byte("overlapping"),
	}
	rec2 := WALRecord{Type: RecordChunk, Fingerprint: 12345, Chunk: &chunk2}
	b2, _ := json.Marshal(rec2)
	writer.WriteRecord(b2)

	chunk3 := Chunk{
		ID:   "chunk-3",
		From: 201,
		To:   300,
		Data: []byte("new"),
	}
	rec3 := WALRecord{Type: RecordChunk, Fingerprint: 12345, Chunk: &chunk3}
	b3, _ := json.Marshal(rec3)
	writer.WriteRecord(b3)

	f.Sync()

	seriesMap, err := ReplayWAL(tmpDir, lastFlushedTime)
	if err != nil {
		t.Fatal(err)
	}

	s, exists := seriesMap[12345]
	if !exists {
		t.Fatal("expected series 12345 to be recovered")
	}

	expectedChunks := map[string]bool{
		"chunk-2": true,
		"chunk-3": true,
	}

	if len(s.Chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(s.Chunks))
	}

	for _, c := range s.Chunks {
		if !expectedChunks[c.ID] {
			t.Errorf("unexpected chunk recovered: %s", c.ID)
		}
	}
}

func TestWALReplay_Idempotency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	walFilePath := filepath.Join(tmpDir, "000001.wal")
	f, err := os.Create(walFilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	writer := NewWALWriter(f)

	chunk := Chunk{
		ID:   "chunk-dup",
		From: 100,
		To:   200,
		Data: []byte("data"),
	}
	rec := WALRecord{Type: RecordChunk, Fingerprint: 12345, Chunk: &chunk}
	b, _ := json.Marshal(rec)
	writer.WriteRecord(b)
	writer.WriteRecord(b)

	f.Sync()

	seriesMap, err := ReplayWAL(tmpDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	s := seriesMap[12345]
	if len(s.Chunks) != 1 {
		t.Fatalf("expected 1 chunk due to deduplication, got %d", len(s.Chunks))
	}
}
