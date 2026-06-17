package ingester

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
)

var (
	ErrCorrupt = errors.New("corrupt WAL record")
	SyncMarker = []byte{0xAA, 0xBB, 0xCC, 0xDD}
)

type Chunk struct {
	ID   string `json:"id"`
	From int64  `json:"from"`
	To   int64  `json:"to"`
	Data []byte `json:"data"`
}

type Series struct {
	Fingerprint uint64  `json:"fingerprint"`
	Chunks      []Chunk `json:"chunks"`
}

type RecordType string

const (
	RecordSeries RecordType = "series"
	RecordChunk  RecordType = "chunk"
)

type WALRecord struct {
	Type        RecordType `json:"type"`
	Fingerprint uint64     `json:"fingerprint,omitempty"`
	Chunk       *Chunk     `json:"chunk,omitempty"`
}

type WALWriter struct {
	w io.Writer
}

func NewWALWriter(w io.Writer) *WALWriter {
	return &WALWriter{w: w}
}

func (w *WALWriter) WriteRecord(data []byte) error {
	if _, err := w.w.Write(SyncMarker); err != nil {
		return err
	}
	length := uint32(len(data))
	if err := binary.Write(w.w, binary.BigEndian, length); err != nil {
		return err
	}
	h := fnv.New32a()
	h.Write(data)
	checksum := h.Sum32()
	if err := binary.Write(w.w, binary.BigEndian, checksum); err != nil {
		return err
	}
	if _, err := w.w.Write(data); err != nil {
		return err
	}
	return nil
}

type WALReader struct {
	r io.ReadSeeker
}

func NewWALReader(r io.ReadSeeker) *WALReader {
	return &WALReader{r: r}
}

func (r *WALReader) Next() ([]byte, error) {
	for {
		marker := make([]byte, 4)
		_, err := io.ReadFull(r.r, marker)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, io.EOF
			}
			return nil, err
		}

		if !bytes.Equal(marker, SyncMarker) {
			if _, err := r.r.Seek(-3, io.SeekCurrent); err != nil {
				return nil, err
			}
			continue
		}

		var length uint32
		if err := binary.Read(r.r, binary.BigEndian, &length); err != nil {
			log.Printf("Warning: corrupt record length, skipping: %v", err)
			continue
		}

		var checksum uint32
		if err := binary.Read(r.r, binary.BigEndian, &checksum); err != nil {
			log.Printf("Warning: corrupt record checksum, skipping: %v", err)
			continue
		}

		payload := make([]byte, length)
		n, err := io.ReadFull(r.r, payload)
		if err != nil {
			log.Printf("Warning: corrupt record payload (read %d/%d bytes), skipping: %v", n, length, err)
			continue
		}

		h := fnv.New32a()
		h.Write(payload)
		if h.Sum32() != checksum {
			log.Printf("Warning: checksum mismatch, skipping corrupt record")
			continue
		}

		return payload, nil
	}
}

func ReplayWAL(walDir string, lastFlushedTime int64) (map[uint64]*Series, error) {
	seriesMap := make(map[uint64]*Series)

	files, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return seriesMap, nil
		}
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filePath := filepath.Join(walDir, file.Name())
		f, err := os.Open(filePath)
		if err != nil {
			log.Printf("Warning: failed to open WAL file %s: %v", filePath, err)
			continue
		}

		reader := NewWALReader(f)
		for {
			recordBytes, err := reader.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Printf("Warning: error reading next WAL record in %s: %v", filePath, err)
				continue
			}

			var record WALRecord
			if err := json.Unmarshal(recordBytes, &record); err != nil {
				log.Printf("Warning: failed to unmarshal WAL record: %v", err)
				continue
		}

			switch record.Type {
			case RecordSeries:
				if _, exists := seriesMap[record.Fingerprint]; !exists {
					seriesMap[record.Fingerprint] = &Series{
						Fingerprint: record.Fingerprint,
						Chunks:      []Chunk{},
					}
				}
			case RecordChunk:
				if record.Chunk == nil {
					continue
				}
				if record.Chunk.To <= lastFlushedTime {
					continue
				}

				s, exists := seriesMap[record.Fingerprint]
				if !exists {
					s = &Series{
						Fingerprint: record.Fingerprint,
						Chunks:      []Chunk{},
					}
					seriesMap[record.Fingerprint] = s
				}

				found := false
				for i, existing := range s.Chunks {
					if existing.ID == record.Chunk.ID {
						if len(record.Chunk.Data) > len(existing.Data) {
							s.Chunks[i].Data = record.Chunk.Data
						}
						if record.Chunk.From < existing.From {
							s.Chunks[i].From = record.Chunk.From
						}
						if record.Chunk.To > existing.To {
							s.Chunks[i].To = record.Chunk.To
						}
						found = true
						break
					}
				}
				if !found {
					s.Chunks = append(s.Chunks, *record.Chunk)
				}
			}
		}
		f.Close()
	}

	return seriesMap, nil
}
