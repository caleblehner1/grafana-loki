package ingester

func (s *Series) AddChunk(c Chunk) {
	s.Chunks = append(s.Chunks, c)
}
