package main

import (
	"fmt"

	"github.com/google/uuid"
)

// Error types
var (
	ErrBlockNotFound = fmt.Errorf("block not found")
)

// NewShard constructs a new shard
func NewShard() *Shard {
	var s Shard

	s.seedData, _ = uuid.New().MarshalBinary()
	s.data = make(map[int]byte)
	s.Healthy = true

	return &s
}

// Shard represents a vdisk shard
type Shard struct {
	Healthy  bool
	data     map[int]byte
	seedData []byte
}

// SetBlock sets data in the shard
func (s *Shard) SetBlock(blockAddress int, data byte) {
	s.data[blockAddress] = data
}

// GetBlock resturns the data from given block address
func (s *Shard) GetBlock(blockAddress int) (byte, error) {
	data, ok := s.data[blockAddress]
	if !ok {
		return 0, ErrBlockNotFound
	}
	return data, nil
}

// Clone return a clone of a shard
func (s *Shard) Clone() *Shard {
	newShard := new(Shard)

	newShard.Healthy = s.Healthy
	newShard.seedData = s.seedData
	newShard.data = make(map[int]byte)
	for blockIndex, data := range s.data {
		newShard.data[blockIndex] = data
	}

	return newShard
}

// OK returns the current health of the shard
func (s *Shard) OK() bool {
	return s.Healthy
}

// SetHealth sets the health of the shard
func (s *Shard) SetHealth(health bool) {
	s.Healthy = health
}

// BlockCount returns the current blockcount of a shard
func (s *Shard) BlockCount() int {
	return len(s.data)
}
