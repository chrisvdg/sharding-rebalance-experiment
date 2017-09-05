package main

import (
	"fmt"
)

// defines the get shard algorithm
var (
	getShard = getShardIndexModulo
)

// NewVdisk constructs a new vdisk
func NewVdisk(shardCount int) *Vdisk {
	var vdisk Vdisk
	for i := 0; i < shardCount; i++ {
		vdisk.Shards = append(vdisk.Shards, NewShard())
	}

	return &vdisk
}

// Vdisk vdisk represents a vdisk
type Vdisk struct {
	Shards []*Shard
}

// SetBlock sets a block in a vdisk
func (vdisk *Vdisk) SetBlock(blockIndex int, data byte) {
	shardIndex := getShard(vdisk, blockIndex)
	vdisk.Shards[shardIndex].SetBlock(blockIndex, data)
}

// GetBlock gets the data from a block in a vdisk
func (vdisk *Vdisk) GetBlock(blockIndex int) (byte, error) {
	shardIndex := getShard(vdisk, blockIndex)
	return vdisk.Shards[shardIndex].GetBlock(blockIndex)
}

// PrintShardingState prints out the current block count for each shard
func (vdisk *Vdisk) PrintShardingState() {
	for i := range vdisk.Shards {
		s := vdisk.Shards[i]
		blocks := s.BlockCount()
		fmt.Printf("Shard %d has \t%d blocks\n", i, blocks)
	}
}

// GetShardIndex returns a shardindex for a given blockindex
func getShardIndexModulo(vdisk *Vdisk, blockIndex int) int {
	return blockIndex % len(vdisk.Shards)
}

func getShardGeertAlgo(vdisk *Vdisk, blockIndex int) int {
	return 0
}
