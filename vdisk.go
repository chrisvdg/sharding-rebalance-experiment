package main

import (
	"fmt"
)

// defines the get shard algorithm
var (
	getShard = getShardGeertsAlgo
)

// Errors
var (
	ErrShardIndexNotFound = fmt.Errorf("could not find shard index")
	ErrShardNotHealthy    = fmt.Errorf("shard is not healthy")
)

// NewVdisk constructs a new vdisk
func NewVdisk(shardCount int64) *Vdisk {
	var vdisk Vdisk
	for i := int64(0); i < shardCount; i++ {
		vdisk.Shards = append(vdisk.Shards, NewShard())
	}
	vdisk.offlineShards = make(map[int64]struct{})
	vdisk.shardCount = shardCount

	return &vdisk
}

// Vdisk vdisk represents a vdisk
type Vdisk struct {
	Shards        []*Shard
	offlineShards map[int64]struct{}
	shardCount    int64
}

// SetBlock sets a block in a vdisk
func (vdisk *Vdisk) SetBlock(blockIndex int64, data byte) error {
	shardIndex, err := getShard(vdisk, blockIndex)
	if err != nil {
		return err
	}
	s := vdisk.Shards[shardIndex]
	if !s.OK() {
		return ErrShardNotHealthy
	}

	s.SetBlock(blockIndex, data)
	return nil
}

// GetBlock gets the data from a block in a vdisk
func (vdisk *Vdisk) GetBlock(blockIndex int64) (byte, error) {
	shardIndex, err := getShard(vdisk, blockIndex)
	if err != nil {
		return 0, err
	}

	s := vdisk.Shards[shardIndex]
	if !s.OK() {
		return 0, ErrShardNotHealthy
	}

	return s.GetBlock(blockIndex)
}

// Clone returns a clone of a vdisk
func (vdisk *Vdisk) Clone() *Vdisk {
	newVdisk := new(Vdisk)

	for _, shard := range vdisk.Shards {
		newVdisk.Shards = append(newVdisk.Shards, shard.Clone())
	}

	newVdisk.offlineShards = make(map[int64]struct{})
	for shardIndex := range vdisk.offlineShards {
		newVdisk.offlineShards[shardIndex] = struct{}{}
	}

	newVdisk.shardCount = vdisk.shardCount

	return newVdisk
}

// FailShard set a shard to unhealthy and redistributes the data of the failed shard
func (vdisk *Vdisk) FailShard(shardIndex int64) error {
	if shardIndex >= int64(len(vdisk.Shards)) {
		return ErrShardIndexNotFound
	}

	if _, offline := vdisk.offlineShards[shardIndex]; offline {
		return nil // nothing to do
	}

	vdisk.offlineShards[shardIndex] = struct{}{}

	s := vdisk.Shards[shardIndex]
	s.SetHealth(false)

	// redistribute
	for blockIndex, data := range s.data {
		err := vdisk.SetBlock(blockIndex, data)
		if err != nil {
			return err
		}
	}

	return nil
}

// HealthyShards returns the count of healthy shard in a vdisk
func (vdisk *Vdisk) HealthyShards() int64 {
	var healthyCount int64

	for _, shard := range vdisk.Shards {
		if shard.OK() {
			healthyCount++
		}
	}

	return healthyCount
}

// PrintShardingState prints out the current block count for each shard
func (vdisk *Vdisk) PrintShardingState() {
	fmt.Println("\n\t--- Current vdisk state ---")
	for i := range vdisk.Shards {
		s := vdisk.Shards[i]
		blocks := s.BlockCount()
		if s.OK() {
			fmt.Printf("Shard %d is healthy and has %d blocks\n", i, blocks)
		} else {
			fmt.Printf("Shard %d is unhealthy\n", i)
		}
	}
}

// GetShardIndex returns a shardindex for a given blockindex
func getShardIndexSimpleModulo(vdisk *Vdisk, blockIndex int64) (int64, error) {
	return blockIndex % vdisk.shardCount, nil
}

func getShardGeertsAlgo(vdisk *Vdisk, blockIndex int64) (int64, error) {
	shardIndex := blockIndex % vdisk.shardCount
	s := vdisk.Shards[shardIndex]
	if s.OK() {
		return shardIndex, nil
	}

	//shardIndex = jumpConsistentHash(blockIndex, vdisk.shardCount)
	shardIndex = blockIndex % vdisk.HealthyShards()
	var shardCounter int64

	for i := int64(0); i < vdisk.shardCount; i++ {
		if !vdisk.Shards[i].OK() {
			continue
		}
		if shardCounter == shardIndex {
			return i, nil
		}
		shardCounter++
	}
	return 0, ErrShardIndexNotFound
}

func getShardIndexGlen(vdisk *Vdisk, blockIndex int64) (int64, error) {
	// first try the modulo sharding,
	// which will work for all default online shards
	// and thus keep it as cheap as possible
	si := blockIndex % vdisk.shardCount
	var offline bool
	if _, offline = vdisk.offlineShards[si]; !offline {
		return si, nil
	}

	// keep trying until we find a non-offline shard
	// in the same reproducable manner
	// (another kind of tracing)
	var i int64
	for {
		si = int64(jumpConsistentHash(uint64(blockIndex), int32(vdisk.shardCount)))
		if _, offline = vdisk.offlineShards[si]; !offline {
			return si, nil
		}
		blockIndex++
		if i > vdisk.shardCount {
			break
		}
	}

	return 0, ErrShardIndexNotFound
}

// jumpConsistentHash taken from https://arxiv.org/pdf/1406.2294.pdf
func jumpConsistentHash(key uint64, numBuckets int32) int32 {
	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}
