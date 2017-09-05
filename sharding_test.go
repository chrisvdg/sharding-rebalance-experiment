package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	orgVdiskData *Vdisk
)

func TestModuloSharding(t *testing.T) {
	assert := assert.New(t)
	getShard = getShardIndexModulo

	vdisk := orgVdiskData

	vdisk.PrintShardingState()

	err := vdisk.FailShard(2)

	assert.Equal(err, ErrShardNotHealthy)

	vdisk.PrintShardingState()

}

func TestGeertsAlgo(t *testing.T) {
	assert := assert.New(t)
	getShard = getShardGeertsAlgo

	vdisk := orgVdiskData

	vdisk.PrintShardingState()

	err := vdisk.FailShard(6)
	assert.NoError(err)

	// check if original shard 6 data is preserved
	for blockindex, data := range orgVdiskData.Shards[6].data {
		newData, err := vdisk.GetBlock(blockindex)

		assert.NoError(err)
		assert.Equal(data, newData)
	}

	vdisk.PrintShardingState()

}

func generateVdisk(shards int, blocks int) *Vdisk {
	vdisk := NewVdisk(shards)

	fmt.Println("generating vdisk....")
	for i := 0; i < blocks; i++ {
		data := genRandomByte()
		vdisk.SetBlock(i, data)
	}
	fmt.Println("vdisk generated.")

	return vdisk
}

func genRandomByte() byte {
	rand.Seed(int64(time.Now().Nanosecond()))
	n := rand.Intn(255)

	return byte(n)
}

func init() {
	var (
		shards = 10
		blocks = 1000
	)
	orgVdiskData = generateVdisk(shards, blocks)
}
