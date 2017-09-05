package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	orgVdiskData           Vdisk
	vdiskWithUnheathyShard *Vdisk
	shardToFail            = 6
	callList               []int
	callAmount             = 1000000
)

func testSimpleModuloSharding(t *testing.T) {
	assert := assert.New(t)
	getShard = getShardIndexSimpleModulo

	vdisk := orgVdiskData.Clone()

	vdisk.PrintShardingState()

	err := vdisk.FailShard(2)

	assert.Equal(ErrShardNotHealthy, err, "Expected to fail since it rebalaces to unhealthy shard")

	vdisk.PrintShardingState()

}

func TestGeertsAlgo(t *testing.T) {
	assert := assert.New(t)
	getShard = getShardGeertsAlgo
	vdisk := orgVdiskData.Clone()

	fmt.Println("\nreading with healthy shards")
	err := loopCallList(callList, vdisk)
	assert.NoError(err, "didn't expect an error")

	err = vdisk.FailShard(6)
	assert.NoError(err)

	fmt.Println("\nreading with 1 unhealthy shard")
	err = loopCallList(callList, vdisk)
	assert.NoError(err, "didn't expect an error")

	// check if original shard 6 data is preserved
	for blockindex, data := range orgVdiskData.Shards[6].data {
		newData, err := vdisk.GetBlock(blockindex)

		assert.NoError(err)
		assert.Equal(data, newData)
	}

	vdisk.PrintShardingState()
}

func BenchmarkHealthyShard(b *testing.B) {
	vdisk := orgVdiskData.Clone()
	loopCallList(callList, vdisk)
}

func BenchmarkUnHealthyShard(b *testing.B) {
	vdisk := vdiskWithUnheathyShard
	loopCallList(callList, vdisk)
}

func loopCallList(callList []int, vdisk *Vdisk) error {
	fmt.Printf("Reading vdisk %d times...\n", callAmount)
	start1 := time.Now()

	for _, blockAddress := range callList {
		_, err := vdisk.GetBlock(blockAddress)
		if err != nil {
			return err
		}
	}

	fmt.Printf("Reading took %s\n", time.Since(start1))
	return nil
}

func generateVdisk(shards int, blocks int) *Vdisk {
	vdisk := NewVdisk(shards)

	for i := 0; i < blocks; i++ {
		data := genRandomByte()
		vdisk.SetBlock(i, data)
	}

	return vdisk
}

func genRandomByte() byte {
	rand.Seed(time.Now().Unix())
	n := rand.Intn(255)

	return byte(n)
}

// generateCallList generates a list of addresses the calling test portion can use
func generateCallList(lenght int, shard *Shard) []int {
	var callList []int
	rand.Seed(time.Now().Unix())

	var blockAddresses []int
	for blockAddress := range shard.data {
		blockAddresses = append(blockAddresses, blockAddress)
	}
	addressLen := len(blockAddresses)
	for i := 0; i < lenght; i++ {
		callList = append(callList, blockAddresses[rand.Intn(addressLen)])
	}

	return callList
}

func init() {
	var (
		shards = 10
		blocks = 10000
	)
	orgVdiskData = *generateVdisk(shards, blocks)
	callList = generateCallList(callAmount, orgVdiskData.Shards[shardToFail])
	vdiskWithUnheathyShard = orgVdiskData.Clone()
	vdiskWithUnheathyShard.FailShard(shardToFail)
}
