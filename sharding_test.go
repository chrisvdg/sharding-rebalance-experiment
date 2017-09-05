package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var (
	orgData *Vdisk
)

func TestModuloSharding(t *testing.T) {
	getShard = getShardIndexModulo

	vdisk := orgData

	vdisk.PrintShardingState()

	v, _ := vdisk.GetBlock(245)

	fmt.Printf("%v\n", v)

}

func TestModuloFail(t *testing.T) {

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
	orgData = generateVdisk(shards, blocks)
}
