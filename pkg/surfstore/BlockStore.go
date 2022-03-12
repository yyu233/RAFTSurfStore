package surfstore

import (
	context "context"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//panic("todo")
	if bk, ok := bs.BlockMap[blockHash.Hash]; ok {
		return bk, nil
	}
	return nil, fmt.Errorf("block %v doesn't exit", blockHash.Hash)
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	//panic("todo")
	key := GetBlockHashString(block.BlockData)
	bs.BlockMap[key] = block
	return &Success{
		Flag: true,
	}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	//panic("todo")
	//panic("todo")
	res := BlockHashes{Hashes: make([]string, 0)}
	for _, key := range blockHashesIn.GetHashes() {
		if _, ok := bs.BlockMap[key]; ok {
			res.Hashes = append(res.Hashes, key)
		}
	}
	return &res, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
