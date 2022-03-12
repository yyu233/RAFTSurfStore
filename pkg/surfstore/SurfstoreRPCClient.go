package surfstore

import (
	context "context"
	"fmt"
	"log"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Println("Client GetBlock dial failed")
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		log.Println("Client GetBlock failed")
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	//panic("todo")
	//panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Println("Client PutBlock dial failed")
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := c.PutBlock(ctx, block)
	if err != nil {
		log.Println("Client PutBlock failed")
		conn.Close()
		return err
	}
	*succ = status.Flag
	if *succ {
		log.Printf("Put block %v successfully \n", GetBlockHashString(block.BlockData))
	}

	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	//panic("todo")
	//panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Println("Client HasBlocks dial failed")
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		log.Printf("blockStoreAddr: %s\n", blockStoreAddr)
		log.Println("Client HasBlocks failed")
		conn.Close()
		return err
	}
	for i := range res.Hashes {
		log.Printf("Client HashBlock : %v\n", res.Hashes[i])
	}
	*blockHashesOut = res.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	//panic("todo")
	//panic("todo")
	for idx := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithInsecure())
		if err != nil {
			log.Println("Client GetFileInfoMap dial failed")
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		log.Printf("Contacting %d server, addr: %s\n", idx, surfClient.MetaStoreAddrs[idx])
		res, err := c.GetFileInfoMap(ctx, &(emptypb.Empty{}))
		if err != nil {
			conn.Close()
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				log.Printf("GetFileInfoMap server %d crashed, addr: %s\n", idx, surfClient.MetaStoreAddrs[idx])
				continue
			} else if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				log.Printf("GetFileInfoMap server %d not leader, addr: %s\n", idx, surfClient.MetaStoreAddrs[idx])
				continue
			} else {
				//other uknown error
				log.Println("Client GetFileInfoMap failed")
				return err
			}
			//return err
		} else { // leader is found
			log.Printf("server %d is leader", idx)
			*serverFileInfoMap = res.FileInfoMap
			conn.Close()
			break
		}
	}

	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	//panic("todo")
	for idx := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithInsecure())
		if err != nil {
			log.Println("Client UpdateFile dial failed")
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			conn.Close()
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				log.Printf("UpdateFile server %d crashed, addr: %s\n", idx, surfClient.MetaStoreAddrs[idx])
				continue
			} else if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				log.Printf("UpdateFile server %d not leader, addr: %s\n", idx, surfClient.MetaStoreAddrs[idx])
				continue
			} else {
				//other uknown error
				log.Println("Client UpdateFile failed")
				return err
			}
		} else {
			if res.Version != *latestVersion {
				conn.Close()
				return fmt.Errorf("-1")
			}
			return nil
		}
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	//panic("todo")
	for idx := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithInsecure())
		if err != nil {
			log.Println("Client GetBlockStoreAddr dial failed")
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := c.GetBlockStoreAddr(ctx, &(emptypb.Empty{}))
		if err != nil {
			conn.Close()
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				log.Printf("GetBlockStoreAddr server %d crashed, addr: %s\n", idx, surfClient.MetaStoreAddrs[idx])
				continue
			} else if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				log.Printf("GetBlockStoreAddr server %d not leader, addr: %s\n", idx, surfClient.MetaStoreAddrs[idx])
				continue
			} else {
				//other uknown error
				log.Println("Client GetBlockStoreAddr failed")
				return err
			}
		} else { // leader is found
			log.Printf("server %d is leader", idx)
			log.Printf("GetBlockStoreAddr: %s\n", res.Addr)
			*blockStoreAddr = res.Addr
			conn.Close()
			break
		}

	}

	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
