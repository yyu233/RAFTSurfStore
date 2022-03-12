package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	//panic("todo")
	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	//panic("todo")
	//panic("todo")
	fn := fileMetaData.GetFilename()
	if cloudMetaData, ok := m.FileMetaMap[fn]; ok {
		if fileMetaData.GetVersion()-cloudMetaData.GetVersion() == 1 {
			m.FileMetaMap[fn] = fileMetaData
			return &Version{Version: m.FileMetaMap[fn].GetVersion()}, nil
		} else {
			return &Version{Version: -1},
				fmt.Errorf("failed to update cloud file: local file version is not 1 greater than remote file version ")
		}
	} else {
		m.FileMetaMap[fn] = fileMetaData
		return &Version{Version: m.FileMetaMap[fn].GetVersion()}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	//panic("todo")
	//panic("todo")
	return &BlockStoreAddr{
		Addr: m.BlockStoreAddr,
	}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
