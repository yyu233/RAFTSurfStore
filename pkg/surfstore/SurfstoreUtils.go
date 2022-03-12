package surfstore

import (
	"errors"
	"io"
	"log"
	"os"
	"strings"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	//panic("todo")
	basedir := client.BaseDir
	blocksize := client.BlockSize
	err := setup(basedir, blocksize)
	if err != nil {
		log.Println("setup failed")
		log.Fatal(err)
	}

	log.Println("######## DEBUG ScanLocalFileUpdate ########")
	// get local base directory file info
	localFileMetaMap, localFileUpdateMetaMap, localHashBlockMap, err := ScanLocalFileUpdate(basedir, blocksize)
	if err != nil {
		//handle error
		log.Println("local scan failed")
		log.Fatal(err)
	}
	for k, v := range localFileMetaMap {
		log.Printf("Local file name : %s\n", k)
		log.Printf("Version: %d\n", v.GetVersion())
		hashes := v.GetBlockHashList()
		for i := range hashes {
			log.Printf("Hash: %v\n", hashes[i])
		}
	}

	for k, v := range localFileUpdateMetaMap {
		log.Printf("Updated file name : %s\n", k)
		log.Printf("Updated Version: %d\n", v.GetVersion())
		hashes := v.GetBlockHashList()
		for i := range hashes {
			log.Printf("Hash: %v\n", hashes[i])
		}
	}

	for k, v := range localHashBlockMap {
		log.Printf("Local Hash : %v\n", k)
		log.Printf("Local Block size: %d\n", v.BlockSize)
	}
	log.Println("###############################################")

	log.Println("######## DEBUG ScanRemoteFileUpdate ########")
	// get cloud file info
	downloadFileMetaMap, err := ScanRemoteFileUpdate(client, localFileMetaMap, localFileUpdateMetaMap)
	if err != nil {
		log.Println("remote scan failed")
		log.Fatal(err)
	}

	for k, _ := range downloadFileMetaMap {
		log.Printf("Need to pull from remote: %s\n", k)
	}

	var blkAddr *string
	a := ""
	blkAddr = &a
	err = client.GetBlockStoreAddr(blkAddr)
	if err != nil {
		log.Println("get block address failed")
		log.Fatal(err)
	}
	log.Printf("ClientSync, blkAddr: %s\n", *blkAddr)
	log.Println("###############################################")

	log.Println("######## DEBUG download ########")
	// download
	err = download(client, *blkAddr, localFileMetaMap, downloadFileMetaMap)
	if err != nil {
		log.Println("download failed")
		log.Fatal(err)
	}
	log.Println("###############################################")

	log.Println("######## DEBUG upload ########")
	// upload
	err = upload(client, *blkAddr, localFileUpdateMetaMap, localHashBlockMap)
	if err != nil {
		log.Println("upload failed")
		log.Fatal(err)
	}
	log.Println("###############################################")

	log.Println("######## DEBUG commit ########")
	//apply uncommitted change
	err = commit(basedir, localFileMetaMap, localFileUpdateMetaMap, downloadFileMetaMap)
	if err != nil {
		log.Println("commit failed")
		log.Fatal(err)
	}
	log.Println("###############################################")

}

func buildIndex(basedir string, blocksize int) (err error) {
	localFileMetaMap := make(map[string]*FileMetaData)

	fh, err := os.Open(basedir)
	if err != nil {
		log.Println("build index failed")
		return err
	}
	defer fh.Close()

	fInfos, err := fh.Readdir(-1)
	if err != nil {
		return err
	}

	for _, fInfo := range fInfos {
		fname := fInfo.Name()
		if strings.Compare(fname, DEFAULT_META_FILENAME) != 0 {
			fpath := ConcatPath(basedir, fname)
			f, err := os.Open(fpath)
			if err != nil {
				return err
			}
			defer f.Close()

			curHashList := make([]string, 0)
			buf := make([]byte, blocksize)

			for {
				n, err := f.Read(buf)
				if err != nil {
					if err != io.EOF {
						return err
					}
					break
				}
				hash := GetBlockHashString(buf[0:n])
				curHashList = append(curHashList, hash)
			}
			curFileMetaData := FileMetaData{
				Filename:      fname,
				Version:       1,
				BlockHashList: make([]string, len(curHashList)),
			}
			copy(curFileMetaData.BlockHashList, curHashList)
			localFileMetaMap[fname] = &curFileMetaData
		}
	}

	err = WriteMetaFile(localFileMetaMap, basedir)
	if err != nil {
		return err
	}

	return nil
}

func setup(basedir string, blocksize int) (err error) {
	indexPath := ConcatPath(basedir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(basedir); err == nil {
		if _, err := os.Stat(indexPath); err == nil {
			return nil
		} else if errors.Is(err, os.ErrNotExist) {
			//index.txt doesn't exist
			f, err := os.Create(indexPath)
			if err != nil {
				// handle error
				return err
			}
			defer f.Close()

			//reconstruct index.txt
			/*err = buildIndex(basedir, blocksize)
			if err != nil {
				return err
			}*/
		} else {
			//other errors
			return err
		}

	} else if errors.Is(err, os.ErrNotExist) {
		//base dir doesn't exist
		err := os.Mkdir(basedir, os.ModePerm)
		if err != nil {
			//handle error
			return err
		}
	} else {
		//other erros
		return err
	}

	return nil
}

func commit(basedir string, localFileMetaMap map[string]*FileMetaData, localFileUpdateMetaMap map[string]*FileMetaData,
	downloadFileMetaMap map[string]*FileMetaData) (err error) {

	newLocalFileMetaMap := make(map[string]*FileMetaData)
	// populate unchanged and modified file meta
	for lfname, lfMetaData := range localFileMetaMap {
		// unchanged file meta
		if lufMetaData, ok := localFileUpdateMetaMap[lfname]; !ok {
			newLocalFileMetaMap[lfname] = lfMetaData
		} else {
			//modifed file meta
			newLocalFileMetaMap[lfname] = lufMetaData
		}
	}

	// populate new file meta
	for lufname, lufMetaData := range localFileUpdateMetaMap {
		// new file meta
		if _, ok := localFileMetaMap[lufname]; !ok {
			newLocalFileMetaMap[lufname] = lufMetaData
		}
	}

	// populate download file meta
	for rfname, rfMetaData := range downloadFileMetaMap {
		newLocalFileMetaMap[rfname] = rfMetaData
	}

	// overwrite index file
	err = WriteMetaFile(newLocalFileMetaMap, basedir)
	if err != nil {
		return err
	}

	return nil
}

func upload(client RPCClient, blkAddr string, localFileUpdateMetaMap map[string]*FileMetaData, localHashBlockMap map[string]*Block) (err error) {
	log.Printf("Debug upload: localFileUpdateMetaMap size: %d\n", len(localFileUpdateMetaMap))
	for k, v := range localHashBlockMap {
		log.Printf("local hash: %v\n", k)
		log.Printf("local block size: %d\n", v.BlockSize)
	}
	for lufname, lufMetaData := range localFileUpdateMetaMap {
		log.Printf("File to be upload: %s\n", lufname)
		hashesIn := lufMetaData.BlockHashList
		for i := range hashesIn {
			log.Printf("hashes in: %v\n", hashesIn[i])
		}
		var hashesOut *[]string //hashes in cloud
		a := make([]string, 0)
		hashesOut = &a
		err := client.HasBlocks(hashesIn, blkAddr, hashesOut)
		if err != nil {
			log.Fatal(err)
		}
		hashesInCloud := make(map[string]int)
		for i := range *hashesOut {
			hash := (*hashesOut)[i]
			hashesInCloud[hash] = 1
			log.Printf("Debug upload: hashesInCloud: %v\n", hash)
		}

		//1. update block store
		for i := range hashesIn {
			hash := hashesIn[i]
			//hash not in cloud
			if _, ok := hashesInCloud[hash]; !ok {
				if blk, ok := localHashBlockMap[hash]; ok {
					log.Printf("Will put block: %v\n", hash)
					var succ *bool
					a := true
					succ = &a
					err = client.PutBlock(blk, blkAddr, succ)
					if err != nil {
						log.Println("Put block failed")
						log.Fatal(err)
					}
					if !*succ {
						log.Fatal("Put block status: failed\n")
					}
				}
				// tombstone don't need to update block store
			}
		}

		//2. update meta store
		err = client.UpdateFile(lufMetaData, &lufMetaData.Version)
		if err != nil {
			if strings.Compare(err.Error(), "-1") == 0 {
				log.Printf("upload failed: local file is elder than remote file.\n")
			} else {
				log.Println("Update meta store failed")
				log.Fatal(err)
			}
		}
	}

	return nil
}

func download(client RPCClient, blkAddr string, localFileMetaMap map[string]*FileMetaData, downloadFileMetaMap map[string]*FileMetaData) (err error) {
	log.Printf("Debug download: downloadFileUpdateMetaMap size: %d\n", len(downloadFileMetaMap))
	for rfname, rfMetaData := range downloadFileMetaMap {
		absFilePath := ConcatPath(client.BaseDir, rfname)
		tmpFilePath := absFilePath + ".tmp"

		// 1. remote metadata is tombstone
		rmBlkHashes := rfMetaData.GetBlockHashList()
		if len(rmBlkHashes) == 1 && strings.Compare(rmBlkHashes[0], "0") == 0 {
			log.Printf("File to be deleted: %s\n", rfname)
			// remove local file
			_, err = os.Stat(absFilePath)
			if err == nil {
				err = os.Remove(absFilePath)
				if err != nil {
					return err
				}
			}
		} else {
			log.Printf("File to be download: %s\n", rfname)
			// 2. not tombstone
			// tmp file
			fh, err := os.Create(tmpFilePath)
			if err != nil {
				return err
			}
			// write each remote block to tmp file
			for i := range rmBlkHashes {
				var blk = &Block{
					BlockData: nil,
					BlockSize: 0,
				}
				err = client.GetBlock(rmBlkHashes[i], blkAddr, blk)
				if err != nil {
					return err
				}
				n, err := fh.Write(blk.GetBlockData())
				if err != nil {
					return err
				}
				if n != int(blk.GetBlockSize()) {

				}
			}
			_, err = fh.Seek(0, io.SeekStart)
			if err != nil {
				return err
			}

			// reconstitute file
			fd, err := os.OpenFile(absFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				return err
			}
			defer fd.Close()

			for {
				buf := make([]byte, client.BlockSize)
				n, err := fh.Read(buf)
				if err != nil {
					if err != io.EOF {
						return err
					}
					break
				}
				n, err = fd.Write(buf[0:n])
				if err != nil {
					return err
				}
				if n != client.BlockSize {

				}
			}

			fh.Close()
			err = os.Remove(tmpFilePath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func ScanRemoteFileUpdate(client RPCClient, localFileMetaMap map[string]*FileMetaData,
	localFileUpdateMetaMap map[string]*FileMetaData) (downloadFileMetaMap map[string]*FileMetaData, err error) {

	var remoteFileMetaMap *map[string]*FileMetaData
	a := make(map[string]*FileMetaData)
	remoteFileMetaMap = &a
	err = client.GetFileInfoMap(remoteFileMetaMap)
	if err != nil {
		//handle error
		return nil, err
	}
	log.Printf("Debug Scan Remote: remoteFileMetaMap size: %d\n", len(*remoteFileMetaMap))
	downloadFileMetaMap = make(map[string]*FileMetaData)
	for rfname, rfMetadata := range *remoteFileMetaMap {
		log.Printf("Remote file name: %s\n", rfname)
		log.Printf("Remote file version: %d\n", rfMetadata.Version)
		//1. file exists remotely not in local index
		if lfMetadata, ok := localFileMetaMap[rfname]; !ok {
			//download
			downloadFileMetaMap[rfname] = rfMetadata
			//ignore local update
			delete(localFileUpdateMetaMap, rfname)
		} else {
			// remote file exists in local index
			// no local modification in the base dir
			if _, ok := localFileUpdateMetaMap[rfname]; !ok {
				//3. remote file has higher verison than local file, local file has no modification
				// need to download
				if lfMetadata.GetVersion() < rfMetadata.GetVersion() {
					downloadFileMetaMap[rfname] = rfMetadata
				}
			} else {
				//local modificatin in the base dir
				// 4. local file version higher or equal than the remote file version
				if lfMetadata.GetVersion() >= rfMetadata.GetVersion() {
					// sync local update to remote

				} else {
					// remote index version is higher, need to download, abort local update
					downloadFileMetaMap[rfname] = rfMetadata
					delete(localFileUpdateMetaMap, rfname)
				}
			}
		}
	}

	return downloadFileMetaMap, nil
}

func ScanLocalFileUpdate(basedir string, blocksize int) (localFileMetaMap map[string]*FileMetaData,
	localFileUpdateMetaMap map[string]*FileMetaData, localHashBlockMap map[string]*Block, err error) {
	// get local file meta map from index
	localFileMetaMap, err = LoadMetaFromMetaFile(basedir)
	if err != nil {
		return localFileMetaMap, nil, nil, err
	}

	localFileUpdateMetaMap = make(map[string]*FileMetaData)
	localHashBlockMap = make(map[string]*Block)
	localFileNameMap := make(map[string]int)

	// scan files in base directory
	fh, err := os.Open(basedir)
	if err != nil {
		return localFileMetaMap, nil, nil, err
	}
	defer fh.Close()

	fInfos, err := fh.Readdir(-1)
	if err != nil {
		return localFileMetaMap, nil, nil, err
	}

	// check modified or newly added file
	for _, fInfo := range fInfos {
		// exclude index file
		fname := fInfo.Name()
		fpath := ConcatPath(basedir, fname)
		localFileNameMap[fname] = 1
		if strings.Compare(fname, DEFAULT_META_FILENAME) != 0 {
			f, err := os.Open(fpath)
			if err != nil {
				return localFileMetaMap, nil, nil, err
			}
			defer f.Close()

			curHashList := make([]string, 0)
			buf := make([]byte, blocksize)

			for {
				n, err := f.Read(buf)
				if err != nil {
					if err != io.EOF {
						return localFileMetaMap, nil, nil, err
					}
					break
				}
				blk := &Block{
					BlockData: make([]byte, n),
					BlockSize: int32(n),
				}
				copy(blk.BlockData, buf)
				hash := GetBlockHashString(buf[0:n])
				curHashList = append(curHashList, hash)
				localHashBlockMap[hash] = blk
			}
			// not new added file
			if fMetadata, ok := localFileMetaMap[fname]; ok {
				prevHashList := fMetadata.GetBlockHashList()
				prevVersion := fMetadata.GetVersion()
				// block length not equal
				if len(curHashList) != len(prevHashList) {
					//file updated
					curMetaData := FileMetaData{
						Filename:      fname,
						Version:       prevVersion + 1,
						BlockHashList: make([]string, len(curHashList)),
					}
					copy(curMetaData.BlockHashList, curHashList)
					localFileUpdateMetaMap[fname] = &curMetaData
				} else { // equal block length
					for i, curHash := range curHashList {
						prevHash := prevHashList[i]
						// different hash
						if strings.Compare(curHash, prevHash) != 0 {
							//file updated
							curMetaData := FileMetaData{
								Filename:      fname,
								Version:       prevVersion + 1,
								BlockHashList: make([]string, len(curHashList)),
							}
							copy(curMetaData.BlockHashList, curHashList)
							localFileUpdateMetaMap[fname] = &curMetaData
						}
					}
				}
			} else {
				//newly added file
				curMetaData := FileMetaData{
					Filename:      fname,
					Version:       1,
					BlockHashList: make([]string, len(curHashList)),
				}
				copy(curMetaData.BlockHashList, curHashList)
				localFileUpdateMetaMap[fname] = &curMetaData
			}
		}
	}

	//check deleted file
	// file exists in index file
	for fname, fMetadata := range localFileMetaMap {
		//file doesn't exist in base directory
		if _, ok := localFileNameMap[fname]; !ok {
			// a tombStone file already, skip
			if len(fMetadata.GetBlockHashList()) == 1 && strings.Compare(fMetadata.GetBlockHashList()[0], "0") == 0 {
				continue
			} else {
				//not a tombStone file already
				tombStoneMetaData := FileMetaData{
					Filename:      fname,
					Version:       fMetadata.GetVersion() + 1,
					BlockHashList: []string{"0"},
				}
				localFileUpdateMetaMap[fname] = &tombStoneMetaData
			}
		}
	}

	return localFileMetaMap, localFileUpdateMetaMap, localHashBlockMap, nil
}
