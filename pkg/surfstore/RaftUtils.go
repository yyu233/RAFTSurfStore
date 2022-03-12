package surfstore

import (
	"bufio"
	"net"

	//	"google.golang.org/grpc"
	"io"
	"log"

	//	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	grpc "google.golang.org/grpc"
)

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal("Error During Reading Config", e)
		}

		if e == io.EOF {
			return
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount, serverCount)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}

}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {
	// TODO any initialization you need to do here

	isCrashedMutex := sync.RWMutex{}
	pendingCommitMutex := sync.RWMutex{}

	server := RaftSurfstore{
		// TODO initialize any fields you add here
		isLeader:  false,
		term:      0,
		log:       make([]*UpdateOperation, 0),
		metaStore: NewMetaStore(blockStoreAddr),

		ip:       ips[id],
		ipList:   ips,
		serverId: id,

		commitIndex: -1,
		lastApplied: -1,

		nextIndex:      make([]int64, len(ips)),
		matchIndex:     make([]int64, len(ips)),
		pendingCommits: make(map[int64]chan bool, 0),
		crashCount:     0,

		pendingCommitMutex:   &pendingCommitMutex,
		pendingCommitCond:    sync.NewCond(&pendingCommitMutex),
		isPendingCommitReady: false,

		isCrashed:      false,
		notCrashedCond: sync.NewCond(&isCrashedMutex),
		isCrashedMutex: &isCrashedMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	//panic("todo")
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)

	l, e := net.Listen("tcp", server.ip)
	if e != nil {
		return e
	}

	return s.Serve(l)
}
