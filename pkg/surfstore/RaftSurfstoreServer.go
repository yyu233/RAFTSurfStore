package surfstore

import (
	context "context"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	//server state info
	ip       string
	ipList   []string
	serverId int64

	commitIndex int64
	lastApplied int64

	//leader state info
	nextIndex      []int64
	matchIndex     []int64
	pendingCommits map[int64]chan bool
	crashCount     int64

	pendingCommitMutex   *sync.RWMutex
	pendingCommitCond    *sync.Cond
	isPendingCommitReady bool

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

//leader calls
func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	for { // blocking
		_, err := s.SendHeartbeat(ctx, empty)
		//Leader crashed or step down
		if err != nil {
			return nil, err
		}
		// majority of followers are alive
		if s.crashCount <= int64(len(s.ipList)-1)/2 {
			return &FileInfoMap{
				FileInfoMap: s.metaStore.FileMetaMap,
			}, nil
		}
	}

}

//leader calls
func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	//panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	for { // blocking
		_, err := s.SendHeartbeat(ctx, empty)
		//Leader crashed or step down
		if err != nil {
			return nil, err
		}
		// majority of followers are alive
		if s.crashCount <= int64(len(s.ipList)-1)/2 {
			return &BlockStoreAddr{
				Addr: s.metaStore.BlockStoreAddr,
			}, nil
		}
	}

}

//leader calls
func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	//panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	// append log entry
	s.log = append(s.log, &op)
	committed := make(chan bool)

	s.pendingCommits[s.commitIndex+1] = committed
	//s.pendingCommits = append(s.pendingCommits, committed)
	//try to replicate on follower
	go s.attemptCommit()

	//blocking
	s.pendingCommitMutex.Lock()
	s.isPendingCommitReady = true
	s.pendingCommitCond.Broadcast()
	s.pendingCommitMutex.Unlock()

	success := <-committed
	s.pendingCommitMutex.Lock()
	s.isPendingCommitReady = true
	s.pendingCommitCond.Broadcast()
	s.pendingCommitMutex.Unlock()
	if success {
		//commit to state machine
		ver, err := s.metaStore.UpdateFile(ctx, filemeta)
		if err != nil {
			return ver, err
		}
		s.lastApplied = s.commitIndex
		//s.commitIndex++
		log.Printf("Leader applied succesfully. lastApplied : %d, commitIndex: %d\n", s.lastApplied, s.commitIndex)
		return ver, nil
	} else {
		//leader crashed
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
		//leader step down
		return nil, ERR_NOT_LEADER
	}
}

//leader calls
func (s *RaftSurfstore) attemptCommit() {
	targetIdx := s.commitIndex + 1
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		//try to replicate on each follower
		go s.commitEntry(int64(idx), targetIdx, commitChan)
	}

	commitCount := 1 //include leader
	//crashCount := 0
	for { //retry
		//leader crash
		if s.isCrashed {
			s.pendingCommitCond.L.Lock()
			for !s.isPendingCommitReady {
				s.pendingCommitCond.Wait()
			}
			s.pendingCommitCond.L.Unlock()

			s.pendingCommits[targetIdx] <- false
			s.isPendingCommitReady = false
			break
		}
		//leader step down
		if !s.isLeader {
			s.pendingCommitCond.L.Lock()
			for !s.isPendingCommitReady {
				s.pendingCommitCond.Wait()
			}
			s.pendingCommitCond.L.Unlock()

			s.pendingCommits[targetIdx] <- false
			s.isPendingCommitReady = false
			break
		}
		//blocking
		commit := <-commitChan // commit == nil, indicates commit failed
		if commit != nil && commit.Success {
			commitCount++
		}
		//handle crashed follower
		/*
			if commit == nil {
				crashCount++
			}*/

		// what if ipList <= 2 ?
		if commitCount > len(s.ipList)/2 { //majority of followers commit succesfully, return
			s.commitIndex = targetIdx
			log.Printf("Leader is server %d, commitIndex %d. log length %d\n", s.serverId, s.commitIndex, len(s.log))
			s.pendingCommitCond.L.Lock()
			for !s.isPendingCommitReady {
				s.pendingCommitCond.Wait()
			}
			s.pendingCommitCond.L.Unlock()

			s.pendingCommits[targetIdx] <- true
			s.isPendingCommitReady = false
			break
		}

		/*
			if crashCount >= len(s.ipList)/2 {
				// request failed
				s.pendingCommits[targetIdx] <- false
				break
			}*/
	}
}

//leader calls
func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {

	prevLogIndex := int64(math.Min(float64(s.nextIndex[serverIdx]-1), float64(s.commitIndex)))
	prevLogTerm := int64(-1) //1. follower log is empty 2. leader log has only one entry, uncommitted
	if prevLogIndex != -1 {
		prevLogTerm = s.log[prevLogIndex].Term
	}
	entryStartIndex := prevLogIndex + 1 //1. follower log is empty 2. follower log non-empty 3. no update in leader log

	for { // retry if append failed
		if s.isCrashed { // leader crashed
			break
		}

		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			commitChan <- nil
			return
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s.nextIndex, etc
		input := &AppendEntryInput{ // empty leader log
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      []*UpdateOperation{}, // overwrite all follower's log entries or no update
			LeaderCommit: -1,
		}
		//log.Printf("log lenghth : %d\n", len(s.log))
		//non-empty leader log
		if len(s.log) != 0 {
			input = &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: prevLogIndex,
				Entries:      s.log[entryStartIndex:],
				LeaderCommit: s.commitIndex,
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				// Follower crashed, retry
				commitChan <- nil
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				// stale leader
				s.isLeader = false
				commitChan <- output
				return
			}
			log.Printf("Try to contact server %d  addr %s", serverIdx, addr)
			log.Printf(err.Error())
		}

		if output != nil {
			if output.Success {
				s.matchIndex[serverIdx] = output.MatchedIndex
				if s.nextIndex[serverIdx] <= output.MatchedIndex { // before appending, follower's log is less than leader
					s.nextIndex[serverIdx] = output.MatchedIndex + 1
				}
				commitChan <- output
				return
			} else {
				// 2. log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
				//prevLogIndex points at umatched, backtracing
				s.nextIndex[serverIdx] = prevLogIndex
				prevLogIndex--
				if prevLogIndex != -1 {
					prevLogTerm = s.log[prevLogIndex].Term
				} else { // all follower entries unmatched
					prevLogTerm = int64(-1)
				}
				entryStartIndex = prevLogIndex + 1
			}
		}
	}
}

//follower calls
//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	//panic("todo")
	output := &AppendEntryOutput{
		ServerId:     s.serverId,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}
	// follower crashed
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// update terrm
	if input.Term > s.term {
		s.isLeader = false
		s.term = input.Term
		output.Term = s.term
	}
	//1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		//reject request; Tell stale leader to step down
		return nil, ERR_NOT_LEADER
	}

	// update follower log
	if len(s.log) == 0 { // follower log is empty
		s.log = append(s.log, input.Entries...)
		log.Printf("Input Entries length : %d\n", len(input.Entries))
	} else { //follower log is nonempty
		if len(input.Entries) == 0 { //empty leader log
			s.log = append(s.log, input.Entries...)
		} else {
			//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
			//matches prevLogTerm (§5.3)
			// leader log only has one entry, and it is uncommitted, prevLogTerm == -1, prevLogIndex == -1
			// all follower entries unmatched with leader, prevLogTerm == -1, prevLogIndex == -1
			if input.PrevLogIndex == -1 {
				s.log = append(s.log, input.Entries...)
				// overwrite all follower log entries
			} else if input.PrevLogTerm != s.log[input.PrevLogIndex].Term {
				//reject request; Tell leader to decrement nextIndex
				return output, nil
			} else {
				//3. If an existing entry conflicts with a new one (same index but different
				//terms), delete the existing entry and all that follow it (§5.3)
				//4. Append any new entries not already in the log
				// TODO: need lock?
				s.log = append(s.log[:input.PrevLogIndex+1], input.Entries...)
			}
		}
	}

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	if input.LeaderCommit == -1 && len(input.Entries) == 1 { // leader has only one entry, and it is uncommitted, and will be commited after follower commits
		s.commitIndex = 0
	} else {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	//commit
	if len(s.log) != 0 {
		for s.lastApplied < s.commitIndex { // empty leader log, doesn't apply to state machine
			entry := s.log[s.lastApplied+1]
			res, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
			if err != nil {
				if res.Version == -1 {
					continue
				} else {
					log.Printf("Append entries: update file error")
					return nil, err
				}
			}
			s.lastApplied++
		}
	}

	output.Success = true
	output.MatchedIndex = int64(len(s.log) - 1) // empty follower log, -1

	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//panic("todo")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader { // new leader
		s.nextIndex = make([]int64, len(s.ipList))
		s.matchIndex = make([]int64, len(s.ipList))
		s.crashCount = 0

		for i := range s.nextIndex {
			// empty leader log, next index starts from 1
			//s.nextIndex[i] = int64(math.Max(1.0, float64(len(s.log))))
			s.nextIndex[i] = int64(len(s.log))
			s.matchIndex[i] = -1

		}

		s.pendingCommits = make(map[int64]chan bool, 0)
	}

	s.isLeader = true
	s.term++

	return &Success{
		Flag: true,
	}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed { // leader crashed
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader { // leader step down
		return nil, ERR_NOT_LEADER
	}
	isAliveChan := make(chan *AppendEntryOutput, len(s.ipList))
	//panic("todo")
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		// try each follower
		go s.SendHeartbeatSlave(addr, idx, isAliveChan)
	}

	s.crashCount = 0
	respCount := len(s.ipList) - 1 // exclude leader
	for {
		if s.isCrashed { // leader crashed
			return nil, ERR_SERVER_CRASHED
		}

		if !s.isLeader { // leader step down
			return nil, ERR_NOT_LEADER
		}

		isAliveOutput := <-isAliveChan
		if isAliveOutput == nil {
			s.crashCount++
		}
		respCount--
		if respCount == 0 {
			break
		}

	}

	return &Success{Flag: true}, nil
}

//leader calls
func (s *RaftSurfstore) SendHeartbeatSlave(addr string, idx int, isAliveChan chan *AppendEntryOutput) {

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		isAliveChan <- nil
		return
	}
	client := NewRaftSurfstoreClient(conn)

	prevLogIndex := int64(math.Min(float64(s.nextIndex[idx]-1), float64(s.commitIndex)))
	prevLogTerm := int64(-1) //1. follower log is empty 2. leader log has only one entry, uncommitted
	if prevLogIndex != -1 {
		prevLogTerm = s.log[prevLogIndex].Term
	}
	entryStartIndex := prevLogIndex + 1 //1. follower log is empty 2. follower log non-empty 3. no update in leader log 4. leader log empty
	output := new(AppendEntryOutput)

	for {
		input := &AppendEntryInput{ // empty leader log
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      []*UpdateOperation{}, // overwrite all follower's log entries or no update
			LeaderCommit: -1,
		}
		//log.Printf("log lenghth : %d\n", len(s.log))
		//non-empty leader log
		if len(s.log) != 0 {
			input = &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: prevLogIndex,
				Entries:      s.log[entryStartIndex:],
				LeaderCommit: s.commitIndex,
			}
		}
		// TODO create correct AppendEntryInput from s.nextIndex, etc

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err = client.AppendEntries(ctx, input)
		//log.Printf("slave looping\n")
		/*if output == nil {
			log.Printf("slave output is nil\n")
		}*/
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				// Follower crashed
				isAliveChan <- nil
				return
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				s.isLeader = false
				isAliveChan <- output
				return
			}
			log.Printf("Try to contact server %d  addr %s", idx, addr)
			log.Printf(err.Error())
		}
		if output != nil {
			// server is alive
			if output.Success {
				s.matchIndex[idx] = output.MatchedIndex
				if s.nextIndex[idx] <= output.MatchedIndex {
					s.nextIndex[idx] = output.MatchedIndex + 1
				}
				break
			} else {
				// 2. log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
				//backtracing
				s.nextIndex[idx] = prevLogIndex //follower needs to overwrite unmatched
				prevLogIndex--
				if prevLogIndex != -1 {
					prevLogTerm = s.log[prevLogIndex].Term
				} else { // all follower entries unmatched
					prevLogTerm = int64(-1)
				}
				entryStartIndex = prevLogIndex + 1
			}
		}
	}

	isAliveChan <- output

}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
