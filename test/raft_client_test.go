package SurfTest

import (
	"os"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	//	"time"
)

// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
func TestSyncTwoClientsSameFileLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromMetaFile(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromMetaFile(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}
}

func TestSyncTwoClientsMultipleUpdates(t *testing.T) {
	t.Logf("client1 creates and syncs. client1 modifies and syncs. client2 syncs. client2 modifies and syncs. client1 syncs.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	leaderIdx := 0

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	err = worker1.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		if err != nil {
			if state == nil {
				t.Log("nil file info map")
			}
			t.Log(err)
			t.Fail()
		}
		if state.Term != int64(1) {
			t.Logf("Server  %d should be in term %d, got %d", idx, 1, state.Term)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}

		if len(state.Log) != 2 {
			t.Logf("Server %d log length is wrong, got %d", idx, len(state.Log))
			t.Logf("Log file latest file verison: %d", state.Log[0].FileMetaData.Version)
			t.Fail()
		}
	}
	//test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	//test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	//test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	err = worker2.UpdateFile(file1, "update text")
	if err != nil {
		t.FailNow()
	}

	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		if err != nil {
			if state == nil {
				t.Log("nil file info map")
			}
			t.Log(err)
			t.Fail()
		}
		if state.Term != int64(1) {
			t.Logf("Server %d should be in term %d, got %d", idx, 1, state.Term)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}

		if len(state.Log) != 3 {
			t.Logf("Server %d log length is wrong, got %d", idx, len(state.Log))
			t.Logf("Log file latest file verison: %d", state.Log[0].FileMetaData.Version)
			t.Fail()
		}
	}
	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromMetaFile(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file1].Version != 3 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	/*c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}*/

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromMetaFile(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta1[file1].Version != 3 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}
	/*
		c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
		if e != nil {
			t.Fatalf("Could not read files in client base dirs.")
		}
		if !c {
			t.Fatalf("wrong file2 contents at client2")
		}*/
}

func TestSyncTwoClientsFileUpdateLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs. leader change. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	leaderIdx := 0

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	//file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs

	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		if err != nil {
			if state == nil {
				t.Log("nil file info map")
			}
			t.Log(err)
			t.Fail()
		}
		if state.Term != int64(1) {
			t.Logf("Server  %d should be in term %d, got %d", idx, 1, state.Term)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}

		if len(state.Log) != 1 {
			t.Logf("Server %d log length is wrong, got %d", idx, len(state.Log))
			//t.Logf("Log file latest file verison: %d", state.Log[0].FileMetaData.Version)
			t.Fail()
		}
	}
	//test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	//test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	//test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	t.Log("Change Leader\n")
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	leaderIdx = 1

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state.Term != int64(2) {
			t.Logf("Server %d should be in term %d", idx, 2)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}
	}

	err = worker2.UpdateFile(file1, "update text")
	if err != nil {
		t.FailNow()
	}

	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		if err != nil {
			if state == nil {
				t.Log("nil file info map")
			}
			t.Log(err)
			t.Fail()
		}
		if state.Term != int64(2) {
			t.Logf("Server %d should be in term %d, got %d", idx, 2, state.Term)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}

		if len(state.Log) != 2 {
			t.Logf("Server %d log length is wrong, got %d", idx, len(state.Log))
			//t.Logf("Log file latest file verison: %d", state.Log[0].FileMetaData.Version)
			t.Fail()
		}
	}
	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromMetaFile(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	/*c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}*/

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromMetaFile(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta1[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}
}
