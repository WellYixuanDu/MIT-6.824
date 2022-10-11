package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// Role
//
type Role int

const (
	ROLE_LEADER    Role = 0
	ROLE_FOLLOWER  Role = 1
	ROLE_CANDIDATE Role = 2
)

//
// log entry struct
//
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int
	logBuffer   []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	role Role

	heartbeatTimer *time.Timer
	electionTimer  *time.Timer

	applyCh chan ApplyMsg

	applyCond *sync.Cond
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getElectionTime() time.Duration {
	return time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生 150--350ms
}

func (rf *Raft) getHeartbeatTime() time.Duration {
	return (50 * time.Millisecond)
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logBuffer[len(rf.logBuffer)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logBuffer[len(rf.logBuffer)-1].Index
}

func (rf *Raft) getFirstLogIndex() int {
	return rf.logBuffer[0].Index
}

func (rf *Raft) getFirstLogTerm() int {
	return rf.logBuffer[0].Term
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	if rf.role == ROLE_LEADER {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

func (rf *Raft) becomeFollower(term int) {
	rf.role = ROLE_FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.role = ROLE_CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	MyPrint(rf, "getelectionTime in becomeCandidate")
	rf.electionTimer.Reset(rf.getElectionTime())
}

func (rf *Raft) becomeLeader() {
	rf.role = ROLE_LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.readySendEntries()
}

func (rf *Raft) needApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastApplied < rf.commitIndex {
		return true
	} else {
		return false
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logBuffer)

	return w.Bytes()
}

func (rf *Raft) persist() {

	data := rf.encodeState()
	//MyPrint(rf, "RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]", rf.me, rf.currentTerm, rf.votedFor, rf.logBuffer)
	rf.persister.SaveRaftState(data)

}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	MyPrint(rf, "the readPersist is used")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	LPrint(rf, "get the lock in readPersist")
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logBuffer)
	rf.commitIndex = rf.getFirstLogIndex()
	rf.lastApplied = rf.getFirstLogIndex()
	MyPrint(rf, "readPersist the rf.commitIndex is %v and the lastApplied is %v and the currentTerm is %v", rf.commitIndex, rf.lastApplied, rf.currentTerm)
}

// ----------------------------- snapshot begin -------------------------------------------------
// ----------------------------------------------------------------------------------------------

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) deleteIndexLog(index int) {
	logLen := rf.getLastLogIndex() - index + 1
	newLog := make([]LogEntry, logLen)
	copy(newLog, rf.logBuffer[index-rf.getFirstLogIndex():])
	rf.logBuffer = newLog
	rf.logBuffer[0].Command = nil
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	LPrint(rf, "get the lock in InstallSnapshot")

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	MyPrint(rf, "getelectionTime in InstallSnapshot")
	rf.electionTimer.Reset(rf.getElectionTime())
	if args.Term > rf.currentTerm {
		MyPrint(rf, "InstallSnapshot args.Term is %v, bigger than rf.currentTerm is %v", args.Term, rf.currentTerm)
		rf.becomeFollower(args.Term)
	}
	if args.LastIncludeIndex <= rf.commitIndex {
		return
	}
	rf.mu.Unlock()
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludeTerm,
			SnapshotIndex: args.LastIncludeIndex,
		}
	}()
	LPrint(rf, "release the lock in InstallSnapshot")
}

func (rf *Raft) sendSanpshotToPeer(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.currentTerm != args.Term {
		return false
	}
	if reply.Term > rf.currentTerm {
		MyPrint(rf, "InstallSnapshotReply.Term %v is bigger than rf.currentTerm %v", reply.Term, rf.currentTerm)
		rf.becomeFollower(reply.Term)
	} else {
		rf.nextIndex[server] = Max(rf.nextIndex[server], rf.getFirstLogIndex()+1)
		rf.matchIndex[server] = Max(rf.nextIndex[server], rf.getFirstLogIndex())
	}
	return true
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LPrint(rf, "get the lock in ConInstall Snapshot")
	if lastIncludedIndex < rf.commitIndex {
		MyPrint(rf, "CondInstallSnapshot rejects the snapshot The rf.commitIndex is %v is bigger than lastIncludeIndex %v", rf.commitIndex, lastIncludedIndex)
		LPrint(rf, "release the lock in ConInstall Snapshot")
		return false
	}

	if lastIncludedIndex >= rf.getLastLogIndex() {
		rf.logBuffer = make([]LogEntry, 1)
	} else {
		rf.deleteIndexLog(lastIncludedIndex)
	}
	rf.logBuffer[0].Term, rf.logBuffer[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	MyPrint(rf, "state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.role,
		rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLogIndex(), rf.getLastLogIndex(), lastIncludedTerm, lastIncludedIndex)
	LPrint(rf, "release the lock in ConInstall Snapshot")
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LPrint(rf, "get the lock in Snapshot")
	if index <= rf.getFirstLogIndex() || index > rf.commitIndex {
		MyPrint(rf, "Snapshot refused, bescause the index is %v, but the rf,getFirstLogIndex is %v", index, rf.getFirstLogIndex())
		LPrint(rf, "release the lock in Snapshot")
		return
	}
	// rf.deleteIndexLog(index)
	// rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	// LPrint(rf, "release the lock in Snapshot")

	for _, entry := range rf.logBuffer {
		if entry.Index == index {
			rf.deleteIndexLog(index)
			rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
			return
		}
	}
}

// ----------------------------- snapshot end ---------------------------------------------------
// ----------------------------------------------------------------------------------------------

// ----------------------------- leader select begin --------------------------------------------
// ----------------------------------------------------------------------------------------------

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) isLogUpToDate(term int, index int) bool {
	tmp_term, tmp_index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return term > tmp_term || (term == tmp_term && index >= tmp_index)
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {

	lastIndex := rf.getLastLogIndex()
	lastTerm := 0

	if lastIndex == rf.getFirstLogIndex() {
		lastTerm = rf.getFirstLogTerm()
	} else {
		lastTerm = rf.getLastLogTerm()
	}
	voteRequestArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	return &voteRequestArgs
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LPrint(rf, "ger the lock in RequestVote")
	if rf.killed() {
		reply.VoteGranted = false
		reply.Term = -1
		MyPrint(rf, "server killed")
		LPrint(rf, "release the lock in RequestVote")
		return
	}

	if rf.currentTerm > args.Term || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		MyPrint(rf, "currentTerm > args.Term, args.id is %v or votedFor others, refuse to vote", args.CandidateId)
		//MyPrint(rf, "the current time is %v", )
		LPrint(rf, "release the lock in RequestVote")
		return
	}

	// 状态重置
	if rf.currentTerm < args.Term {
		MyPrint(rf, "RequestVote rf.currentTerm is %v less than args.Term is %v", rf.currentTerm, args.Term)
		if rf.role == ROLE_LEADER {
			rf.electionTimer.Reset(rf.getElectionTime())
		}
		rf.becomeFollower(args.Term)
	}

	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		MyPrint(rf, "log does not match, refuse to vote")
		LPrint(rf, "release the lock in RequestVote")
		return
	}

	MyPrint(rf, "getelectionTime in RequestVote")
	rf.electionTimer.Reset(rf.getElectionTime())
	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.persist()
	MyPrint(rf, "vote successfully")

	LPrint(rf, "release the lock in RequestVote")
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) leaderSelect() {
	rf.votedFor = rf.me
	voteCount := 1
	voteRequestArgs := rf.genRequestVoteArgs()
	MyPrint(rf, "starts election with RequestVoteRequest %v", voteRequestArgs)
	rf.persist()
	for serveId, _ := range rf.peers {
		if rf.me == serveId {
			continue
		}

		go func(serveId int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(serveId, voteRequestArgs, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					MyPrint(rf, "finds a node %v with the higher term %v, change the role to follower", serveId, reply.Term)
					rf.becomeFollower(reply.Term)
				}
				if rf.role != ROLE_CANDIDATE || rf.currentTerm != voteRequestArgs.Term {
					return
				}
				MyPrint(rf, "receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", reply, serveId, voteRequestArgs, rf.currentTerm)
				if rf.currentTerm == reply.Term && rf.role == ROLE_CANDIDATE {
					if reply.VoteGranted {
						voteCount++
						if voteCount > len(rf.peers)/2 {
							MyPrint(rf, "win the majority votes in term %v", rf.currentTerm)

							voteCount = 0
							rf.becomeLeader()

						}
					}
					// } else if reply.Term > rf.currentTerm {
					// 	MyPrint(rf, "finds a node %v with the higher term %v, change the role to follower", serveId, reply.Term)
					// 	rf.becomeFollower(reply.Term)
					// }
				}
			}
		}(serveId)
	}
}

// ----------------------------- leader select end --------------------------------------------
// --------------------------------------------------------------------------------------------

// ----------------------------- append entries begin --------------------------------------------
// -----------------------------------------------------------------------------------------------
// committed 为应用到状态机上的 commited当日志条目在大多数followers中提交
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// appendEntries's status
type AppendEntriesStatus int

const (
	SUCCESS     AppendEntriesStatus = 0 // success
	OUTOFLEADER AppendEntriesStatus = 1 // fail
	CONFLICT    AppendEntriesStatus = 2
	COMMITED    AppendEntriesStatus = 3
)

type AppendEntriesReply struct {
	Term          int
	Status        AppendEntriesStatus
	ConflictIndex int
	ConflictTerm  int
}

// func (rf *Raft) applyLog() {
// 	rf.mu.Lock()
// 	LPrint(rf, "get the lock in applyLog")

// 	commitIndex := rf.commitIndex
// 	lastApplied := rf.lastApplied

// 	var applyEntries = make([]LogEntry, rf.commitIndex-rf.lastApplied, rf.commitIndex-rf.lastApplied)
// 	copy(applyEntries, rf.logBuffer[lastApplied+1-rf.getFirstLogIndex():commitIndex+1-rf.getFirstLogIndex()])
// 	rf.mu.Unlock()

// 	//解锁后进行apply
// 	for _, entry := range applyEntries {
// 		rf.applyCh <- ApplyMsg{
// 			CommandValid: true,
// 			Command:      entry.Command,
// 			CommandIndex: entry.Index,
// 		}
// 	}

// 	rf.mu.Lock()
// 	rf.lastApplied = Max(rf.lastApplied, commitIndex)
// 	rf.mu.Unlock()
// 	LPrint(rf, "release the lock in applyLog")
// }

func (rf *Raft) doApplyLog(applyCh chan ApplyMsg) {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for rf.killed() == false {
		for !rf.needApply() {
			rf.applyCond.Wait()
			if rf.killed() {
				return
			}
		}
		rf.mu.Lock()
		MyPrint(rf, "ready to do ApplyLog, the rf.lastApplied is %v and the rf.commitIndex is %v", rf.lastApplied, rf.commitIndex)
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		firstIndex := rf.getFirstLogIndex()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		var applyEntries = make([]LogEntry, commitIndex-lastApplied)
		copy(applyEntries, rf.logBuffer[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		//解锁后进行apply
		for _, entry := range applyEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
		MyPrint(rf, "apply succeed, the rf.lastApplied is %v and the rf.commitIndex is %v", rf.lastApplied, rf.commitIndex)
		LPrint(rf, "release the lock in applyLog")
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LPrint(rf, "get the lock in Start")
	if rf.role != ROLE_LEADER || rf.killed() {
		index := -1
		term := -1
		isLeader := false

		LPrint(rf, "release the lock in Start")
		return index, term, isLeader
	}
	term := rf.currentTerm
	addIndex := rf.getLastLogIndex() + 1
	logEntry := LogEntry{Command: command, Term: term, Index: addIndex}
	rf.logBuffer = append(rf.logBuffer, logEntry)
	// note
	//index := len(rf.logBuffer) - 1
	index := rf.getLastLogIndex()
	isLeader := true
	MyPrint(rf, "the leader append a log, and now the length is %v", len(rf.logBuffer))
	rf.persist()
	LPrint(rf, "release the lock in Start")
	return index, term, isLeader
}

func (rf *Raft) matchLog(term int, index int) bool {
	if rf.getLastLogIndex() < index {
		return false
	}
	return rf.logBuffer[index-rf.getFirstLogIndex()].Term == term
}

func (rf *Raft) findLastTermLog(term int) int {
	i := 1
	flag := 0
	for i < len(rf.logBuffer) {
		if rf.logBuffer[i].Term == term {
			flag = 1
		}
		if flag == 1 && rf.logBuffer[i].Term != term {
			return i - 1
		}
		i++
	}
	return -1
}

func (rf *Raft) getMajorityIndex() int {
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
	idx := (len(tmp)) / 2
	return tmp[idx]
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//	MyPrint(rf, "Before Lock to appendEntries, len(entries) is %v", len(args.Entries))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LPrint(rf, "get the lock in appendEntries")
	defer MyPrint(rf, "state is {state %v,term %v,commitIndex %v,lastApplied %v} before processing", rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	MyPrint(rf, "ready to appendEntries, len(entries) is %v", len(args.Entries))

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Status = OUTOFLEADER
		MyPrint(rf, "AppendEntries rf.currentTerm > %d.Term, Wrong", args.LeaderID)
		LPrint(rf, "release the lock in appendEntries")
		return
	}
	// if rf.currentTerm < args.Term {
	// 	rf.currentTerm = args.Term
	// 	rf.votedFor = -1
	// }
	rf.becomeFollower(args.Term)
	MyPrint(rf, "getelectionTime in AppendEntries")
	rf.electionTimer.Reset(rf.getElectionTime())
	// 日志是否冲突
	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		MyPrint(rf, "log is not match")
		reply.Term, reply.Status = rf.currentTerm, CONFLICT
		lastIndex := rf.getLastLogIndex()
		//		MyPrint(rf, "The detailed information is lastindex is %v, the args.PrevLogIndex is %v", lastIndex, args.PrevLogIndex)
		if lastIndex < args.PrevLogIndex {
			reply.ConflictIndex = lastIndex + 1
			reply.ConflictTerm = -1
		} else {
			firstIndex := rf.getFirstLogIndex()
			reply.ConflictTerm = rf.logBuffer[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex - 1
			//			MyPrint(rf, "the replt.ConflictTerm is %v, the index is %v", reply.ConflictTerm, index)
			for index >= firstIndex && rf.logBuffer[index-firstIndex].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		rf.persist()

		LPrint(rf, "release the lock in appendEntries")
		return
	}

	reply.Status = SUCCESS
	reply.Term = rf.currentTerm
	if args.Entries != nil {
		MyPrint(rf, "The log buffer length is %v before", len(rf.logBuffer))
		// todo change the copy method
		rf.logBuffer = rf.logBuffer[:args.PrevLogIndex+1-rf.getFirstLogIndex()]
		rf.logBuffer = append(rf.logBuffer, args.Entries...)
		MyPrint(rf, "success append into the logbuffer, the length is %v, the log is %v", len(rf.logBuffer), rf.logBuffer)
	}
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
		MyPrint(rf, "commit succed,the commitIdex is %v", rf.commitIndex)
		if rf.commitIndex > rf.lastApplied {
			LPrint(rf, "ready to broadcast")
			rf.applyCond.Broadcast()
		}
		//go rf.applyLog()
	}
	rf.persist()

	LPrint(rf, "release the lock in appendEntries")
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	//	MyPrint(rf, "wait for the append entries status")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok == false {
		//	MyPrint(rf, "the appendEntries reply is false")
		return false
	}
	LPrint(rf, "get the lock in sendAppendEntries")
	if reply.Term > rf.currentTerm {
		MyPrint(rf, "sendAppendEntries replt.Term is %v, bigger than rf.currentTerm %v", reply.Term, rf.currentTerm)
		if rf.role == ROLE_LEADER {
			rf.electionTimer.Reset(rf.getElectionTime())
		}
		rf.becomeFollower(reply.Term)
	}
	if rf.role != ROLE_LEADER || rf.currentTerm != args.Term {

		LPrint(rf, "release the lock in sendAppendEntries")
		return false
	}
	//	MyPrint(rf, "append the entries the reply is %v, the status is %v", ok, reply.Status)
	switch reply.Status {
	case SUCCESS:
		if rf.role == ROLE_LEADER {
			// need to delete
			//rf.electionTimer.Reset(rf.getElectionTime())
			MyPrint(rf, "receive the appendentries, the reply is true")
			// add term 不一致
			if reply.Term != rf.currentTerm {
				MyPrint(rf, "error ,the term has become the old, reply.Term is %v, and the rf.currentTerm is %v", reply.Term, rf.currentTerm)

				LPrint(rf, "release the lock in sendAppendEntries")
				return false
			}

			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			MyPrint(rf, "The rf.matchIndex[server] is %v, the prevLogIndex is %v, the len(args.Entries) is %v", rf.matchIndex[server], args.PrevLogIndex, len(args.Entries))
			majorityIndex := rf.getMajorityIndex()
			//	MyPrint(rf, "the matchIndex is %v", rf.matchIndex)
			//	MyPrint(rf, "Commit info: majorityIndex is %v, rf,logBuffer[majorityIndex].Term is %v, rf.currentTerm is %v, the rf.commitIndex is %v", majorityIndex, rf.logBuffer[majorityIndex].Term, rf.currentTerm, rf.commitIndex)
			if rf.logBuffer[majorityIndex-rf.getFirstLogIndex()].Term == rf.currentTerm && majorityIndex > rf.commitIndex {
				rf.commitIndex = majorityIndex
				MyPrint(rf, "leader commit succeed, the commitIndex is %v", rf.commitIndex)
				//	go rf.applyLog()
				if rf.commitIndex > rf.lastApplied {
					LPrint(rf, "ready to broadcast")
					rf.applyCond.Broadcast()
				}

			}
		}

	case OUTOFLEADER:
		rf.becomeFollower(reply.Term)
		MyPrint(rf, "OUTOFLEADER")

	case CONFLICT:
		if args.Term != rf.currentTerm {
			return false
		}
		lastTermLog := rf.findLastTermLog(reply.ConflictTerm)
		if lastTermLog != -1 {
			rf.nextIndex[server] = lastTermLog + 1
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
		MyPrint(rf, "CONFLICT, rf.nextIndex[%v] is %v", server, reply.ConflictIndex)

	}
	rf.persist()
	LPrint(rf, "release the lock in sendAppendEntries")
	return ok
}

func (rf *Raft) readySendEntries() {
	for serverID, _ := range rf.peers {
		if serverID == rf.me {
			rf.nextIndex[serverID] = rf.getLastLogIndex() + 1
			rf.matchIndex[serverID] = rf.getLastLogIndex()
		} else {
			if rf.nextIndex[serverID] <= rf.getFirstLogIndex() {
				MyPrint(rf, "ready send snapshot to %v node", serverID)
				args := InstallSnapshotArgs{
					Term:             rf.currentTerm,
					LeaderId:         rf.me,
					LastIncludeIndex: rf.getFirstLogIndex(),
					LastIncludeTerm:  rf.getFirstLogTerm(),
					Data:             rf.persister.ReadSnapshot(),
				}
				reply := InstallSnapshotReply{}
				// 进行远程RPC调用时需要另起一个线程进行调用，否则会导致主线程阻塞。
				go rf.sendSanpshotToPeer(serverID, &args, &reply)
				return

			} else {
				logLen := len(rf.logBuffer)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[serverID] - 1,
					PrevLogTerm:  rf.logBuffer[rf.nextIndex[serverID]-1-rf.getFirstLogIndex()].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}

				if rf.getLastLogIndex() >= rf.nextIndex[serverID] {
					// args.PrevLogIndex = rf.nextIndex[serverID] - 1
					// args.PrevLogTerm = rf.logBuffer[args.PrevLogIndex].Term
					args.Entries = make([]LogEntry, rf.getLastLogIndex()-rf.nextIndex[serverID]+1)
					copy(args.Entries, rf.logBuffer[(rf.nextIndex[serverID]-rf.getFirstLogIndex()):])
					//	MyPrint(rf, "copy the entries is %v and the args.Entries is %v", rf.logBuffer[rf.nextIndex[serverID]:], args.Entries)
				}
				MyPrint(rf, "send entries process, the logLen is %v, the rf.nextIndex[%v] is %v, the log is %v", logLen, serverID, rf.nextIndex[serverID], rf.logBuffer)
				go rf.sendAppendEntries(serverID, &args, &reply)
			}

		}
	}
	rf.heartbeatTimer.Reset(rf.getHeartbeatTime())
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// 重构计时器
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			if rf.killed() {
				MyPrint(rf, "The rf has been killed")
				return
			}
			rf.mu.Lock()
			LPrint(rf, "get the lock in electionTimer ticker")
			if rf.role != ROLE_LEADER {
				LPrint(rf, "electionTimer get the lock")
				rf.becomeCandidate()
				MyPrint(rf, "begin to leaderSelect")
				rf.leaderSelect()

				LPrint(rf, "electionTimer release the lock")
			}
			LPrint(rf, "release the lock in electionTimer ticker")
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			//	MyPrint(rf, "send the heartbeat, the role is the %v", rf.role)
			if rf.killed() {
				MyPrint(rf, "The rf has been killed")
				return
			}
			//	LPrint(rf, "ready to send entries")
			rf.mu.Lock()
			LPrint(rf, "get the lock in heartbeatTimer ticker")
			//	LPrint(rf, "get the LOCK")
			if rf.role == ROLE_LEADER {
				MyPrint(rf, "ready send entries")
				rf.readySendEntries()
				//	rf.heartbeatTimer.Reset(rf.getHeartbeatTime())
			}
			LPrint(rf, "release the lock in heartbeatTimer ticker")
			rf.mu.Unlock()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	// logentry从下标索引为1的地方开始存储
	rf.logBuffer = []LogEntry{}
	rf.logBuffer = append(rf.logBuffer, LogEntry{nil, 0, 0})
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.role = ROLE_FOLLOWER
	rf.applyCh = applyCh

	go rf.doApplyLog(applyCh)
	rf.electionTimer = time.NewTimer(rf.getElectionTime())
	rf.heartbeatTimer = time.NewTimer(rf.getHeartbeatTime())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
