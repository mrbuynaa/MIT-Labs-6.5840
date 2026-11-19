package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state string // leader, follower, or candidate

	log         []interface{}
	votedFor    int
	currentTerm int
	commitIndex int
	lastLogIdx  int
	lastLogTerm int
	lastApplied int
	isLeaderOk  bool

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	isleader := rf.state == "leader"
	return rf.currentTerm, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (self *Raft) StartElection() {
	self.currentTerm += 1
	self.state = "candidate"
	self.votedFor = self.me
	args := RequestVoteArgs{
		CandidateId:  self.me,
		Term:         self.currentTerm,
		LastLogIndex: self.lastLogIdx,
		LastLogTerm:  self.lastLogTerm,
	}

	votes := 1
	majority := len(self.peers)/2 + 1
	totalPeers := len(self.peers)

	ch := make(chan bool, totalPeers-1)

	for idx := range self.peers {
		if idx == self.me {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply

			for {
				ok := self.sendRequestVote(peer, &args, &reply)
				if ok {
					ch <- reply.VoteGranted
					return
				}
			}
		}(idx)
	}

	for i := 0; i < len(self.peers)-1; i++ {
		if <-ch {
			votes++
		}
		if votes >= majority {
			self.state = "leader"
			return
		}
	}

	if votes < majority {
		self.state = "follower"
	}

}

func (self *Raft) RequestVote(candidate *RequestVoteArgs, reply *RequestVoteReply) {
	// already voted in this term
	if self.votedFor != 0 && self.currentTerm == candidate.Term {
		reply.VoteGranted = false
		reply.Term = self.currentTerm
		return
	}

	if candidate.Term < self.currentTerm {
		reply.VoteGranted = false
		reply.Term = self.currentTerm
		return
	}

	if candidate.LastLogIndex >= self.lastLogIdx && candidate.LastLogTerm >= self.lastLogTerm {
		if candidate.Term > self.lastLogTerm {
			self.currentTerm = candidate.Term
		}
		self.votedFor = candidate.CandidateId // Үүнийг хэзээ null болгох ёстой вэ?
		self.isLeaderOk = true                // Олон давхар election болохгүй гэдгийг яаж батлах билээ?

		reply.VoteGranted = true
		reply.Term = self.currentTerm
	} else {
		self.isLeaderOk = false
	}
}

func (self *Raft) ReplicateLog(log []interface{}, replicated chan<- bool) {
	args := AppendEntriesArgs{
		LeaderId:     self.me,
		Term:         self.currentTerm,
		PrevLogIndex: len(self.log) - 1,
		PrevLogTerm:  self.lastLogTerm,
		Entries:      log,
		LeaderCommit: self.commitIndex,
	}
	majority := len(self.peers)/2 + 1
	totalPeers := len(self.peers)

	replicatedNode := 0
	ch := make(chan bool, totalPeers-1)

	for idx := range self.peers {
		if idx == self.me {
			continue
		}

		go func(peer int) {
			var reply AppendEntriesReply

			for {
				ok := self.sendAppendEntries(peer, &args, &reply)
				if ok {
					ch <- reply.Success
					return
				}
			}
		}(idx)
	}
	// Хэрэв зарим node-үүд нь killed болсон байвал яах вэ? Killed болсон node-үүдийг хасах нь байна.
	for i := 0; i < len(self.peers)-1; i++ {
		if <-ch {
			replicatedNode += 1
		}
		if replicatedNode == majority {
			replicated <- true
		}
	}

	if replicatedNode < majority {
		replicated <- false
	}
}

func (self *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.isLeaderOk = true
	currentIdx := len(self.log) - 1
	if currentIdx == args.PrevLogIndex && self.lastLogTerm == args.PrevLogTerm {
		self.log = append(self.log, args.Entries...)
		self.lastLogTerm = args.Term
	} else {
		// Log нь зөрсөн байна.
	}
}

func (self *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := self.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (self *Raft) Start(command interface{}) (int, int, bool) {
	self.mu.Lock()
	defer self.mu.Unlock()
	replicated := make(chan bool)
	isLeader := self.state == "leader"
	if !isLeader {
		return -1, -1, isLeader
	}

	logs := []interface{}{command}
	self.ReplicateLog(logs, replicated)

	if <-replicated {
		self.log = append(self.log, command)
		self.lastLogTerm = self.currentTerm
		return len(self.log) - 1, self.currentTerm, isLeader
	}
	return -1, -1, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startDaemon() {
	for rf.killed() == false {
		rf.mu.Lock()
		// Check if a leader election should be started.
		if !rf.isLeaderOk {
			rf.StartElection()
		}

		rf.isLeaderOk = false
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	rf.state = "follower"
	rf.isLeaderOk = true

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastLogIdx = 0
	rf.lastLogTerm = 0
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startDaemon()

	return rf
}
