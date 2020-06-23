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

import "sync"
import "time"
import "sync/atomic"
import "math/rand"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// constant value
//
const heartBeatRate = 150
const randomTimeRange = 200
const timeoutMin = 400
const follower = 1
const candidate = 2
const leader = 3

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// struct for each log entry
//
type Entry struct {
	Term    int
	Index   int
	Command interface{}
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

	n        int           // number of peers
	majority int           // number of majority, n/2
	applyCh  chan ApplyMsg // ApplyMsg Channel

	// Look at the paper's Figure 2 for a description of what state a Raft server must maintain.
	state           int       // follower(1), candidate(2), leader(3)
	electionTimeout time.Time // random

	// persistent states on all servers
	curTerm int     // latest term server has seen on first boot
	voteFor int     // candidateID that received vote in current term
	log     []Entry // log entries (each contains command, term, index)

	// Volatile states on all servers
	commitIndex int // index of highest log entry known to be committed (init=0)
	lastApplied int // index of highest log entry applied to state machine (init=0)

	// Volatile states on Leaders (re-init after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest logy entry known to be replicated

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order.
//
// persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any.
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyMsg messages.
//
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())

	rf := &Raft{}
	rf.me = me
	rf.peers = peers
	rf.persister = persister
	rf.n = len(peers)
	rf.majority = (rf.n + 1) / 2
	rf.applyCh = applyCh

	DPrintf("[%d] raft init\n", rf.me)
	rf.state = follower // init as follower
	rf.resetElectionTimeOut()

	rf.curTerm = 0
	rf.voteFor = -1
	rf.log = append(rf.log, Entry{0, 0, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	// start go routine
	go rf.routine()
	go rf.tryCommit()
	go rf.checkCommitIndex()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//
// return currentTerm and whether this server
// believes it is the leader.
//	1. LOCK
// 	2. UNLOCK
//
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, rf.state == leader
}

//
// reset election timeout
// call by Make(), RequestVote(RPC), AppendEntries(RPC)
//	1. LOCK
// 	2. UNLOCK
//
func (rf *Raft) resetElectionTimeOut() {
	rf.mu.Lock()
	rf.electionTimeout = time.Now().Add(time.Duration(rand.Int63n(randomTimeRange)+timeoutMin) * time.Millisecond)
	rf.mu.Unlock()
}

//
// routine for each raft.
//	1. LOCK
// 		go checkTimeout()
// 		go heartbeat()
// 	2. UNLOCK
//
func (rf *Raft) routine() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		// DPrintf("[%d](%d) state(%d).\n", rf.me, rf.curTerm, rf.state)
		if rf.state == follower {
			// routine for follower
			// only need check timeout periodcially
			// reset election tiemout when received valid 'AppendEntries',
			// when timeout, convert to candaite state, call raiseVote
			go rf.checkElectionTimeout()

		} else if rf.state == candidate {
			// routine for candidate
			// rf.raiseVote() happen on conversion from follower to candidate
			// 		if win, convert to leader
			// 		if timeout during raiseVote, go raiseVote() again
			// 		if receive valid 'AppendEntries' from leader, convert to follower
			go rf.checkElectionTimeout()

		} else if rf.state == leader {
			// 1. send periodic heartbeat to all followers
			go rf.heartbeat()
			// if command received from client, append entry to local log [in func Start()]
			// respond after entry applied to state machine
			// if last log idnex >= nextIndex for this follower
			// 	  send AppendEntries with log entries starting at nextIndex
			// 		if successful: update nextIndex and matchIndex for follower
			// 		else decrement nextIndex and retry
		}
		rf.mu.Unlock()

		time.Sleep(heartBeatRate * time.Millisecond)
	}
}

//
// check whether election timeout, raise vote if so
// call by go.routine()
//	1. LOCK
//		raiseVote
// 	2. UNLOCK
//
func (rf *Raft) checkElectionTimeout() {
	rf.mu.Lock()

	now := time.Now()
	if now.After(rf.electionTimeout) { // if election timeout, become candidate

		rf.state = candidate            // become candidate, ready to request vote
		rf.curTerm++                    // increment current Term whenever timeout
		rf.voteFor = -1                 // reset voteFor
		rf.electionTimeout = time.Now() // reset election timer

		DPrintf("[%d] election Timeout, become candidate, start election, Term [%d].\n", rf.me, rf.curTerm)
		rf.raiseVote() // raise a request vote
	}
	rf.mu.Unlock()
}

// ----------------------------------------2B----------------------------------------
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
// if this server isn't the leader, returns false.
// otherwise start the agreement and return immediately.
// there is no guarantee that this command will ever be committed to the Raft log,
// since the leader may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current term.
// the third return value is true if this server believes it is the leader.
//
// 	LOCK
// 		if is leader
// 			append command to the log
// 	UNLOCK
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	term := rf.curTerm
	index := len(rf.log)
	isLeader := rf.state == leader
	if isLeader {
		L2BPrint("[%d](%d.%d)(leader) RECEIVE CLIENT!......................!\n", rf.me, term, index)
		rf.log = append(rf.log, Entry{term, index, command})
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// increment rf.commitIndex if majority has commit on rf.commitIndex
func (rf *Raft) checkCommitIndex() {
	for {
		rf.mu.Lock()
		if rf.state == leader {
			// find possible N
			for N := len(rf.log); N > rf.commitIndex; N-- {
				cnt := 0
				for _, matchIndex := range rf.matchIndex {
					// don't commit previous term
					if matchIndex >= N && rf.log[N].Term == rf.curTerm {
						cnt++
					}
				}
				if cnt+1 >= rf.majority {
					rf.commitIndex = N
					break
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(heartBeatRate * time.Millisecond)
	}
}

//
// go routine to check and update commitIndex
//
func (rf *Raft) tryCommit() {

	prevCommit := 0
	for {
		rf.mu.Lock()
		for rf.commitIndex > prevCommit {
			prevCommit++
			L2BPrint("[%d] commit %d \n", rf.me, prevCommit)
			rf.applyCh <- ApplyMsg{true, rf.log[prevCommit].Command, rf.log[prevCommit].Index}
		}
		rf.mu.Unlock()
		time.Sleep(heartBeatRate * time.Millisecond)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any routine with a long-running loop
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

// ----------------------------------------2C----------------------------------------
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
