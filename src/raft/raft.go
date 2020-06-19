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
const heartBeatRate = 125
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           int       // follower(1), candidate(2), leader(3)
	electionTimeout time.Time // random

	// persistent
	curTerm int
	voteFor int
	log     []Entry

	// Volatile
	CommitIndex int
	lastApplied int

	// Leader States: 		// re-init after election
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
	rf.log = append(rf.log, Entry{0, 0, nil})
	rf.applyCh = applyCh

	DPrintf("[%d] raft init\n", rf.me)
	rf.state = follower // init as follower
	rf.resetElectionTimeOut()

	// start go routine
	go rf.routine()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
//	1. LOCK
// 	2. UNLOCK
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, rf.state == leader
}

// reset election timeout
// call by Make(), RequestVote(RPC), AppendEntries(RPC)
//	1. LOCK
// 	2. UNLOCK
func (rf *Raft) resetElectionTimeOut() {
	rf.mu.Lock()
	rf.electionTimeout = time.Now().Add(time.Duration(rand.Int63n(randomTimeRange)+timeoutMin) * time.Millisecond)
	rf.mu.Unlock()
}

// routine for each raft.
//	1. LOCK
// 		go checkTimeout()
// 		go heartbeat()
// 	2. UNLOCK
func (rf *Raft) routine() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		DPrintf("[%d](%d) state(%d).\n", rf.me, rf.curTerm, rf.state)
		if rf.state == follower {
			// routine for follower
			// only need check time periodcially
			// when received AppendEntry(heartbeat), timeout been reset
			// when timeout, convert to candaite state, call raiseVote
			go rf.checkElectionTimeout()

		} else if rf.state == candidate {
			// routine for candidate
			// 1. rf.raiseVote() happen on conversion from follower to candidate
			// 		if win, convert to leader
			// 2. if receive AppendEntry(heartbeat) from leaders passively
			//   	if RPC received has smaller term (not as up-to-date), rejects this RPC, continue as candidate
			//		otherwise, return to follower
			// 3. if timeout with no winner start new election
			go rf.checkElectionTimeout()

		} else if rf.state == leader {
			// 1. send periodic heartbeat to all followers
			go rf.heartbeat()

			// if command received from client, append entry to local log
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

// check whether election timeout, raise vote if so
// call by go.routine()
//	1. LOCK
//		raiseVote
// 	2. UNLOCK
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

// **************************************************************
// ****************** Candidate RequestVote RPC *****************
// **************************************************************
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// request vote from all other servers
// call by checkElectionTimeout() (enter with LOCK)
// 	1. UNLOCK
// 	waitGroup
// 		go func()
// 			sendRequestVote
//			waitReply...
// 			LOCK  (process reply after LOCK)
// 				if become leader
//					go heartbeat()
// 			UNLOCK
//	wait()
//	2. LOCK
//
func (rf *Raft) raiseVote() {

	DPrintf("[%d] raise vote\n", rf.me)

	lastLog := rf.log[len(rf.log)-1]
	rf.voteFor = rf.me

	// load args (information about candidate)
	args := RequestVoteArgs{}
	args.Term = rf.curTerm
	args.CandidateID = rf.me
	args.LastLogTerm = lastLog.Term
	args.LastLogIndex = lastLog.Index

	// number of granted from receivers (local in this function)
	granted := 1

	rf.mu.Unlock()
	DPrintf("[%d] raise vote inside unlock\n", rf.me)

	// use waitGroup to wait reply from other servers
	wg := sync.WaitGroup{}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			reply := RequestVoteReply{}
			rf.sendRequestVote(id, &args, &reply)

			rf.mu.Lock()
			DPrintf("[%d] sendRequestVote to [%d]. at term (%d vs. %d)\n", rf.me, id, rf.curTerm, args.Term)
			// what if break in for loop below and no receiver channel

			if rf.curTerm == args.Term { // current term can be newer than that at the sending time
				if reply.VoteGranted {
					DPrintf("[%d] vote granted from [%d]\n", rf.me, id)
					granted++
					if rf.state == candidate && granted >= rf.majority {
						DPrintf("[%d] BECOMES NEW LEADER!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n", rf.me)
						rf.state = leader
						go rf.heartbeat()
					}
				} else {
					DPrintf("[%d] vote rejected from [%d]\n", rf.me, id)
				}
			}
			rf.mu.Unlock()
		}(i)
	}
	wg.Wait()
	rf.mu.Lock()
}

//
// The labrpc package simulates a lossy network, in which servers may be unreachable,
// and in which requests and replies may be lost.
// Call() sends a request and waits for a reply.
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by
// 1. a dead server
// 2. a live server that can't be reached
// 3. a lost request
// 4. a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RequestVote RPC handler.(receiver raft called by candidate RPC)
// 	1. LOCK
// 	2. defer UNLOCK
// 	 if curTerm < candidate
//		UNLOCK
//		resetElectionTimeOut() (LOCK+UNLOCK)
//		LOCK
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	candidateTerm := args.Term
	candidateID := args.CandidateID
	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm
	reply.Term = rf.curTerm

	DPrintf("[%d](%d) receive 'RequestVote' from [%d](%d)\n", rf.me, rf.curTerm, candidateID, candidateTerm)

	// reject candidate's term if it's out-of-date
	if rf.curTerm > candidateTerm {
		DPrintf("[%d](%d) reject [%d](%d)(out-of-date)\n", rf.me, rf.curTerm, candidateID, candidateTerm)
		reply.VoteGranted = false
		return
	}
	// update receiver's term if it's out-of-date
	if rf.curTerm < candidateTerm {
		DPrintf("[%d](%d)(out-of-date become FOLLOWER), accept candidate[%d]'s term (%d)\n", rf.me, rf.curTerm, candidateID, candidateTerm)
		rf.curTerm = candidateTerm // update current term
		rf.voteFor = -1            // clear voteFor (make sure previously voteFor can't have majority)
		rf.state = follower        // become follower

		rf.mu.Unlock()
		rf.resetElectionTimeOut()
		rf.mu.Lock()
	}

	// use last log term/index to compare
	lastLog := rf.log[len(rf.log)-1]
	DPrintf("lastLogTerm: [%d](%d) vs. candidate [%d](%d)\n", rf.me, lastLog.Term, candidateID, candidateLastLogTerm)
	if lastLog.Term < candidateLastLogTerm {
		DPrintf("lastLogTerm: [%d] < [%d](candidate)\n", rf.me, candidateID)
		if rf.voteFor == -1 {
			DPrintf("[%d] has no voteFor -> vote for [%d]\n", rf.me, candidateID)
			rf.voteFor = candidateID
			reply.VoteGranted = true
		} else {
			DPrintf("[%d] has voteFor [%d] -> reject vote for [%d]\n", rf.me, rf.voteFor, candidateID)
			reply.VoteGranted = false
		}

	} else if lastLog.Term == candidateLastLogTerm { // same term, should compare lastLogIndex
		DPrintf("lastLogTerm: [%d] == [%d](candidate) compare index\n", rf.me, candidateID)

		if lastLog.Index <= candidateLastLogIndex {
			DPrintf("lastLogIndex: [%d](%d) <= [%d](%d)(candidate).\n", rf.me, lastLog.Index, candidateID, candidateLastLogIndex)
			if rf.voteFor == -1 {
				DPrintf("[%d] has no voteFor -> vote for [%d]\n", rf.me, candidateID)
				rf.voteFor = candidateID
				reply.VoteGranted = true
			} else {
				DPrintf("[%d] has voteFor [%d] -> reject vote for [%d]. \n", rf.me, rf.voteFor, candidateID)
				reply.VoteGranted = false
			}
		} else {
			DPrintf("lastLogIndex: [%d](%d) <= [%d](%d)(candidate) -> reject vote\n", rf.me, lastLog.Index, candidateID, candidateLastLogIndex)
			reply.VoteGranted = false
		}
	} else if lastLog.Term > candidateLastLogTerm {
		DPrintf("lastLogTerm: [%d] > [%d](candidate) -> reject vote\n", rf.me, candidateID)
		reply.VoteGranted = false
	}
	return
}

// *************** Candidate RequestVote RPC DONE ***************

// **************************************************************
// ***************** LEADER AppendEntries RPC  ******************
// **************************************************************
type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderID     int     // leader ID
	PrevLogIndex int     //index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex Entry
	Entries      []Entry // log entries to store (empty for heartbeat) // TODO: send more than one
	LeaderCommit int     // leader commit index
}

type AppendEntriesReply struct {
	Term    int  // current term
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// 1. LOCK
// load args (leader info)
// 2. UNLOCK
//
// go func() sendAppendEntries
//  waitReply...
//  TODO: send message/command
// 	LOCK
// 	UNLOCK
//
func (rf *Raft) heartbeat() {
	DPrintf("[%d] sends heartbeat\n", rf.me)

	args := AppendEntriesArgs{}
	rf.mu.Lock()
	args.Term = rf.curTerm
	args.LeaderID = rf.me
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(id int) {

				reply := AppendEntriesReply{}
				rf.sendAppendEntries(id, &args, &reply)

				rf.mu.Lock()
				if reply.Term > rf.curTerm {
					DPrintf("[%d] 'AppendEntries' out-of-date(become FOLLOWER)\n", rf.me)
					rf.curTerm = reply.Term // update current term
					rf.voteFor = -1         // clear voteFor (make sure previously voteFor can't have majority)
					rf.state = follower     // become follower
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}

// AppendEntries RPC handler.(receiver raft called by leader RPC)
// 	1. LOCK
// 	2. defer UNLOCK
//	  UNLOCK
//	  resetElectionTimeOut() (LOCK+UNLOCK)
//	  LOCK
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { // heart beat
	rf.mu.Lock()
	defer rf.mu.Unlock()

	leaderTerm := args.Term
	leaderID := args.LeaderID
	// leaderPrevLogIndex := args.PrevLogIndex
	// leaderPrevLogTerm := args.PrevLogTerm
	// leaderEntries := args.Entries
	// leaderCommit := args.LeaderCommit
	reply.Term = rf.curTerm

	DPrintf("[%d](%d) receive 'AppendEntries' from [%d](%d)\n", rf.me, rf.curTerm, leaderID, leaderTerm)

	// reject leader's term if it's out-of-date
	if rf.curTerm > leaderTerm {

		DPrintf("[%d](%d) reject [%d](%d)(out-of-date)\n", rf.me, rf.curTerm, leaderID, leaderTerm)
		reply.Success = false
		// rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()
	rf.resetElectionTimeOut()
	rf.mu.Lock()

	if rf.curTerm <= leaderTerm {
		DPrintf("[%d](%d)(out-of-date become FOLLOWER), accept leader[%d]'s term (%d)\n", rf.me, rf.curTerm, leaderID, leaderTerm)

		rf.curTerm = leaderTerm // update current term
		rf.voteFor = -1         // clear voteFor (make sure previously voteFor can't have majority)
		rf.state = follower     // become follower
		// reply.Success = true
	}
	// rf.mu.Unlock()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// **************** LEADER AppendEntries RPC ****************

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
