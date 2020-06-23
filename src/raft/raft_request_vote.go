package raft

import "sync"

// ****************** Candidate RequestVote RPC *****************

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

	L1BPrint("[%d] raise vote\n", rf.me)

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
			L1BPrint("[%d] sendRequestVote to [%d]. at term (%d vs. %d)\n", rf.me, id, rf.curTerm, args.Term)

			if rf.curTerm == args.Term { // current term can be newer than that at the sending time
				if reply.VoteGranted {
					L1BPrint("[%d] vote granted from [%d]\n", rf.me, id)
					granted++
					if rf.state == candidate && granted >= rf.majority {
						L1BPrint("[%d] BECOMES NEW LEADER!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n", rf.me)
						rf.state = leader
						lastLogIndex := len(rf.log) - 1
						L1BPrint("[%d] len(rf.log)=(%d)\n", rf.me, 1+lastLogIndex)
						rf.nextIndex = rf.nextIndex[:0]
						for i := 0; i < rf.n; i++ {
							rf.nextIndex = append(rf.nextIndex, lastLogIndex+1)
							rf.matchIndex = append(rf.matchIndex, 0)
						}
						go rf.heartbeat()
					}
				} else {
					L1BPrint("[%d] vote rejected from [%d]\n", rf.me, id)
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
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	candidateTerm := args.Term
	candidateID := args.CandidateID
	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm
	reply.Term = rf.curTerm

	L1BPrint("[%d](%d) receive 'RequestVote' from [%d](%d)\n", rf.me, rf.curTerm, candidateID, candidateTerm)

	// reject candidate's term if it's out-of-date
	if rf.curTerm > candidateTerm {
		L1BPrint("[%d](%d) reject [%d](%d)(out-of-date)\n", rf.me, rf.curTerm, candidateID, candidateTerm)
		reply.VoteGranted = false
		return
	}
	// update receiver's term if it's out-of-date
	if rf.curTerm < candidateTerm {
		L1BPrint("[%d](%d)(out-of-date become FOLLOWER), accept candidate[%d]'s term (%d)\n", rf.me, rf.curTerm, candidateID, candidateTerm)
		rf.curTerm = candidateTerm // update current term
		rf.voteFor = -1            // clear voteFor (make sure previously voteFor can't have majority)
		rf.state = follower        // become follower
	}
	if rf.voteFor != -1 { // if already vote for other server
		reply.VoteGranted = (rf.voteFor == candidateID)
		return
	}

	// use last log term/index to compare
	lastLog := rf.log[len(rf.log)-1]
	L1BPrint("lastLogTerm: [%d](%d) vs. candidate [%d](%d)\n", rf.me, lastLog.Term, candidateID, candidateLastLogTerm)
	if lastLog.Term < candidateLastLogTerm {
		L1BPrint("lastLogTerm: [%d] < [%d](candidate)\n", rf.me, candidateID)
		if rf.voteFor == -1 {
			L1BPrint("[%d] has no voteFor -> vote for [%d]\n", rf.me, candidateID)
			rf.voteFor = candidateID
			reply.VoteGranted = true
		} else {
			L1BPrint("[%d] has voteFor [%d] -> reject vote for [%d]\n", rf.me, rf.voteFor, candidateID)
			reply.VoteGranted = false
		}

	} else if lastLog.Term == candidateLastLogTerm { // same term, should compare lastLogIndex
		L1BPrint("lastLogTerm: [%d] == [%d](candidate) compare index\n", rf.me, candidateID)

		if lastLog.Index <= candidateLastLogIndex {
			L1BPrint("lastLogIndex: [%d](%d) <= [%d](%d)(candidate).\n", rf.me, lastLog.Index, candidateID, candidateLastLogIndex)
			if rf.voteFor == -1 {
				L1BPrint("[%d] has no voteFor -> vote for [%d]\n", rf.me, candidateID)
				rf.voteFor = candidateID
				reply.VoteGranted = true
			} else {
				L1BPrint("[%d] has voteFor [%d] -> reject vote for [%d]. \n", rf.me, rf.voteFor, candidateID)
				reply.VoteGranted = false
			}
		} else {
			L1BPrint("lastLogIndex: [%d](%d) > [%d](%d)(candidate) -> reject vote\n", rf.me, lastLog.Index, candidateID, candidateLastLogIndex)
			reply.VoteGranted = false
		}
	} else if lastLog.Term > candidateLastLogTerm {
		L1BPrint("lastLogTerm: [%d] > [%d](candidate) -> reject vote\n", rf.me, candidateID)
		reply.VoteGranted = false
	}
	return
}
