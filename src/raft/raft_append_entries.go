package raft

// ***************** LEADER AppendEntries RPC  ******************

type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderID     int     // leader ID
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex Entry
	Entries      []Entry // log entries to store (empty for heartbeat) (TODO: send more than one)
	LeaderCommit int     // leader commitIndex
}

type AppendEntriesReply struct {
	Term    int  // current term (for leader to update itself)
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// call by 'go routine()' with 'go heartbeat()'
// go func() sendAppendEntries
// 	LOCK
// 	UNLOCK
//  notice that leader might become follower
//  send and waitReply...
// 	LOCK
// 	UNLOCK
//
func (rf *Raft) heartbeat() {
	L2BPrint("[%d] sends heartbeat\n", rf.me)

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(x int) {
				// load args (leader information)
				rf.mu.Lock()
				lastLogIndex := len(rf.log) - 1
				nextIndex := rf.nextIndex[x]

				args := AppendEntriesArgs{}
				args.Term = rf.curTerm
				args.LeaderID = rf.me
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = nextIndex - 1
				L2BPrint("[%d] nextIndex:======%d\n", x, nextIndex)
				args.PrevLogTerm = rf.log[nextIndex-1].Term

				if lastLogIndex >= nextIndex {
					L2BPrint("[%d] sent [%d:%d](next:last) to [%d]", rf.me, nextIndex, lastLogIndex, x)
					args.Entries = rf.log[nextIndex:]
				} else {
					// follower is updated, no need to send
					L2BPrint("[%d](lastIndex=%d) send empty entry to [%d](nextIndex=%d)\n", rf.me, lastLogIndex, x, nextIndex)
				}
				rf.mu.Unlock()

				if rf.state != leader {
					return
				}

				reply := AppendEntriesReply{}
				// send AppendEntries and wait for reply....
				rf.sendAppendEntries(x, &args, &reply)

				// process reply
				rf.mu.Lock()
				if reply.Term > rf.curTerm {
					L2BPrint("[%d]'s 'AppendEntries' out-of-date(become FOLLOWER)\n", rf.me)
					rf.curTerm = reply.Term // update current term
					rf.voteFor = -1         // clear voteFor (make sure previously voteFor can't have majority)
					rf.state = follower     // become follower

				} else if reply.Success {
					rf.nextIndex[x] = lastLogIndex + 1
					rf.matchIndex[x] = lastLogIndex
					L2BPrint("[%d](leader) update [%d]'s nextIndex = (%d)\n", rf.me, x, rf.nextIndex[x])
				} else {
					// it can happen that follower disconnected
					// we should not decrease nextIndex if follower disconnected and reply is empty(Term=0)
					// reply.Term = 0 -> follower is disconnected
					// first check if reply is real reply or disconnected reply
					// also check if whether this heartbeat has been sent twice: twice load, then twice sent
					// decrement nextIndex and retry
					if reply.Term != 0 && rf.nextIndex[x] == nextIndex {
						rf.nextIndex[x]--
						L2BPrint("[%d] reply false,  [%d] decrease its nextIndex = (%d)\n", x, rf.me, rf.nextIndex[x])
					}
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
	leaderPrevLogIndex := args.PrevLogIndex
	leaderPrevLogTerm := args.PrevLogTerm
	leaderEntries := args.Entries
	leaderCommit := args.LeaderCommit

	reply.Term = rf.curTerm
	L2BPrint("[%d](%d) receive 'AppendEntries' from [%d](%d)\n", rf.me, rf.curTerm, leaderID, leaderTerm)

	// update term if out-of-date
	if rf.curTerm < leaderTerm {
		L2BPrint("[%d](%d) is out-of-date, after self term=(%d)\n", rf.me, rf.curTerm, leaderTerm)
		rf.curTerm = leaderTerm
		rf.voteFor = -1
		rf.state = follower
	}

	// reject leader's term if it's out-of-date
	if rf.curTerm > leaderTerm {
		L2BPrint("[%d](%d) reject [%d](%d)(out-of-date)\n", rf.me, rf.curTerm, leaderID, leaderTerm)
		reply.Success = false
		return
	}

	L2BPrint("[%d](%d), accept leader[%d]'s term (%d)\n", rf.me, rf.curTerm, leaderID, leaderTerm)
	// reset timer, become follower, clear voteFor
	rf.mu.Unlock()
	rf.resetElectionTimeOut()
	rf.mu.Lock()

	// reject if follower's log does not contain leaderPrevLogIndex || term does not match
	if len(rf.log) <= leaderPrevLogIndex || rf.log[leaderPrevLogIndex].Term != leaderPrevLogTerm {
		L2BPrint("[%d](%d) reject [%d](%d)'s prev log Index'\n", rf.me, rf.curTerm, leaderID, leaderTerm)
		reply.Success = false
		return
	}

	// might receive one same entry more than one time
	L2BPrint("[%d] has %d existing entries\n", rf.me, len(rf.log))
	for _, e := range leaderEntries {
		L2BPrint("[%d] receive entry(%d(%d)) from leader[%d]\n", rf.me, e.Index, e.Term, leaderID)
		if e.Index >= len(rf.log) { // append new entries
			L2BPrint("[%d]append entry (%d(%d))\n", rf.me, e.Index, e.Term)
			rf.log = append(rf.log, e)
		} else { // existing entry
			if rf.log[e.Index].Term != e.Term { // term conflict, delete all following entries
				L2BPrint("[%d](follower) conflicts entry (%d(%d))\n", rf.me, e.Index, e.Term)
				rf.log = rf.log[:e.Index+1]
				rf.log[e.Index] = e
			}
		}
	}
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = leaderCommit
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// **************** LEADER AppendEntries RPC ****************
