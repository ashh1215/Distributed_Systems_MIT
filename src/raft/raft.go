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
	"bytes"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"
	"6.824/src/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	//2A
	CurrentTerm     int
	VotedFor        int
	State           int
	ElectionTimeout time.Time
	LastHeartbeat   time.Time

	//2B
	Log         []LogEntry
	CommitIndex int
	LastApplied int
	NextIndex   []int
	MatchIndex  []int
	ApplyCh     chan ApplyMsg
	triggerCh   chan struct{}

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// State types
const (
	LeaderState    = 0
	CandidateState = 1
	FollowerState  = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	if rf.State == LeaderState {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	d.Decode(&currentTerm)
	d.Decode(&votedFor)
	d.Decode(&log)
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.Log = log
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
	PeerNumber   int //needed for sending which peer is asking for vote
	CurrentTerm  int //whichever term current peer asking for vote is in
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteVerdict bool //yes or no
	CurrentTerm int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.CurrentTerm = rf.CurrentTerm

	if args.CurrentTerm < rf.CurrentTerm {
		reply.VoteVerdict = false
		return
	}

	if args.CurrentTerm > rf.CurrentTerm {
		rf.State = FollowerState
		rf.CurrentTerm = args.CurrentTerm
		rf.VotedFor = -1
	}

	lastLogInd := len(rf.Log) - 1
	lastTerm := rf.Log[lastLogInd].Term

	uptoDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastLogInd)
	if (rf.VotedFor == -1 || rf.VotedFor == args.PeerNumber) && uptoDate {
		reply.VoteVerdict = true
		rf.VotedFor = args.PeerNumber
		rf.resetElectionTimer()

	}

	rf.persist()

}

func (rf *Raft) resetElectionTimer() {
	timeout := time.Duration(rand.IntN(151)+450) * time.Millisecond
	rf.ElectionTimeout = time.Now().Add(timeout)
}

type AppendEntriesArgs struct {
	CurrentTerm      int
	LeaderPeerNumber int

	//2B
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	CurrentTerm   int
	SuccessStatus bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.CurrentTerm = rf.CurrentTerm
	if args.CurrentTerm < rf.CurrentTerm {
		reply.SuccessStatus = false
		reply.ConflictTerm = rf.CurrentTerm
		return
	}

	if args.CurrentTerm > rf.CurrentTerm {
		rf.CurrentTerm = args.CurrentTerm
		rf.VotedFor = -1
	}
	rf.persist()
	rf.State = FollowerState
	if rf.State == FollowerState {
		rf.resetElectionTimer()
	}

	if len(rf.Log) <= args.PrevLogIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.Log)

		return
	}

	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.SuccessStatus = false
		reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
		reply.ConflictIndex = args.PrevLogIndex
		for reply.ConflictIndex > 1 && rf.Log[reply.ConflictIndex-1].Term == reply.ConflictTerm {
			reply.ConflictIndex--
		}
		return
	}

	for j, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + j
		if idx >= len(rf.Log) {
			rf.Log = append(rf.Log, args.Entries[j:]...)
			break
		}
		if entry.Term != rf.Log[idx].Term {
			rf.Log = rf.Log[:idx]
			rf.Log = append(rf.Log, args.Entries[j:]...)
			break
		}
	}

	rf.persist()
	if args.LeaderCommitIndex > rf.CommitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		rf.CommitIndex = min(args.LeaderCommitIndex, min(lastNewIndex, len(rf.Log)-1))
	}

	reply.SuccessStatus = true
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

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != LeaderState {
		return index, term, false
	}

	index = len(rf.Log)
	term = rf.CurrentTerm
	//fmt.Printf("Start(): Append to Log at Index:%v, Term:%v\n", index, term)

	rf.Log = append(rf.Log, LogEntry{Command: command, Term: rf.CurrentTerm})
	rf.persist()
	select {
	case rf.triggerCh <- struct{}{}:
	default:
	}
	isLeader = true

	return index, term, isLeader
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State == LeaderState {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// duration := time.Duration((rand.IntN(151) + 300) * 1e6)
		if time.Now().After(rf.ElectionTimeout) {
			rf.State = CandidateState
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			rf.persist()
			rf.resetElectionTimer()

			args := RequestVoteArgs{}
			args.CurrentTerm = rf.CurrentTerm
			args.PeerNumber = rf.me
			args.LastLogIndex = len(rf.Log) - 1
			args.LastLogTerm = rf.Log[len(rf.Log)-1].Term

			rf.mu.Unlock()

			count := 1
			var countmu sync.Mutex
			for i := range rf.peers {
				if i != rf.me {
					go func(server int) {
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(server, &args, &reply)
						if ok && reply.VoteVerdict == true {
							countmu.Lock()
							count++

							if count > len(rf.peers)/2 {
								rf.mu.Lock()
								if rf.State == CandidateState && rf.CurrentTerm == args.CurrentTerm {
									rf.State = LeaderState
									rf.resetElectionTimer()
									go rf.sendHeartbeat()

								}
								rf.mu.Unlock()
							}
							countmu.Unlock()
						}
					}(i)
				}
			}
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}

}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	for i := range rf.peers {
		rf.NextIndex[i] = len(rf.Log)
		rf.MatchIndex[i] = 0
	}
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.State != LeaderState {
			rf.mu.Unlock()
			return
		}

		for i := range rf.peers {
			if i != rf.me {
				prevlogind := rf.NextIndex[i] - 1
				var entries []LogEntry
				if rf.NextIndex[i] < len(rf.Log) {
					entries = append([]LogEntry{}, rf.Log[rf.NextIndex[i]:]...)
				} else {
					entries = nil
				}
				// tot := len(rf.Log)
				// entries := make([]LogEntry, tot-prevlogind-1)
				// for ind := prevlogind + 1; ind < tot; ind++ {
				// 	entries[ind-prevlogind-1] = rf.Log[ind]
				// }

				args := AppendEntriesArgs{
					CurrentTerm:       rf.CurrentTerm,
					LeaderPeerNumber:  rf.me,
					Entries:           entries,
					PrevLogIndex:      prevlogind,
					PrevLogTerm:       rf.Log[prevlogind].Term,
					LeaderCommitIndex: rf.CommitIndex,
				}
				go func(server int, args AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntry(server, &args, &reply)
					if !ok {
						return
					}
					rf.mu.Lock()

					if rf.State != LeaderState || rf.CurrentTerm != args.CurrentTerm {
						rf.mu.Unlock()
						return
					}

					if reply.SuccessStatus {
						rf.NextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
						rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)

						for N := rf.CommitIndex + 1; N < len(rf.Log); N++ {
							count := 1 //start from 1, leader anyways counted for itself
							for i := range rf.peers {
								if rf.MatchIndex[i] >= N {
									count++
								}
							}
							if count > len(rf.peers)/2 && rf.Log[N].Term == rf.CurrentTerm {
								//fmt.Printf("CommitIndex increase from %v to %v\n", rf.CommitIndex, N)
								rf.CommitIndex = N
							}
						}

					} else {
						if reply.CurrentTerm > rf.CurrentTerm {
							rf.CurrentTerm = reply.CurrentTerm
							rf.State = FollowerState
							rf.VotedFor = -1
							rf.persist()
							rf.mu.Unlock()
							return
						}

						rf.NextIndex[server] = reply.ConflictIndex
					}
					rf.mu.Unlock()
				}(i, args)
			}

		}
		rf.mu.Unlock()

		select {
		case <-time.After(100 * time.Millisecond):
		case <-rf.triggerCh:
		}
	}

}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.CommitIndex >= len(rf.Log) {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if rf.LastApplied < rf.CommitIndex {
			extras := make([]ApplyMsg, 0)
			for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
				extras = append(extras, ApplyMsg{true, rf.Log[i].Command, i})
			}
			//fmt.Printf("applier() from %v to %v\n", rf.LastApplied, rf.CommitIndex)
			rf.LastApplied = rf.CommitIndex
			rf.mu.Unlock()
			for _, msg := range extras {
				rf.ApplyCh <- msg
			}
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//2A
	rf.resetElectionTimer()

	rf.VotedFor = -1
	rf.State = FollowerState
	rf.CurrentTerm = 0
	rf.LastApplied = 0
	rf.Log = []LogEntry{{Term: 0, Command: nil}}
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.ApplyCh = applyCh
	rf.triggerCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}
