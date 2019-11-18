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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"



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

type AppendEntryLog struct {
	term int64
}

type raftState int
const (
	follower raftState = iota
	candidate
	leader
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	votedFor int
	log	 []AppendEntryLog

	commitIndex int64
	lastApplied int64

	nextIndex []int64
	matchIndex []int64
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electTimer *time.Timer
	electTimeout chan bool

	randVoteTimer *time.Timer
	heartBeatTimer *time.Timer

	state raftState
	stop chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	} else {
		isleader= false
	}

	return term, isleader
}


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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int64
	LastLogTerm int64
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntryArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int64

	PrevLogTerm int64
	Entries[] interface{}

	LeaderCommit int64
}

type AppendEntryReply struct {
	Term int
	Success bool
}

func (rf *Raft) changeToFollower() {
	fmt.Printf("node %d changged to follower\n", rf.me)

	rf.state = follower
	if rf.electTimer == nil {
		rf.electTimer = time.NewTimer(time.Millisecond*500)
	} else {
		rf.electTimer.Stop()
		rf.electTimer.Reset(time.Millisecond*500)
	}

	go func() {
		<- rf.electTimer.C
		fmt.Printf("node %d election timer expired triggered\n", rf.me)
		rf.electTimeout <- true
	}()
}

func (rf *Raft) electTimeoutRestart() {
	fmt.Printf("node %d elect timer restart\n", rf.me)
	rf.electTimer.Stop()
	rf.electTimer.Reset(time.Millisecond*500)
	go func() {
		<- rf.electTimer.C
		fmt.Printf("node %d election timer expired triggered\n", rf.me)
		rf.electTimeout <- true
	}()
}

func (rf *Raft) electTimeoutStop() {
	fmt.Printf("node %d stop elect timeout timer\n", rf.me)
	rf.electTimer.Stop()
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("node %d received new vote request term %d, current term %d\n",
		rf.me, args.Term, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term == rf.currentTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.changeToFollower()

		if rf.randVoteTimer != nil {
			fmt.Printf("node %d cancel vote timer\n", rf.me)
			rf.randVoteTimer.Stop()
			rf.randVoteTimer = nil
		}
	} else {
		rf.changeToFollower()
		rf.votedFor = -1 // always grant the vote for the new term
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = args.Term
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	if args.Term < rf.currentTerm {
		fmt.Printf("node %d received bad term: %d\n",rf.me, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.state != leader {
		rf.electTimeoutRestart()
	}

	if args.Term > rf.currentTerm {
		rf.changeToFollower()
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

	if len(args.Entries) == 0 {
		if rf.state == candidate {
			fmt.Printf("node %d received leader result notify message, leader node is %d, Term is %d\n",
				rf.me, args.LeaderId, args.Term)
			rf.changeToFollower()
			rf.votedFor = args.LeaderId
		} else if rf.state == follower {
			fmt.Printf("node %d received heartbeat message, Term %d\n", rf.me, args.Term)
		}
	} else {
		fmt.Printf("node %d received append log\n", rf.me)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("node %d send vote msg to node %d, term %d\n", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		fmt.Printf("node %d send vote msg to node %d success\n", rf.me, server)
	} else {
		fmt.Printf("node %d send vote msg to node %d failed\n", rf.me, server)
	}

	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	fmt.Printf("node %d send heartbeat msg to node %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		fmt.Printf("node %d send heartbeat msg to node %d success\n", rf.me, server)
	} else {
		fmt.Printf("node %d send heartbeat msg to node %d failed\n", rf.me, server)
	}

	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	fmt.Println("killed")
	rf.stop <- true
}

func (rf *Raft)startVote()  {
	voteCount := 1 // self
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.currentTerm
	rf.mu.Unlock()

	fmt.Printf("node %d starting vote, term %d\n", rf.me, rf.currentTerm)

	for i := range rf.peers {
		if rf.me != i {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
			}
			if rf.commitIndex != 0 {
				args.LastLogIndex = rf.commitIndex
				args.LastLogTerm = rf.log[rf.commitIndex].term
			}

			var reply RequestVoteReply
			if rf.sendRequestVote(i, &args, &reply) {
				if reply.VoteGranted {
					fmt.Printf("node %d receive vote grant from node %d\n", rf.me, i)
					voteCount++
				} else {
					fmt.Printf("node %d receive vote deny from node %d\n", rf.me, i)
				}
			} else {
				fmt.Printf("node %d send requestVote message to node %d failed\n", rf.me, i)
			}
		}
	}

	if voteCount > len(rf.peers)/2 {
		fmt.Printf("node %d become leader\n", rf.me)
		rf.state = leader
		rf.electTimeoutStop()

		for i := range rf.peers {
			if rf.me != i {
				args := AppendEntryArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      nil,
					LeaderCommit: 0,
				}
				var reply AppendEntryReply
				rf.sendHeartBeat(i, &args, &reply)
				if reply.Success {
					fmt.Printf("node %d receive heartbeat reply success\n", rf.me)
				} else {
					fmt.Printf("node %d send heartbeat to node %d failed\n", rf.me, i)
				}
			}
		}

		rf.heartBeatTimer = time.AfterFunc(time.Millisecond*200, func() {
			if rf.state == leader {
				fmt.Printf("leader node %d send heartbeat to follower\n", rf.me)
				for i := range rf.peers {
					if i != rf.me {
						args := AppendEntryArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: 0,
							PrevLogTerm:  0,
							Entries:      nil,
							LeaderCommit: 0,
						}
						var reply AppendEntryReply
						if rf.sendHeartBeat(i, &args, &reply) {
							if reply.Success {
								fmt.Printf("leader %d recv heatbeat reply success from node %d\n", rf.me, i)
							} else {
								fmt.Printf("leader %d recv heatbeat reply failed from node %d\n", rf.me, i)
							}
						}
					}
				}

				rf.heartBeatTimer.Reset(time.Millisecond*200)
			} else {
				fmt.Println("I am not leader current")
			}
		})
	} else {
		fmt.Printf("node %d restart election timer for next vote\n", rf.me)
		rf.electTimeout <- true
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.electTimeout = make(chan bool)
	rf.stop = make(chan bool)

	fmt.Println("Make-----------------------")
	rf.changeToFollower()

	go func() {
		for {
			select {
			case <- rf.electTimeout:
				fmt.Printf("node %d election time out happened\n", rf.me)
				if rf.randVoteTimer != nil {
					rf.randVoteTimer.Stop()
				}
				rf.randVoteTimer = time.AfterFunc(time.Duration(150 + rand.Intn(150))*time.Millisecond, rf.startVote)

			case <- rf.stop:
				fmt.Println("raft stopped")
				if rf.randVoteTimer != nil {
					rf.randVoteTimer.Stop()
				}

				rf.electTimer.Stop()

				if rf.heartBeatTimer != nil {
					rf.heartBeatTimer.Stop()
				}
				break
			}
		}
	}()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
