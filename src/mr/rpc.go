package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type MasterTaskAssign struct {
	TaskType     int    //either map ("map") or reduce ("reduce") or wait ("wait") or done ("done")
	Filename     string //which files to work on
	TaskNum      int
	NReduce      int
	MapTaskTotal int
}

type WorkerTaskRequest struct {
	Message string //Give task ("free")
}

type WorkerTaskCompleted struct {
	Message  string //probably "completed"
	TaskNum  int
	TaskType int // map or reduce (0 or 1)
}

type MasterCompleteAck struct {
	Message        string
	CompletedTasks int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
