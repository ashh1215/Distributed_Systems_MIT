package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	TaskNum    int
	TaskType   int
	Taskstatus int
	StartTime  time.Time
}

const (
	Idle       = 0
	InProgress = 1
	Completed  = 2
)

// Task types
const (
	MapTask    = 0
	ReduceTask = 1
	Wait       = 2
)

type Master struct {
	// Your definitions here.
	InputFiles      []string
	Nreduce         int
	MapTasksList    []Task
	ReduceTasksList []Task
	mu              sync.Mutex
	completedTasks  int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignTask(args *WorkerTaskRequest, reply *MasterTaskAssign) error {

	if args.Message != "Waiting" {
		return nil
	}

	m.mu.Lock()

	done := false
	for i := range m.MapTasksList {
		if m.MapTasksList[i].Taskstatus == InProgress && time.Since(m.MapTasksList[i].StartTime) >= 10*time.Second {
			m.MapTasksList[i].Taskstatus = Idle
		}
	}
	for i := range m.ReduceTasksList {
		if m.ReduceTasksList[i].Taskstatus == InProgress && time.Since(m.ReduceTasksList[i].StartTime) >= 10*time.Second {
			m.ReduceTasksList[i].Taskstatus = Idle
		}
	}
	for i := range m.MapTasksList {
		if m.MapTasksList[i].Taskstatus == Idle {
			m.MapTasksList[i].Taskstatus = InProgress
			m.MapTasksList[i].StartTime = time.Now() //Need to figure out how to use this

			reply.NReduce = m.Nreduce
			reply.TaskNum = m.MapTasksList[i].TaskNum
			reply.Filename = m.InputFiles[m.MapTasksList[i].TaskNum]
			reply.TaskType = 0
			reply.MapTaskTotal = len(m.InputFiles)

			done = true
			break
		}
	}

	if !done && m.completedTasks >= len(m.InputFiles) {
		for i := range m.ReduceTasksList {
			if m.ReduceTasksList[i].Taskstatus == Idle {
				m.ReduceTasksList[i].Taskstatus = InProgress
				m.ReduceTasksList[i].StartTime = time.Now() //Need to figure out how to use this

				reply.NReduce = m.Nreduce
				reply.TaskNum = m.ReduceTasksList[i].TaskNum
				reply.TaskType = 1
				reply.MapTaskTotal = len(m.InputFiles)

				done = true
				break
			}
		}
	}

	if !done {
		reply.TaskType = 2
	}

	m.mu.Unlock()
	return nil
}

func (m *Master) CompleteTask(args *WorkerTaskCompleted, reply *MasterCompleteAck) error {

	if args.Message != "Completed" {
		return nil
	}

	m.mu.Lock()

	if args.TaskType == MapTask {
		if m.MapTasksList[args.TaskNum].Taskstatus != Completed {
			m.MapTasksList[args.TaskNum].Taskstatus = Completed
			m.completedTasks++
		}
	} else {
		if m.ReduceTasksList[args.TaskNum].Taskstatus != Completed {
			m.ReduceTasksList[args.TaskNum].Taskstatus = Completed
			m.completedTasks++
		}
	}
	reply.CompletedTasks = m.completedTasks

	m.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	m.mu.Lock()
	if m.completedTasks == m.Nreduce+len(m.InputFiles) {
		ret = true
	}
	m.mu.Unlock()

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.InputFiles = files
	m.Nreduce = nReduce

	i := 0
	for i < len(files) {
		newTask := Task{}
		newTask.TaskNum = i
		newTask.TaskType = MapTask
		newTask.Taskstatus = Idle

		m.MapTasksList = append(m.MapTasksList, newTask)

		i++
	}

	i = 0
	for i < nReduce {
		newTask := Task{}
		newTask.TaskNum = i
		newTask.TaskType = ReduceTask
		newTask.Taskstatus = Idle

		m.ReduceTasksList = append(m.ReduceTasksList, newTask)

		i++
	}

	m.server()
	return &m
}
