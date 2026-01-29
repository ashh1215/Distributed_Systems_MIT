package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for true {
		// Your worker implementation here.
		mainArgs, ok := RequestTask()

		if !ok {
			return
		}
		if mainArgs.TaskType == Wait {
			time.Sleep(time.Second)
			continue
		}

		if mainArgs.TaskType == MapTask {
			filename := mainArgs.Filename

			files := make([]*os.File, mainArgs.NReduce)
			encoders := make([]*json.Encoder, mainArgs.NReduce)
			tempNames := make([]string, mainArgs.NReduce)
			for i := 0; i < mainArgs.NReduce; i++ {
				// fname := fmt.Sprintf("mr-%d-%d", mainArgs.TaskNum, i)
				files[i], _ = os.CreateTemp("", "mr-tmp-*")
				tempNames[i] = files[i].Name()
				encoders[i] = json.NewEncoder(files[i])
			}

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			for _, kv := range kva {
				bucket := ihash(kv.Key) % mainArgs.NReduce
				encoders[bucket].Encode(&kv)
			}

			for i, f := range files {
				f.Close()
				fname := fmt.Sprintf("mr-%d-%d", mainArgs.TaskNum, i)
				os.Rename(tempNames[i], fname)
			}
		} else {
			kva := []KeyValue{}
			for i := 0; i < mainArgs.MapTaskTotal; i++ {
				fname := fmt.Sprintf("mr-%d-%d", i, mainArgs.TaskNum)
				file, err := os.Open(fname)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					kv := KeyValue{}
					if dec.Decode(&kv) != nil {
						break
					}
					kva = append(kva, kv)
				}

				file.Close()
			}

			sort.Sort(ByKey(kva))

			i := 0
			oname := fmt.Sprintf("mr-out-%d", mainArgs.TaskNum)
			ofile, _ := os.CreateTemp("", "mr-tmp-*")
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()
			os.Rename(ofile.Name(), oname)
		}

		SendCompletion(&mainArgs)
		// uncomment to send the Example RPC to the master.
		// CallExample()
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func RequestTask() (MasterTaskAssign, bool) {
	args := WorkerTaskRequest{}
	args.Message = "Waiting"

	reply := MasterTaskAssign{}

	// fmt.Printf("Worker TaskType %v, Worker TaskNum %v", reply.TaskType, reply.TaskNum)

	ok := call("Master.AssignTask", &args, &reply)
	return reply, ok
}

func SendCompletion(task *MasterTaskAssign) {
	args := WorkerTaskCompleted{}
	args.Message = "Completed"
	args.TaskNum = task.TaskNum
	args.TaskType = task.TaskType
	reply := MasterCompleteAck{}

	call("Master.CompleteTask", &args, &reply)

	// fmt.Printf("Worker TaskType %v, Worker TaskNum %v completed, TOTAL COMPLETED TASKS=%v", args.TaskType, args.TaskNum, reply.CompletedTasks)

}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
		// log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
