package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

var workerCounter int32

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

func generateWorkerID() int32 {
	return atomic.AddInt32(&workerCounter, 1)
}

func requestTask(workerID int32) *RequestTaskReply {
	request := RequestTaskArgs{WorkerID: workerID}
	reply := RequestTaskReply{}
	assignedTask := call("Coordinator.RequestMapTask", &request, &reply)
	if !assignedTask {
		log.Fatalf("Assign Error")
	}
	if !reply.IsTaskValid {
		log.Printf("No task assigned to worker %d", workerID)
		return nil
	}
	return &reply
}

func performMapTask(mapf func(string, string) []KeyValue, task *RequestTaskReply, workerID int32) bool {
	filename := task.MapFile
	//read the file and perform map on them.
	contents, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v: %v", filename, err)
	}
	mapResults := mapf(filename, string(contents)) //array of values
	print(mapResults)
	//var newFile string
	//for _, KeyValue := range mapResults {
	//	key := KeyValue.Key
	//	value := KeyValue.Value
	//	reduceNumber := ihash(key) % task.nReduce
	//	newFile = "mr-"
	//}

	partitionedMatrix := make([][]KeyValue, task.nReduce)

	for _, keyvalue := range mapResults {
		partition := ihash(keyvalue.Key) % task.nReduce
		partitionedMatrix[partition] = append(partitionedMatrix[partition], keyvalue) //writing all same kv pairs to one partition
	}

	//now gotta write it to a file in json format.
	for bucket := 0; bucket < task.nReduce; bucket++ {
		tempfile, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-", task.TaskId, bucket))
		if err != nil {
			log.Fatalf("cannot create tempfile: %v", err)
		}
		encoder := json.NewEncoder(tempfile)
		for _, keyvalue := range partitionedMatrix[bucket] {
			err := encoder.Encode(&keyvalue)
			if err != nil {
				log.Fatalf("cannot encode %v: %v", keyvalue.Key, err)
			}
		}
		tempfile.Close()
		finalFile := fmt.Sprintf("mr-%d-%d", task.TaskId, bucket)
		err = os.Rename(tempfile.Name(), filename)
		if err != nil {
			log.Fatalf("cannot rename tempfile: %v", err)
		}
	}
	return true
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := generateWorkerID()
	//call the coordinator to request for task.
	for {
		assignedTask := requestTask(workerID)
		if assignedTask == nil {
			//wait for sometime and again do the same request.
			time.Sleep(200 * time.Millisecond)
			continue
		}
		//got a task, then perform the task based on task type.
		if assignedTask.TaskType == MapPhase {
			generatedFile := performMapTask(mapf, assignedTask, workerID)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
