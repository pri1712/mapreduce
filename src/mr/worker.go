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
	assignedTask := call("Coordinator.RequestTask", &request, &reply)
	if !assignedTask {
		log.Printf("RPC failed in requestTask. Retrying...")
		time.Sleep(500 * time.Millisecond)
		return nil
	}
	if !reply.IsTaskValid {
		log.Printf("No task assigned to worker %d", workerID)
		return nil
	}
	return &reply
}

func performMapTask(mapf func(string, string) []KeyValue, task *RequestTaskReply, workerID int32) error {
	filename := task.MapFile
	//read the file and perform map on them.
	contents, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v: %v", filename, err)
	}
	mapResults := mapf(filename, string(contents)) //array of values
	//log.Printf("Map results: %v", mapResults)

	partitionedMatrix := make([][]KeyValue, task.NReduce)

	for _, keyvalue := range mapResults {
		partition := ihash(keyvalue.Key) % task.NReduce
		partitionedMatrix[partition] = append(partitionedMatrix[partition], keyvalue) //writing all same kv pairs to one partition
	}

	//now gotta write it to a file in json format.
	for bucket := 0; bucket < task.NReduce; bucket++ {
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
		err = os.Rename(tempfile.Name(), finalFile)
		if err != nil {
			log.Fatalf("cannot rename tempfile: %v", err)
		}
	}
	return nil
}

func informCoordinator(workerID int32, completedTask *RequestTaskReply) TaskCompletionReply {
	args := MapTaskCompletionArgs{TaskId: completedTask.TaskId, WorkerID: workerID}
	reply := TaskCompletionReply{}
	call("Coordinator.ReportMapTaskCompletion", &args, &reply)
	return reply
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := generateWorkerID()
	//log.Printf("Worker ID: %d", workerID)
	for {
		//fmt.Println("Here")
		assignedTask := requestTask(workerID)
		//fmt.Printf("Assigned Task: %v", assignedTask)
		if assignedTask == nil {
			//wait for sometime and again do the same request.
			time.Sleep(200 * time.Millisecond)
			//log.Printf("Waiting for assigned task")
			continue
		}

		//got a task, then perform the task based on task type.
		if assignedTask.TaskType == MapPhase {
			//log.Printf("Assigned task: %v", assignedTask.TaskId)
			err := performMapTask(mapf, assignedTask, workerID)
			if err != nil {
				//log.Printf("cannot perform map task: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			//tell coordinator that job successfully done.
			jobReport := informCoordinator(workerID, assignedTask)
			if jobReport.Recorded {
				log.Printf("Job successfully completed")
			} else {
				log.Printf("Issue in job reporting path")
			}
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
