package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskPhase int
type TaskStatus int

const InvalidWorkerID = -1
const WaitTime = 10
const (
	MapPhase TaskPhase = iota
	ReducePhase
	DonePhase
)

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type TaskInfo struct {
	TaskId     int
	TaskType   TaskPhase  //map,reduce or done
	TaskStatus TaskStatus // done,inprogress or idle
	file       string
	StartTime  time.Time
	workerID   int
	InputFiles []string
	OutputFile string
}

type Coordinator struct {
	mu                   sync.Mutex
	rwmu                 sync.RWMutex
	files                []string
	nMap                 int
	NReduce              int
	done                 bool
	phase                TaskPhase
	TaskQueue            chan int //channel of taskid.
	MapTasks             map[int]*TaskInfo
	ReduceTasks          map[int]*TaskInfo
	MapTasksCompleted    int
	ReduceTasksCompleted int
	callDone             bool
}

type RequestTaskArgs struct {
	WorkerID int32
}

type RequestTaskReply struct {
	TaskId      int
	TaskType    TaskPhase
	MapFile     string
	ReduceFiles []string //input to reduce tasks are a bunch of files, to be precise 1 per mapper
	ReduceId    int
	IsTaskValid bool //to check if valid data is sent or not.
	NReduce     int
	doExit      bool
}

type TaskCompletionArgs struct {
	TaskId   int
	TaskType TaskPhase
	WorkerID int32
}

type TaskCompletionReply struct {
	Recorded bool
}

//func (c *Coordinator) UpdateTaskState(workerID int, info *TaskInfo) {
//	info.workerID = workerID
//	info.TaskStatus = InProgress
//	info.StartTime = time.Now()
//	return
//}

func (c *Coordinator) collectReduceFiles(taskid int) []string {
	//return all intermediate files of the form mr-*-workerid
	var files []string
	for i := 0; i < len(c.files); i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskid)
		files = append(files, filename)
	}
	return files
}

func (c *Coordinator) emptyChannelUnsafe() {
	//assuming that the mu.lock in the reportmaptask method is still held when this function is called
	for {
		select {
		case <-c.TaskQueue:

		default:
			return
		}
	}
}

func (c *Coordinator) fillReduceTaskMap() {
	c.ReduceTasks = make(map[int]*TaskInfo)
	log.Println("fillReduceTaskMap")
	for i := 0; i < c.NReduce; i++ {
		//fill the reduce task map over here.
		t := TaskInfo{}
		t.TaskId = i
		t.TaskType = ReducePhase
		t.TaskStatus = Idle
		t.StartTime = time.Time{}
		c.ReduceTasks[i] = &t
		c.TaskQueue <- i
	}
	log.Println("fillReduceTaskMap completed")
}

func (c *Coordinator) updatePhase() {
	if c.phase == MapPhase {
		c.phase = ReducePhase
	} else if c.phase == ReducePhase {
		c.phase = DonePhase
	}
}

func checkTaskCompletion(completed int, total int) bool {
	if completed == total {
		return true
	}
	return false
}

func CheckDeadWorkers(info map[int]*TaskInfo) []int {
	//find all deadworkers.
	var deadWorkers []int
	for _, task := range info {
		if task.TaskStatus == InProgress && time.Since(task.StartTime).Seconds() > WaitTime {
			deadWorkers = append(deadWorkers, task.workerID)
		}
	}
	return deadWorkers
}

func (c *Coordinator) HandleDeadWorkers(deadWorkers []int) {
	//manage all the tasks which have the deadworker as the workerID.
	for _, workerID := range deadWorkers {
		if c.phase == MapPhase {
			for taskID, task := range c.MapTasks {
				if task.workerID == workerID && task.TaskStatus == InProgress {
					task.TaskStatus = Idle
					task.workerID = InvalidWorkerID
					c.TaskQueue <- taskID
				}
			}
		} else if c.phase == ReducePhase {
			for taskID, task := range c.ReduceTasks {
				if task.workerID == workerID && task.TaskStatus == InProgress {
					task.TaskStatus = Idle
					task.workerID = InvalidWorkerID
					c.TaskQueue <- taskID
				}
			}
		}
	}
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	select {
	case NextTaskId := <-c.TaskQueue:
		log.Printf("RequestTask nextTaskId: %v", NextTaskId)
		if c.phase == MapPhase {
			task := c.MapTasks[NextTaskId]
			if task.TaskStatus != Idle {
				reply.IsTaskValid = false //to show that its an invalid task.
				log.Printf("RequestTask task status is %v", task.TaskStatus)
				return nil
			}
			log.Println("working on map task now")
			//c.UpdateTaskState(int(args.WorkerID), task)
			task.workerID = int(args.WorkerID) //updates internal state of the task.
			task.StartTime = time.Now()
			task.TaskStatus = InProgress
			//send using rpc now to the worker with workerid given in args.
			reply.TaskId = task.TaskId
			reply.TaskType = task.TaskType
			reply.MapFile = task.file
			reply.IsTaskValid = true
			reply.NReduce = c.NReduce
		} else if c.phase == ReducePhase {
			task := c.ReduceTasks[NextTaskId]
			if task.TaskStatus != Idle {
				reply.IsTaskValid = false //to show that its an invalid task.
				log.Printf("RequestTask task status is %v", task.TaskStatus)
				return nil
			}
			//give the worker all the files of the type mr-*-reduceid.
			reduceFiles := c.collectReduceFiles(task.TaskId)
			//c.UpdateTaskState(int(args.WorkerID), task)
			task.workerID = int(args.WorkerID)
			task.StartTime = time.Now()
			task.TaskStatus = InProgress
			reply.ReduceFiles = reduceFiles
			reply.IsTaskValid = true
			reply.NReduce = c.NReduce
			reply.TaskId = task.TaskId
			reply.TaskType = task.TaskType
			reply.doExit = false
		}
	default:
		reply.doExit = true
		reply.TaskType = DonePhase
		reply.IsTaskValid = true
		c.callDone = true
	}
	c.mu.Unlock()
	if c.callDone {
		c.Done()
	}
	return nil
}

func (c *Coordinator) ReportTaskCompletion(args *TaskCompletionArgs, reply *TaskCompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == MapPhase {
		task, exists := c.MapTasks[args.TaskId]
		if !exists {
			log.Printf("Invalid TaskId %d reported by worker %d", args.TaskId, args.WorkerID)
			reply.Recorded = false
			return nil
		}
		if task.TaskStatus != Completed {
			task.TaskStatus = Completed
			//log.Print("Report MapTaskCompletion completed")
			log.Print("Time taken: ", time.Now().Sub(task.StartTime))
			reply.Recorded = true
			c.MapTasksCompleted++
			log.Print("Number of tasks completed: ", c.MapTasksCompleted)
			log.Printf("Len files %v", len(c.files))
			if checkTaskCompletion(c.MapTasksCompleted, len(c.files)) {
				//finish the map phase, update the phase to reduce phase.
				log.Printf("Finished map tasks")
				c.updatePhase()
				c.emptyChannelUnsafe()
				c.fillReduceTaskMap()
				log.Printf("Finished updating task transition info")
			}
		} else {
			reply.Recorded = false
		}
	} else if args.TaskType == ReducePhase {
		task, exists := c.ReduceTasks[args.TaskId]
		if !exists {
			log.Printf("Invalid TaskId %d reported by worker %d", args.TaskId, args.WorkerID)
			reply.Recorded = false
			return nil
		}
		if task.TaskStatus != Completed {
			task.TaskStatus = Completed
			log.Print("Time taken: ", time.Now().Sub(task.StartTime))
			reply.Recorded = true
			c.ReduceTasksCompleted++
			if checkTaskCompletion(c.ReduceTasksCompleted, c.NReduce) {
				c.updatePhase() //go to done phase.
				//c.workerExitHandler() //exit all the ongoing workers.
			}
		}
	}
	return nil
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false
	allDone := true
	//check if the all the map and reduce tasks are done, then return true.
	for _, task := range c.MapTasks {
		if task.TaskStatus != Completed {
			allDone = false
			break
		}
	}
	if allDone {
		for _, task := range c.ReduceTasks {
			if task.TaskStatus != Completed {
				allDone = false
				break
			}
		}
	}
	if allDone {
		ret = true
	}
	return ret
}

// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NReduce = nReduce
	c.files = files
	c.nMap = len(files) //number of map tasks.
	c.done = false
	c.phase = MapPhase
	c.MapTasks = make(map[int]*TaskInfo, len(files))
	c.TaskQueue = make(chan int, 100) //buffered channel.
	//fmt.Println("Init coordinator")
	for i, file := range files {
		t := TaskInfo{}
		//log.Println("File: ", file)
		t.file = file
		t.TaskId = i
		t.TaskType = MapPhase
		t.TaskStatus = Idle
		t.StartTime = time.Time{} //zero value
		c.MapTasks[i] = &t
		c.TaskQueue <- i //inserting into task channel.
	}
	go func() {
		for {
			time.Sleep(2 * time.Second)
			c.mu.Lock()
			if c.phase == MapPhase {
				deadWorkers := CheckDeadWorkers(c.MapTasks)
				c.HandleDeadWorkers(deadWorkers)
			} else if c.phase == ReducePhase {
				deadWorkers := CheckDeadWorkers(c.MapTasks)
				c.HandleDeadWorkers(deadWorkers)
			}
			c.mu.Unlock()
		}
	}()
	c.server()
	return &c
}
