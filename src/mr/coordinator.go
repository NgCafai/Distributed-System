package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatusEnum int

const (
	Sleeping TaskStatusEnum = iota
	Runnable
	Running
	Completed
)

type Task struct {
	Id         int
	TaskStatus TaskStatusEnum
}

type Coordinator struct {
	// Your definitions here.
	Mutex            sync.Mutex
	nReduce          int
	IsFinish         bool
	MapTaskStatus    map[string]*Task
	ReduceTaskStatus []Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.IsFinish = true
	return nil
}

//
// Reset a task as Runnable after timeout
//
func (c *Coordinator) ResetTimeoutTask(reply *TaskReply) error {
	time.Sleep(10 * time.Second)
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if reply.TaskType == MapTaskType {
		task, ok := c.MapTaskStatus[reply.FileName]
		if ok && task.TaskStatus == Running {
			// the task has timeout
			task.TaskStatus = Runnable
		}
	} else if reply.TaskType == ReduceTaskType &&
		reply.TaskId < len(c.ReduceTaskStatus) {
		ptr := &c.ReduceTaskStatus[reply.TaskId]
		if ptr.TaskStatus == Running {
			ptr.TaskStatus = Runnable
		}

	}
	return nil
}

func (c *Coordinator) SetFinish() {
	time.Sleep(3 * time.Second)
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.IsFinish = true
}

//
// Return a task
//
func (c *Coordinator) AskForTask(args *TaskArgs, reply *TaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// return a map task
	mapCompletedNum := 0
	for fileName, task := range c.MapTaskStatus {
		if task.TaskStatus == Runnable {
			reply.TaskType = MapTaskType
			reply.TaskId = task.Id
			reply.NReduce = c.nReduce
			reply.FileName = fileName
			task.TaskStatus = Running
			fmt.Printf("dispatch a task: %#v\n", reply)
			go c.ResetTimeoutTask(reply)
			return nil
		} else if task.TaskStatus == Completed {
			mapCompletedNum++
		}
	}

	if mapCompletedNum != len(c.MapTaskStatus) {
		// some map tasks are running
		reply.TaskType = AskLater
		return nil
	}

	// all map tasks have completed, and begin to dispatch reduce tasks
	reduceCompletedNum := 0
	for i := 0; i < len(c.ReduceTaskStatus); i++ {
		ptr := &c.ReduceTaskStatus[i]
		if ptr.TaskStatus == Runnable {
			reply.TaskType = ReduceTaskType
			reply.TaskId = ptr.Id
			reply.NReduce = c.nReduce
			reply.NMap = len(c.MapTaskStatus)
			ptr.TaskStatus = Running
			fmt.Printf("dispatch a task: %#v\n", reply)
			go c.ResetTimeoutTask(reply)
			return nil
		} else if ptr.TaskStatus == Completed {
			reduceCompletedNum++
		}
	}

	if reduceCompletedNum != len(c.ReduceTaskStatus) {
		reply.TaskType = AskLater
		return nil
	} else {
		// all reduce tasks have completed
		reply.TaskType = NoMoreTask
		if !c.IsFinish {
			go c.SetFinish()
		}
		return nil
	}
}

func (c *Coordinator) ReportTaskResult(args *ReportArgs, reply *ReportReply) error {
	// worker only reports after it successfully completed a task
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if args.TaskType == MapTaskType {
		task, ok := c.MapTaskStatus[args.FileName]
		if ok {
			task.TaskStatus = Completed
			fmt.Printf("a map task completed: %#v\n", args)
		} else {
			fmt.Printf("no such task: %#v\n", args)
		}
	} else if args.TaskType == ReduceTaskType {
		if args.TaskId < len(c.ReduceTaskStatus) {
			ptr := &c.ReduceTaskStatus[args.TaskId]
			ptr.TaskStatus = Completed
			fmt.Printf("a reduce task completed: %#v\n", args)
		} else {
			fmt.Printf("no such task: %#v\n", args)
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	return c.IsFinish
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{Mutex: sync.Mutex{}, nReduce: nReduce, IsFinish: false,
		MapTaskStatus:    map[string]*Task{},
		ReduceTaskStatus: make([]Task, 0, nReduce)}

	// Your code here.
	for idx, fileName := range files {
		c.MapTaskStatus[fileName] = &Task{idx, Runnable}
	}

	for i := 0; i < nReduce; i++ {
		// set the initial status of each reduce task as Runnable and
		// the coordinator will only distribute a reduce task after all map tasks have finished
		c.ReduceTaskStatus = append(c.ReduceTaskStatus, Task{i, Runnable})
	}
	c.server()
	return &c
}
