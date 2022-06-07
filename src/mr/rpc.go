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
type TaskTypeEnum int

const (
	MapTaskType TaskTypeEnum = iota
	ReduceTaskType
	AskLater   // there is no runnable task now
	NoMoreTask // when a worker receive this type, it can exit
)

type TaskArgs struct {
}

type TaskReply struct {
	TaskType TaskTypeEnum
	TaskId   int
	NReduce  int
	NMap     int
	FileName string
}

type ReportArgs struct {
	TaskType TaskTypeEnum
	TaskId   int
	FileName string
}

type ReportReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
