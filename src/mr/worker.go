package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// Do a map task and write the result to corresponding files
//
func DoMapTask(mapf func(string, string) []KeyValue,
	reply *TaskReply) bool {

	// read the input file
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v, error: %v\n", reply.FileName, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v, error: %v\n", reply.FileName, err)
	}
	file.Close()

	// map the content
	kva := mapf(reply.FileName, string(content))

	// sort and write to files
	sort.Sort(ByKey(kva))
	outputFiles := make([]*os.File, 0, reply.NReduce)
	encoders := make([]*json.Encoder, 0, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		tmpFile, err := ioutil.TempFile("./", "intermediate-file")
		if err != nil {
			log.Fatalf("cannot create tmpFile, error: %v\n", err)
		}
		outputFiles = append(outputFiles, tmpFile)
		encoders = append(encoders, json.NewEncoder(tmpFile))
	}

	curKey := ""
	curFileIdx := 0

	for _, kv := range kva {
		if kv.Key != curKey {
			curKey = kv.Key
			hash := ihash(curKey)
			curFileIdx = hash % len(outputFiles)
		}
		err := encoders[curFileIdx].Encode(&kv)
		if err != nil {
			fmt.Printf("fail to encode: %#v, error: %v\n", kv, err)
		}
	}

	for idx, file := range outputFiles {
		os.Rename(file.Name(), fmt.Sprintf("./mr-%d-%d", reply.TaskId, idx))
	}

	return true
}

//
// Do a reduce task and write the result to corresponding files
//
func DoReduceTask(reducef func(string, []string) string,
	reply *TaskReply) bool {

	kva := []KeyValue{}
	for idx := 0; idx < reply.NMap; idx++ {
		fileName := fmt.Sprintf("./mr-%d-%d", idx, reply.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v, error: %v\n", fileName, err)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	// reduce each key and write the result to file
	tmpFile, err := ioutil.TempFile("./", "tmp-reduce")
	if err != nil {
		log.Fatalf("cannot create tmpFile, error: %v\n", err)
	}
	idx := 0
	for idx < len(kva) {
		j := idx + 1
		for j < len(kva) && kva[j].Key == kva[idx].Key {
			j++
		}
		values := []string{}
		for k := idx; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[idx].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", kva[idx].Key, output)

		idx = j
	}

	tmpFile.Close()
	os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", reply.TaskId))

	return true
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		reply := AskServerForTask()
		if reply == nil {
			fmt.Printf("cannot connect to coordinator\n")
			os.Exit(0)
		} else if reply.TaskType == MapTaskType {
			ok := DoMapTask(mapf, reply)
			if ok {
				go ReportTaskResult(reply, true)
			}
		} else if reply.TaskType == ReduceTaskType {
			ok := DoReduceTask(reducef, reply)
			if ok {
				go ReportTaskResult(reply, true)
			}
		} else if reply.TaskType == AskLater {
			time.Sleep(time.Second)
		} else if reply.TaskType == NoMoreTask {
			fmt.Printf("all tasks have completed, now exit\n")
			os.Exit(0)
		} else {
			break
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// ask for a map or reduce task
//
func AskServerForTask() *TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.AskForTask", &args, &reply)
	if ok {
		fmt.Printf("receive a task: %#v\n", reply)
		return &reply
	} else {
		return nil
	}
}

func ReportTaskResult(taskReply *TaskReply, isCompleted bool) {
	if !isCompleted {
		return
	}
	args := ReportArgs{
		TaskType: taskReply.TaskType,
		TaskId:   taskReply.TaskId,
		FileName: taskReply.FileName}
	reply := ReportReply{}
	fmt.Printf("report a task: %#v\n", taskReply)
	ok := call("Coordinator.ReportTaskResult", &args, &reply)
	if !ok {
		fmt.Printf("fail to report: %#v\n", *taskReply)
	}
	return
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
