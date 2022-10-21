package mr

import (
	//"errors"
	"fmt"
	//"hash"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"

	//"golang.org/x/tools/go/analysis/passes/nilfunc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// PID as Worker ID
	id := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s started\n", id)

	var LastTaskIndex int
	var LastTaskType string
	for {
		args := ApplyforTaskArgs{
			WorkerID:      id,
			LastTaskType:  LastTaskType,
			LastTaskIndex: LastTaskIndex,
		}
		reply := ApplyforTaskReply{}
		log.Printf("Prepare to call PRC! LastTaskType is %s, LastTaskIndex is %d\n", args.LastTaskType, args.LastTaskIndex)
		ok := call("Coordinator.ApplyforTask", &args, &reply)
		if ok {
			// reply.Y should be 100.
			log.Printf("call Coordinator.ApplyforTask succeed!\n")
		} else {
			log.Printf("call Coordinator.ApplyforTask failed!\n")
			break
		}
		if reply.TaskType == "" {
			// mr finished
			log.Printf("Received job finish signal from coordinator\n")
			break
		}

		log.Printf("Received %s task %d from coordinator", reply.TaskType, reply.TaskIndex)
		if reply.TaskType == MAP {
			// handle MAP task
			file, err := os.Open(reply.MapInputFile)
			if err != nil {
				log.Fatalf("Failed to open map input file %s: %e", reply.MapInputFile, err)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("Failed to read map input file %s: %e", reply.MapInputFile, err)
			}

			kvslices := mapf(reply.MapInputFile, string(content))
			hashedmap := make(map[int][]KeyValue)
			for _, kv := range kvslices {
				hashednum := ihash(kv.Key) % reply.ReduceNum
				hashedmap[hashednum] = append(hashedmap[hashednum], kv)
			}
			
			// i is reduce task index
			for i := 0; i < reply.ReduceNum; i++ {
				ofile, err := os.Create("tmp" + MapOutFile(reply.TaskIndex, i))
				if err != nil {
					log.Printf("Fail to create file tmp-mr-%v-%v", reply.TaskIndex, i)
				}
				for _, kv := range hashedmap[i] {
					fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
				}
				ofile.Close()
			}

		} else if reply.TaskType == REDUCE {
			// handle REDUCE task
			// 读取文件(对于每个map任务都有一个对应的reduce感兴趣输出文件)
			// 进行排序，输入REDUCE function，得到结果

			var lines []string
			for mi := 0; mi < reply.MapNum; mi++ {
				inputFile := MapOutFile(mi, reply.TaskIndex)
				file, err := os.Open(inputFile)
				if err != nil {
					log.Fatalf("Failed to open the '%d'st map output file %s: %e", mi, inputFile, err)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("Failed to read map output file %s: %e", inputFile, err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)
			}
			var kvslices []KeyValue

			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				parts := strings.Split(line, "\t")
				kvslices = append(kvslices, KeyValue{
					Key:   parts[0],
					Value: parts[1],
				})
			}
			sort.Sort(ByKey(kvslices))
			ofile, err := os.Create("tmp-mr-out-" + strconv.Itoa(reply.TaskIndex))
			if err != nil {
				log.Printf("Fail to create tmp-mr-out-%v file", reply.TaskIndex)
			}
			i := 0
			for i < len(kvslices) {
				j := i + 1
				for j < len(kvslices) && kvslices[j].Key == kvslices[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvslices[k].Value)
				}
				output := reducef(kvslices[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kvslices[i].Key, output)

				i = j
			}

			ofile.Close()
		}

		LastTaskIndex = reply.TaskIndex
		LastTaskType = reply.TaskType
		log.Printf("Finished %s task, index is: %d", reply.TaskType, reply.TaskIndex)
	}
	log.Printf("Worker %s exit\n", id)

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func MapOutFile(maptasknum int, reducetasknum int) string {
	ans := "mr-" + strconv.Itoa(maptasknum) + "-" + strconv.Itoa(reducetasknum)
	return ans
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
