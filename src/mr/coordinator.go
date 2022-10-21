package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
)

type Task struct {
	Type     string
	filename string
	index    int
	ddl time.Time
	// WorkerID string
}
type Coordinator struct {
	// Your definitions here.
	lock sync.Mutex

	stage          string
	nMap           int
	nReduce        int
	tasks          map[string]Task // 正在进行中的 task
	availableTasks chan Task       // 待分配的 task
}

// Your code here -- RPC handlers for the worker to call.

func TaskID(LasttaskType string, LasttaskIndex int) string {
	return fmt.Sprintf("%s-%d", LasttaskType, LasttaskIndex)
}

// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }
func (c *Coordinator) ApplyforTask(args *ApplyforTaskArgs, reply *ApplyforTaskReply) error {
	
	// 已经完成上次的任务，这次来找新任务，先存好输出
	if args.LastTaskType != "" {
		c.lock.Lock()
		
		LastTaskID := TaskID(args.LastTaskType, args.LastTaskIndex)
		if task, exist := c.tasks[LastTaskID]; exist {
			log.Printf("%s task %d has finished.\n", task.Type, task.index)
			if args.LastTaskType == MAP {
				for ri := 0; ri < c.nReduce; ri++ {
					err := os.Rename("tmp" + MapOutFile(args.LastTaskIndex, ri),
						MapOutFile(args.LastTaskIndex, ri))	
					if err != nil {
						log.Fatalf("Failed to rename map output file %s.\n", MapOutFile(args.LastTaskIndex, ri))
					}
					log.Printf("Have renamed map file!\n")
				}
			} else if args.LastTaskType == REDUCE {
				err := os.Rename("tmp-mr-out-" + strconv.Itoa(args.LastTaskIndex),
					"mr-out-" + strconv.Itoa(args.LastTaskIndex))
				if err != nil {
					log.Fatalf("Failed to rename reduce output file %s.\n","mr-out-" + strconv.Itoa(args.LastTaskIndex))
				}
				log.Printf("Have renamed reduce file!\n")
			}
			delete(c.tasks, LastTaskID)
			if len(c.tasks) == 0 {
				c.transit()
			}
		}
		c.lock.Unlock()
	}

	//接新任务
	log.Printf("Prepare to get new task!!!\n")
	task, ok := <- c.availableTasks
	if !ok {
		log.Printf("Cannot find new task in availableTasks")
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Assigning %s task %d to worker %s, the filename is %s\n", task.Type, task.index, args.WorkerID, task.filename)
	task.ddl = time.Now().Add(10 * time.Second)
	c.tasks[TaskID(task.Type, task.index)] = task
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	reply.TaskIndex = task.index
	reply.TaskType = task.Type
	reply.MapInputFile = task.filename
	

	return nil
}

func (c * Coordinator) transit() {
	if c.stage == MAP {
		log.Printf("All MAP tasks have finished. Transiting to REDUCE stage.\n")
		c.stage = REDUCE
		for i := 0; i < c.nReduce; i++ {
			task := Task {
				Type: REDUCE,
				index: i,
			}
			c.tasks[TaskID(task.Type, task.index)] = task
			c.availableTasks <- task
		}
	} else if c.stage == REDUCE {
		log.Printf("All REDUCE tasks have finished. Exiting now.\n")
		close(c.availableTasks)
		c.stage = ""
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
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
	ret := c.stage == ""

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	// stage          string
	// nMap           int
	// nReduce        int
	// tasks          map[string]Task // 正在进行中的 task
	// availableTasks chan Task       // 待分配的 task
	for i, file := range files {
		task := Task{
			Type:     MAP,
			filename: file,
			index:    i,
		}
		// Type     string
		// filename string
		// index    int
		// ddl time.Time
		
		c.tasks[TaskID(task.Type, task.index)] = task
		c.availableTasks <- task
	}
	log.Printf("Coordinator start\n")
	c.server()

	// use goroutine to collect Task
	go func()  {
		for {
			time.Sleep(5000 * time.Millisecond)

			c.lock.Lock()
			for _, task := range c.tasks {
				if time.Now().After(task.ddl) {
					log.Printf("Found timeout %s task %d. Reassigning now.", task.Type, task.index)
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}
