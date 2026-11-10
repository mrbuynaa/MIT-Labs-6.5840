package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

const taskTimeout = 10 * time.Second

type Coordinator struct {
	mu      sync.Mutex
	tasks   []Task // map tasks during the map phase, reduce tasks after
	phase   TaskType
	jobDone bool
	nMap    int
	nReduce int
	workers []Slave
}

type Task struct {
	id        int
	taskType  TaskType
	status    string
	file      string
	worker    *string
	startTime time.Time
}

type Slave struct {
	id   string
	task *Task
}

func Find(tasks []Task, targetFile string) *Task {
	for i := range tasks {
		if tasks[i].file == targetFile {
			return &tasks[i]
		}
	}
	return nil
}

func (c *Coordinator) Subscribe(args *SubscribeArgs, reply *SubscribeReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers = append(c.workers, Slave{
		id:   args.WorkerId,
		task: nil,
	})
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	if c.jobDone {
		reply.JobDone = true
		return nil
	}

	t := c.findIdleTask()
	if t == nil {
		t = c.findTimedOutTask()
	}
	if t == nil {
		return nil
	}

	workerId := args.WorkerId
	t.status = "in-progress"
	t.worker = &workerId
	t.startTime = time.Now()

	reply.File = t.file
	reply.TaskId = t.id
	reply.TaskType = t.taskType
	return nil
}

func (c *Coordinator) allTasksDone() bool {
	for i := range c.tasks {
		if c.tasks[i].status != "Done" {
			return false
		}
	}
	return true
}

func (c *Coordinator) findIdleTask() *Task {
	for i := range c.tasks {
		if c.tasks[i].status == "idle" {
			return &c.tasks[i]
		}
	}
	return nil
}

func (c *Coordinator) findTimedOutTask() *Task {
	for i := range c.tasks {
		t := &c.tasks[i]
		if t.status == "in-progress" && time.Since(t.startTime) > taskTimeout {
			return t
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.tasks {
		t := &c.tasks[i]
		if t.taskType != args.TaskType || t.id != args.TaskId {
			continue
		}
		if t.status == "in-progress" && t.worker != nil && *t.worker == args.WorkerId {
			t.status = "Done"
			reply.IsOk = true
			if c.allTasksDone() {
				c.nextPhase()
			}
		}
		return nil
	}
	return nil
}

func (c *Coordinator) nextPhase() {
	if c.phase == MapTask {
		c.tasks = make([]Task, c.nReduce)
		for y := 0; y < c.nReduce; y++ {
			c.tasks[y] = Task{
				id:       y,
				taskType: ReduceTask,
				status:   "idle",
			}
		}
		c.phase = ReduceTask
		return
	}
	c.jobDone = true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
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
	return c.jobDone
}

// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := []Task{}
	for i, file := range files {
		tasks = append(tasks, Task{
			id:       i,
			taskType: MapTask,
			status:   "idle",
			file:     file,
			worker:   nil,
		})
	}

	c := &Coordinator{
		tasks:   tasks,
		phase:   MapTask,
		nMap:    len(files),
		nReduce: nReduce,
		workers: []Slave{},
	}

	c.server()
	return c
}
