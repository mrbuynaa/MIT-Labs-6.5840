package mr

import "encoding/json"
import "fmt"
import "log"
import "net"
import "net/http"
import "net/rpc"
import "os"
import "sort"
import "time"
import "hash/fnv"
import "github.com/google/uuid"

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

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := uuid.New().String()

	l := startWorkerServer(workerId)
	defer l.Close()

	Subscribe(SubscribeArgs{WorkerId: workerId})

	for {
		reply, ok := RequestTask(workerId)
		if !ok {
			return
		}
		if reply.JobDone {
			fmt.Printf("Worker %v: job done, exiting\n", workerId)
			return
		}

		switch reply.TaskType {
		case NoTask:
			time.Sleep(requestRetryInterval)
		case MapTask:
			fmt.Printf("Worker %v got map task %v (nReduce=%d)\n", workerId, reply.File, reply.NReduce)
			kva := runMap(mapf, reply.File)
			writeIntermediate(kva, reply.TaskId, reply.NReduce)
			fmt.Printf("Worker %v: map %d on %v wrote %d key/values\n", workerId, reply.TaskId, reply.File, len(kva))
			ReportTaskDone(workerId, MapTask, reply.TaskId)
		case ReduceTask:
			fmt.Printf("Worker %v got reduce task %d (nMap=%d)\n", workerId, reply.TaskId, reply.NMap)
			runReduce(reducef, reply.TaskId, reply.NMap)
			fmt.Printf("Worker %v: reduce %d wrote mr-out-%d\n", workerId, reply.TaskId, reply.TaskId)
			ReportTaskDone(workerId, ReduceTask, reply.TaskId)
		}
	}
}

func runMap(mapf func(string, string) []KeyValue, filename string) []KeyValue {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v: %v", filename, err)
	}
	return mapf(filename, string(content))
}

func writeIntermediate(kva []KeyValue, mapId int, nReduce int) {
	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for y := 0; y < nReduce; y++ {
		f, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-*", mapId, y))
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}
		files[y] = f
		encoders[y] = json.NewEncoder(f)
	}

	for _, kv := range kva {
		y := ihash(kv.Key) % nReduce
		if err := encoders[y].Encode(&kv); err != nil {
			log.Fatalf("cannot encode %v: %v", kv, err)
		}
	}

	for y, f := range files {
		if err := f.Close(); err != nil {
			log.Fatalf("cannot close %v: %v", f.Name(), err)
		}
		final := fmt.Sprintf("mr-%d-%d", mapId, y)
		if err := os.Rename(f.Name(), final); err != nil {
			log.Fatalf("cannot rename %v to %v: %v", f.Name(), final, err)
		}
	}
}

func runReduce(reducef func(string, []string) string, reduceId int, nMap int) {
	kva := []KeyValue{}
	for x := 0; x < nMap; x++ {
		name := fmt.Sprintf("mr-%d-%d", x, reduceId)
		f, err := os.Open(name)
		if err != nil {
			log.Fatalf("cannot open %v: %v", name, err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}

	sort.Slice(kva, func(i, j int) bool { return kva[i].Key < kva[j].Key })

	out, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-*", reduceId))
	if err != nil {
		log.Fatalf("cannot create temp file: %v", err)
	}

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		fmt.Fprintf(out, "%v %v\n", kva[i].Key, reducef(kva[i].Key, values))
		i = j
	}

	if err := out.Close(); err != nil {
		log.Fatalf("cannot close %v: %v", out.Name(), err)
	}
	final := fmt.Sprintf("mr-out-%d", reduceId)
	if err := os.Rename(out.Name(), final); err != nil {
		log.Fatalf("cannot rename %v to %v: %v", out.Name(), final, err)
	}
}

const requestRetryInterval = 500 * time.Millisecond

// WorkerServer holds the RPC methods the coordinator can call on a worker.
type WorkerServer struct{}

func (w *WorkerServer) Ping(args *PingArgs, reply *PingReply) error {
	reply.Ok = true
	return nil
}

// startWorkerServer serves WorkerServer RPCs on this worker's socket.
func startWorkerServer(workerId string) net.Listener {
	server := rpc.NewServer()
	server.Register(&WorkerServer{})

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, server)

	sockname := workerSock(workerId)
	os.Remove(sockname)
	l, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("worker listen error:", err)
	}
	go http.Serve(l, mux)
	return l
}

func Subscribe(args SubscribeArgs) {
	reply := SubscribeReply{}
	ok := call("Coordinator.Subscribe", &args, &reply)
	if ok {
		fmt.Printf("Worker %v subscribed\n", args.WorkerId)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func RequestTask(workerId string) (reply RequestTaskReply, ok bool) {
	args := RequestTaskArgs{WorkerId: workerId}
	ok = call("Coordinator.RequestTask", &args, &reply)
	return reply, ok
}

// ReportTaskDone tells the coordinator this worker finished a task.
// It returns false if the RPC failed or the coordinator rejected the report
// (the task had already been reassigned after a timeout).
func ReportTaskDone(workerId string, taskType TaskType, taskId int) bool {
	args := TaskDoneArgs{WorkerId: workerId, TaskType: taskType, TaskId: taskId}
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	return ok && reply.IsOk
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
