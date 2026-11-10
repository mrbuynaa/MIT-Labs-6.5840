package mr

import "fmt"
import "log"
import "net"
import "net/http"
import "net/rpc"
import "os"
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

	// start listening before subscribing so the first ping can reach us
	l := startWorkerServer(workerId)
	defer l.Close()

	Subscribe(SubscribeArgs{WorkerId: workerId})

	// keep the worker alive so it can answer pings
	select {}
}

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
