package mr

import "os"
import "strconv"

type SubscribeArgs struct {
	File     string
	WorkerId string
}

type SubscribeReply struct {
	Resp string
}

type TaskDoneArgs struct {
	WorkerId string
	TaskType TaskType
	TaskId   int
}

type TaskDoneReply struct {
	IsOk bool // false if the task was no longer assigned to this worker
}

type RequestTaskArgs struct {
	WorkerId string
}

type TaskType int

const (
	NoTask TaskType = iota
	MapTask
	ReduceTask
)

type RequestTaskReply struct {
	TaskType TaskType
	TaskId   int
	File     string
	NMap     int
	NReduce  int
	JobDone  bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
