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

// worker获取任务的请求
type GetTaskRequest struct {
	Id int
}

type JobType int32

const (
	MAPTASK    JobType = 0
	REDUCETASK JobType = 1
	SLEEP      JobType = 2
)

// coordinator 任务分发的响应
type TaskReply struct {
	Job_type   JobType
	Mfile_name string   // map 任务的文件名
	Task_id    string   // 任务id，全局唯一编号
	Rfile_name []string // reduce 任务的文件名
	Reduce_num int
}

// worker 完成任务后发送状态
type TaskFinishedRequest struct {
	Task_id   string   //任务id
	File_name []string // map生成的中间文件名
}

// coordinator 对任务完成后的响应
type TaskFinishedReply struct {
	Reply int
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
