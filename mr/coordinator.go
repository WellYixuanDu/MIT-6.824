package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//全局变量，用于区分不同的task
var taskID int = 0

type IsTimeOut int32

const (
	NORMAL  IsTimeOut = 0
	TIMEOUT IsTimeOut = 1
)

// 正在执行的任务
type Task struct {
	name       string    //任务名字
	job_type   JobType   // 任务类别
	status     IsTimeOut // 是否超时
	mfile_name string    // 如果是map任务，则记录分配给该任务的文件名字
	rfile_name int       // reduce任务的任务编号
}

// 任务执行情况
type Status int32

const (
	TODO  Status = 0
	DOING Status = 1
	DONE  Status = 2
)

type Coordinator struct {

	// 需要完成的map文件、map产生的中间文件、当前正在执行的任务、
	map_task          map[string]Status //map任务的记录
	reduce_task       map[int]Status    //reduce任务的记录
	intermediate_file map[int][]string  //中间文件
	task_list_map     map[string]*Task  // 当前正在执行的任务
	mcount            int               // 已经完成的map数量
	rcount            int               // 已经完成的reduce数量
	reduce_num        int               // 需要完成的reduce任务数量
	m_finished        bool              // map任务是否已经完成
	mutex             sync.Mutex        //锁
}

// 超时处理
func (c *Coordinator) Handle_timeout(task_id string) {
	time.Sleep(time.Second * 10)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if t, ok := c.task_list_map[task_id]; ok {
		t.status = TIMEOUT
		if t.job_type == MAPTASK {
			f := t.mfile_name
			if c.map_task[f] == DOING {
				c.map_task[f] = TODO
			}
		} else if t.job_type == REDUCETASK {
			f := t.rfile_name
			if c.reduce_task[f] == DOING {
				c.reduce_task[f] = TODO
			}
		}
	}
}

// 通过rpc获取任务
func (c *Coordinator) Get_task(req *GetTaskRequest, reply *TaskReply) error {
	// 多个work并发访问任务请求，需要加锁
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.Mfile_name = ""
	reply.Rfile_name = make([]string, 0)
	reply.Reduce_num = c.reduce_num
	reply.Task_id = strconv.Itoa(taskID)
	taskID += 1
	// map任务是否全部完成
	if c.m_finished {
		// 遍历reduce任务，选择undo的任务进行执行
		for k, _ := range c.reduce_task {
			flag := c.reduce_task[k]
			if flag == DOING || flag == DONE {
				continue
			}
			c.reduce_task[k] = DOING
			for _, filename := range c.intermediate_file[k] {
				reply.Rfile_name = append(reply.Rfile_name, filename)
			}
			reply.Job_type = REDUCETASK
			tmp := &Task{reply.Task_id, REDUCETASK, NORMAL, "", k}
			c.task_list_map[reply.Task_id] = tmp
			// 超时任务处理
			go c.Handle_timeout(reply.Task_id)
			return nil
		}
		reply.Job_type = SLEEP
		return nil
	} else {
		// 遍历map任务，选择UNDO的任务进行执行
		for k, v := range c.map_task {
			flag := v
			if flag == DOING || flag == DONE {
				continue
			}
			c.map_task[k] = DOING
			reply.Mfile_name = k
			reply.Job_type = MAPTASK
			tmp := &Task{reply.Task_id, MAPTASK, NORMAL, reply.Mfile_name, -1}
			c.task_list_map[reply.Task_id] = tmp
			// 超时任务处理
			go c.Handle_timeout(reply.Task_id)
			return nil
		}
		// 若都没有则进行休眠
		reply.Job_type = SLEEP
		return nil
	}
	return nil
}

// 响应任务完成发来的请求
func (c *Coordinator) Report_task(req *TaskFinishedRequest, reply *TaskFinishedReply) error {
	reply.Reply = 1
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if t, ok := c.task_list_map[req.Task_id]; ok {
		flag := t.status
		// 任务超时，需要忽略该任务的请求，并需要将该任务删除
		if flag == TIMEOUT {
			delete(c.task_list_map, req.Task_id)
			return nil
		}
		if t.job_type == MAPTASK {
			filename := t.mfile_name
			c.map_task[filename] = DONE
			c.mcount += 1
			// 是否完成所有map任务
			if c.mcount == len(c.map_task) {
				c.m_finished = true
			}
			for _, v := range req.File_name {
				index := strings.LastIndex(v, "_")
				num, err := strconv.Atoi(v[index+1:])
				if err != nil {
					log.Fatal(err)
				}
				c.intermediate_file[num] = append(c.intermediate_file[num], v)
			}
			return nil
		} else if t.job_type == REDUCETASK {
			filename := t.rfile_name
			c.reduce_task[filename] = DONE
			c.rcount += 1
			delete(c.task_list_map, t.name)
			return nil

		} else {
			log.Fatal("任务类型错误")
			return nil
		}
	}
	log.Println("%s 任务不在Coordinator的任务列表中", req.Task_id)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.rcount == c.reduce_num {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		map_task:          make(map[string]Status),
		reduce_task:       make(map[int]Status),
		intermediate_file: make(map[int][]string),
		task_list_map:     make(map[string]*Task),
		mcount:            0,
		rcount:            0,
		m_finished:        false,
		reduce_num:        nReduce,
		mutex:             sync.Mutex{},
	}

	c.map_task = make(map[string]Status)
	for i := 0; i < len(files); i++ {
		c.map_task[files[i]] = TODO
	}

	c.reduce_task = make(map[int]Status)
	for i := 0; i < nReduce; i++ {
		c.reduce_task[i] = TODO
	}
	c.server()
	return &c
}
