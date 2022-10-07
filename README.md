# My MIT-6.824
## lab1：MapReduce
### 任务说明
lab1主要是实现一个分布式的MapReduce程序，其中包括coordinator与多个worker，它们间通过RPC交互来完成给定的MapReduce任务。
### MapReduce理解
> ![avatar](pic/MapReduce.png)

`MapReduce` 内部通常由一个Master节点以及多个worker节点组成，运行流程如上图所示。
1. `MapReduce` 程序将用户输入文件划分为 M 块，然后创建大量程序副本。
2. 其中一个程序为master，其余程序为worker，master节点负责将用户需要的 `map` 以及 `reduce` 任务分配给空闲的worker节点来执行。
3. 被分配 `map` 任务的worker程序读取将读取用户提供的输入片段，并解析出 `key/value pair`, 传递给用户自定义的 `Map` 函数，`Map` 函数执行用户的需求后会生成中间 `key/value pair` 并缓存在内存中。
4. 缓存中的 `key/value pair` 通过分区函数划分为R个区域，周期性的写入到本地磁盘中，并将存储位置信息发送给master，master将位置信息存储，为后续发送给负责 `reduce` 任务的worker作准备。
5. master将位置信息发送给负责 `reduce` 的worker节点，该类worker节点根据接收到的位置信息，通过RPC向 `map` worker所在主机的磁盘中读取这些缓存数据，当 `reduce` worker读取了所有的中间数据后，通过对key进行排序使得所有key值相同的数据聚合在一起。（这会经历shuffler这一过程，排序的原因是许多不同的key可能会映射到相同的 `reduce` 任务上。）
6. `reduce` worker将key值以及对应的value值传递给用户自定义的 `Reduce` 函数来完成归并操作， `Reduce` 函数的输出被追加到所属分区的输出文件中。
7. 当所有的 `map` 和 `reduce` 任务完成后，master便唤醒用户程序，在这个时候，用户程序对 `MapReduce` 的调用将会返回。
### 代码结构
`mapreduce` 部分代码位于 `src/mr` 文件夹下，其中，`coordinator.go` 文件中实现的为master程序的功能， `worker.go` 文件中实现的是worker程序的功能， `rpc.go` 中封装了两者间用于交互的数据结构。

其中，`Coordinator` 结构描述如下,它需要对 `map`、以及 `reduce`任务进行记录。
```go
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
```
- 程序运行
```bash
cd src/main

go build -race -buildmode=plugin ../mrapps/wc.go

rm mr-out*

go run -race mrcoordinator.go pg-*.txt
```
- 另起一终端
```bash
cd src/main
go run -race mrworker.go wc.so
cat mr-out-* | sort | more
```
### 框架实现
采取worker节点循环通过RPC向Coordinator节点获取任务的方式进行,并根据任务的类型进行不同的函数处理，处理完成后，再通过RPC发送通知给Coordinator进行消息的同步。
```go
// worker.go
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 循环获取任务
	for {
		req := GetTaskRequest{
			Id: 0,
		}

		reply := TaskReply{}
		// 获取任务的响应
		call("Coordinator.Get_task", &req, &reply)

		// map 任务的执行
		if reply.Job_type == MAPTASK {
            // 执行map任务
			filenames := handle_map(mapf, reply.Mfile_name, reply.Reduce_num, reply.Task_id)
			task_req := TaskFinishedRequest{
				Task_id:   reply.Task_id,
				File_name: filenames,
			}

			task_reply := TaskFinishedReply{
				Reply: 0,
			}

			call("Coordinator.Report_task", &task_req, &task_reply)
		} else if reply.Job_type == REDUCETASK {
            // 执行reduce任务
			handle_reduce(reducef, reply.Rfile_name)

			task_req := TaskFinishedRequest{
				Task_id:   reply.Task_id,
				File_name: make([]string, 0),
			}

			task_reply := TaskFinishedReply{
				Reply: 0,
			}
			call("Coordinator.Report_task", &task_req, &task_reply)
		} else if reply.Job_type == SLEEP {
			time.Sleep(time.Millisecond * 10)
		} else {
			log.Fatal("task is wrong")
		}

	}

}
```
`Coordinator` 中实现任务的分发，在任务分发过程中主要进行任务类别的判断，即当 `map` 任务全部执行完成后便开始执行 `reduce` 任务。
```go
// coordinator.go
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
}
```
work在获取到对应的任务之后，便会去执行相应的 `handle_map` 或者 `handle_reduce` 函数，在对map函数的处理中主要是将文件传送给用户自定义的 `Map` 函数中，然后将结果保存至中间文件中，对于reduce函数的处理则主要进行中间文件的获取并排序后，将文件输入到对应的用户自定义的 `Reduce` 函数中，获取到结果后，再将结果写入磁盘中即可。

待任务完成之后，则会调用RPC告诉Coordinator节点任务已完成的信息，Coordinator根据完成的信息进行整合，以待后续任务的分配。
```go
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
	return nil
}
```
