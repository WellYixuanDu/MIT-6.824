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
	"strconv"
	"strings"
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

// for sorting by key.
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

// 处理map任务
func handle_map(mapf func(string, string) []KeyValue, filename string, reduce_num int, task_id string) []string {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		//		println("hello world ", filename)
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// 执行用户自定义的mapfunction
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	filenames := make([]string, reduce_num)
	files := make([]*os.File, reduce_num)
	// 新建中间文件
	for i := 0; i < reduce_num; i++ {
		oname := "mr"
		oname = oname + "_" + task_id + "_" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		files[i] = ofile
		filenames[i] = oname
	}
	// 将数据以json格式写入到文件中
	for _, kv := range intermediate {
		index := ihash(kv.Key) % reduce_num
		enc := json.NewEncoder(files[index])
		enc.Encode(&kv)
	}
	return filenames
}

//处理reduce任务
func handle_reduce(reducef func(string, []string) string, filenames []string) string {

	files := make([]*os.File, len(filenames))
	intermediate := []KeyValue{}
	for i := 0; i < len(filenames); i++ { //读取所有的文件信息
		files[i], _ = os.Open(filenames[i])
		kv := KeyValue{}
		dec := json.NewDecoder(files[i])
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	// 进行排序
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-"

	index := filenames[0][strings.LastIndex(filenames[0], "_")+1:]
	oname = oname + index
	ofile, _ := os.Create(oname) //创建特定名字的输出文件

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	return oname
}

//
// main/mrworker.go calls this function.
//
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
