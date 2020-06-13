package mr

import "os"
import "fmt"
import "log"
import "sort"
import "time"
import "net/rpc"
import "strconv"
import "hash/fnv"
import "io/ioutil"
import "encoding/json"
import "path/filepath"

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// request task from master
	// get jobName, fileName
	// if map: fileName = input file
	// if reduce: fileName = string(reduceID)
	for {
		jobName, fileName, jobID, nReduce := WorkerRequestJob()
		if jobName == "done" {
			break
		}
		if jobName == "map" {
			file, err := os.Open(fileName)			
			// empty filename means currently no map job available
			if fileName == "" {
				// fmt.Printf("......\n")
				time.Sleep(2 * time.Second)
				continue
			}
			if err != nil { log.Fatalf("cannot open %v", fileName) }
			
			content, err := ioutil.ReadAll(file)
			if err != nil { log.Fatalf("cannot read %v", fileName) }
			file.Close()

			// intermediate key value
			kva := mapf(fileName, string(content))
			intermediate := make([][]KeyValue, nReduce)
			for _, kv := range kva {
				h := ihash(kv.Key)
				h %= nReduce
				intermediate[h] = append(intermediate[h], kv)
			}

			// save intermediate in tmp file (incase of err)
			for i := 0; i < nReduce; i++ {
				tmpfile, err := ioutil.TempFile("", "") // dir, pattern string
				if err != nil { log.Fatal(err) }
				
				enc := json.NewEncoder(tmpfile)
				for _, kv := range intermediate[i] {
					if err:=enc.Encode(&kv); err != nil {
						log.Fatal(err)
					}
				}

				if err:= tmpfile.Close(); err != nil {
					log.Fatal(err)
				}

				// fmt.Printf("map[%d] gen itm_%d_%d\n", jobID, jobID, i)
				os.Rename(tmpfile.Name(), fmt.Sprintf("itm_%d_%d", jobID, i))
			}
			fmt.Printf("map(%d) done [%s]\n", jobID, fileName)
			WorkerReplyJob(jobName, fileName, jobID)
		}

		if jobName == "reduce" {
			kva := []KeyValue{}
			reduceID, _ := strconv.Atoi(fileName)
			if reduceID == -1 {
				// fmt.Printf("------\n")
				time.Sleep(2 * time.Second)
				continue
			}
			fmt.Printf("reduce(%d) todo [%d]\n", jobID, reduceID)

			itms, err := filepath.Glob(fmt.Sprintf("itm_*_%d", reduceID))

			if err != nil { log.Fatalf("cannot open itm_*_%d", reduceID) }

			// load from all intermediates of this reduce
			for _, itm := range itms {
				file, err := os.Open(itm)
				if err != nil { log.Fatal(err) }
				defer file.Close()
				// var r io.Reader = file
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			
			oname := fmt.Sprintf("mr-out-%d", reduceID)
			ofile, _ := os.Create(oname)
			
			i := 0
			for i < len(kva) {
				j := i+1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				// fmt.Printf("my value: %v %v\n", kva[i].Key, output)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()
			fmt.Printf("reduce(%d) done [%d]\n", jobID, reduceID)
			// delete itm
			for _, itm := range itms {
				os.Remove(itm)
			}

			WorkerReplyJob(jobName, fileName, jobID)
		}
		// relax
		time.Sleep(1 * time.Second)
	}
}

//
// worker call this function to request master for job
//
func WorkerRequestJob() (jobName string, fileName string, jobID int, nReduce int) {
	args := WorkerReqArgs{}
	reply := MasterRepArgs{}
	call("Master.WorkerRequest", &args, &reply)

	jobName = reply.JobName
	fileName = reply.FileName
	jobID = reply.JobID
	nReduce = reply.NReduce

	fmt.Printf("%s(%d) REQ: GET [%s]\n", jobName, jobID, fileName)
	return jobName, fileName, jobID, nReduce
}

//
// worker call this function to reply master the job status
//
func WorkerReplyJob(jobName string, fileName string, jobID int) {
	args := WorkerRepArgs{}
	reply := MasterRepArgs{}
	args.JobName = jobName
	args.FileName = fileName
	args.JobID = jobID

	call("Master.WorkerReply", &args, &reply)
	fmt.Printf("%s(%d) REP: DONE [%s]\n", jobName, jobID, fileName)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
