package mr

import "os"
import "fmt"
import "log"
import "net"
import "sync"
import "time"
import "net/rpc"
import "net/http"
import "strconv"

type Master struct {
	files []string // input files
	nReduce int    // number of reduce'
	filesNum int
	
	todoMap map[string]bool
	doingMap map[string]bool
	doneMap map[string]bool

	todoReduce map[int]bool
	doingReduce map[int]bool
	doneReduce map[int]bool

	mapDone bool
	reduceDone bool

	exMapNum int	
	exReduceNum int	
	mux sync.Mutex
}

//
// Timer(10s) for map worker when assigning map job
//
func (m* Master) mapTimer10(name string) {
	time.Sleep(10 * time.Second)
	m.mux.Lock()
	defer m.mux.Unlock()
	
	_, found := m.doingMap[name]
	if found { // not deleted from doing (meaning not mapped after timeout)
		fmt.Printf("[%s] not mapped!\n", name)

		delete(m.doingMap, name)
		m.todoMap[name] = true

	} else { // already deleted from doing
		fmt.Printf("MAPPED [%s]\n", name)

		// already deleted at reply before 10s
		// del from todo, add into done
		delete(m.todoMap, name)
		m.doneMap[name] = true
		
		// if map done. ready to reduce(init)
		if m.mapDone == false && len(m.doneMap) == m.filesNum {
			m.mapDone = true
			for i:=0; i < m.nReduce; i++ {
				m.todoReduce[i] = true
			}
		}
	}
}

//
// Timer(10s) for reduce worker when assigning reduce job
//
func (m* Master) reduceTimer10(name int) {
	time.Sleep(10 * time.Second)
	m.mux.Lock()
	defer m.mux.Unlock()

	_, found := m.doingReduce[name]
	if found {
		fmt.Printf("[%d] not reduced!\n", name)

		// not deleted after 10s
		delete(m.doingReduce, name)
		m.todoReduce[name] = true

	} else { // not found
		fmt.Printf("REDUCED [%d]\n", name)

		// already deleted at reply before 10s
		// del from todo, add into done
		delete(m.todoReduce, name)
		m.doneReduce[name] = true

		// reduce Done
		if m.reduceDone == false && len(m.doneReduce) == m.nReduce {
			m.reduceDone = true
		}
	}
}

//
// the RPC request from worker to master (asking for map/reduce job)
//
func (m *Master) WorkerRequest(args *WorkerReqArgs, reply *MasterRepArgs) error {

	m.mux.Lock()
	defer m.mux.Unlock()
	
	reply.NReduce = m.nReduce
	// load requested info
	// find unvisit files and assign one worker
	if m.mapDone==false {
		m.exMapNum++
		reply.JobName = "map"
		reply.JobID = m.exMapNum

		// select one map job todo
		var fileName string
		for fn, _ := range m.todoMap {
			fileName = fn
			break
		}
		reply.FileName = fileName

		// not found map job todo
		if fileName == "" {
			return nil
		} else {
			// temporary delete file from todo, add into doing 
			delete(m.todoMap, fileName)
			m.doingMap[fileName] = true
			go m.mapTimer10(fileName)
		}

	} else if m.reduceDone == false {
		m.exReduceNum++
		reply.JobName = "reduce"
		reply.JobID = m.exReduceNum

		// select one reduce job todo
		var reduceID int = -1
		for rid, _ := range m.todoReduce {
			reduceID = rid
			break
		}
		reply.FileName = strconv.Itoa(reduceID)

		// not found reduce job todo
		if reduceID == -1 {
			return nil
		} else {
			// temporary delete file from todo, add into doing
			delete(m.todoReduce, reduceID)
			m.doingReduce[reduceID] = true
			go m.reduceTimer10(reduceID)
		}

	} else {
		reply.JobName = "done"
	}
	return nil
}

//
// the RPC request from worker to master (reply status for map/reduce job)
//
func (m *Master) WorkerReply(args *WorkerRepArgs, reply *MasterRepArgs) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	// prepare reply info
	fileName := args.FileName
	jobName := args.JobName

	if jobName == "map" {
		delete(m.doingMap, fileName)
	}
	if jobName == "reduce" {
		reduceID, _ := strconv.Atoi(fileName)
		delete(m.doingReduce, reduceID)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.reduceDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.files = files
	m.filesNum = len(files)
	
	m.todoMap = make(map[string]bool)
	m.doingMap = make(map[string]bool)
	m.doneMap = make(map[string]bool)

	m.todoReduce = make(map[int]bool)
	m.doingReduce = make(map[int]bool)
	m.doneReduce = make(map[int]bool)
	
	for _, file := range files {
		m.todoMap[file] = true
	}

	m.mapDone = false
	m.reduceDone = false
	m.exMapNum = 0
	m.exReduceNum = 0

	m.server()
	return &m
}
