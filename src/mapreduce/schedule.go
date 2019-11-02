package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	debug("Total %d files", len(mapFiles))
	workerPoolChan := make(chan string, ntasks)

	finish_count := make(chan int)

	for i, file := range mapFiles {
		i := i
		file := file
	  	go func() {
	  		addr := <- workerPoolChan
	  		debug("file %s task push to worker %s", file, addr)
			call(addr, "Worker.DoTask",
				DoTaskArgs{jobName, file, phase, i, n_other},
				new(struct{}))
	  		workerPoolChan <- addr
	  		finish_count <- 1
	  	}()
	}

	w := <- registerChan
	workerPoolChan <- w

	current := 0
	for {
		var w string
		select {
		case w = <- registerChan :
			debug("new worker found: %s", w)
			workerPoolChan <- w
		default:
			debug("non new worker registered")
		}

		<-finish_count
		current += 1;
		if current == len(mapFiles) {
			break
		}
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
