package poolparty

import (
	"context"
	"errors"
	"github.com/idsulik/go-collections/v3/queue"
	log "github.com/sirupsen/logrus"
	"sync"
)

type WorkerPool struct {
	workerSize        int
	workers           []*worker
	dispatcherCtx     context.Context
	cancelDispatcher  context.CancelFunc
	workerCtx         context.Context
	cancelWorkers     context.CancelFunc
	workerChannels    chan chan Job
	dispatcherRunning *sync.WaitGroup
	runningThreads    *sync.WaitGroup
	qMu               *sync.Mutex
	jobQueue          *queue.Queue[*Job]
	idle              bool
	idleCond          *sync.Cond
}

func NewWorkerPool(maxWorkers int, initJobQueueSize int) (*WorkerPool, error) {
	if maxWorkers <= 0 {
		return nil, errors.New("worker thread pool size must be greater than 0")
	}
	if initJobQueueSize <= 0 {
		return nil, errors.New("init job queue size must be greater than 1")
	}

	runningThreads := sync.WaitGroup{}
	workerChannels := make(chan chan Job, maxWorkers)
	workers := make([]*worker, maxWorkers)

	for i := 0; i < maxWorkers; i++ {
		workers[i] = NewWorker(i+1, workerChannels, &runningThreads)
	}

	dispatcherCtx, cancelDispatcher := context.WithCancel(context.Background())
	workerCtx, cancelWorkers := context.WithCancel(context.Background())

	return &WorkerPool{
		workerSize:        maxWorkers,
		workers:           workers,
		dispatcherCtx:     dispatcherCtx,
		cancelDispatcher:  cancelDispatcher,
		workerCtx:         workerCtx,
		cancelWorkers:     cancelWorkers,
		workerChannels:    workerChannels,
		dispatcherRunning: &sync.WaitGroup{},
		runningThreads:    &runningThreads,
		qMu:               &sync.Mutex{},
		jobQueue:          queue.New[*Job](initJobQueueSize),
		idle:              true,
		idleCond:          sync.NewCond(&sync.Mutex{}),
	}, nil
}

// Start simply starts the dispatcher and keeps on running until Stop()
// is called
func (wp *WorkerPool) Start() {
	for _, w := range wp.workers {
		w.Start(wp.workerCtx)
	}

	log.Debug("Worker thread pool initialized. Starting dispatcher...")
	go wp.startDispatcher()
}

// RunAndSelfTerminate will start the thread pool and run until the job
// queue is empty and the workers are all free. It will then terminate
// the thread pool
func (wp *WorkerPool) RunAndSelfTerminate() {
	wp.Start()
	wp.Await()
	wp.Stop()
}

// Await waits for the job queue to be empty and all workers to idle
func (wp *WorkerPool) Await() {
	wp.idleCond.L.Lock()
	defer wp.idleCond.L.Unlock()

	for !wp.idle {
		wp.idleCond.Wait()
	}
}

func (wp *WorkerPool) startDispatcher() {
	wp.dispatcherRunning.Add(1)
	defer wp.dispatcherRunning.Done()

	for {
		select {
		case <-wp.dispatcherCtx.Done():
			log.Debug("Received worker pool termination message")
			wp.cancelWorkers()
			wp.runningThreads.Wait()
			return
		default:
		}
		wp.qMu.Lock()
		if t, hasVal := wp.jobQueue.Dequeue(); hasVal {
			wp.qMu.Unlock()
			chosenWorker := <-wp.workerChannels
			chosenWorker <- *t
			continue
		} else if len(wp.workerChannels) == wp.workerSize { // all threads free and job queue is empty
			wp.idleCond.L.Lock()
			wp.idle = true
			wp.idleCond.Broadcast()
			wp.qMu.Unlock()

			// wait until more work gets queued up to reduce CPU spinning
			for wp.idle {
				wp.idleCond.Wait()
			}
			wp.idleCond.L.Unlock()
			continue
		}
		wp.qMu.Unlock()
	}
}

// Stop sends kill signal to worker threads and waits on
// dispatcher to stop. Note that Stop notifies the dispatcher
// that it is no longer idle.
// Stop can be called multiple times without issue.
func (wp *WorkerPool) Stop() {
	wp.cancelDispatcher()

	wp.idleCond.L.Lock()
	wp.idle = false
	wp.idleCond.Broadcast()
	wp.idleCond.L.Unlock()

	wp.dispatcherRunning.Wait()
}

func (wp *WorkerPool) Enqueue(job Job) {
	wp.qMu.Lock()
	defer wp.qMu.Unlock()

	emptyQ := wp.jobQueue.IsEmpty()
	wp.jobQueue.Enqueue(&job)

	if emptyQ {
		wp.idleCond.L.Lock()
		if wp.idle {
			wp.idle = false
			wp.idleCond.Signal()
		}
		wp.idleCond.L.Unlock()
	}
}

func (wp *WorkerPool) EnqueueJobs(jobs []*Job) {
	wp.qMu.Lock()
	defer wp.qMu.Unlock()

	emptyQ := wp.jobQueue.IsEmpty()
	for _, job := range jobs {
		wp.jobQueue.Enqueue(job)
	}

	if emptyQ {
		wp.idleCond.L.Lock()
		if wp.idle {
			wp.idle = false
			wp.idleCond.Signal()
		}
		wp.idleCond.L.Unlock()
	}
}
