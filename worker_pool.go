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
	}, nil
}

// Start simply starts the dispatcher and keeps on running until Stop()
// is called
func (wp *WorkerPool) Start() {
	wp.startWithSelfTermination(false)
}

// RunTillTermination starts but will auto terminate itself when all
// work is done. i.e. job queue is empty and all workers are free
func (wp *WorkerPool) RunTillTermination() {
	wp.startWithSelfTermination(true)
}

// startWithSelfTermination will terminate the dispatcher
// and thread pool once the job queue is empty and all the
// workers are free if selfTerminate is set to true
func (wp *WorkerPool) startWithSelfTermination(selfTerminate bool) {
	for _, w := range wp.workers {
		w.Start(wp.workerCtx)
	}

	log.Debug("Worker thread pool initialized. Starting dispatcher...")
	wp.startDispatcher(selfTerminate)
}

func (wp *WorkerPool) startDispatcher(terminateSelf bool) {
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
		} else if terminateSelf && len(wp.workerChannels) == wp.workerSize {
			log.Debug("All jobs completed and workers free. Terminating workers and dispatcher.")
			wp.cancelDispatcher()
		}
		// TODO: maybe add an else w/ time.Sleep to reduce CPU cycle spinning
		wp.qMu.Unlock()
	}
}

// Stop sends kill signal to worker threads and waits on
// dispatcher to stop. Stop can be called multiple times
// without issue
func (wp *WorkerPool) Stop() {
	wp.cancelDispatcher()
	wp.dispatcherRunning.Wait()
}

func (wp *WorkerPool) Enqueue(job Job) {
	wp.qMu.Lock()
	defer wp.qMu.Unlock()
	wp.jobQueue.Enqueue(&job)
}

func (wp *WorkerPool) EnqueueJobs(jobs []*Job) {
	wp.qMu.Lock()
	defer wp.qMu.Unlock()

	for _, job := range jobs {
		wp.jobQueue.Enqueue(job)
	}
}
