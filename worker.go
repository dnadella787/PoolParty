package poolparty

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
)

type worker struct {
	id             int
	runningThreads *sync.WaitGroup
	workerChannels chan chan Job // buffered chan to let dispatcher know the worker is free
	jobChan        chan Job
}

func NewWorker(id int, workerChannels chan chan Job, runningThreads *sync.WaitGroup) *worker {
	return &worker{
		id:             id,
		runningThreads: runningThreads,
		workerChannels: workerChannels,
		jobChan:        make(chan Job),
	}
}

func (w *worker) Start(ctx context.Context) {
	go func() {
		l := log.WithFields(log.Fields{"workerThreadId": w.id})

		defer func() {
			w.runningThreads.Done()
			l.Debug("Worker thread terminated.")
		}()

		w.runningThreads.Add(1)
		l.Debug("Worker thread running.")
		for {
			w.workerChannels <- w.jobChan
			select {
			case <-ctx.Done():
				return
			case job := <-w.jobChan:
				job.Execute()
			}
		}
	}()
}
