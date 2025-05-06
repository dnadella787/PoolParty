# PoolParty
Thread pool implementation in golang. 


## How to install
`go get github.com/dnadella787/poolparty`
## Examples
### Simple start
```go
type ExampleJob struct{}

func (e *ExampleJob) Execute() {
    time.Sleep(2 * time.Second)
    log.Debug("example job completed")
}

func main() {
    pool, err := poolparty.NewWorkerPool(10, 100)
    defer pool.Stop()
    
    if err != nil {
        log.Fatal("Error initializing thread pool")
    }
    
    pool.Start()
    // execute rest along main thread
    for i := range 10 {
        pool.Enqueue(&ExampleJob{})
    }
	// await all jobs to complete 
    pool.Await()
}
```

### Simple auto terminate
```go
type ExampleJob struct{}

func (e *ExampleJob) Execute() {
    time.Sleep(2 * time.Second)
    log.Debug("example job completed")
}

func main() {
    pool, err := poolparty.NewWorkerPool(10, 100)
    defer pool.Stop()
    
    if err != nil {
        log.Fatal("Error initializing thread pool")
    }
	
    pool.Enqueue(&ExampleJob{}) 
    // Auto terminate once all jobs are completed
    pool.RunAndSelfTerminate()
}
```
