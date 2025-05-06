# PoolParty
Thread pool implementation in golang. 

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

	pool.Enqueue(&ExampleJob{})
	pool.Start()
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
    pool.RunTillTermination()
}
```
