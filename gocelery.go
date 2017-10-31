package gocelery

import (
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron"

	// import rabbitmq broker
	_ "github.com/timezstyle/gocelery/broker/rabbitmq"

	"github.com/twinj/uuid"
)

const (
	// DefaultQueue is the default task queue name
	DefaultQueue = "celery"
)

// GoCelery creates an instance of entry
type GoCelery struct {
	config        *Config
	workerManager *workerManager
	cron          *cron.Cron
}

// New creates a GoCelery instance with given config
func New(config *Config) *GoCelery {
	if len(config.Queue) == 0 {
		config.Queue = []string{DefaultQueue}
	}
	if config.BrokerURL == "" {
		config.BrokerURL = "amqp://localhost"
	}
	gocelery := &GoCelery{
		config: config,
		workerManager: &workerManager{
			brokerURL: config.BrokerURL,
			queue:     config.Queue,
		},
		cron: cron.New(),
	}

	// try connect to worker
	if gocelery.workerManager.Connect() != nil {
		panic(fmt.Sprintf("Failed to connect to broker: %s", config.BrokerURL))
	}
	// start cron work
	gocelery.cron.Start()
	return gocelery
}

// Close disconnects with broker and cleans up all resources used.
// Use a defer statement to make sure resources are closed
func (gocelery *GoCelery) Close() {
	// make sure we're closed
	gocelery.workerManager.Close()
	gocelery.cron.Stop()
}

// EnqueueInQueue adds a task to queue to be executed immediately. If ignoreResult is true
// the function returns immediately with a nil channel returned. Otherwise, a result
// channel is returned so client can wait for the result.
func (gocelery *GoCelery) EnqueueInQueue(queueName string, taskName string, ignoreResult bool, args ...interface{}) (chan *TaskResult, error) {
	// log.Debugf("Enqueuing [%s] in queue [%s]", taskName, queueName)
	task := &Task{
		Task:    taskName,
		Args:    args,
		Kwargs:  nil,
		Eta:     celeryTime{time.Time{}},
		Expires: celeryTime{time.Time{}},
	}
	task.ID = uuid.NewV4().String()
	taskResult := make(chan *TaskResult)

	// make sure we subscribe to task result before we submit task
	// to avoid the problem that task may finish execution before we even subscribe
	// to result
	var wg sync.WaitGroup
	wg.Add(1)
	if !ignoreResult {
		// log.Debug("Waiting for Task Result: ", task.ID)
		taskResult = gocelery.workerManager.GetTaskResult(task)
		wg.Done()
	} else {
		wg.Done()
	}

	wg.Wait()
	// log.Debug("Publishing task: ", task.ID)
	task, err := gocelery.workerManager.PublishTask(queueName, task, ignoreResult)
	if err != nil {
		return nil, err
	}
	if ignoreResult {
		// log.Debug("Task Result is ignored.")
		return nil, nil
	}

	return taskResult, nil
}

func (gocelery *GoCelery) Enqueue(taskName string, ignoreResult bool, args ...interface{}) (chan *TaskResult, error) {
	queueName, ok := workerQueueRegistery[taskName]
	if !ok {
		queueName = DefaultQueue
	}
	return gocelery.EnqueueInQueue(queueName, taskName, ignoreResult, args...)
}

// EnqueueInQueueWithSchedule adds a task that is scheduled repeatedly.
// Schedule is specified in a string with cron format
func (gocelery *GoCelery) EnqueueInQueueWithSchedule(spec string, queueName string, taskName string, args ...interface{}) error {
	return gocelery.cron.AddFunc(spec, func() {
		// log.Infof("Running scheduled task %s: %s", spec, taskName)
		gocelery.EnqueueInQueue(queueName, taskName, true, args...)
	})
}

func (gocelery *GoCelery) EnqueueWithSchedule(spec string, taskName string, args ...interface{}) error {
	queueName, ok := workerQueueRegistery[taskName]
	if !ok {
		queueName = DefaultQueue
	}
	return gocelery.EnqueueInQueueWithSchedule(spec, queueName, taskName, args...)
}

// StartWorkers start running the workers with default queue
func (gocelery *GoCelery) StartWorkers() {
	gocelery.workerManager.Start(gocelery.config.Queue)
}
