package gocelery

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-errors/errors"
	"github.com/timezstyle/gocelery/broker"
	"github.com/timezstyle/gocelery/serializer"
)

// WorkerManager starts and stop worker jobs
type workerManager struct {
	sync.Mutex
	brokerURL string
	broker    broker.Broker
	ticker    *time.Ticker // ticker for heartbeat
	queue     []string

	ch chan *broker.Message
}

// Connect to broker. Returns an error if connection fails.
func (manager *workerManager) Connect(ignoreErr bool) error {
	broker, err := broker.NewBroker(manager.brokerURL)
	if err != nil {
		if ignoreErr {
			log.Println("Failed to connect to broker: ", err)
		} else {
			log.Fatal("Failed to connect to broker: ", err)
		}
		return err
	}

	log.Println("Connected to broker: ", manager.brokerURL)
	manager.broker = broker
	return nil
}

func merge(cs ...<-chan *broker.Message) chan *broker.Message {
	var wg sync.WaitGroup
	out := make(chan *broker.Message)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan *broker.Message) {
		for n := range c {
			select {
			case out <- n:
				// case <-done:
			}
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// Start worker runs the worker command
func (manager *workerManager) Start(queues []string) {
	log.Println("Worker sendWorkerEvent")
	// now loops to wait for messages
	manager.sendWorkerEvent(WorkerOnline)

	log.Println("Worker after sendWorkerEvent")
	if manager.ch != nil {
		close(manager.ch)
	} else {
		// start hearbeat
		manager.startHeartbeat()
	}

	// start getting tasks
	taskChannels := make([]<-chan *broker.Message, 3)
	for _, queue := range queues {
		taskChannel := manager.broker.GetTasks(queue)
		taskChannels = append(taskChannels, taskChannel)
	}
	manager.ch = merge(taskChannels...)
	log.Println("Worker is now running")
	for {
		select {
		case message, ok := <-manager.ch:
			if !ok {
				log.Println("close old message channel")
				return
			}
			// log.Println("Message type: ", message.ContentType, " body:", string(message.Body))
			go func(message *broker.Message) {
				serializer, err := serializer.NewSerializer(message.ContentType)
				// convert message body to task
				if err == nil {
					var task Task

					err = serializer.Deserialize(message.Body, &task)
					if err == nil {
						task.ContentType = message.ContentType // stores the content type for the task
						// publish task received event
						// taskEventPayload, _ := serializer.Serialize(NewTaskReceivedEvent(&task))
						// manager.broker.PublishTaskEvent(strings.Replace(TaskReceived.RoutingKey(), "-", ".", -1),
						// 	&broker.Message{
						// 		Timestamp:   time.Now(),
						// 		ContentType: message.ContentType,
						// 		Body:        taskEventPayload,
						// 	})
						// log.Debug("Processing task: ", task.Task, " ID:", task.ID)

						// check eta
						duration := time.Duration(0)
						if !task.Eta.IsZero() {
							duration = task.Eta.Sub(time.Now().UTC())
						}
						if duration > 0 {
							timer := time.NewTimer(duration) // wait until ready
							go func() {
								<-timer.C
								manager.runTask(&task)
							}()
						} else {
							// execute the task in a worker immediately
							manager.runTask(&task)
						}
					}
				} else {
					// send errors to server
					// log.Error("Cannot deserialize message:", err)
				}
			}(message)
		}
	}
}

func (manager *workerManager) Reconnect() {
	manager.Lock()
	defer manager.Unlock()

	manager.Close()
	err := manager.broker.Connect(manager.brokerURL)
	log.Println("connect to", manager.brokerURL, err)
}

// PublishTask sends a task to task queue as a client
func (manager *workerManager) PublishTask(queueName string, task *Task, ignoreResult bool) (*Task, error) {
	res, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}
	message := &broker.Message{
		Timestamp:   time.Now(),
		ContentType: JSON,
		Body:        res,
	}
	err = manager.broker.PublishTask(queueName, task.ID, message, ignoreResult)
	if err != nil {
		manager.Reconnect()
	}
	// return the task object
	return task, err
}

// GetTaskResult listens to celery and returns the task result for given task
func (manager *workerManager) GetTaskResult(task *Task) chan *TaskResult {
	ch := manager.broker.GetTaskResult(task.ID)
	tc := make(chan *TaskResult)
	go func() {
		// listen to queue
		message := <-ch
		serializer, _ := serializer.NewSerializer(message.ContentType)
		// convert message body to task
		var taskResult TaskResult
		if message.Body != nil {
			serializer.Deserialize(message.Body, &taskResult)
		} else {
			// log.Errorf("Task result message is nil")
		}
		tc <- &taskResult
	}()
	return tc
}

func (manager *workerManager) startHeartbeat() {
	ticker := time.NewTicker(time.Second * 2)
	manager.ticker = ticker
	go func() {
		for _ = range ticker.C {
			// log.Println("Sent heartbeat at:", t)
			// send heartbeat
			manager.sendWorkerEvent(WorkerHeartbeat)
		}
	}()
}

func (manager *workerManager) sendTaskEvent(eventType EventType, payload []byte) {
	manager.broker.PublishTaskEvent(strings.Replace(eventType.RoutingKey(), "-", ".", -1),
		&broker.Message{Timestamp: time.Now(), ContentType: JSON, Body: payload})
}

// Send Worker Events
func (manager *workerManager) sendWorkerEvent(eventType EventType) {
	manager.Lock()
	defer manager.Unlock()
	workerEventPayload, _ := json.Marshal(NewWorkerEvent(eventType))
	err := manager.broker.PublishTaskEvent(strings.Replace(eventType.RoutingKey(), "-", ".", -1),
		&broker.Message{Timestamp: time.Now(), ContentType: JSON, Body: workerEventPayload})
	if err != nil {
		log.Println(err)
		manager.Close()
		err2 := manager.Connect(true)
		if err2 == nil {
			go manager.Start(manager.queue)
		}
		log.Println("worker reconnect", err2)
	}
}

func (manager *workerManager) stopHeartbeat() {
	if manager.ticker != nil {
		manager.ticker.Stop()
	}
}

// Stop the worker
func (manager *workerManager) Stop() {
	manager.stopHeartbeat()
	manager.sendWorkerEvent(WorkerOffline)
}

// Close the worker
func (manager *workerManager) Close() {
	manager.broker.Close()
}

func (manager *workerManager) runTask(task *Task) (*TaskResult, error) {

	// create task result
	taskResult := &TaskResult{
		ID:     task.ID,
		Status: Started,
	}

	taskEventType := None
	serializer, _ := serializer.NewSerializer(task.ContentType)
	var taskError error
	var taskEventPayload []byte

	if execute, ok := workerRegistery[task.Task]; ok {
		// log.Println("Working on task: ", task.Task)

		taskEventPayload, _ = serializer.Serialize(NewTaskStartedEvent(task))
		taskEventType = TaskStarted
		manager.sendTaskEvent(TaskStarted, taskEventPayload)

		// check expiration
		if !task.Expires.IsZero() && task.Expires.Before(time.Now().UTC()) {
			// time expired, make the task Revoked
			// log.Println("Task has expired", task.Expires)
			taskResult.Status = Revoked
			taskError = errors.New("Task has expired")
			taskEventPayload, _ = serializer.Serialize(NewTaskFailedEvent(task, taskResult, taskError))
			taskEventType = TaskRevoked
		} else {
			start := time.Now()
			result, err := execute(task)
			elapsed := time.Since(start)

			if err != nil {
				// log.Printf("Failed to execute task [%s]: %s\n", task.Task, err)
				err = errors.Wrap(err, 1)
			}

			if err != nil {
				taskError = err
				taskResult.Status = Failure
				taskResult.TraceBack = err.(*errors.Error).ErrorStack()
				taskEventPayload, _ = serializer.Serialize(NewTaskFailedEvent(task, taskResult, err))
				taskEventType = TaskFailed
			} else {
				taskResult.Status = Success
				taskResult.Result = result
				taskEventPayload, _ = serializer.Serialize(NewTaskSucceedEvent(task, taskResult, elapsed))
				taskEventType = TaskSucceeded
			}
		}
	} else {
		taskErrorMessage := fmt.Sprintf("Worker for task [%s] not found", task.Task)
		taskError = errors.New(taskErrorMessage)
		taskResult.Status = Failure
		taskResult.TraceBack = taskError.(*errors.Error).ErrorStack()
		taskEventPayload, _ = serializer.Serialize(NewTaskFailedEvent(task, taskResult, taskError))
		taskEventType = TaskFailed
	}

	res, _ := serializer.Serialize(taskResult)
	//key := strings.Replace(task.ID, "-", "", -1)
	manager.broker.PublishTaskResult(task.ID, &broker.Message{Timestamp: time.Now(), ContentType: task.ContentType, Body: res})

	// send task completed event
	if taskEventType != None {
		manager.sendTaskEvent(taskEventType, taskEventPayload)
	}

	return taskResult, nil
}
