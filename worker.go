package gocelery

import "sort"

// Execute is the definition of task execution
type Execute = func(*Task) (interface{}, error)

var (
	workerRegistery      = make(map[string]Execute)
	workerQueueRegistery = make(map[string]string)
)

// Constants
const (
	JSON string = "application/json"
)

func RegisterQueue(taskName string, queueName string) {
	workerQueueRegistery[taskName] = queueName
}

// RegisterWorker registers the worker with given task name
func RegisterWorker(taskName string, execute Execute) {
	workerRegistery[taskName] = execute
}

// RegisteredWorkers List all registered workers
func RegisteredWorkers() []string {
	keys := make([]string, 0, len(workerRegistery))
	for key := range workerRegistery {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// IsWorkerRegistered checks if worker exists for the task name
func IsWorkerRegistered(name string) bool {
	_, ok := workerRegistery[name]
	return ok
}
