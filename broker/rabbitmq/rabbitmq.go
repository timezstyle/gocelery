package rabbitmq

import (
	"fmt"
	"sync"
	"time"

	"github.com/timezstyle/gocelery/broker"

	// ampq broker
	"github.com/streadway/amqp"
)

const (
	// DefaultExchange name
	DefaultExchange = "celery"
	// DefaultTaskResultExchange name
	DefaultTaskResultExchange = "celeryresults"
	// DefaultTaskEventExchange name
	DefaultTaskEventExchange = "celeryev"
)

// RabbitMqBroker implements RabbitMq broker
type RabbitMqBroker struct {
	sync.Mutex
	amqpURL string
	queues  map[string]bool

	connection      *amqp.Connection
	channel         *amqp.Channel
	resultsChannels map[string]*amqp.Channel
}

//
func init() {
	// register rabbitmq
	broker.Register("amqp", &RabbitMqBroker{})
	broker.Register("amqps", &RabbitMqBroker{})
}

func (b *RabbitMqBroker) String() string {
	return fmt.Sprintf("AMQP Broker [%s]", b.amqpURL)
}

// Connect to rabbitmq
func (b *RabbitMqBroker) Connect(uri string) error {
	b.amqpURL = uri
	// log.Debugf("Dialing [%s]", uri)
	// dial the server
	conn, err := amqp.Dial(b.amqpURL)
	if err != nil {
		return err
	}

	// create the channel
	b.connection = conn
	b.channel, err = b.connection.Channel()
	if err != nil {
		return err
	}

	b.resultsChannels = make(map[string]*amqp.Channel)

	// log.Debug("Connected to rabbitmq")
	//create exchanges
	// note that the exchange must be the same as celery to avoid fatal errors
	err = b.newExchange(DefaultExchange, "direct", true, false)
	if err != nil {
		return err
	}
	if err = b.newExchange(DefaultTaskResultExchange, "direct", true, false); err != nil {
		return err
	}
	if err = b.newExchange(DefaultTaskEventExchange, "topic", true, false); err != nil {
		return err
	}

	// log.Debug("Created exchanges")
	b.queues = make(map[string]bool)
	return nil
}

// Close the broker and cleans up resources
func (b *RabbitMqBroker) Close() error {
	// log.Debug("Closing broker: ", b.amqpURL)
	return b.connection.Close()
}

// GetTasks waits and fetches the tasks from queue
func (b *RabbitMqBroker) GetTasks(queueName string) <-chan *broker.Message {
	// check if queue has been created, if not, create a queue
	b.Lock()
	if val, exists := b.queues[queueName]; !exists || !val {
		if err := b.ensureQueueBind(queueName); err != nil {
			// log.Error("Failed to bind to queue: ", err)
			return nil
		}
		b.queues[queueName] = true
	}
	b.Unlock()

	msg := make(chan *broker.Message)
	go func() {
		// fetch messages
		// log.Infof("Waiting for tasks at: %s", b.amqpURL)
		deliveries, err := b.channel.Consume(
			queueName,
			"",    // Consumer
			false, // AutoAck
			false, false, false, nil)
		if err != nil {
			// log.Error("Failed to consume task messages: ", err)
			//TODO: deal with channel failure
			return
		}
		for delivery := range deliveries {
			// log.Debug("Got a message!")
			msg <- &broker.Message{
				ContentType: delivery.ContentType,
				Body:        delivery.Body,
			}
		}
		close(msg) // close message after channel closed
		// fake tests
		// for i := 0; i < 3; i++ {
		// 	args := []int{i, 5}
		// 	newArgs := make([]interface{}, len(args))
		// 	for i, v := range args {
		// 		newArgs[i] = interface{}(v)
		// 	}
		// 	task := &Task{
		// 		Task: "test.add",
		// 		ID:   fmt.Sprintf("%d", i),
		// 		Args: newArgs,
		// 	}
		// 	body, _ := json.Marshal(task)
		//
		// 	msg <- &broker.Message{ContentType: "application/json", Body: body}
		// 	time.Sleep(1 * time.Second)
		// }
	}()
	return msg
}

func (b *RabbitMqBroker) channelForTask(name string) *amqp.Channel {
	b.Lock()
	defer b.Unlock()

	if channel, ok := b.resultsChannels[name]; ok {
		return channel
	}
	channel, err := b.connection.Channel()
	if err != nil {
		return nil
	}
	b.resultsChannels[name] = channel
	return channel
}

// GetTaskResult fetchs task result for the specified taskID
func (b *RabbitMqBroker) GetTaskResult(taskID string) <-chan *broker.Message {
	msg := make(chan *broker.Message)
	// fetch messages
	// log.Debug("Waiting for Task Result Messages: ", taskID)
	channel := b.channelForTask(taskID)
	if channel == nil {
		// log.Error("Cannot get channel for task")
		return nil
	}

	// log.Debug("Creating queues for Task:", taskID)
	// create task result queue
	var arguments amqp.Table
	// queueExpires := viper.GetInt("resultQueueExpires") //ARGV:
	// if queueExpires > 0 {
	// 	arguments = amqp.Table{"x-expires": queueExpires}
	// }
	if err := b.newQueue(taskID, true, true, arguments); err != nil {
		// log.Error("Failed to create queue: ", err)
		return nil
	}
	// log.Debug("Created Task Result Queue")
	// bind queue to exchange
	if err := b.channelForTask(taskID).QueueBind(
		taskID,          // queue name
		taskID,          // routing key
		"celeryresults", // exchange name
		false,           // noWait
		nil,             // arguments
	); err != nil {
		return nil
	}

	go func() {
		deliveries, err := channel.Consume(
			taskID,
			taskID, // Consumer tag
			false,  // AutoAck
			false, false, false, nil)

		// delete channel
		if err != nil {
			// log.Error("Failed to consume task result messages: ", taskID, " error: ", err)
			//b.channel.QueueUnbind(taskID, taskID, "celeryresults", nil)
			// b.channel.Cancel(taskID, false)
			return
		}
		delivery := <-deliveries
		if delivery.Body == nil {
			// log.Error("Got a task result message: ", taskID, " body: ", string(delivery.Body))
		}
		msg <- &broker.Message{
			ContentType: delivery.ContentType,
			Body:        delivery.Body,
		}
		channel.Close()

		b.Lock()
		delete(b.resultsChannels, taskID)
		b.Unlock()
		// delete queue
		//b.channel.QueueUnbind(taskID, taskID, "celeryresults", nil)
		// err = b.channel.Cancel(taskID, false)
		//log.Info("Deleting queue: ", taskID, " err: ", err)

	}()
	return msg
}

func (b *RabbitMqBroker) ensureQueueBind(queueName string) error {
	// create and bind queues
	var arguments amqp.Table
	// queueExpires := viper.GetInt("queueExpires") //ARGV:
	// if queueExpires > 0 {
	// 	arguments = amqp.Table{"x-expires": queueExpires}
	// }
	if err := b.newQueue(queueName, true, false, arguments); err != nil {
		return err
	}
	// bind queue to exchange
	if err := b.channel.QueueBind(
		queueName,       // queue name
		queueName,       // routing key
		DefaultExchange, // exchange name
		false,           // noWait
		nil,             // arguments
	); err != nil {
		return err
	}
	// log.Debug("Queue is bound to exchange")
	return nil
}

// PublishTask sends a task to queue
func (b *RabbitMqBroker) PublishTask(queueName string, taskID string, message *broker.Message, ignoreResults bool) error {
	// check if queue has been created, if not, create a queue
	b.Lock()
	if val, exists := b.queues[queueName]; !exists || !val {
		if err := b.ensureQueueBind(queueName); err != nil {
			// log.Error("Failed to bind to queue: ", err)
			return err
		}
		b.queues[queueName] = true
	}
	b.Unlock()

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  message.ContentType,
		Body:         message.Body,
	}

	if !ignoreResults {

	} else {
		// log.Debug("Task Result ignored")
	}
	// log.Debug("Publishing Task to queue")
	routingKey := queueName // routing key is the same as queuename
	return b.channel.Publish(DefaultExchange, routingKey, false, false, msg)
}

// PublishTaskResult sends task result back to task queue
func (b *RabbitMqBroker) PublishTaskResult(key string, message *broker.Message) error {
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  message.ContentType,
		Body:         message.Body,
	}
	// log.Debug("Publishing Task Result:", key)
	return b.channel.Publish("celeryresults", key, false, false, msg)
}

// PublishTaskEvent sends task events back to event queue
func (b *RabbitMqBroker) PublishTaskEvent(key string, message *broker.Message) error {
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  message.ContentType,
		Body:         message.Body,
	}
	return b.channel.Publish("celeryev", key, false, false, msg)
}

func (b *RabbitMqBroker) newExchange(name string, exchangeType string, durable bool, autoDelete bool) error {
	err := b.channel.ExchangeDeclare(
		name,         // empty name exchange
		exchangeType, // direct or topic
		durable,      // durable true or false
		autoDelete,   // autoDelete
		false,        // internal
		false,        // noWait
		nil,
	)
	return err
}

func (b *RabbitMqBroker) newQueue(name string, durable bool, autoDelete bool, arguments amqp.Table) error {
	_, err := b.channel.QueueDeclare(
		name,       // queue name
		durable,    // durable
		autoDelete, // autoDelete
		false,      // exclusive
		false,      // noWait
		arguments,
	)
	return err
}

func queueName() string {
	return "celery"
}
