package main

import (
	"container/list"
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Message struct {
	Value string
}

type Queue struct {
	messages []Message
	waiters  *list.List
	mu       sync.Mutex
}

type Waiter struct {
	msgChan chan Message
	ctx     context.Context
}

func NewQueue() *Queue {
	return &Queue{
		messages: make([]Message, 0),
		waiters:  list.New(),
	}
}

func (q *Queue) Put(msg Message) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.waiters.Len() > 0 {
		front := q.waiters.Front()
		waiter := front.Value.(*Waiter)
		q.waiters.Remove(front)

		select {
		case waiter.msgChan <- msg:
			return true
		default:
			q.messages = append(q.messages, msg)
			return true
		}
	}

	q.messages = append(q.messages, msg)
	return true
}

func (q *Queue) Get(ctx context.Context) (Message, bool) {
	q.mu.Lock()

	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		q.mu.Unlock()
		return msg, true
	}

	select {
	case <-ctx.Done():
		q.mu.Unlock()
		return Message{}, false
	default:
	}

	msgChan := make(chan Message, 1)
	waiter := &Waiter{
		msgChan: msgChan,
		ctx:     ctx,
	}

	element := q.waiters.PushBack(waiter)
	q.mu.Unlock()

	select {
	case msg := <-msgChan:
		return msg, true
	case <-ctx.Done():
		q.mu.Lock()
		if element.Value != nil {
			q.waiters.Remove(element)
		}
		q.mu.Unlock()
		return Message{}, false
	}
}

type MessageBroker struct {
	queues map[string]*Queue
	mu     sync.RWMutex
}

func NewMessageBroker() *MessageBroker {
	return &MessageBroker{
		queues: make(map[string]*Queue),
	}
}

func (b *MessageBroker) getOrCreateQueue(queueName string) *Queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	if q, exists := b.queues[queueName]; exists {
		return q
	}

	q := NewQueue()
	b.queues[queueName] = q
	return q
}

func main() {
	port := flag.String("port", "8080", "port to listen on")
	flag.Parse()

	broker := NewMessageBroker()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		queueName := strings.TrimPrefix(r.URL.Path, "/")
		if queueName == "" {
			http.Error(w, "queue name required", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodPut:
			v := r.URL.Query().Get("v")
			if v == "" {
				http.Error(w, "missing v parameter", http.StatusBadRequest)
				return
			}

			queue := broker.getOrCreateQueue(queueName)
			queue.Put(Message{Value: v})
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			queue := broker.getOrCreateQueue(queueName)

			timeoutStr := r.URL.Query().Get("timeout")
			var ctx context.Context

			if timeoutStr != "" {
				timeoutSec, err := strconv.Atoi(timeoutStr)
				if err != nil || timeoutSec < 0 {
					http.Error(w, "invalid timeout", http.StatusBadRequest)
					return
				}

				if timeoutSec == 0 {
					ctx = context.Background()
				} else {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
					defer cancel()
				}
			} else {
				ctx = context.Background()
			}

			msg, ok := queue.Get(ctx)
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}

			w.Write([]byte(msg.Value))

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	addr := ":" + *port
	fmt.Printf("Starting server on %s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		panic(err)
	}
}
