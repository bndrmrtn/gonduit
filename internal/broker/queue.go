package broker

import (
	"context"
	"errors"
	"sync"
	"time"
)

type inflightMessage struct {
	message    *Message
	leaseUntil time.Time
}

type queue struct {
	name     string
	messages chan *Message

	inflight map[string]*inflightMessage
	mu       sync.RWMutex
}

func newQueue(name string) *queue {
	return &queue{
		name:     name,
		messages: make(chan *Message, 100),
		inflight: make(map[string]*inflightMessage),
	}
}

func (q *queue) send(ctx context.Context, msg *Message) error {
	select {
	case q.messages <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *queue) receive(ctx context.Context) (*Message, error) {
	select {
	case msg := <-q.messages:
		q.mu.Lock()
		q.inflight[msg.ID()] = &inflightMessage{
			message:    msg,
			leaseUntil: time.Now().Add(30 * time.Second),
		}
		q.mu.Unlock()
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *queue) inflightCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.inflight)
}

func (q *queue) ack(messageID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.inflight[messageID]; !ok {
		return errors.New("message is not inflight")
	}

	delete(q.inflight, messageID)

	return nil
}

func (q *queue) nack(ctx context.Context, messageID string) error {
	q.mu.Lock()

	inflight, ok := q.inflight[messageID]
	if !ok {
		q.mu.Unlock()
		return errors.New("message is not inflight")
	}

	delete(q.inflight, messageID)
	msg := inflight.message
	msg.header.attempts++

	q.mu.Unlock()

	select {
	case q.messages <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *queue) touch(messageID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	inflight, ok := q.inflight[messageID]
	if !ok {
		return errors.New("message is not inflight")
	}

	inflight.leaseUntil = time.Now().Add(30 * time.Second)

	return nil
}
