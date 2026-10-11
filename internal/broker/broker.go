package broker

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-msgpack/v2/codec"
)

type Broker struct {
	queues map[string]*queue

	mu sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*queue),
	}
}

func (b *Broker) Publish(ctx context.Context, queue string, m Message) error {
	var body []byte
	if err := codec.NewEncoderBytes(&body, msgpackHandle).
		Encode(m.Data); err != nil {
		return err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return err
	}

	m.header = messageHeader{
		id:        id.String(),
		createdAt: time.Now(),
	}
	m.body = body
	m.Data = nil

	return b.getQueue(queue).send(ctx, &m)
}

func (b *Broker) Receive(ctx context.Context, queue string) (*Delivery, error) {
	message, err := b.getQueue(queue).receive(ctx)
	if err != nil {
		return nil, err
	}

	return &Delivery{
		Message: message,
		queue:   queue,
		broker:  b,
	}, nil
}

func (b *Broker) getQueue(name string) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if ok {
		return q
	}

	q = newQueue(name)
	b.queues[name] = q

	return q
}

func (b *Broker) ack(queue string, messageID string) error {
	return b.getQueue(queue).ack(messageID)
}

func (b *Broker) nack(ctx context.Context, queue string, messageID string) error {
	return b.getQueue(queue).nack(ctx, messageID)
}

func (b *Broker) touch(queue string, messageID string) error {
	return b.getQueue(queue).touch(messageID)
}
