package broker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type resizeImageData struct {
	Any string
	Num int
}

func TestPublishReceive(t *testing.T) {
	ctx := context.Background()
	broker := NewBroker()

	err := broker.Publish(ctx, "jobs", Message{
		Event: "image.resize",
		Data: resizeImageData{
			Any: "data",
			Num: 20,
		},
	})
	require.NoError(t, err)

	delivery, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	msg := delivery.Message

	require.NotEmpty(t, msg.ID())
	require.False(t, msg.CreatedAt().IsZero())

	var data resizeImageData
	require.NoError(t, msg.Decode(&data))

	require.Equal(t, "image.resize", msg.Event)
	require.Equal(t, "data", data.Any)
	require.Equal(t, 20, data.Num)

	require.NoError(t, delivery.Ack())
}

func TestReceiveContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := NewBroker()

	cancel()

	delivery, err := broker.Receive(ctx, "jobs")

	require.Nil(t, delivery)
	require.ErrorIs(t, err, context.Canceled)
}

func TestQueueFIFO(t *testing.T) {
	ctx := context.Background()
	broker := NewBroker()

	require.NoError(t, broker.Publish(ctx, "jobs", Message{
		Event: "first",
	}))

	require.NoError(t, broker.Publish(ctx, "jobs", Message{
		Event: "second",
	}))

	require.NoError(t, broker.Publish(ctx, "jobs", Message{
		Event: "third",
	}))

	first, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	second, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	third, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	require.Equal(t, "first", first.Message.Event)
	require.Equal(t, "second", second.Message.Event)
	require.Equal(t, "third", third.Message.Event)

	require.NoError(t, first.Ack())
	require.NoError(t, second.Ack())
	require.NoError(t, third.Ack())
}

func TestQueueIsolation(t *testing.T) {
	ctx := context.Background()
	broker := NewBroker()

	require.NoError(t, broker.Publish(ctx, "jobs", Message{
		Event: "job.event",
	}))

	require.NoError(t, broker.Publish(ctx, "emails", Message{
		Event: "email.event",
	}))

	jobDelivery, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	emailDelivery, err := broker.Receive(ctx, "emails")
	require.NoError(t, err)

	require.Equal(t, "job.event", jobDelivery.Message.Event)
	require.Equal(t, "email.event", emailDelivery.Message.Event)

	require.NoError(t, jobDelivery.Ack())
	require.NoError(t, emailDelivery.Ack())
}

func TestAckRemovesInflightMessage(t *testing.T) {
	ctx := context.Background()
	broker := NewBroker()

	require.NoError(t, broker.Publish(ctx, "jobs", Message{
		Event: "image.resize",
	}))

	delivery, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	q := broker.getQueue("jobs")
	require.Equal(t, 1, q.inflightCount())

	require.NoError(t, delivery.Ack())

	require.Equal(t, 0, q.inflightCount())
}

func TestNackRedeliversMessage(t *testing.T) {
	ctx := context.Background()
	broker := NewBroker()

	require.NoError(t, broker.Publish(ctx, "jobs", Message{
		Event: "image.resize",
	}))

	first, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	messageID := first.Message.ID()

	require.NoError(t, first.Nack(ctx))

	second, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	require.Equal(t, messageID, second.Message.ID())
	require.Equal(t, "image.resize", second.Message.Event)

	require.NoError(t, second.Ack())
}

func TestNackIncrementsAttempts(t *testing.T) {
	ctx := context.Background()
	broker := NewBroker()

	require.NoError(t, broker.Publish(ctx, "jobs", Message{
		Event: "image.resize",
	}))

	first, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	require.Equal(t, 0, first.Message.Attempts())

	require.NoError(t, first.Nack(ctx))

	second, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	require.Equal(t, 1, second.Message.Attempts())

	require.NoError(t, second.Ack())
}

func getLeaseUntil(q *queue, messageID string) time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.inflight[messageID].leaseUntil
}

func TestTouchExtendsLease(t *testing.T) {
	ctx := context.Background()
	broker := NewBroker()

	require.NoError(t, broker.Publish(ctx, "jobs", Message{
		Event: "image.resize",
	}))

	delivery, err := broker.Receive(ctx, "jobs")
	require.NoError(t, err)

	q := broker.getQueue("jobs")
	id := delivery.Message.ID()

	before := getLeaseUntil(q, id)

	require.NoError(t, delivery.Touch())

	after := getLeaseUntil(q, id)

	require.True(t, after.After(before))

	require.NoError(t, delivery.Ack())
}
