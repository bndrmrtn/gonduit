package broker

import "context"

type Delivery struct {
	Message *Message

	queue  string
	broker *Broker
}

func (d *Delivery) Ack() error {
	return d.broker.ack(d.queue, d.Message.ID())
}

func (d *Delivery) Nack(ctx context.Context) error {
	return d.broker.nack(ctx, d.queue, d.Message.ID())
}

func (d *Delivery) Touch() error {
	return d.broker.touch(d.queue, d.Message.ID())
}
