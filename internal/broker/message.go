package broker

import (
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
)

type messageHeader struct {
	id        string
	createdAt time.Time
	attempts  int
}

type Message struct {
	Event   string
	Data    any
	Headers map[string]string

	header messageHeader
	body   []byte
}

func (m *Message) ID() string {
	return m.header.id
}

func (m *Message) CreatedAt() time.Time {
	return m.header.createdAt
}

func (m *Message) Attempts() int {
	return m.header.attempts
}

func (m *Message) Decode(v any) error {
	return codec.NewDecoderBytes(m.body, msgpackHandle).Decode(v)
}
