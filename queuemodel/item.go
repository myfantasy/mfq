package queuemodel

import "time"

// Item - item of queue
type Item struct {
	ID   int64
	DT   time.Time
	Data []byte
}
