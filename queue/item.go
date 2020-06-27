package queue

import "time"

// Item - item of queue
type Item struct {
	ID   int64
	DT   time.Time
	Data []byte
}

// IndexBefor true when index bafor this block and not NeedRemove
func (qb Item) IndexBefor(i int64) bool {
	return i < qb.ID
}
