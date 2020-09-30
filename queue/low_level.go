package queue

import "github.com/myfantasy/mft"

const (
	// SaveBackgroundWait save background after add and wait for save
	SaveBackgroundWait int = 0

	// SaveImmediately save immediately after add
	SaveImmediately int = 1
	// SaveBackground save background after add
	SaveBackground int = 2
)

// Queue of Items
type Queue interface {
	// Add data
	Add(data []byte) (err *mft.Error)
	// Get items after key or include key (when include == true) out not more count items
	Get(key int64, include bool, count int) (itms []Item, ok bool, err *mft.Error)
}

// Segmentation Queue with Segmentation of Items
type Segmentation interface {
	// Add data
	Add(segment int64, data []byte) (err *mft.Error)
	// Get items after key or include key (when include == true) out not more count items
	Get(segments []int64, key int64, include bool, count int) (itms []Item, ok bool, err *mft.Error)
}
