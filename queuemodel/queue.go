package queuemodel

import (
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/myfantasy/mffc/fh"
	"github.com/myfantasy/mfq/queue"
	"github.com/myfantasy/mft"
)

// Queue - очередь
type Queue struct {
	Blocks []*QueueBlock `json:"blocks"`

	mx  sync.RWMutex
	mxF sync.RWMutex

	Rvg *mft.G `json:"-"`
	fp  fh.FileProvider

	MaxL   int           `json:"max_lenght"`
	MaxCnt int           `json:"max_count"`
	MaxD   time.Duration `json:"max_duration"`

	BackgroundTimeout time.Duration `json:"background_timeout"`

	Path string `json:"data_place"`

	SaveType int `json:"save_type"`

	needSave bool

	mapOfSync map[int64]*savwWait
	chs       []chan bool

	isOnline bool

	ep mft.ErrorProvider
}

// QueueCreate create new queue
func QueueCreate(rvg *mft.G, fp fh.FileProvider, path string, saveType int, ep mft.ErrorProvider, maxL int, maxCnt int, maxD time.Duration, backgroundTimeout time.Duration) (q *Queue, err *mft.Error) {
	q = &Queue{
		Blocks:    make([]*QueueBlock, 0),
		mapOfSync: make(map[int64]*savwWait),
		chs:       make([]chan bool, 0),

		SaveType: saveType,

		isOnline: true,
		needSave: true,

		Rvg: rvg,
		fp:  fp,
		ep:  ep,

		Path: path,

		MaxL:   maxL,
		MaxCnt: maxCnt,
		MaxD:   maxD,

		BackgroundTimeout: backgroundTimeout,
	}

	err = q.SaveAllIlockRaw()

	if err != nil {
		return q, err
	}

	go q.backgroundProcessRun()

	return q, err
}

func (q *Queue) backgroundProcessRun() {
	for q.isOnline {
		time.Sleep(q.BackgroundTimeout)

		err := q.SaveAllIlockRaw()
		if err != nil {
			q.ep.Panicln(err)
		}
	}
	{
		err := q.SaveAllIlockRaw()
		if err != nil {
			q.ep.Panicln(err)
		}
	}
}

type savwWait struct {
	QB  *QueueBlock
	Chs []chan bool
}

// Close close queue
func (q *Queue) Close() {
	q.mx.Lock()
	defer q.mx.Unlock()
	q.isOnline = false
}

func mkSaveWait(qb *QueueBlock) *savwWait {
	sv := &savwWait{
		QB:  qb,
		Chs: make([]chan bool, 0),
	}
	return sv
}

func (q *Queue) addSaveWait(qb *QueueBlock, ch ...chan bool) {
	q.mx.Lock()
	defer q.mx.Unlock()
	q.addSaveWaitRaw(qb, ch...)
}
func (q *Queue) addSaveWaitRaw(qb *QueueBlock, ch ...chan bool) {
	_, ok := q.mapOfSync[qb.ID]
	if !ok {
		q.mapOfSync[qb.ID] = mkSaveWait(qb)
	}
	q.mapOfSync[qb.ID].Chs = append(q.mapOfSync[qb.ID].Chs, ch...)
}

// PathSelf file path
func (q *Queue) PathSelf() string {
	path := q.Path + "block_queue_data"
	return path
}

// SaveAllIlockRaw - save all changes without long data lock
func (q *Queue) SaveAllIlockRaw() (err *mft.Error) {

	q.mxF.Lock()
	defer q.mxF.Unlock()

	path := q.PathSelf()

	var b []byte
	var e error

	q.mx.Lock()
	ns := q.needSave
	mos := q.mapOfSync
	chs := q.chs
	if !ns && len(mos) == 0 {
		if len(chs) > 0 {

			q.chs = make([]chan bool, 0)

			for _, c := range chs {
				c <- true
			}
		}
		q.mx.Unlock()
		return nil
	}

	if ns {
		b, e = json.MarshalIndent(q, "", "\t")

		if e != nil {
			q.mx.Unlock()
			return mft.ErrorCE(100121, e).AppendS("Serialize Error")
		}

		q.needSave = false
	}

	if len(chs) > 0 {
		q.chs = make([]chan bool, 0)
	}
	if len(mos) > 0 {
		q.mapOfSync = make(map[int64]*savwWait)
	}

	q.mx.Unlock()

	var er0 *mft.Error
	for _, v := range mos {
		if er0 != nil {
			q.addSaveWait(v.QB, v.Chs...)
		} else {
			er0 = v.QB.SaveIlockRaw(true, q.fp, q.Path)
			if er0 != nil {
				q.addSaveWait(v.QB, v.Chs...)
			} else {
				for _, c := range v.Chs {
					c <- true
				}
			}
		}
	}

	if er0 != nil {
		if ns {
			q.mx.Lock()
			q.needSave = true
			q.mx.Unlock()
		}
		if len(chs) > 0 {
			q.mx.Lock()
			q.chs = append(q.chs, chs...)
			q.mx.Unlock()
		}
		return er0
	}

	if ns {
		ef := q.fp.FileReplace(path, b)

		if ef != nil {
			q.mx.Lock()
			q.needSave = true
			if len(chs) > 0 {
				q.chs = append(q.chs, chs...)
			}
			q.mx.Unlock()
			return mft.ErrorCE(100102, ef).AppendS("Save file error path = " + path)
		}
	}
	for _, c := range chs {
		c <- true
	}
	return nil
}

// Add message
func (q *Queue) Add(data []byte) (err *mft.Error) {
	q.mx.RLock()
	needW, isNew, qb, err := q.AddRaw(data, true)
	q.mx.RUnlock()

	if err != nil {
		return err
	}
	if needW {
		q.mx.Lock()
		_, isNew, qb, err = q.AddRaw(data, false)
		if err != nil {
			q.mx.Unlock()
			return err
		}
		q.mx.Unlock()
	}

	if q.SaveType == queue.SaveImmediately {
		q.addSaveWait(qb)
		erOut := q.SaveAllIlockRaw()
		return erOut
	}

	if q.SaveType == queue.SaveBackground {
		q.addSaveWait(qb)
		return nil
	}

	// else SaveBackgroundWait

	cQ := make(chan bool, 1)
	cB := make(chan bool, 1)

	q.addSaveWait(qb, cB)

	if isNew {
		q.mx.Lock()
		q.chs = append(q.chs, cQ)
		q.mx.Unlock()
	} else {
		cQ <- true
	}

	<-cQ
	<-cB

	return nil
}

// AddRaw message
func (q *Queue) AddRaw(data []byte, isR bool) (needW bool, isNew bool, qb *QueueBlock, err *mft.Error) {
	if len(q.Blocks) == 0 || !q.Blocks[len(q.Blocks)-1].Check(len(data), q.MaxL, q.MaxCnt, q.MaxD) {
		if isR {
			return true, false, qb, nil
		}
		qb = QueueBlockCreate(data, q.Rvg)
		q.Blocks = append(q.Blocks, qb)
		q.needSave = true
		return false, true, qb, nil

	}
	qb = q.Blocks[len(q.Blocks)-1]
	e := qb.Add(data)
	return false, false, qb, e
}

// SaveRaw save block on disk
func (q *Queue) SaveRaw() (err *mft.Error) {
	q.mxF.Lock()
	defer q.mxF.Unlock()

	if !q.needSave {
		return nil
	}

	path := q.PathSelf()

	b, e := json.MarshalIndent(q, "", "\t")

	if e != nil {
		return mft.ErrorCE(100101, e).AppendS("Serialize Error")
	}

	ef := q.fp.FileReplace(path, b)

	if ef != nil {
		return mft.ErrorCE(100102, ef).AppendS("Save file error path = " + path)
	}
	q.needSave = false

	return nil
}

// Get queue.Items from queue from KEY INCLUDE this key (or more only) and not more then COUNT
func (q *Queue) Get(key int64, include bool, count int) (itms []queue.Item, ok bool, err *mft.Error) {

	qb, ok, erS := q.FindBlock(key, include)

	if erS != nil {
		return nil, false, erS
	}

	if !ok {
		return make([]queue.Item, 0), ok, nil
	}

	er0 := qb.LoadIfNeed(q.fp, q.Path)
	if er0 != nil {
		return nil, false, erS
	}

	itms, ok = qb.FindElenment(key, count, include)

	return itms, ok, nil
}

// FindBlock find block with key
func (q *Queue) FindBlock(key int64, include bool) (qb *QueueBlock, ok bool, err *mft.Error) {
	if len(q.Blocks) == 0 {
		return nil, false, nil
	}

	if len(q.Blocks) == 0 {
		return nil, false, nil
	}

	idx := sort.Search(len(q.Blocks), func(i int) bool {
		return q.Blocks[i].IndexBefor(key)
	})

	if idx >= len(q.Blocks) {
		return nil, false, nil
	}

	if idx == 0 {
		return q.Blocks[idx], true, nil
	}

	er0 := q.Blocks[idx-1].LoadIfNeed(q.fp, q.Path)
	if er0 != nil {
		return nil, false, er0
	}

	_, okE := q.Blocks[idx-1].FindElenment(key, 1, include)

	if okE {
		return q.Blocks[idx-1], true, nil
	}

	return q.Blocks[idx], true, nil
}
