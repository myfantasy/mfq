package queuemodel

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/myfantasy/mffc/fh"
	"github.com/myfantasy/mft"
)

// QueueBlock - block of items of queue
type QueueBlock struct {
	ID int64 `json:"id"`

	Create time.Time `json:"create"`
	Last   time.Time `json:"last"`
	Count  int       `json:"count"`
	Size   int       `json:"size"`

	Items []Item `json:"-"`

	mx  sync.RWMutex
	mxF sync.RWMutex

	NeedSave bool `json:"-"`

	Rvg *mft.G `json:"-"`

	IsLoaded bool      `json:"-"`
	LastUse  time.Time `json:"-"`
}

// QueueBlockCreate QueueBlock create
func QueueBlockCreate(data []byte, rvg *mft.G) *QueueBlock {
	qb := &QueueBlock{
		IsLoaded: true,
		ID:       rvg.RvGet(),
		Create:   time.Now(),
		Items:    make([]Item, 0),
		Rvg:      rvg,
	}

	e := qb.Add(data)

	if e != nil {
		panic(e)
	}

	return qb
}

// Lock - lock
func (qb *QueueBlock) Lock() {
	qb.mx.Lock()
}

// Unlock - un lock
func (qb *QueueBlock) Unlock() {
	qb.mx.Unlock()
}

// RLock - lock read
func (qb *QueueBlock) RLock() {
	qb.mx.RLock()
}

// RUnlock - un lock read
func (qb *QueueBlock) RUnlock() {
	qb.mx.RUnlock()
}

// ILock - lock
func (qb *QueueBlock) ILock(ok bool) {
	if ok {
		qb.mx.Lock()
	}
}

// IUnlock - un lock
func (qb *QueueBlock) IUnlock(ok bool) {
	if ok {
		qb.mx.Unlock()
	}
}

// IRLock - lock read
func (qb *QueueBlock) IRLock(ok bool) {
	if ok {
		qb.mx.RLock()
	}
}

// IRUnlock - un lock read
func (qb *QueueBlock) IRUnlock(ok bool) {
	if ok {
		qb.mx.RUnlock()
	}
}

// AddRaw message
func (qb *QueueBlock) AddRaw(data []byte) (err *mft.Error) {

	if !qb.IsLoaded {
		return mft.ErrorCS(100001, "Block is Unloaded")
	}

	i := Item{
		Data: data,
		DT:   time.Now(),
		ID:   qb.Rvg.RvGet(),
	}

	qb.Items = append(qb.Items, i)
	qb.NeedSave = true
	qb.LastUse = time.Now()
	qb.Last = time.Now()
	qb.Count++
	qb.Size += len(data)

	return nil
}

// Add message
func (qb *QueueBlock) Add(data []byte) (err *mft.Error) {
	qb.mx.Lock()
	defer qb.mx.Unlock()

	return qb.AddRaw(data)
}

// Path file path
func (qb *QueueBlock) Path(path string) string {
	path += "block_" + strconv.Itoa(int(qb.ID))
	return path
}

// SaveIlockRaw save block on disk without long lock data
func (qb *QueueBlock) SaveIlockRaw(il bool, fp fh.FileProvider, path string) (err *mft.Error) {
	qb.mxF.Lock()
	defer qb.mxF.Unlock()
	path = qb.Path(path)

	qb.ILock(il)
	if !qb.IsLoaded {
		qb.IUnlock(il)
		return mft.ErrorCS(100022, "Block is Unloaded")
	}

	if !qb.NeedSave {
		qb.IUnlock(il)
		return nil
	}

	b, e := json.MarshalIndent(qb.Items, "", "\t")

	if e != nil {
		qb.IUnlock(il)
		return mft.ErrorCE(100023, e).AppendS("Serialize Error")
	}
	qb.NeedSave = false

	qb.IUnlock(il)

	ef := fp.FileReplace(path, b)

	if ef != nil {
		qb.ILock(il)
		qb.NeedSave = true
		qb.IUnlock(il)
		return mft.ErrorCE(100024, ef).AppendS("Save file error path = " + path)
	}

	return nil
}

// SaveRaw save block on disk
func (qb *QueueBlock) SaveRaw(fp fh.FileProvider, path string) (err *mft.Error) {
	qb.mxF.Lock()
	defer qb.mxF.Unlock()
	if !qb.IsLoaded {
		return mft.ErrorCS(100002, "Block is Unloaded")
	}

	if !qb.NeedSave {
		return nil
	}

	path = qb.Path(path)

	b, e := json.MarshalIndent(qb.Items, "", "\t")

	if e != nil {
		return mft.ErrorCE(100003, e).AppendS("Serialize Error")
	}

	ef := fp.FileReplace(path, b)

	if ef != nil {
		return mft.ErrorCE(100004, ef).AppendS("Save file error path = " + path)
	}
	qb.NeedSave = false

	return nil
}

// Save save block on disk
func (qb *QueueBlock) Save(fp fh.FileProvider, path string) (err *mft.Error) {
	qb.mx.Lock()
	defer qb.mx.Unlock()

	return qb.SaveRaw(fp, path)
}

// LoadRaw block from disk to memory
func (qb *QueueBlock) LoadRaw(fp fh.FileProvider, path string) (err *mft.Error) {
	qb.mxF.Lock()
	defer qb.mxF.Unlock()

	if qb.IsLoaded {
		return mft.ErrorCS(100005, "Block is Loaded")
	}

	path = qb.Path(path)

	b, ex, e := fp.FileLoadAndFix(path)

	if !ex {
		return mft.ErrorCS(100006, "File not exists path = "+path)
	}

	if e != nil {
		return mft.ErrorCE(100007, e).AppendS("Load file error path = " + path)
	}

	em := json.Unmarshal(b, &qb.Items)
	if e != nil {
		return mft.ErrorCE(100008, em).AppendS("Unmarshal file error path = " + path)
	}

	qb.IsLoaded = true
	qb.NeedSave = false
	qb.LastUse = time.Now()

	qb.Count = len(qb.Items)

	return nil
}

// Load block from disk to memory
func (qb *QueueBlock) Load(fp fh.FileProvider, path string) (err *mft.Error) {
	qb.mx.Lock()
	defer qb.mx.Unlock()

	return qb.LoadRaw(fp, path)
}

// UnloadRaw block from memory
func (qb *QueueBlock) UnloadRaw() bool {

	if qb.IsLoaded && !qb.NeedSave {
		qb.Items = make([]Item, 0)
		qb.IsLoaded = false
	}

	return false
}

// Unload block from memory
func (qb *QueueBlock) Unload() bool {
	qb.mx.Lock()
	defer qb.mx.Unlock()

	return qb.UnloadRaw()

}

// DeleteRaw block from memory
func (qb *QueueBlock) DeleteRaw(fp fh.FileProvider, path string) (err *mft.Error) {
	qb.mxF.Lock()
	defer qb.mxF.Unlock()

	path = qb.Path(path)

	ok, e := fp.Exists(path)
	if e != nil {
		return mft.ErrorCE(100009, e).AppendS("Exists file check fail path = " + path)
	}
	if !ok {
		return nil
	}

	ed := fp.Remove(path)
	if ed != nil {
		return mft.ErrorCE(100010, ed).AppendS("Remove file fail path = " + path)
	}

	return nil
}

// Delete block from memory
func (qb *QueueBlock) Delete(fp fh.FileProvider, path string) (err *mft.Error) {
	qb.mx.Lock()
	defer qb.mx.Unlock()

	return qb.DeleteRaw(fp, path)
}

// CheckRaw check can append
func (qb *QueueBlock) CheckRaw(l int, maxL int, maxCnt int, maxD time.Duration) bool {
	if !qb.IsLoaded {
		return false
	}

	if qb.Count+1 > maxCnt {
		return false
	}

	if qb.Size+l > maxL {
		return false
	}

	if qb.Create.Add(maxD).Before(time.Now()) {
		return false
	}

	return true
}

// Check check can append
func (qb *QueueBlock) Check(l int, maxL int, maxCnt int, maxD time.Duration) bool {
	qb.mx.Lock()
	defer qb.mx.Unlock()

	return qb.CheckRaw(l, maxL, maxCnt, maxD)
}
