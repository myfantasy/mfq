package queuemodel

import (
	"testing"
	"time"

	"github.com/myfantasy/mffc/fod"
	"github.com/myfantasy/mft"

	log "github.com/sirupsen/logrus"
)

func TestQueue(t *testing.T) {

	fod := fod.FileOnDiskSimpleCreate()
	err := fod.MkDirIfNotExists("test/tq1/")
	if err != nil {
		t.Fatal("MkDirIfNotExists", err)
	}

	rvg := &mft.G{}

	// t.Fatal("Step0")

	q, er0 := QueueCreate(rvg, fod, "test/tq1/", SaveBackgroundWait, log.New(), 1e6, 1e4, time.Minute*10, time.Millisecond*300)

	if er0 != nil {
		t.Fatal("QueueCreate", er0)
	}

	er1 := q.Add([]byte("test"))
	if er1 != nil {
		t.Fatal("Queue q.Add", er1)
	}

	er2 := q.Add([]byte("test2"))
	if er2 != nil {
		t.Fatal("Queue q.Add 2", er2)
	}
	er3 := q.Add([]byte("test3"))
	if er3 != nil {
		t.Fatal("Queue q.Add 3", er3)
	}
	er4 := q.Add([]byte("test4"))
	if er4 != nil {
		t.Fatal("Queue q.Add 4", er4)
	}
	er5 := q.Add([]byte("test5"))
	if er5 != nil {
		t.Fatal("Queue q.Add 5", er5)
	}

}

func BenchmarkQueueSaveBackgroundWait(b *testing.B) {
	fod := fod.FileOnDiskSimpleCreate()
	err := fod.MkDirIfNotExists("test/tq2/")
	if err != nil {
		b.Fatal("MkDirIfNotExists", err)
	}

	rvg := &mft.G{}

	// t.Fatal("Step0")

	q, er0 := QueueCreate(rvg, fod, "test/tq2/", SaveBackgroundWait, log.New(), 1e6, 1e4, time.Minute*10, time.Millisecond*300)

	if er0 != nil {
		b.Fatal("QueueCreate", er0)
	}

	for i := 0; i < b.N; i++ {
		er1 := q.Add([]byte("test"))
		if er1 != nil {
			b.Fatal("Queue q.Add", er1)
		}
	}
}

func BenchmarkQueueSaveBackground(b *testing.B) {
	fod := fod.FileOnDiskSimpleCreate()
	err := fod.MkDirIfNotExists("test/tq3/")
	if err != nil {
		b.Fatal("MkDirIfNotExists", err)
	}

	rvg := &mft.G{}

	// t.Fatal("Step0")

	q, er0 := QueueCreate(rvg, fod, "test/tq3/", SaveBackground, log.New(), 1e7, 1e4, time.Minute*10, time.Millisecond*300)

	if er0 != nil {
		b.Fatal("QueueCreate", er0)
	}

	for i := 0; i < b.N; i++ {
		er1 := q.Add([]byte("test sjdfg kjhdgs hfgdjshf gjdhs gfjhsdg fjhs jgdhj gajfg adhjf gjhag fjhadg f gajh gfhjag fhjgadhjfg"))
		if er1 != nil {
			b.Fatal("Queue q.Add", er1)
		}
		if (i % 10000) == 0 {
			er2 := q.SaveAllIlockRaw()
			if er2 != nil {
				b.Fatal("Queue q.Add", er2)
			}
		}
	}
}
