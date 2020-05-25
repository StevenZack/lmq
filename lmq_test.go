package lmq

import (
	"testing"
	"time"
)

func Test_sigleFile(t *testing.T) {
	l, e := NewLocalMessageQueue(".", 12)
	if e != nil {
		t.Error(e)
		return
	}
	defer l.Clean()
	e = l.Push("msg")
	if e != nil {
		t.Error(e)
		return
	}
	e = l.Push("msg1")
	if e != nil {
		t.Error(e)
		return
	}
	data, e := l.Consume(2, 0)
	if e != nil {
		t.Error(e)
		return
	}
	if len(data) != 2 {
		t.Error("len(data) is not 2 , but ", len(data))
		return
	}
	if data[0] != `msg` {
		t.Error("data[0] is not `msg` , but ", data[0])
		return
	}
	if data[1] != `msg1` {
		t.Error("data[1] is not `msg1` , but ", data[1])
		return
	}
}

func Test_fileSwitching(t *testing.T) {
	l, e := NewLocalMessageQueue(".", 4)
	if e != nil {
		t.Error(e)
		return
	}
	defer l.Clean()
	e = l.Push("msg")
	if e != nil {
		t.Error(e)
		return
	}
	e = l.Push("msg1")
	if e != nil {
		t.Error(e)
		return
	}
	data, e := l.Consume(2, 0)
	if e != nil {
		t.Error(e)
		return
	}
	if len(data) != 2 {
		t.Error("len(data) is not 2 , but ", len(data))
		return
	}
	if data[0] != `msg` {
		t.Error("data[0] is not `msg` , but ", data[0])
		return
	}
	if data[1] != `msg1` {
		t.Error("data[1] is not `msg1` , but ", data[1])
		return
	}
}

func Test_block(t *testing.T) {
	l, e := NewLocalMessageQueue(".", 4)
	if e != nil {
		t.Error(e)
		return
	}
	defer l.Clean()
	go func() {
		time.Sleep(time.Millisecond * 100)
		e = l.Push("msg")
		if e != nil {
			t.Error(e)
			return
		}
		time.Sleep(time.Millisecond * 100)
		e = l.Push("msg1")
		if e != nil {
			t.Error(e)
			return
		}
	}()
	data, e := l.Consume(2, 0)
	if e != nil {
		t.Error(e)
		return
	}
	if len(data) != 2 {
		t.Error("len(data) is not 2 , but ", len(data))
		return
	}
	if data[0] != `msg` {
		t.Error("data[0] is not `msg` , but ", data[0])
		return
	}
	if data[1] != `msg1` {
		t.Error("data[1] is not `msg1` , but ", data[1])
		return
	}
}

func Test_timeout(t *testing.T) {
	l, e := NewLocalMessageQueue(".", 4)
	if e != nil {
		t.Error(e)
		return
	}
	defer l.Clean()
	go func() {
		e := l.Push("msg")
		if e != nil {
			t.Error(e)
			return
		}
		time.Sleep(time.Millisecond * 50)
		e = l.Push("msg1")
		if e != nil {
			t.Error(e)
			return
		}
		time.Sleep(time.Millisecond * 100)
		e = l.Push("msg2")
		if e != nil {
			t.Error(e)
			return
		}
	}()
	data, e := l.Consume(10, time.Millisecond*100)
	if e != nil {
		t.Error(e)
		return
	}
	if len(data) != 2 {
		t.Error("len(data) is not 2 , but ", len(data))
		return
	}
	if data[0] != `msg` {
		t.Error("data[0] is not `msg` , but ", data[0])
		return
	}
	if data[1] != `msg1` {
		t.Error("data[1] is not `msg1` , but ", data[1])
		return
	}
}
