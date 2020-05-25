package lmq

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"xiao/common/logx"
	"xiao/service/hera/base/tools/strx"
)

// LocalMessageQueue 本地的消息队列（用于将内存压力转嫁到磁盘上）
// 只保存还未消费的消息，消费过的消息将于所在文件消费完成之后被删除
type LocalMessageQueue struct {
	dir                   string
	currentPush           string // current pushing file name
	currentConsume        string // current consuming file name
	pushFile, consumeFile *os.File
	maxFileSize           int64
	readOffset            int64
	broadcast             *sync.Cond
}

var DefaultFileName = "localexecutor-data"

// NewLocalMessageQueue 新建本地消息队列，传入文件存储目录，和最大单个文件大小(单位Byte);
// example: `NewLocalMessageQueue(".",24<<20)`
func NewLocalMessageQueue(dir string, maxFileSize int64) (*LocalMessageQueue, error) {
	l := &LocalMessageQueue{
		maxFileSize:    maxFileSize,
		dir:            strx.Getrpath(dir),
		currentPush:    DefaultFileName,
		currentConsume: DefaultFileName,
		broadcast:      sync.NewCond(&sync.Mutex{}),
	}
	e := os.MkdirAll(dir, 0755)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	l.pushFile, e = os.OpenFile(l.dir+l.currentPush, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0644)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	l.consumeFile, e = os.OpenFile(l.dir+l.currentConsume, os.O_RDONLY, 0644)
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	return l, nil
}

// Push 推送消息进去
func (l *LocalMessageQueue) Push(msg string) error {
	defer l.broadcast.Broadcast()
	b, e := json.Marshal(msg)
	if e != nil {
		logx.Error(e)
		return e
	}
	_, e = l.pushFile.Write(b)
	if e != nil {
		logx.Error(e)
		return e
	}
	_, e = l.pushFile.Write([]byte{'\n'})
	if e != nil {
		logx.Error(e)
		return e
	}

	if l.maxFileSize <= 0 {
		return nil
	}
	// check size
	size, e := l.pushFile.Seek(0, 2)
	if e != nil {
		logx.Error(e)
		return e
	}
	if size >= l.maxFileSize {
		e = l.pushFile.Close()
		if e != nil {
			logx.Error(e)
			return e
		}
		l.currentPush = strx.DuplicateName(l.currentPush, 1)
		l.pushFile, e = os.OpenFile(l.dir+l.currentPush, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0644)
		if e != nil {
			logx.Error(e)
			return e
		}
	}
	return nil
}

// Consume 消费消息，传入批量的大小限制（没有则填0），和timeout（没有则填0）；
// 消费只能是单人消费，因为消费完了之后会把文件删掉。
func (l *LocalMessageQueue) Consume(batchSize int, timeout time.Duration) ([]string, error) {
	if batchSize == 0 {
		batchSize = 1
	}
	var ctx = context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	}

	data, e := l.consume(ctx, batchSize)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	return data, nil
}

func (l *LocalMessageQueue) consume(ctx context.Context, batchSize int) ([]string, error) {
	var out []string
	reader := bufio.NewReader(l.consumeFile)
	for len(out) < batchSize {
		select {
		case <-ctx.Done():
			return out, nil
		default:
		}
		line, e := reader.ReadBytes('\n')
		if e != nil && e != io.EOF {
			logx.Error(e)
			return nil, e
		}
		if e == io.EOF {
			if l.currentConsume == l.currentPush {
				ch := make(chan struct{})
				go func() {
					l.broadcast.L.Lock()
					l.broadcast.Wait()
					l.broadcast.L.Unlock()
					ch <- struct{}{}
				}()
				select {
				case <-ch:
					continue
				case <-ctx.Done():
					return out, nil
				}
			}
			// switch file to read
			e = l.consumeFile.Close()
			if e != nil {
				logx.Error(e)
				return nil, e
			}
			e = os.Remove(l.currentConsume)
			if e != nil {
				logx.Error(e)
				return nil, e
			}
			l.currentConsume = strx.DuplicateName(l.currentConsume, 1)
			l.consumeFile, e = os.OpenFile(l.dir+l.currentConsume, os.O_RDONLY, 0644)
			if e != nil {
				logx.Error(e)
				return nil, e
			}
			reader = bufio.NewReader(l.consumeFile)
		}
		if len(line) == 0 {
			continue
		}
		if line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
		}
		var s string
		e = json.Unmarshal(line, &s)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		out = append(out, s)
	}
	return out, nil
}

// Clean 清空数据文件
func (l *LocalMessageQueue) Clean() error {
	e := os.Remove(l.currentConsume)
	if e != nil && !os.IsNotExist(e) {
		logx.Error(e)
		return e
	}
	if l.currentConsume == l.currentPush {
		return nil
	}
	l.currentConsume = strx.DuplicateName(l.currentConsume, 1)
	return l.Clean()
}
