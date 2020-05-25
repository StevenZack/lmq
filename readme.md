# 本地的消息队列

用于将内存压力转嫁到磁盘上，相当于一个落盘的`BulkExecutor`

只保存还未消费的消息，消费过的消息将于所在文件消费完成之后被删除

# 使用示例

```go
package main

import (
	"fmt"
	"log"
	"xiao/service/hera/base/tools/lmq"
)

func main() {
	l, e := lmq.NewLocalMessageQueue(".", 24<<20)
	if e != nil {
		log.Println(e)
		return
	}
	defer l.Clean()
	e = l.Push("msg")
	if e != nil {
		log.Println(e)
		return
	}
	e = l.Push("msg1")
	if e != nil {
		log.Println(e)
		return
	}
	data, e := l.Consume(2, 0)
	if e != nil {
		log.Println(e)
		return
	}
	fmt.Println(data)
}

```
