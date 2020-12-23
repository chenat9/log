package log

import (
	"bytes"
	"gopkg.in/natefinch/lumberjack.v2"
	"time"
)

type FileWriteAsyncer struct {
	innerLogger *lumberjack.Logger
	ch          chan []byte
	syncChan    chan struct{}
}

func NewFileWriteAsyncer(lg *lumberjack.Logger) *FileWriteAsyncer {
	fa := &FileWriteAsyncer{}
	fa.innerLogger = lg
	fa.ch = make(chan []byte, 10000)
	fa.syncChan = make(chan struct{})
	// 批量异步写入到文件中
	go batchWriteLog(fa)
	return fa

}

func (fa *FileWriteAsyncer) Write(data []byte) (int, error) {
	fa.ch <- data
	return len(data), nil
}

func (fa *FileWriteAsyncer) Sync() error {
	fa.syncChan <- struct{}{}
	return nil
}

func batchWriteLog(fa *FileWriteAsyncer) {
	buffer := bytes.NewBuffer(make([]byte, 0, 10240))

	ticker := time.NewTicker(time.Millisecond * 200)
	var err error
	for {
		select {
		case <-ticker.C:
			if len(buffer.Bytes()) > 0 {
				_, err = fa.innerLogger.Write(buffer.Bytes())
				if err != nil {
					panic(err)
				}
				buffer.Reset()
			}

		case record := <-fa.ch:
			buffer.Write(record)
			if len(buffer.Bytes()) >= 1024*4 {
				_, err = fa.innerLogger.Write(buffer.Bytes())
				if err != nil {
					panic(err)
				}
				buffer.Reset()
			}
		case <-fa.syncChan:
			if len(buffer.Bytes()) > 0 {
				_, err = fa.innerLogger.Write(buffer.Bytes())
				if err != nil {
					panic(err)
				}
				buffer.Reset()
			}
			break
		}
	}

}
