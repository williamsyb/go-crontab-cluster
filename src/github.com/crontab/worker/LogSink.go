package worker

import (
	"context"
	"fmt"
	"go-crontab-cluster/src/github.com/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

//批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

//日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch //当前的批次
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch //超时批次
	)
	for {
		select {
		case log = <-logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				//让这个批次超时自动提交（给一个时间），防止日志太少，而赞不满lobBatch数量导致页面看不到日志长久等待
				commitTimer = time.AfterFunc(
					time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func() { //超时回调函数会进入单独的一个goroutine，所以这里要特别处理
						return func() {
							logSink.autoCommitChan <- batch
						} //这里使用了闭包，类似于python的装饰器，为了让logBatch进入一个独立的上下文空间中
						//发出超时通知，不要直接提交batch
						//logSink.autoCommitChan<-logBatch
					}(logBatch),
				)

			}

			//把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)

			//如果批次满了就立即发送
			if len(logBatch.Logs) > G_config.JobLogBatchSize {
				//发送日志
				logSink.saveLogs(logBatch)
				//清空logBatch
				logBatch = nil
				//取消定时器
				commitTimer.Stop()

			}
		case timeoutBatch = <-logSink.autoCommitChan: // 过期的批次
			//判断过期批次是否仍是当前的批次，因为上述commitTimer.Stop()的时间可能正好是上述满足了len(logBatch.Logs) > G_config.JobLogBatchSize
			//于是，timer的定时执行如果正巧发生在了logSink.saveLogs于logBatch=nil之间，会将batch传入timeoutBatch，而来不及stop
			//但是logBatch=nil会赶在logSink.autoCommitChan <- batch之前执行，此时收到的case timeoutBatch和logBatch（已经变成nil或开始下一轮了）就不一样了
			//而timeoutBatch就是上一轮已经提交过的logBatch，不能重复提交
			if timeoutBatch != logBatch {
				continue //跳过已经被提交的批次
			}
			//把过期的批次写入mongo
			logSink.saveLogs(timeoutBatch)
			//清空logBatch
			logBatch = nil
		}

	}
}

func InitLogSink() (err error) {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		client *mongo.Client
	)

	//建立mongodb连接
	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(G_config.EtcdDialTimeout)*time.Millisecond)
	defer cancel()
	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(G_config.MongodbUri)); err != nil {
		fmt.Println(err)
		return
	}

	//选择db和collection
	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	//启动一个mongodb处理协程
	go G_logSink.writeLoop()

	return
}

//发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog: //队列可能会满，导致阻塞，就会跳到default中，丢弃日志（不写入了，因为也不是很重要）
	default:
		//队列满了就丢弃
	}
}
