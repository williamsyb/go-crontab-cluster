package master

import (
	"context"
	"fmt"
	"go-crontab-cluster/src/github.com/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_LogMgr *LogMgr
)

func InitLogMgr() (err error) {
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

	G_LogMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}

	return
}

func (logMgr *LogMgr) ListLog(name string, skip int64, limit int64) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		cursor  *mongo.Cursor
		logSort *common.SortLogByStartTime
		jobLog  *common.JobLog
	)
	logArr = make([]*common.JobLog, 0)

	//过滤条件
	filter = &common.JobLogFilter{JobName: name}
	//按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, &options.FindOptions{
		Limit: &skip,
		Skip:  &limit,
		Sort:  logSort,
	}); err != nil {
		fmt.Println(err)
		return
	}

	// 延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		// 反序列化bson到对象
		if err = cursor.Decode(jobLog); err != nil {
			fmt.Println(err)
			continue //有日志不合法
		}
		logArr = append(logArr, jobLog)

	}

	return
}
