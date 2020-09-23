package worker

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go-crontab-cluster/src/github.com/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"time"
)

type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	G_jobMgr *JobMgr
)

//监听任务变化
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp            *clientv3.GetResponse
		kvpair             *mvccpb.KeyValue
		job                *common.Job
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobName            string
		jobEvent           *common.JobEvent
	)

	//fmt.Println("start watching ")
	//1.get一下/cron/jobs目录下的所有任务，并且或者当前集群的revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		fmt.Println("error happened")
		return
	}

	//当前有哪些任务
	for _, kvpair = range getResp.Kvs {
		if job, err = common.UnpackJob(kvpair.Value); err == nil {
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			// 把这个job同步给scheduler(调度协程)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}
	//2.从该revision向后监听变化事件
	go func() { //监听协程
		//	从Get时刻的后续版本开始监听变化
		watchStartRevision = getResp.Header.Revision + 1
		//监听/cron/jobs/目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR,
			clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		//处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //任务保存事件
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					//构建一个Event事件
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)

				case mvccpb.DELETE: //任务被删除了
					//Delete /cron/jobs/job10
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
					job = &common.Job{
						Name: jobName,
					}
					//构建一个删除Event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}
				//推一个更新/删除事件给scheduler
				G_scheduler.PushJobEvent(jobEvent)

			}
		}

	}()
	return
}

//监听任务强杀
func (jobMgr *JobMgr) watchKiller() {
	var (
		//getResp            *clientv3.GetResponse
		job *common.Job
		//watchStartRevision int64
		watchChan  clientv3.WatchChan
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName    string
		jobEvent   *common.JobEvent
	)
	//2.从该revision向后监听变化事件
	go func() { //监听协程1
		//监听/cron/killer/目录的变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR,
			clientv3.WithPrefix())
		//处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //杀死任务事件
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key))
					job = &common.Job{
						Name: jobName,
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: //任务被删除了
				}
				////推一个更新/删除事件给scheduler
				//G_scheduler.PushJobEvent(jobEvent)

			}
		}

	}()
}

func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)

	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)
	// 赋值单例
	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}
	// 启动任务监听
	G_jobMgr.watchJobs()

	// 启动监听killer
	G_jobMgr.watchKiller()

	return
}

//创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	//返回一把锁
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}
