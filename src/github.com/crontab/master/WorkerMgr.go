package master

import (
	"context"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go-crontab-cluster/src/github.com/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"time"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_workerMgr *WorkerMgr
)

//获取在线worker列表
func (workerMgr *WorkerMgr) ListWorkers() (workerArr []string, err error) {
	var (
		getResp  *clientv3.GetResponse
		kv       *mvccpb.KeyValue
		workerIP string
	)
	//初始化数组
	workerArr = make([]string, 0)

	//获取目录下所有kv
	if getResp, err = workerMgr.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	//解析每个节点IP
	for _, kv = range getResp.Kvs {
		//
		workerIP = common.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}

	return
}

func InitWorkerMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}
