package main

import (
	"flag"
	"fmt"
	"go-crontab-cluster/src/github.com/crontab/master"
	"runtime"
	"time"
)

var (
	confFile string
)

func initArgs() {
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
	flag.Parse()
}

//初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)

	//初始化命令行参数
	initArgs()

	//初始化线程
	initEnv()

	//加载配置
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	//初始化服务发现
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}

	//日志管理器
	if err = master.InitLogMgr(); err != nil {
		goto ERR
	}

	//任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	//启动API http服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	//正常退出
	for {
		time.Sleep(1 * time.Second)
	}

ERR:
	fmt.Println(err)
}
