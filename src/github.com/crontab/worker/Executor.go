package worker

import (
	"go-crontab-cluster/src/github.com/crontab/common"
	"os/exec"
	"time"
)

//任务执行器
type Executor struct {
}

var (
	G_executor *Executor
)

//执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {

	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)
		//任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		//初始化分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		//记录任务开始时间
		result.StartTime = time.Now()

		//上锁
		//随机睡眠(0~1秒)，为了防止不同机器之间分布式锁一直被某一台机器抢占（由于cpu的分时特性），为此牺牲一点定时任务的准时性
		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil { // 上锁失败
			result.Err = err
			result.EndTime = time.Now()

		} else { // 上锁成功

			//上锁成功后，重置任务启动时间
			result.StartTime = time.Now()
			//执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "C:\\cygwin64\\bin\\bash.exe", "-c", info.Job.Command) //TODO 这里选用了windows下开发，放入linux前需要将bash改掉
			//执行并捕获输出
			output, err = cmd.CombinedOutput()

			//记录任务结束时间
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err

		}
		//任务执行完成后，把执行的结果返回给Scheduler,Scheduler会从executingTable中删除调执行记录
		G_scheduler.PushJobResult(result)
	}()
}

// 初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
