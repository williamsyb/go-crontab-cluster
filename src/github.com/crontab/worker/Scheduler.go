package worker

import (
	"fmt"
	"go-crontab-cluster/src/github.com/crontab/common"
	"time"
)

//任务调度
type Scheduler struct {
	JobEventChan      chan *common.JobEvent              //etcd任务事件队列
	jobPlanTable      map[string]*common.JobSchedulePlan // 任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo  //任务执行表
	jobResultChan     chan *common.JobExecuteResult      //任务结果队列
}

var (
	G_scheduler *Scheduler
)

// 处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		err             error
		jobExecuting    bool
		jobExecuteInfo  *common.JobExecuteInfo
		jobExisted      bool
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: // 保存任务事件
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE: //删除任务事件
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL: // 强杀任务事件
		//取消掉Command 执行
		if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc() //触发command杀死shell子进程，任务得到退出
		}

	}
}

//尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	//调度和执行是2件不同的事情
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)
	//执行的任务可能运行很久，1分钟会调度60次，但是只能执行1次，防止并发!

	//如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		fmt.Println("上次任务正在执行， 跳过执行：", jobExecuteInfo.Job.Name)
		return
	}

	//构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	//保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	//执行任务
	fmt.Println("执行任务：", jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)

}

//重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan  *common.JobSchedulePlan
		now      time.Time
		nearTime *time.Time
	)
	//当前时间
	now = time.Now()

	//如果任务表为空的话，随便睡眠多久
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	// 1.遍历所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			//TODO 尝试执行任务
			scheduler.TryStartJob(jobPlan)
			fmt.Println("执行任务：", jobPlan.Job.Name)
			jobPlan.NextTime = jobPlan.Expr.Next(now)

		}

		//统计最近一个要过期的任务的时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}
	//下次调度间隔（最近要执行的任务调度时间-当前时间）
	scheduleAfter = (*nearTime).Sub(now)
	return
}

//处理任务结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)

	//删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	//生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			OutPut:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      result.EndTime.UnixNano() / 1000 / 1000,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		G_logSink.Append(jobLog)
	}

	fmt.Println("任务执行完成：", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
}

//调度协程
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)

	//初始化一次
	scheduleAfter = scheduler.TrySchedule()

	//调度的延迟定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	//定时任务commonJob
	for {
		select {
		case jobEvent = <-scheduler.JobEventChan: // 监听任务变化时
			// 对内存中维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: //最近的任务到期了
		case jobResult = <-scheduler.jobResultChan: // 监听任务执行结果
			scheduler.handleJobResult(jobResult)
		}

		//调度一次任务，因为JobEventChan中的事件可能会修改scheduleTable，所以需要重新执行schedule，扫描一遍新的（到期的就执行）
		scheduleAfter = scheduler.TrySchedule()

		//重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}
}

//推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.JobEventChan <- jobEvent
}

//初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		JobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}
	go G_scheduler.scheduleLoop()
	return
}

//回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}
