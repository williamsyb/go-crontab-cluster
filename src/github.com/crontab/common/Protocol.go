package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

//定时任务
type Job struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

//任务调度计划
type JobSchedulePlan struct {
	Job      *Job                 //要调度的任务信息
	Expr     *cronexpr.Expression //解析好的cronexpr表达式
	NextTime time.Time            //下次调度时间
}

//任务执行状态
type JobExecuteInfo struct {
	Job        *Job               //任务信息
	PlanTime   time.Time          //理论的调度时间
	RealTime   time.Time          //真实的调度时间
	CancelCtx  context.Context    //任务command的context
	CancelFunc context.CancelFunc //用于取消command执行的cancel函数
}

//HTTP接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

//变化实践
type JobEvent struct {
	EventType int // SAVE, DELETE
	Job       *Job
}

//任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo //执行状态
	Output      []byte          //脚本输出
	Err         error           //脚本错误原因
	StartTime   time.Time       //启动时间
	EndTime     time.Time       //结束时间
}

//任务日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

//任务日志排序规则
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"` //按照startTime: -1
}

//任务执行日志
type JobLog struct {
	JobName      string `json:"jobName" bson:"jobName"`
	Command      string `json:"command" bson:"command"`
	Err          string `json:"err" bson:"err"`
	OutPut       string `json:"output" bson:"output"`
	PlanTime     int64  `json:"planTime" bson:"planTime"`
	ScheduleTime int64  `json:"scheduleTime" bson:"scheduleTime"`
	StartTime    int64  `json:"startTime" bson:"startTime"`
	EndTime      int64  `json:"endTime" bson:"endTime"`
}

//日志批次，用于批量插入到mongodb表
type LogBatch struct {
	Logs []interface{} //多条日志
}

//应答方法
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	//1.定义一个response
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data
	//2.序列化json
	resp, err = json.Marshal(response)
	return

}

//反序列化Job
func UnpackJob(value []byte) (ret *Job, err error) {
	var (
		job *Job
	)

	job = &Job{}

	if err = json.Unmarshal(value, job); err != nil {
		return
	}
	ret = job
	return
}

//从etcd的key中提取任务名字, 从/cron/jobs/job10中抹除/cron/jobs
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

//从etcd的key中提取任务名字, 从/cron/killer/job10中提取job10
func ExtractKillerName(killerKey string) string {
	return strings.TrimPrefix(killerKey, JOB_KILLER_DIR)
}

//任务变化事件有2种：
//1) 更新任务
//2）删除任务
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

// 构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)

	//解析Job的cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	//生成任务调度计划对象
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

//构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime,
		RealTime: time.Now(),
	}

	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())

	return
}

func ExtractWorkerIP(regKey string) string {
	return strings.TrimPrefix(regKey, JOB_WORKER_DIR)
}
