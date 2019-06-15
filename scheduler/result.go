package scheduler

import (
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/xnffdd/gospider/database"
	"time"
)

const (
	successJobExecuteState = "SUCCESS"
	failJobExecuteState    = "FAIL"
	runningJobExecuteState = "RUNNING"
	defaultJobExecuteState = runningJobExecuteState
)

type JobResult struct {
	// 数据库记录通用信息
	id         string    // 作业执行结果ID，唯一ID
	deleted    bool      // 是否删除，软删除
	createTime time.Time // 记录创建时间
	updateTime time.Time // 记录更新时间

	// 作业信息
	jobId         string // 作业ID
	jobName       string // 作业名称
	jobCronRule   string // 作业调度规则
	jobRunnerName string // 作业执行函数名称
	jobRunnerArgs string // 作业执行函数参数

	// 执行信息
	startTime    time.Time
	endTime      time.Time
	executeState string
	log          string
}

func NewJobResult(job *Job) *JobResult {
	return &JobResult{
		id:           uuid.New().String(),
		deleted:      false,
		executeState: defaultJobExecuteState,

		jobId:         job.Id,
		jobName:       job.Name,
		jobCronRule:   job.CronRule,
		jobRunnerName: job.RunnerName,
		jobRunnerArgs: job.RunnerArgs,
	}
}

func (result *JobResult) SaveAtStart() error {
	result.atStart()
	return result.insert()
}

func (result *JobResult) SaveAtEnd(isSuccess bool, log string) error {
	result.atEnd(isSuccess, log)
	return result.update()
}

func (result *JobResult) atStart() {
	t := time.Now()

	result.startTime = t
	//result.endTime = mysql.NullTime{}
	result.createTime = t
	result.updateTime = t

	result.executeState = runningJobExecuteState
}

func (result *JobResult) atEnd(isSuccess bool, log string) {
	t := time.Now()

	result.endTime = t
	result.updateTime = t

	result.log = log

	if isSuccess {
		result.executeState = successJobExecuteState
	} else {
		result.executeState = failJobExecuteState
	}
}

func (result *JobResult) insert() error {
	sql := "insert into job_result(id,deleted,ctime,utime,job_id,job_name,job_cron_rule,job_runner_name," +
		"job_runner_args,start_time,end_time,execute_state,log) values(?,?,?,?,?,?,?,?,?,?,?,?,?)"

	stmt, err := database.MySQL.Prepare(sql)
	if err != nil {
		return err
	}
	res, err := stmt.Exec(result.id, result.deleted, result.createTime, result.updateTime,
		result.jobId, result.jobName, result.jobCronRule, result.jobRunnerName, result.jobRunnerArgs,
		result.startTime, mysql.NullTime{}, result.executeState, result.log)
	if err != nil {
		return err
	}

	_, err = res.RowsAffected()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	return err
}

func (result *JobResult) update() error {
	sql := "update job_result set deleted=?,ctime=?,utime=?,job_name=?,job_cron_rule=?,job_runner_name=?," +
		"job_runner_args=?,start_time=?,end_time=?,execute_state=?,log=? where id=?"

	stmt, err := database.MySQL.Prepare(sql)
	if err != nil {
		return err
	}

	res, err := stmt.Exec(result.deleted, result.createTime, result.updateTime,
		result.jobName, result.jobCronRule, result.jobRunnerName, result.jobRunnerArgs,
		result.startTime, result.endTime, result.executeState, result.log, result.id)
	if err != nil {
		return err
	}

	_, err = res.RowsAffected()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	return err
}
