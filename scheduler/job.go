package scheduler

import (
	"fmt"
	"github.com/xnffdd/gospider/database"
	"github.com/xnffdd/gospider/logs"
	"github.com/xnffdd/gospider/spiders"
	"time"
)

// 作业核心字段
type JobCore struct {
	Id         string // 作业唯一ID，默认""，对应MySQL的job表id字段
	Name       string // 作业名称（昵称），默认""，对应MySQL的job表name字段
	CronRule   string // 调度时间规则，默认""，对应MySQL的job表cron_rule字段
	RunnerName string // 作业函数名称，默认""，对应MySQL的job表runner_name字段
	RunnerArgs string // 作业函数参数，引用类型，默认nil，对应MySQL的job表runner_args字段
}

// 作业
type Job struct {
	JobCore
	CreateTime time.Time          // 创建时间，默认time.Time{}：IsZero()->true，对应MySQL的job表ctime字段
	UpdateTime time.Time          // 修改时间，默认time.Time{}：IsZero()->true，对应MySQL的job表utime字段
	Deleted    bool               // 是否删除，默认false，对应MySQL的job表deleted字段，软删除
	Opened     bool               // 是否启用，默认false，对应MySQL的job表opened字段
	Runner     func(string) error // 运行函数，引用类型，默认nil
	Cron       *Cron              // 调度时间计算器，引用类型，默认nil
	Next       time.Time          // 下一次执行时间，根据Cron调度规则计算，默认time.Time{}：IsZero()->true
}

// 按下一次执行时间Next排序
type JobsByTime []*Job

func (s JobsByTime) Len() int      { return len(s) }
func (s JobsByTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s JobsByTime) Less(i, j int) bool {
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

func (job *Job) String() string {
	return fmt.Sprintf("Job->"+
		"\n\t唯一标识:%v"+
		"\n\t作业名称:%v"+
		"\n\t创建时间:%v"+
		"\n\t更新时间:%v"+
		"\n\t是否删除:%v"+
		"\n\t是否开启:%v"+
		"\n\t调度规则:%v"+
		"\n\t调度时间:%v"+
		"\n\t爬虫名称:%v"+
		"\n\t爬虫参数:%v\n",
		job.Id, job.Name, job.CreateTime, job.UpdateTime,
		job.Deleted, job.Opened, job.CronRule, job.Cron.Next(time.Now()), job.RunnerName, job.RunnerArgs)
}

func (job *Job) build() error {
	cron, err := NewCron(job.CronRule)
	if err != nil {
		return err
	}
	runner, err := spiders.GetRunnerByName(job.RunnerName)
	if err != nil {
		return err
	}
	job.Cron = cron
	job.Runner = runner
	return nil
}

func LoadJobs() ([]*Job, error) {
	var jobs []*Job

	sql := "select id,ctime,utime,deleted,name,cron_rule,opened,runner_name,runner_args from job where deleted=?"

	rows, err := database.MySQL.Query(sql, false)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		job := &Job{}
		if err = rows.Scan(&job.Id, &job.CreateTime, &job.UpdateTime, &job.Deleted, &job.Name, &job.CronRule,
			&job.Opened, &job.RunnerName, &job.RunnerArgs); err != nil {
			return nil, err
		}
		err = job.build()
		if err != nil {
			logs.ErrorLogger.Printf("构建作业失败，%s", err.Error())
			continue
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func DeleteJob(job *Job) (affect int64, err error) {
	sql := "update job set utime=?,deleted=? where id=?"

	stmt, err := database.MySQL.Prepare(sql)
	if err != nil {
		return
	}

	t := time.Now()
	res, err := stmt.Exec(t, true, job.Id)
	if err != nil {
		return
	}

	affect, err = res.RowsAffected()
	if err != nil {
		return
	}

	err = stmt.Close()
	if err != nil {
		return
	}

	job.Deleted = true
	job.UpdateTime = t

	return
}

func InsertJob(job *Job) (affect int64, err error) {
	sql := "insert into job(id,ctime,utime,deleted,name,cron_rule,opened,runner_name,runner_args) values(?,?,?,?,?,?,?,?,?)"

	stmt, err := database.MySQL.Prepare(sql)

	if err != nil {
		return
	}

	t := time.Now()
	res, err := stmt.Exec(job.Id, t, t, job.Deleted, job.Name, job.CronRule, job.Opened, job.RunnerName, job.RunnerArgs)
	if err != nil {
		return
	}

	affect, err = res.RowsAffected()
	if err != nil {
		return
	}

	err = stmt.Close()
	if err != nil {
		return
	}

	job.CreateTime = t
	job.UpdateTime = t

	return
}

func UpdateJob(job *Job) (affect int64, err error) {
	sql := "update job set utime=?,name=?,cron_rule=?,opened=?,runner_name=?,runner_args=? where id=?"

	stmt, err := database.MySQL.Prepare(sql)
	if err != nil {
		return
	}

	t := time.Now()
	res, err := stmt.Exec(t, job.Name, job.CronRule, job.Opened, job.RunnerName, job.RunnerArgs, job.Id)
	if err != nil {
		return
	}

	affect, err = res.RowsAffected()
	if err != nil {
		return
	}

	err = stmt.Close()
	if err != nil {
		return
	}

	job.UpdateTime = t

	return
}
