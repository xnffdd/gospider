package scheduler

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/xnffdd/gospider/logs"
	"sort"
	"time"
)

// 调度器
type scheduler struct {
	running           bool          // 调度器是否正在运行标志（使用mutex或者select互斥读写）
	isRunningSnapshot chan bool     // 调度器是否正在运行快照
	start             chan struct{} // 接收启动调度器指令
	stop              chan struct{} // 传递停止调度器指令
	reload            chan struct{} // 传递重启调度器指令
	jobs              []*Job        // 调度中的作业集
	new               chan *JobCore // 传递新建作业指令
	update            chan *JobCore // 传递更新作业指令
	open              chan string   // 传递开启作业指令
	close             chan string   // 传递关闭作业指令
	delete            chan string   // 传递删除作业指令
	jobSnapshot       chan []*Job   // 调度中的作业集快照
}

// 单例模式，全局唯一调度器
var gs *scheduler

func init() {
	gs = &scheduler{
		running:           false,
		isRunningSnapshot: make(chan bool),
		start:             make(chan struct{}),
		stop:              make(chan struct{}),
		reload:            make(chan struct{}),
		jobs:              nil,
		new:               make(chan *JobCore),
		update:            make(chan *JobCore),
		open:              make(chan string),
		close:             make(chan string),
		delete:            make(chan string),
		jobSnapshot:       make(chan []*Job),
	}
	go gs.listen()
}

func (s *scheduler) loadJobs() {
	jobs, err := LoadJobs()
	if err != nil {
		logs.ErrorLogger.Printf("从数据库加载作业失败，%s", err.Error())
	} else {
		logs.InfoLogger.Printf("从数据库成功加载作业%d个", len(jobs))
		s.jobs = jobs
	}
}

func (s *scheduler) calcJobsNextTime(after time.Time) {
	logs.InfoLogger.Println("计算全部作业的下一次执行时刻")
	for _, job := range s.jobs {
		if job.Opened {
			job.Next = job.Cron.Next(after)
		} else {
			job.Next = time.Time{}
		}
	}
}

func (s *scheduler) sortJobsByNextTime() {
	sort.Sort(JobsByTime(s.jobs))
}

func (s *scheduler) buildNextComingTimer() *time.Timer {
	var duration time.Duration
	if len(s.jobs) == 0 || s.jobs[0].Next.IsZero() {
		duration = 24 * time.Hour // 休眠
		logs.InfoLogger.Printf("更新定时器（到达时刻：%v，距离现在：%v）\n",
			time.Now().Add(duration).Truncate(time.Second), duration)
	} else {
		next := s.jobs[0].Next
		duration = s.jobs[0].Next.Sub(time.Now())
		logs.InfoLogger.Printf("更新定时器（到达时刻：%v，距离现在：%v）\n",
			next, duration.Truncate(time.Second)+1*time.Second)
	}
	return time.NewTimer(duration)
}

func (s *scheduler) runJobWithRecover(job *Job) {
	var bf bytes.Buffer
	var err error
	result := NewJobResult(job)
	logs.InfoLogger.Printf("执行作业，作业ID：%s，执行记录ID：%s", job.Id, result.id)

	defer func() {
		if r := recover(); r != nil {
			logs.ErrorLogger.Printf("作业宕机，作业ID：%s，执行记录ID：%s，%v", job.Id, result.id, r)
			bf.WriteString(fmt.Sprintf("作业宕机，%v。", r))
			err = result.SaveAtEnd(false, bf.String())
			if err != nil {
				logs.ErrorLogger.Printf("保存执行结果到数据库时发生错误，作业ID：%s，执行记录ID：%s，%s",
					job.Id, result.id, err.Error())
			}
		} else {
			logs.InfoLogger.Printf("作业退出，作业ID：%s，执行记录ID：%s", job.Id, result.id)
		}
	}()

	err = result.SaveAtStart()
	if err != nil {
		panic(err)
	}

	err = job.Runner(job.RunnerArgs)

	if err != nil {
		bf.WriteString(fmt.Sprintf("任务执行返回错误：%v\n", err))
	} else {
		bf.WriteString(fmt.Sprintf("任务执行成功\n"))
	}

	err = result.SaveAtEnd(err == nil, bf.String())
	if err != nil {
		panic(err)
	}
}

func (s *scheduler) listen() {
	for {
		if s.running {
			s.loadJobs()
			s.calcJobsNextTime(time.Now())
		} else {
			s.jobs = nil // 空转
		}
	SchedulerStateChanged:
		for {
			s.sortJobsByNextTime()
			timer := s.buildNextComingTimer()
		JobsChanged:
			for {
				select {

				case <-s.start:
					logs.InfoLogger.Printf("启动调度器指令到达")
					if !s.running {
						logs.InfoLogger.Printf("调度器启动")
						s.running = true
						break SchedulerStateChanged
					} else {
						logs.ErrorLogger.Printf("重复启动调度器")
					}

				case <-s.stop:
					logs.InfoLogger.Printf("停止调度器指令到达")
					if s.running {
						logs.InfoLogger.Printf("调度器停止")
						s.running = false
						break SchedulerStateChanged
					} else {
						logs.ErrorLogger.Printf("重复停止调度器")
					}

				case <-s.reload:
					logs.InfoLogger.Printf("重载调度器指令到达")
					s.running = true
					break SchedulerStateChanged

				case <-s.jobSnapshot:
					logs.InfoLogger.Printf("作业快照指令到达")
					var jobs []*Job
					for _, job := range s.jobs {
						job2 := *job
						jobs = append(jobs, &job2)
					}
					s.jobSnapshot <- jobs

				case <-s.isRunningSnapshot:
					logs.InfoLogger.Printf("运行状态快照指令到达")
					s.isRunningSnapshot <- s.running

				case now := <-timer.C:
					logs.InfoLogger.Printf("定时器到达")
					for _, job := range s.jobs {
						if job.Next.After(now) || job.Next.IsZero() {
							break
						}
						job.Next = job.Cron.Next(now)
						job2 := *job
						go s.runJobWithRecover(&job2)
					}
					break JobsChanged

				case jobCore := <-s.new:
					logs.InfoLogger.Printf("新建作业指令到达")
					if s.running {
						err := s.processNewJobCMD(jobCore)
						if err != nil {
							logs.ErrorLogger.Printf("新建作业失败，%s", err.Error())
						} else {
							logs.InfoLogger.Printf("新建作业成功")
							timer.Stop()
							break JobsChanged
						}
					} else {
						logs.ErrorLogger.Printf("调度器未启动")
					}

				case id := <-s.delete:
					logs.InfoLogger.Printf("删除作业指令到达")
					if s.running {
						err := s.processDeleteJobCMD(id)
						if err != nil {
							logs.ErrorLogger.Printf("删除作业失败，作业ID：%s，%s", id, err.Error())
						} else {
							logs.InfoLogger.Printf("删除作业成功，作业ID：%s", id)
							timer.Stop()
							break JobsChanged
						}
					} else {
						logs.ErrorLogger.Printf("调度器未启动")
					}

				case jobCore := <-s.update:
					logs.InfoLogger.Printf("更新作业指令到达")
					if s.running {
						err := s.processUpdateJobCMD(jobCore)
						if err != nil {
							logs.ErrorLogger.Printf("更新作业失败，作业ID：%s，%s", jobCore.Id, err.Error())
						} else {
							logs.InfoLogger.Printf("更新作业成功，作业ID：%s", jobCore.Id)
							timer.Stop()
							break JobsChanged
						}
					} else {
						logs.ErrorLogger.Printf("调度器未启动")
					}

				case id := <-s.open:
					logs.InfoLogger.Printf("开启作业指令到达")
					if s.running {
						err := s.processOpenJobCMD(id)
						if err != nil {
							logs.ErrorLogger.Printf("开启作业失败，作业ID：%s，%s", id, err.Error())
						} else {
							logs.InfoLogger.Printf("开启作业成功，作业ID：%s", id)
							timer.Stop()
							break JobsChanged
						}
					} else {
						logs.ErrorLogger.Printf("调度器未启动")
					}

				case id := <-s.close:
					logs.InfoLogger.Printf("关闭作业指令到达")
					if s.running {
						err := s.processCloseJobCMD(id)
						if err != nil {
							logs.ErrorLogger.Printf("关闭作业失败，作业ID：%s，%s", id, err.Error())
						} else {
							logs.InfoLogger.Printf("关闭作业成功，作业ID：%s", id)
							timer.Stop()
							break JobsChanged
						}
					} else {
						logs.ErrorLogger.Printf("调度器未启动")
					}

				}
			}
		}
	}
}

func (s *scheduler) processNewJobCMD(jobCore *JobCore) error {
	var err error
	job := &Job{}
	job.JobCore = *jobCore
	job.Id = uuid.New().String()
	err = job.build()
	if err != nil {
		return fmt.Errorf("构建作业失败，%s", err.Error())
	} else { // Built
		_, err = InsertJob(job)
		if err != nil {
			return fmt.Errorf("执行数据库插入作业失败，%s", err.Error())
		} else { // Inserted into database
			s.jobs = append(s.jobs, job)         // Append to scheduling jobs
			job.Next = job.Cron.Next(time.Now()) // Calculate next execution time
			return nil
		}
	}
}

func (s *scheduler) processDeleteJobCMD(id string) error {
	idx, job := s.findJobById(id)
	if job != nil { // Found
		var err error
		_, err = DeleteJob(job)
		if err != nil {
			return fmt.Errorf("执行数据库删除作业失败，%s", err.Error())
		} else { // Deleted from database
			s.jobs = append(s.jobs[:idx], s.jobs[idx+1:]...) // Remove from scheduling jobs
			return nil
		}
	} else {
		return fmt.Errorf("作业不存在")
	}
}

func (s *scheduler) findJobById(jobId string) (int, *Job) {
	for idx, job := range s.jobs {
		if job.Id == jobId {
			return idx, job
		}
	}
	return -1, nil
}

func (s *scheduler) processUpdateJobCMD(jobCore *JobCore) error {
	idx, job := s.findJobById(jobCore.Id)
	if idx >= 0 { // Found
		var err error
		job2 := *job
		job2.JobCore = *jobCore
		err = job2.build()
		if err != nil { // Built
			return fmt.Errorf("构建作业失败，%s", err.Error())
		} else {
			_, err = UpdateJob(&job2) // Updated to database
			if err != nil {
				return fmt.Errorf("执行数据库更新作业失败，%s", err.Error())
			} else {
				s.jobs[idx] = &job2                    // Replace job in scheduling jobs
				job2.Next = job2.Cron.Next(time.Now()) // Calculate next execution time
				return nil
			}
		}
	} else {
		return fmt.Errorf("作业不存在")
	}
}

func (s *scheduler) processOpenJobCMD(id string) error {
	_, job := s.findJobById(id)
	if job != nil { // Found
		if job.Opened { // Already opened
			return fmt.Errorf("重复开启作业")
		} else {
			var err error
			job.Opened = true
			_, err = UpdateJob(job)
			if err != nil {
				job.Opened = false // Reset
				return fmt.Errorf("执行数据库更新作业失败，%s", err.Error())
			} else { // Updated to database
				job.Next = job.Cron.Next(time.Now()) // Calculate next execution time
				return nil
			}
		}
	} else {
		return fmt.Errorf("作业不存在")
	}
}

func (s *scheduler) processCloseJobCMD(id string) error {
	_, job := s.findJobById(id)
	if job != nil { // Found
		if !job.Opened { // Already closed
			return fmt.Errorf("重复关闭作业")
		} else {
			var err error
			job.Opened = false
			_, err = UpdateJob(job)
			if err != nil {
				job.Opened = true // Reset
				return fmt.Errorf("执行数据库更新作业失败，%s", err.Error())
			} else { // Updated to database
				job.Next = time.Time{} // Reset next execution time to zero(means not scheduled)
				return nil
			}
		}
	} else {
		return fmt.Errorf("作业不存在")
	}
}
