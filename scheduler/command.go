package scheduler

func SendCMDStartScheduler() {
	go func() { gs.start <- struct{}{} }()
}

func SendCMDStopScheduler() {
	go func() { gs.stop <- struct{}{} }()
}

func SendCMDReloadScheduler() {
	go func() { gs.reload <- struct{}{} }()
}

func GetSchedulerIsRunningSnapshot() bool { // 阻塞调用
	gs.isRunningSnapshot <- false
	x := <-gs.isRunningSnapshot
	return x
}

func GetJobsSnapshot() []*Job { // 阻塞调用
	gs.jobSnapshot <- nil
	x := <-gs.jobSnapshot
	return x
}

func SendCMDNewJob(name, cronRule, runnerName, runnerArgs string) {
	go func() {
		job := &JobCore{
			Name:       name,
			CronRule:   cronRule,
			RunnerName: runnerName,
			RunnerArgs: runnerArgs,
		}
		gs.new <- job
	}()
}

func SendCMDDeleteJob(jobId string) {
	go func() { gs.delete <- jobId }()
}

func SendCMDUpdateJob(jobId, name, cronRule, runnerName, runnerArgs string) {
	go func() {
		job := &JobCore{
			Id:         jobId,
			Name:       name,
			CronRule:   cronRule,
			RunnerName: runnerName,
			RunnerArgs: runnerArgs,
		}
		gs.update <- job
	}()
}

func SendCMDOpenJob(jobId string) {
	go func() { gs.open <- jobId }()
}

func SendCMDCloseJob(jobId string) {
	go func() { gs.close <- jobId }()
}
