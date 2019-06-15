package scheduler

import (
	"fmt"
	"testing"
	"time"
)

func Test_BuildCron(t *testing.T) {
	expr := "10-50/3 30 */2 * * *"
	expr = "0 0 0 ? * wed"
	fmt.Printf("时间表达式：%v\n", expr)
	cron, err := NewCron(expr)
	if err != nil {
		t.Error("构造CronSchedule失败")
		t.Error(err)
	} else {
		n := time.Now()
		fmt.Printf("现在时间：%v\n", n)
		nxt := cron.Next(n)
		if nxt.IsZero() {
			t.Error("计算获取Next时间失败")
		} else {
			fmt.Printf("Next时间：%v\n", nxt)
		}
	}
}
