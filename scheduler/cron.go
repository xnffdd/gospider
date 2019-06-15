package scheduler

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type Cron struct {
	Second, Minute, Hour, Dom, Month, Dow uint64 // 位模式，用0-63位表示整数0-63
}

const (
	starBit              uint64 = 1 << 63 // 位模式，用最高位表示*
	cronScheduleFiledNum        = 6       // 秒 分 时 日 月 星期
)

type bounds struct {
	min, max uint
	names    map[string]uint
}

var (
	seconds     = bounds{0, 59, nil}
	minutes     = bounds{0, 59, nil}
	hours       = bounds{0, 23, nil}
	daysOfMonth = bounds{1, 31, nil}
	months      = bounds{1, 12, map[string]uint{
		"jan": 1,
		"feb": 2,
		"mar": 3,
		"apr": 4,
		"may": 5,
		"jun": 6,
		"jul": 7,
		"aug": 8,
		"sep": 9,
		"oct": 10,
		"nov": 11,
		"dec": 12,
	}}
	daysOfWeek = bounds{0, 6, map[string]uint{
		"sun": 0,
		"mon": 1,
		"tue": 2,
		"wed": 3,
		"thu": 4,
		"fri": 5,
		"sat": 6,
	}}
)

/*
Cron表达式格式：
*    *    *    *    *    *
-    -    -    -    -    -
|    |    |    |    |    |
|    |    |    |    |    + day of week (0 - 6) (Sunday=0)
|    |    |    |    +----- month (1 - 12)
|    |    |    +---------- day of month (1 - 31)
|    |    +--------------- hour (0 - 23)
|    +-------------------- min (0 - 59)
+------------------------- second (0 - 59)
*/
func NewCron(cron string) (*Cron, error) {
	if len(cron) == 0 {
		return nil, fmt.Errorf("时间表达式为空")
	}

	fields := strings.Fields(cron)

	if count := len(fields); count != cronScheduleFiledNum {
		return nil, fmt.Errorf("期望%d个字段，然而传入了%d个字段，表达式：\"%s\"", cronScheduleFiledNum, count, cron)
	}

	var err error
	field := func(field string, r bounds) uint64 {
		if err != nil {
			return 0
		}
		var bits uint64
		bits, err = getField(field, r)
		return bits
	}

	var (
		second     = field(fields[0], seconds)
		minute     = field(fields[1], minutes)
		hour       = field(fields[2], hours)
		dayOfMonth = field(fields[3], daysOfMonth)
		month      = field(fields[4], months)
		dayOfWeek  = field(fields[5], daysOfWeek)
	)
	if err != nil {
		return nil, err
	}

	return &Cron{
		Second: second,
		Minute: minute,
		Hour:   hour,
		Dom:    dayOfMonth,
		Month:  month,
		Dow:    dayOfWeek,
	}, nil
}

// 返回5年内晚于给定时间的下一个执行时刻，如果没有，则返回时间0
func (s *Cron) Next(a time.Time) time.Time {
	start := a.Add(time.Second).Truncate(time.Second) // 开始时间，向上取整到秒
	end := a.AddDate(5, 0, 0)                         // 最大结束时间，5年后

	next := start
	for {
		// 时间超过最大区间限制
		if next.After(end) {
			return time.Time{} // 返回时间0值(t.IsZero()->true)
		}

		// 月份不匹配
		for 1<<uint(next.Month())&s.Month == 0 {
			next = next.AddDate(0, 1, 0) // 累加月
			next = time.Date(next.Year(), next.Month(), 1,
				0, 0, 0, 0, next.Location()) //  重置月以下的低位
			if next.Month() == time.January {
				continue // 进位，重新从高位往低位验证
			}
		}

		// 日不匹配
		for !dayMatches(s, next) {
			next = next.AddDate(0, 0, 1) // 累加天
			next = time.Date(next.Year(), next.Month(), next.Day(),
				0, 0, 0, 0, next.Location()) //  重置天以下的低位
			if next.Day() == 1 {
				continue // 进位，重新从高位往低位验证
			}
		}

		// 时钟不匹配
		for 1<<uint(next.Hour())&s.Hour == 0 {
			next = next.Add(1 * time.Hour)  // 累加小时
			next = next.Truncate(time.Hour) // 重置小时以下的低位
			if next.Hour() == 0 {
				continue // 进位，重新从高位往低位验证
			}
		}

		// 分钟不匹配
		for 1<<uint(next.Minute())&s.Minute == 0 {
			next = next.Add(1 * time.Minute)  // 累加分钟
			next = next.Truncate(time.Minute) // 重置分钟以下的低位
			if next.Minute() == 0 {
				continue // 进位，重新从高位往低位验证
			}
		}

		// 秒钟不匹配
		for 1<<uint(next.Second())&s.Second == 0 {
			next = next.Add(1 * time.Second) // 累加秒钟
			if next.Second() == 0 {
				continue // 进位，重新从高位往低位验证
			}
		}

		// 时间超过最大区间限制
		if next.After(end) {
			return time.Time{} // 返回时间0值(t.IsZero()->true)
		}
		break
	}

	return next
}

func dayMatches(s *Cron, t time.Time) bool {
	var (
		domMatch = 1<<uint(t.Day())&s.Dom > 0
		dowMatch = 1<<uint(t.Weekday())&s.Dow > 0
	)
	if s.Dom&starBit > 0 || s.Dow&starBit > 0 {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}

func getField(field string, r bounds) (uint64, error) {
	var bits uint64
	ranges := strings.Split(field, ",")
	for _, expr := range ranges {
		bit, err := getRange(expr, r)
		if err != nil {
			return bits, err
		}
		bits |= bit
	}
	return bits, nil
}

func getRange(expr string, r bounds) (uint64, error) {
	var (
		start, end, step uint
		rangeAndStep     = strings.Split(expr, "/")
		lowAndHigh       = strings.Split(rangeAndStep[0], "-")
		singleDigit      = len(lowAndHigh) == 1
		err              error
	)

	var extra uint64
	if lowAndHigh[0] == "*" || lowAndHigh[0] == "?" {
		start = r.min
		end = r.max
		extra = starBit
	} else {
		start, err = parseIntOrName(lowAndHigh[0], r.names)
		if err != nil {
			return 0, err
		}
		switch len(lowAndHigh) {
		case 1:
			end = start
		case 2:
			end, err = parseIntOrName(lowAndHigh[1], r.names)
			if err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("只允许一个‘_’符号，表达式：\"%s\"", expr)
		}
	}

	switch len(rangeAndStep) {
	case 1:
		step = 1
	case 2:
		step, err = mustParseInt(rangeAndStep[1])
		if err != nil {
			return 0, err
		}
		// 特殊处理 N/step -> N-max/step
		if singleDigit {
			end = r.max
		}
	default:
		return 0, fmt.Errorf("只允许一个‘/’符号，表达式：\"%s\"", expr)
	}

	if start < r.min {
		return 0, fmt.Errorf("该字段起始取值%d低于下限%d，表达式：\"%s\"", start, r.min, expr)
	}
	if end > r.max {
		return 0, fmt.Errorf("该字段结束取值%d高于上限%d，表达式：\"%s\"", end, r.max, expr)
	}
	if start > end {
		return 0, fmt.Errorf("该字段起始取值%d高于结束取值%d，表达式：\"%s\"", start, end, expr)
	}
	if step == 0 {
		return 0, fmt.Errorf("步长必须为正整数，表达式：\"%s\"", expr)
	}
	return getBits(start, end, step) | extra, nil
}

func parseIntOrName(expr string, names map[string]uint) (uint, error) {
	if names != nil {
		if namedInt, ok := names[strings.ToLower(expr)]; ok {
			return namedInt, nil
		}
	}
	return mustParseInt(expr)
}

func mustParseInt(expr string) (uint, error) {
	num, err := strconv.Atoi(expr)
	if err != nil {
		return 0, fmt.Errorf("解析整数失败，表达式：\"%s\"，错误：%s", expr, err)
	}
	if num < 0 {
		return 0, fmt.Errorf("不允许负数，表达式：\"%s\"", expr)
	}
	return uint(num), nil
}

func getBits(min, max, step uint) uint64 {
	if min < 0 || max > 63 {
		panic("位溢出，只允许操作第0至63位，不会运行到这里")
	}

	var bits uint64

	if step == 1 {
		return ^(math.MaxUint64 << (max + 1)) & (math.MaxUint64 << min)
	}

	for i := min; i <= max; i += step {
		bits |= uint64(1 << i)
	}
	return bits
}
