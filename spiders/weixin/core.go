package weixin

import (
	"fmt"
	"math/rand"
	"time"
)

func Crawl(keyword string) error {
	fmt.Printf("开始爬取微信，关键词：%s\n", keyword)
	rand.Seed(time.Now().UnixNano())
	x := rand.Intn(10)
	fmt.Printf("休眠%d秒来模拟爬虫采集程序...\n", x)
	time.Sleep(time.Duration(x) * time.Second)
	fmt.Println("微信爬虫成功结束")
	return nil
}
