package spiders

import (
	"fmt"
	"github.com/xnffdd/gospider/spiders/weixin"
)

var runners map[string]func(string) error

func GetRunnerNames() []string {
	var names []string
	for k, _ := range runners {
		names = append(names, k)
	}
	return names
}

func GetRunnerByName(name string) (func(string) error, error) {
	runner, ok := runners[name]
	if !ok {
		return nil, fmt.Errorf("爬虫不存在，名称：%v", name)
	}
	return runner, nil
}

func init() {
	runners = map[string]func(string) error{
		"WeiXinArticle": weixin.Crawl,
	}
}
