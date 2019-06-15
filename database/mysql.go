package database

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/xnffdd/gospider/logs"
)

const dsn string = "username:password@tcp(localhost:3306)/dbname?charset=utf8&parseTime=True&loc=Local"

var MySQL *sql.DB

func init() {
	MySQL = connectMySQL(dsn)
}

func connectMySQL(conn string) *sql.DB {
	var db, err = sql.Open("mysql", conn)
	if err != nil {
		msg := fmt.Sprintf("数据库连接失败，%s", err.Error())
		logs.ErrorLogger.Println(msg)
		panic(msg)
	} else {
		logs.InfoLogger.Println("数据库连接成功")
		return db
	}
}

func CloseMySQL() {
	err := MySQL.Close()
	if err != nil {
		msg := fmt.Sprintf("数据库关闭失败，%s", err.Error())
		logs.ErrorLogger.Println(msg)
		panic(msg)
	} else {
		logs.InfoLogger.Println("数据库关闭成功")
	}
}
