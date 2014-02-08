package main

import (
	"database/sql"
	"fmt"
	simplejson "github.com/bitly/go-simplejson"
	"github.com/howeyc/fsnotify"
	_ "github.com/mattn/go-sqlite3"
	"github.com/elgs/filesync/api"
	"github.com/elgs/filesync/index"
	"io/ioutil"
	"os"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("CPUs: ", runtime.NumCPU())

	input := args()
	if len(input) >= 1 {
		start(input[0])
	}
}

func start(configFile string) {
	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println(configFile, " not found")
		return
	}
	json, _ := simplejson.NewJson(b)
	ip := json.Get("ip").MustString("127.0.0.1")
	port := json.Get("port").MustInt(6776)

	monitors := json.Get("monitors").MustMap()

	for _, v := range monitors {
		watcher, _ := fsnotify.NewWatcher()
		monitored, _ := v.(string)
		monitored = index.PathSafe(monitored)
		db, _ := sql.Open("sqlite3", index.SlashSuffix(monitored)+".sync/index.db")
		defer db.Close()
		db.Exec("VACUUM;")
		index.InitIndex(monitored, db)
		index.WatchRecursively(watcher, monitored, monitored)
		go index.ProcessEvent(watcher, monitored)
	}

	api.RunWeb(ip, port, monitors)
	//watcher.Close()
}

func args() []string {
	ret := []string{}
	if len(os.Args) <= 1 {
		ret = append(ret, "gsyncd.json")
	} else {
		for i := 1; i < len(os.Args); i++ {
			ret = append(ret, os.Args[i])
		}
	}
	return ret
}
