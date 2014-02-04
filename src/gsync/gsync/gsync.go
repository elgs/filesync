package main

import (
	"fmt"
	"io/ioutil"
	"runtime"
	simplejson "github.com/bitly/go-simplejson"
	"os"
	"net/http"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("CPUs: ", runtime.NumCPU())
	done := make(chan bool)
	input := args()
	if len(input) >= 1 {
		start(input[0])
	}
	<-done
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

	for k, v := range monitors {
		monitored, _ := v.(string)
		go updateDirs(ip, port, k, monitored)
	}
}
func args() []string {
	ret := []string{}
	if len(os.Args) <= 1 {
		ret = append(ret, "gsync.json")
	} else {
		for i := 1; i < len(os.Args); i++ {
			ret = append(ret, os.Args[i])
		}
	}
	return ret
}

func updateDirs(ip string, port int, key string, monitored string) {
	lastIndexed := int64(0)
	sleepTime := time.Second
	for {
		client := &http.Client{}
		req, _ := http.NewRequest("GET", fmt.Sprint("http://", ip, ":", port, "/dirs?last_indexed=", lastIndexed), nil)
		req.Header.Add("AUTH_KEY", key)
		resp, _ := client.Do(req)
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		json, _ := simplejson.NewJson(body)
		dirs := json.MustArray()
		if len(dirs) == 0 {
			sleepTime *=2
			if sleepTime >= time.Minute {
				sleepTime = time.Minute
			}
		} else {
			sleepTime = time.Second
			for i, v := range dirs {
				fmt.Println(i, v)
			}
		}
		fmt.Println(sleepTime)
		lastIndexed = timeFromServer(ip, port, key)
		time.Sleep(sleepTime)
	}
}

func dirsFromServer(ip string, port int, key string) []map[string]interface{} {
	return nil
}

func timeFromServer(ip string, port int, key string) int64 {
	client := &http.Client{}
	req, _ := http.NewRequest("GET", fmt.Sprint("http://", ip, ":", port, "/time"), nil)
	req.Header.Add("AUTH_KEY", key)
	resp, _ := client.Do(req)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	json, _ := simplejson.NewJson(body)
	currentTime := json.Get("current_time").MustInt64(0)
	return currentTime
}
