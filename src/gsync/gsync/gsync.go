package main

import (
	"fmt"
	simplejson "github.com/bitly/go-simplejson"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"time"
	"gsyncd/index"
	"encoding/json"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("CPUs: ", runtime.NumCPU())
	input := args()
	done := make(chan bool)
	if len(input) >= 1 {
		start(input[0], done)
	}
	<-done
}

func start(configFile string, done chan bool) {
	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println(configFile, " not found")
		go func() {
			done <- false
		}()
		return
	}
	json, _ := simplejson.NewJson(b)
	ip := json.Get("ip").MustString("127.0.0.1")
	port := json.Get("port").MustInt(6776)

	monitors := json.Get("monitors").MustMap()

	for k, v := range monitors {
		monitored, _ := v.(string)
		go startWork(ip, port, k, monitored)
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

func startWork(ip string, port int, key string, monitored string) {
	var lastIndexed int64 = 0
	sleepTime := time.Second
	for {
		serverIndexed := timeFromServer(ip, port, key)
		dirs := dirsFromServer(ip, port, key, lastIndexed)
		if len(dirs) == 0 {
			sleepTime *= 2
			if sleepTime >= time.Minute {
				sleepTime = time.Minute
			}
		} else {
			sleepTime = time.Second
			for _, dir := range dirs {
				dirMap, _ := dir.(map[string]interface{})
				dirPath, _ := dirMap["FilePath"].(string)
				dirStatus := dirMap["Status"].(string)
				if dirStatus == "deleted" {
					//TODO: rm -rf monitored + dirPath
					continue
				}
				mode, _ := dirMap["FileMode"].(json.Number)
				dirMode, _ := mode.Int64()
				dir := index.PathSafe(index.SlashSuffix(monitored) + dirPath)
				err := os.MkdirAll(dir, os.FileMode(dirMode))
				if err != nil {
					fmt.Println(err)
				}
				files := filesFromServer(ip, port, key, dirPath, lastIndexed)
				if len(files) > 0 {
					for _, file := range files {
						fileMap, _ := file.(map[string] interface{})
						filePath, _ := fileMap["FilePath"].(string)
						fileStatus := fileMap["Status"].(string)
						if fileStatus == "deleted" {
							//TODO: rm -rf monitored + dirPath
							continue
						}
						fmt.Println(filePath)
					}
				}
			}
		}
		lastIndexed = serverIndexed
		time.Sleep(sleepTime)
	}
}

func filePartsFromServer(ip string, port int, key string, filePath string, lastIndexed int64) []interface{} {
	client := &http.Client{}
	req, _ := http.NewRequest("GET", fmt.Sprint("http://", ip, ":", port,
			"/files?last_indexed=", lastIndexed, "&file_path=", url.QueryEscape(filePath)), nil)
	req.Header.Add("AUTH_KEY", key)
	resp, _ := client.Do(req)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	json, _ := simplejson.NewJson(body)
	files := json.MustArray()
	return files
}

func filesFromServer(ip string, port int, key string, filePath string, lastIndexed int64) []interface{} {
	client := &http.Client{}
	req, _ := http.NewRequest("GET", fmt.Sprint("http://", ip, ":", port,
			"/files?last_indexed=", lastIndexed, "&file_path=", url.QueryEscape(filePath)), nil)
	req.Header.Add("AUTH_KEY", key)
	resp, _ := client.Do(req)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	json, _ := simplejson.NewJson(body)
	files := json.MustArray()
	return files
}

func dirsFromServer(ip string, port int, key string, lastIndexed int64) []interface{} {
	client := &http.Client{}
	req, _ := http.NewRequest("GET", fmt.Sprint("http://", ip, ":", port, "/dirs?last_indexed=", lastIndexed), nil)
	req.Header.Add("AUTH_KEY", key)
	resp, _ := client.Do(req)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	json, _ := simplejson.NewJson(body)
	dirs := json.MustArray()
	return dirs
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
