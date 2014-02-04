package api

import (
	"fmt"
	"github.com/codegangsta/martini"
	"github.com/codegangsta/martini-contrib/encoder"
	"net/http"
	"time"
	"gsyncd/index"
	"database/sql"
	"strconv"
	"io"
	"os"
)

func RunWeb(ip string, port int, monitors map[string]interface{}) {
	m := martini.New()
	route := martini.NewRouter()

	// validate an api key
	m.Use(func(res http.ResponseWriter, req *http.Request) {
		authKey := req.Header.Get("AUTH_KEY")
		if monitors[authKey] == nil {
			res.WriteHeader(http.StatusUnauthorized)
			res.Write([]byte("Unauthorized access."))
		} else {
			monitored, _ := monitors[authKey].(string)
			req.Header.Set("MONITORED", monitored)
		}
	})

	// map json encoder
	m.Use(func(c martini.Context, w http.ResponseWriter) {
		c.MapTo(encoder.JsonEncoder{}, (*encoder.Encoder)(nil))
		w.Header().Set("Content-Type", "application/json")
	})

	route.Get("/time", func(enc encoder.Encoder) (int, []byte) {
			result := map[string]int64{"current_time": time.Now().Unix()}
			return http.StatusOK, encoder.Must(enc.Encode(result))
		})

	route.Get("/dirs", func(enc encoder.Encoder, req *http.Request) (int, []byte) {
			monitored := req.Header.Get("MONITORED")
			lastIndexed, _ := strconv.Atoi(req.FormValue("last_indexed"))
			result := make([]index.IndexedFile, 0)

			db, _ := sql.Open("sqlite3", index.SlashSuffix(monitored)+".sync/index.db")
			defer db.Close()
			psSelectDirs, _ := db.Prepare("SELECT * FROM FILES WHERE FILE_SIZE=-1 AND LAST_INDEXED>?")
			defer psSelectDirs.Close()
			rows, _ := psSelectDirs.Query(lastIndexed)
			defer rows.Close()
			for rows.Next() {
				file := new(index.IndexedFile)
				rows.Scan(&file.FilePath, &file.LastModified, &file.FileSize, &file.FileMode, &file.Status, &file.LastIndexed)
				result = append(result, *file)
			}
			return http.StatusOK, encoder.Must(enc.Encode(result))
		})

	route.Get("/files", func(enc encoder.Encoder, req *http.Request) (int, []byte) {
			monitored := req.Header.Get("MONITORED")
			lastIndexed, _ := strconv.Atoi(req.FormValue("last_indexed"))
			filePath := index.SlashSuffix(index.LikeSafe(req.FormValue("file_path")))
			result := make([]index.IndexedFile, 0)

			db, _ := sql.Open("sqlite3", index.SlashSuffix(monitored)+".sync/index.db")
			defer db.Close()
			psSelectFiles, _ := db.Prepare(`SELECT * FROM FILES
				WHERE LAST_INDEXED>? AND FILE_SIZE>=0 AND FILE_PATH LIKE ? AND FILE_PATH NOT LIKE ?`)
			defer psSelectFiles.Close()
			rows, _ := psSelectFiles.Query(lastIndexed, filePath+"%", filePath+"%/%")
			defer rows.Close()
			for rows.Next() {
				file := new(index.IndexedFile)
				rows.Scan(&file.FilePath, &file.LastModified, &file.FileSize, &file.FileMode, &file.Status, &file.LastIndexed)
				result = append(result, *file)
			}
			return http.StatusOK, encoder.Must(enc.Encode(result))
		})

	route.Get("/file_parts", func(enc encoder.Encoder, req *http.Request) (int, []byte) {
			monitored := req.Header.Get("MONITORED")
			filePath := req.FormValue("file_path")
			result := make([]index.IndexedFilePart, 0)

			db, _ := sql.Open("sqlite3", index.SlashSuffix(monitored)+".sync/index.db")
			defer db.Close()
			psSelectFiles, _ := db.Prepare(`SELECT * FROM FILE_PARTS
				WHERE FILE_PATH=? ORDER BY FILE_PATH,SEQ`)
			defer psSelectFiles.Close()
			rows, _ := psSelectFiles.Query(filePath)
			defer rows.Close()
			for rows.Next() {
				filePart := new(index.IndexedFilePart)
				rows.Scan(&filePart.FilePath, &filePart.Seq, &filePart.StartIndex, &filePart.Offset, &filePart.Checksum, &filePart.ChecksumType)
				result = append(result, *filePart)
			}
			return http.StatusOK, encoder.Must(enc.Encode(result))
		})

	route.Get("/download", func(res http.ResponseWriter, req *http.Request) {
			monitored := req.Header.Get("MONITORED")
			filePath := req.FormValue("file_path")
			start, _ := strconv.ParseInt(req.FormValue("start"), 10, 64)
			length, _ := strconv.ParseInt(req.FormValue("length"), 10, 64)

			res.Header().Set("Content-Type", "application/octet-stream")
			res.Header().Set("Content-Length", strconv.FormatInt(length, 10))

			file, _ := os.Open(index.SlashSuffix(monitored) + filePath)
			defer file.Close()
			file.Seek(start, os.SEEK_SET)
			io.CopyN(res, file, length)
		})

	m.Action(route.Handle)
	fmt.Println(http.ListenAndServe(fmt.Sprint(ip, ":", port), m))
}
