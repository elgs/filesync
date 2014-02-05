package index

import (
	"database/sql"
	"fmt"
	"github.com/howeyc/fsnotify"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type IndexedFile struct {
	FilePath     string
	LastModified int64
	FileSize     int64
	FileMode     os.FileMode
	Status       string
	LastIndexed  int64
}

type IndexedFilePart struct {
	FilePath     string
	Seq          int
	StartIndex   int64
	Offset       int
	Checksum     string
	ChecksumType string
}

const (
	BLOCK_SIZE int64 = 1 << 20
)

func ProcessFileDelete(thePath string, monitored string) {
	thePath = PathSafe(thePath)

	db, _ := sql.Open("sqlite3", SlashSuffix(monitored)+".sync/index.db")
	defer db.Close()

	psDeleteFileParts, _ := db.Prepare("DELETE FROM FILE_PARTS WHERE FILE_PATH=?")
	defer psDeleteFileParts.Close()

	psDeleteFilePartsSub, _ := db.Prepare("DELETE FROM FILE_PARTS WHERE FILE_PATH LIKE ?")
	defer psDeleteFilePartsSub.Close()

	psUpdateFiles, _ := db.Prepare(`UPDATE FILES SET STATUS=?,LAST_INDEXED=? WHERE FILE_PATH=?`)
	defer psUpdateFiles.Close()

	psDeleteFilesSub, _ := db.Prepare(`DELETE FROM FILES WHERE FILE_PATH LIKE ? AND FILE_PATH!=?`)
	defer psDeleteFilesSub.Close()

	psUpdateFileStatus, _ := db.Prepare(`UPDATE FILES
	SET FILE_MODE=?,LAST_MODIFIED=?,LAST_INDEXED=? WHERE FILE_PATH=?`)
	defer psUpdateFileStatus.Close()

	psDeleteFileParts.Exec(thePath[len(monitored):])
	psDeleteFilePartsSub.Exec(thePath[len(monitored):] + "/%")

	psUpdateFiles.Exec("deleted", time.Now().Unix(), thePath[len(monitored):])
	pathDir := SlashSuffix(thePath[len(monitored):])
	psUpdateFiles.Exec("deleted", time.Now().Unix(), pathDir)
	psDeleteFilesSub.Exec(pathDir+"%", pathDir)

	parentDirInfo, _ := os.Lstat(filepath.Dir(thePath))
	psUpdateFileStatus.Exec(parentDirInfo.Mode().Perm(), parentDirInfo.ModTime().Unix(), time.Now().Unix(), SlashSuffix(PathSafe(filepath.Dir(thePath))[len(monitored):]))

}

func ProcessDirChange(thePath string, info os.FileInfo, monitored string) {
	if info == nil {
		fmt.Println("Dir no longer exists: " + thePath)
		return
	}
	thePath = PathSafe(thePath)

	db, _ := sql.Open("sqlite3", SlashSuffix(monitored)+".sync/index.db")
	defer db.Close()

	psUpdateFileStatus, _ := db.Prepare(`UPDATE FILES
	SET LAST_MODIFIED=?,FILE_MODE=?,LAST_INDEXED=? WHERE FILE_PATH=?`)
	defer psUpdateFileStatus.Close()

	psUpdateFileStatus.Exec(info.ModTime().Unix(), info.Mode().Perm(), time.Now().Unix(), SlashSuffix(thePath[len(monitored):]))
}

func ProcessFileChange(thePath string, info os.FileInfo, monitored string) {
	fmt.Println(thePath,"changed")
	if info == nil {
		fmt.Println("File no longer exists: " + thePath)
		return
	}
	thePath = PathSafe(thePath)

	db, _ := sql.Open("sqlite3", monitored+"/.sync/index.db")
	defer db.Close()

	psSelectFile, _ := db.Prepare("SELECT * FROM FILES WHERE FILE_PATH=?")
	defer psSelectFile.Close()

	psSelectFileParts, _ := db.Prepare("SELECT * FROM FILE_PARTS WHERE FILE_PATH=? ORDER BY SEQ")
	defer psSelectFileParts.Close()

	psInsertFiles, _ := db.Prepare(`INSERT INTO FILES
	(FILE_PATH,LAST_MODIFIED,FILE_SIZE,FILE_MODE,STATUS,LAST_INDEXED)
	VALUES(?,?,?,?,?,?)`)
	defer psInsertFiles.Close()

	psUpdateFiles, _ := db.Prepare(`UPDATE FILES
	SET LAST_MODIFIED=?,FILE_SIZE=?,FILE_MODE=?,STATUS=?,LAST_INDEXED=?
	WHERE FILE_PATH=?`)
	defer psUpdateFiles.Close()

	psUpdateFileStatus, _ := db.Prepare(`UPDATE FILES
	SET FILE_MODE=?,STATUS=?,LAST_MODIFIED=?,LAST_INDEXED=? WHERE FILE_PATH=?`)
	defer psUpdateFileStatus.Close()

	psInsertFileParts, _ := db.Prepare(`INSERT INTO FILE_PARTS
	(FILE_PATH,SEQ,START_INDEX,OFFSET,CHECKSUM,CHECKSUM_TYPE)
	VALUES(?,?,?,?,?,?)`)
	defer psInsertFileParts.Close()

	psUpdateFileParts, _ := db.Prepare(`UPDATE FILE_PARTS
	SET START_INDEX=?,OFFSET=?,CHECKSUM=?,CHECKSUM_TYPE=?
	WHERE FILE_PATH=? AND SEQ=?`)
	defer psUpdateFileParts.Close()

	psDeleteFileParts, _ := db.Prepare(`DELETE FROM FILE_PARTS
	WHERE FILE_PATH=? AND SEQ=?`)
	defer psDeleteFileParts.Close()

	insert := false
	file := new(IndexedFile)
	err := psSelectFile.QueryRow(thePath[len(monitored):]).Scan(&file.FilePath, &file.LastModified,
		&file.FileSize, &file.FileMode, &file.Status, &file.LastIndexed)
	if err == sql.ErrNoRows {
		insert = true
	}
	if !insert && info.ModTime().Unix() == file.LastModified && info.Size() == file.FileSize && info.Mode().Perm() == file.FileMode && file.Status != "deleted" {
		// file unchanged
		fmt.Println(file.FilePath + " unchanged.")
		return
	}

	// now we think file has been changed
	if insert {
		psInsertFiles.Exec(thePath[len(monitored):], info.ModTime().Unix(), info.Size(), info.Mode().Perm(), "updating", time.Now().Unix())
	} else {
		psUpdateFiles.Exec(info.ModTime().Unix(), info.Size(), info.Mode().Perm(), "updating", time.Now().Unix(), thePath[len(monitored):])
	}

	blocks := int(math.Ceil(float64(info.Size()) / float64(BLOCK_SIZE)))
	if blocks == 0 {
		blocks = 1
	}

	sliceFileParts := make([]IndexedFilePart, 0, 10)
	rows, _ := psSelectFileParts.Query(thePath[len(monitored):])
	defer rows.Close()
	for rows.Next() {
		filePart := new(IndexedFilePart)
		rows.Scan(&filePart.FilePath, &filePart.Seq, &filePart.StartIndex, &filePart.Offset, &filePart.Checksum, &filePart.ChecksumType)
		sliceFileParts = append(sliceFileParts, *filePart)
	}

	h := crc32.NewIEEE()
	for i := 0; i < blocks; i++ {
		var fp IndexedFilePart
		insertFP := false
		if i < len(sliceFileParts) {
			fp = sliceFileParts[i]
		} else {
			//fp = *new(IndexedFilePart)
			insertFP = true
		}

		file, _ := os.Open(thePath)
		defer file.Close()
		buf := make([]byte, BLOCK_SIZE)
		n, _ := file.ReadAt(buf, int64(i)*BLOCK_SIZE)

		h.Reset()
		h.Write(buf[:n])
		v := fmt.Sprint(h.Sum32())

		if v != fp.Checksum {
			// part changed
			fp.Checksum = v
			fp.ChecksumType = "CRC32"
			fp.StartIndex = int64(i)*BLOCK_SIZE
			fp.Offset = n
			fp.FilePath = thePath[len(monitored):]
			fp.Seq = i

			if insertFP {
				psInsertFileParts.Exec(fp.FilePath, fp.Seq, fp.StartIndex, fp.Offset, fp.Checksum, fp.ChecksumType)
			} else {
				psUpdateFileParts.Exec(fp.StartIndex, fp.Offset, fp.Checksum, fp.ChecksumType, fp.FilePath, fp.Seq)
			}
		}
		fp.Checksum = v
	}
	if len(sliceFileParts) > blocks {
		for i := blocks; i < len(sliceFileParts); i-- {
			psDeleteFileParts.Exec(thePath[len(monitored):], i)
		}
	}
	psUpdateFileStatus.Exec(info.Mode().Perm(), "ready", info.ModTime().Unix(), time.Now().Unix(), thePath[len(monitored):])
	parentDirInfo, _ := os.Lstat(filepath.Dir(thePath))
	psUpdateFileStatus.Exec(parentDirInfo.Mode().Perm(), "ready", parentDirInfo.ModTime().Unix(), time.Now().Unix(), SlashSuffix(PathSafe(filepath.Dir(thePath))[len(monitored):]))

}

func WatchRecursively(watcher *fsnotify.Watcher, root string, monitored string) error {
	safeRoot := PathSafe(root)

	db, _ := sql.Open("sqlite3", SlashSuffix(monitored)+".sync/index.db")
	defer db.Close()

	mapFiles := make(map[string]IndexedFile)
	psSelectFilesLike, _ := db.Prepare("SELECT * FROM FILES WHERE FILE_PATH LIKE ?")
	defer psSelectFilesLike.Close()
	rows, _ := psSelectFilesLike.Query(SlashSuffix(LikeSafe(safeRoot)[len(monitored):]) + "%")
	defer rows.Close()
	for rows.Next() {
		file := new(IndexedFile)
		rows.Scan(&file.FilePath, &file.LastModified, &file.FileSize, &file.FileMode, &file.Status, &file.LastIndexed)
		mapFiles[file.FilePath] = *file
	}
	psInsertFiles, _ := db.Prepare(`INSERT INTO FILES
	(FILE_PATH,LAST_MODIFIED,FILE_SIZE,FILE_MODE,STATUS,LAST_INDEXED)
	VALUES(?,?,?,?,?,?)`)
	defer psInsertFiles.Close()

	psUpdateFiles, _ := db.Prepare(`UPDATE FILES
	SET FILE_MODE=?,STATUS='ready',LAST_MODIFIED=?,LAST_INDEXED=? WHERE FILE_PATH=?`)
	defer psUpdateFiles.Close()

	filepath.Walk(safeRoot,
		(filepath.WalkFunc)(func(path string, info os.FileInfo, err error) error {
			var thePath string
			if info.IsDir() {
				thePath = SlashSuffix(PathSafe(path))
				if strings.HasPrefix(thePath, SlashSuffix(safeRoot)+".sync/") {
					return nil
				}

				watcher.Watch(thePath[0 : len(thePath) - 1])
				// update index
				if v, ok := mapFiles[thePath[len(monitored):]]; !ok {
					psInsertFiles.Exec(thePath[len(monitored):], info.ModTime().Unix(), -1, uint32(info.Mode().Perm()), "ready", time.Now().Unix())
				} else {
					if v.Status != "ready" {
						psUpdateFiles.Exec(info.Mode().Perm(), info.ModTime().Unix(), time.Now().Unix(), v.FilePath)
					}
				}
			} else {
				thePath = PathSafe(path)
				if strings.HasPrefix(PathSafe(filepath.Dir(thePath)), SlashSuffix(safeRoot)+".sync") {
					return nil
				}
				ProcessFileChange(thePath, info, monitored)
			}
			delete(mapFiles, thePath[len(monitored):])
			return nil
		}))
	// remove zombies
	for k, v := range mapFiles {
		if k != "/" && v.Status == "ready" {
			fmt.Println("Zombie removed: ", v.FilePath)
			ProcessFileDelete(monitored+k, monitored)
		}
	}

	return nil
}

func SlashSuffix(path string) string {
	if strings.HasSuffix(path, "/") {
		return path
	} else {
		return path + "/"
	}
}
func PathSafe(path string) string {
	path = regexp.MustCompile("\\\\+").ReplaceAllString(path, "/")
	//path, _ = filepath.Abs(path)
	return path
}
func LikeSafe(path string) string {
	path = strings.Replace(path, "_", "\\_", -1)
	return path
}

func InitIndex(monitored string, db *sql.DB) error {
	var ret error = nil
	exists, _ := exists(SlashSuffix(monitored) + ".sync/index.db")
	if !exists {
		os.MkdirAll(SlashSuffix(monitored)+".sync/", (os.FileMode)(0755))
		db.Exec(`
			CREATE TABLE FILE_PARTS(
				FILE_PATH TEXT NOT NULL,
				SEQ INTEGER NOT NULL,
				START_INDEX INTEGER NOT NULL,
				OFFSET INTEGER NOT NULL,
				CHECKSUM TEXT NOT NULL,
				CHECKSUM_TYPE TEXT NOT NULL,
				PRIMARY KEY(FILE_PATH, SEQ)
			);
		`)
		db.Exec(`
			CREATE TABLE FILES(
				FILE_PATH TEXT PRIMARY KEY,
				LAST_MODIFIED INTEGER NOT NULL,
				FILE_SIZE INTEGER NOT NULL,
				FILE_MODE INTEGER NOT NULL,
				STATUS TEXT NOT NULL,
				LAST_INDEXED INTEGER NOT NULL
			);
		`)
		db.Exec("CREATE INDEX IDX_FILES_FILESIZE ON FILES(FILE_SIZE);")
		db.Exec("CREATE INDEX IDX_FILES_STATUS ON FILES(STATUS);")
		db.Exec("CREATE INDEX IDX_FILES_LASTINDEXED ON FILES(LAST_INDEXED);")
	}
	return ret
}

// exists returns whether the given file or directory exists or not
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func ProcessEvent(watcher *fsnotify.Watcher, monitored string) {
	for {
		select {
		case ev := <-watcher.Event:
			fmt.Println("event:", ev, ":", monitored)
			info, _ := os.Lstat(ev.Name)
			if info == nil {
				ProcessFileDelete(ev.Name, monitored)
			} else if ev.IsCreate() {
				if info.IsDir() {
					WatchRecursively(watcher, ev.Name, monitored)
					//fmt.Println("Created dir: " + ev.Name)
				} else {
					ProcessFileChange(ev.Name, info, monitored)
					//fmt.Println("Created file: " + ev.Name)
				}
			} else if ev.IsModify() {
				if info.IsDir() {
					ProcessDirChange(ev.Name, info, monitored)
					//fmt.Println("Modified dir: " + ev.Name)
				} else {
					ProcessFileChange(ev.Name, info, monitored)
					//fmt.Println("Modified file: " + ev.Name)
				}
			} else if ev.IsDelete() || ev.IsRename() {
				ProcessFileDelete(ev.Name, monitored)
				//fmt.Println("Deleted: " + ev.Name)
			}
		case err := <-watcher.Error:
			fmt.Println("error:", err)
		}
	}
}
