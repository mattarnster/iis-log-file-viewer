package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
	"github.com/rainycape/geoip"
	"github.com/schollz/progressbar/v3"
	_ "modernc.org/sqlite"
)

type LogEntry struct {
	Id          int
	Date        string
	Time        string
	Serverip    string
	Method      string
	Url         string
	Query       string
	Port        int
	Username    string
	Clientip    string
	Useragent   string
	Referer     string
	Status      int
	Substatus   int
	Win32status int
	Timetaken   int
	Lat         float64
	Long        float64
	Country     string
}

var upgrader = websocket.Upgrader{} // use default options
var geoipdb *geoip.GeoIP = &geoip.GeoIP{}
var sqldb *sql.DB

func readFileIntoDB(filePath string) {
	_, err := sqldb.Query(`
	DROP TABLE log;
	CREATE TABLE IF NOT EXISTS log (
		Id int,
		AccDate text,
		Time text,
		Serverip text,
		Method text,
		Url text,
		Query text,
		Port int,
		Username text,
		Clientip text,
		Useragent text,
		Referer text,
		Status int,
		Substatus int,
		Win32status int,
		Timetaken int,
		Lat float,
		Long float,
		Country text
	)`)

	if err != nil {
		fmt.Printf("err: %v\n", err)
	}

	db, dberr := geoip.Open("./GeoLite2-City.mmdb")
	if dberr != nil {
		panic(dberr)
	}
	geoipdb = db

	_, oserr := os.Stat(filePath)
	if err != nil {
		fmt.Printf("%s", oserr.Error())
		return
	}

	lf, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer lf.Close()

	logstream = make(chan interface{})

	lines, _ := lineCounter(lf)

	fmt.Printf("Lines in log file: %v\n", lines)

	lf.Seek(0, 0)

	scanner := bufio.NewScanner(lf)
	bar := progressbar.Default(int64(lines), "Reading log lines into DB")

	ctx := context.Background()

	txn, _ := sqldb.BeginTx(ctx, nil)

	for scanner.Scan() {
		lines -= 1
		splitline := strings.Split(scanner.Text(), " ")
		if len(splitline) != 15 {
			continue
		}

		clientIp := net.ParseIP(splitline[8])
		res, geoiperr := geoipdb.LookupIP(clientIp)

		portInt, _ := strconv.Atoi(splitline[6])
		statusInt, _ := strconv.Atoi(splitline[11])
		substatusInt, _ := strconv.Atoi(splitline[12])
		win32statusInt, _ := strconv.Atoi(splitline[13])
		timetakenInt, _ := strconv.Atoi(splitline[14])

		logline := &LogEntry{
			Id:          lines,
			Date:        splitline[0],
			Time:        splitline[1],
			Serverip:    splitline[2],
			Method:      splitline[3],
			Url:         splitline[4],
			Query:       splitline[5],
			Port:        portInt,
			Username:    splitline[7],
			Clientip:    splitline[8],
			Useragent:   splitline[9],
			Referer:     splitline[10],
			Status:      statusInt,
			Substatus:   substatusInt,
			Win32status: win32statusInt,
			Timetaken:   timetakenInt,
		}

		if res != nil && geoiperr == nil && res.Country != nil {
			logline.Lat = res.Latitude
			logline.Long = res.Longitude
			logline.Country = res.Country.Code
		}

		if false {
			fmt.Println(logline)
		}

		_, inserterr := txn.ExecContext(ctx,
			"INSERT INTO log ( Id, AccDate, Time, Serverip, Method, Url, Query, Port, Username, Clientip, Useragent, Referer, Status, Substatus, Win32status, Timetaken, Lat, Long, Country ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )",
			logline.Id,
			logline.Date,
			logline.Time,
			logline.Serverip,
			logline.Method,
			logline.Url,
			logline.Query,
			logline.Port,
			logline.Username,
			logline.Clientip,
			logline.Useragent,
			logline.Referer,
			logline.Status,
			logline.Substatus,
			logline.Win32status,
			logline.Timetaken,
			logline.Lat,
			logline.Long,
			logline.Country,
		)
		if inserterr != nil {
			fmt.Printf("inserterr: %v\n", inserterr)
		}

		bar.Add(1)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	errd := txn.Commit()
	if errd != nil {
		fmt.Printf("%v", errd.Error())
	}

	bar.Finish()
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func socketHandler(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool {
		return true
	}
	// Upgrade our raw HTTP connection to a websocket based one
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("Error during connection upgradation:", err)
		return
	}
	defer conn.Close()

	// The event loop
	for {
		data, _ := json.Marshal(<-logstream)
		err = conn.WriteMessage(1, data)
		if err != nil {
			log.Println("Error during message writing:", err)
			break
		}
	}
}

func dataHandler(w http.ResponseWriter, r *http.Request) {
	res, err := sqldb.Query("SELECT COUNT(*) FROM log")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	var count int

	res.Next()
	res.Scan(&count)

	w.Write([]byte(fmt.Sprintf("%v", count)))
}

func realtimeDataHandler(w http.ResponseWriter, r *http.Request) {
	res, err := sqldb.Query(`
		SELECT
		AccDate,
		Time,
		Method,
		Status,
		Url,
		Query,
		Clientip,
		Country,
		Useragent,
		Referer FROM log
		LIMIT 100
	`)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	type DBResult struct {
		Date      string
		Time      string
		Method    string
		Status    int
		Url       string
		Query     string
		Clientip  string
		Country   string
		Useragent string
		Referer   string
	}

	type DBResultSet struct {
		Results []DBResult
	}

	rs := &DBResultSet{}

	for res.Next() {
		var Date string
		var Time string
		var Method string
		var Status int
		var Url string
		var Query string
		var Clientip string
		var Country string
		var Useragent string
		var Referer string

		res.Scan(&Date, &Time, &Method, &Status, &Url, &Query, &Clientip, &Country, &Useragent, &Referer)

		r := DBResult{
			Date:      Date,
			Time:      Time,
			Method:    Method,
			Status:    Status,
			Url:       Url,
			Clientip:  Clientip,
			Country:   Country,
			Useragent: Useragent,
			Referer:   Referer,
		}

		rs.Results = append(rs.Results, r)
	}

	// data, _ := json.Marshal(rs)

	// w.Write([]byte(data))

	t, _ := template.ParseFiles("./tmpl/realtime.html")
	t.Execute(w, rs)
}

func statusDataHandler(w http.ResponseWriter, r *http.Request) {
	res, err := sqldb.Query("SELECT Status, COUNT(Status) AS Count FROM log GROUP BY Status ORDER BY Count DESC")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	type DBResult struct {
		Status int
		Count  int
	}

	type DBResultSet struct {
		Results []DBResult
	}

	rs := &DBResultSet{}

	for res.Next() {
		var Status int
		var Count int

		res.Scan(&Status, &Count)

		r := DBResult{
			Status: Status,
			Count:  Count,
		}

		rs.Results = append(rs.Results, r)
	}

	data, _ := json.Marshal(rs)

	w.Write([]byte(data))
}

func topIPsDataHandler(w http.ResponseWriter, r *http.Request) {
	res, err := sqldb.Query("SELECT Clientip, Count(Clientip) As Count, Country FROM log GROUP BY Country ORDER BY Count DESC")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	type DBResult struct {
		Clientip string
		Count    int
		Country  string
	}

	type DBResultSet struct {
		Results []DBResult
	}

	rs := &DBResultSet{}

	for res.Next() {
		var Clientip string
		var Count int
		var Country string

		res.Scan(&Clientip, &Count, &Country)

		r := DBResult{
			Clientip: Clientip,
			Count:    Count,
			Country:  Country,
		}

		rs.Results = append(rs.Results, r)
	}

	// data, _ := json.Marshal(rs)

	// w.Write([]byte(data))

	t, _ := template.ParseFiles("./tmpl/topips.html")
	t.Execute(w, rs)
}

var logstream chan interface{}

func main() {
	var filePath string
	var shouldReadExisting bool

	flag.StringVar(&filePath, "f", "", "The file path")
	flag.BoolVar(&shouldReadExisting, "e", false, "Read existing data")

	flag.Parse()

	sqldb, _ = sql.Open("sqlite", "./logs.sqlite3")

	defer sqldb.Close()

	if shouldReadExisting {
		readFileIntoDB(filePath)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	lf, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer lf.Close()

	logstream = make(chan interface{})

	// done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Println("event:", event)
				if event.Op&fsnotify.Write == fsnotify.Write {
					// Get the last line of the file
					line := ""
					var cursor int64 = 0
					stat, _ := lf.Stat()
					filesize := stat.Size()
					for {
						cursor -= 1
						lf.Seek(cursor, io.SeekEnd)

						char := make([]byte, 1)
						lf.Read(char)

						if cursor != -1 && (char[0] == 10 || char[0] == 13) { // stop if we find a line
							break
						}

						line = fmt.Sprintf("%s%s", string(char), line) // there is more efficient way

						if cursor == -filesize { // stop if we are at the begining
							break
						}
					}
					splitline := strings.Split(line, " ")
					if len(splitline) != 15 {
						break
					}
					log.Println(splitline[8])
					// res, err := geoipdb.Lookup(splitline[8])
					// if err != nil {
					// 	log.Fatalf("DB shit itself %v", err.Error())
					// }

					portInt, _ := strconv.Atoi(splitline[6])
					statusInt, _ := strconv.Atoi(splitline[11])
					substatusInt, _ := strconv.Atoi(splitline[12])
					win32statusInt, _ := strconv.Atoi(splitline[13])
					timetakenInt, _ := strconv.Atoi(splitline[14])

					logline := &LogEntry{
						Date:        splitline[0],
						Time:        splitline[1],
						Serverip:    splitline[2],
						Method:      splitline[3],
						Url:         splitline[4],
						Query:       splitline[5],
						Port:        portInt,
						Username:    splitline[7],
						Clientip:    splitline[8],
						Useragent:   splitline[9],
						Referer:     splitline[10],
						Status:      statusInt,
						Substatus:   substatusInt,
						Win32status: win32statusInt,
						Timetaken:   timetakenInt,
					}
					logstream <- logline
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(filePath)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/socket", socketHandler)
	http.HandleFunc("/data", dataHandler)
	http.HandleFunc("/data/status", statusDataHandler)
	http.HandleFunc("/data/ips", topIPsDataHandler)
	http.HandleFunc("/data/realtime", realtimeDataHandler)
	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}
