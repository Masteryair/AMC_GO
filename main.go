package main

import (
	_ "embed"
	"encoding/base64"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.bug.st/serial"
)

//go:embed index.html
var indexHTML []byte

const (
	PacketSize = 54
	DataSize   = PacketSize - 2
	VarSize    = 23
	BufSize    = 10000
	OutBufSize = 10000000
)

// הגדרת מבנה הנתונים הפיזיקליים
type PhysicsData struct {
	Time   float32
	Values [VarSize]float32
}
type WSMessage struct {
	Message string `json:"message"`
}

var (
	upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	clients  = make(map[*websocket.Conn]bool)
	lock     sync.Mutex
	//s             serial.Port
	port_req      = make(chan string, 10)
	cmdChan       = make(chan []byte, 10)
	broadcastChan = make(chan PhysicsData, OutBufSize)
	out_mesChan   = make(chan string, 10)
	folderName    = fmt.Sprintf("%s_%s", "default", time.Now().Format("02_01_06"))
	fileName      = fmt.Sprintf("%s_%s.csv", "default", time.Now().Format("02_01_06_150405"))
	lockFile      sync.Mutex
	LABELS        = "TIME[S],POS [CNT],POSERR [CNT],CUR [mA],CUR_FILT [mA],CUR_COM [mA],CUR-P [mA],CUR-I [mA],CUR-d [mA],PWM [0.01%],BUS [V],TBD,VEL [CNT/S],COM_NUM,CUR_HD0,CUR_HD1,CUR_HD2,CUR_HD3,CUR_HD4,CUR_HD5,CUR_HD6,CUR_HD7,CUR_HD8,CUR_HD9"
)

func main() {
	rawChan := make(chan [DataSize]byte, 1000)
	physicsChan := make(chan PhysicsData, BufSize)
	data_reqChan := make(chan string, 10)

	// 1. רוטינה לקריאה מהפורט הסריאלי
	go serialReader(rawChan, port_req, data_reqChan)

	// 2. רוטינה לעיבוד הנתונים
	go dataProcessor(rawChan, physicsChan, data_reqChan)

	// 3. רוטינה לשמירה ל-CSV
	go csvSaver(physicsChan)

	// השארת התוכנית רצה
	// 3. שרת אינטרנט
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/ports", getPortsHandler)
	http.HandleFunc("/name", getNameHandler)
	http.HandleFunc("/open-port", openPortsHandler)
	http.HandleFunc("/save-image", saveImageHandler)
	http.HandleFunc("/send-cmd", sendCmdHandler)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	port := "8080"
	url := fmt.Sprintf("http://localhost:%s", port)

	// פקודה ספציפית ל-Windows לפתיחת כתובת בדפדפן ברירת המחדל
	cmd := exec.Command("rundll32", "url.dll,FileProtocolHandler", url)

	err := cmd.Start()
	if err != nil {
		fmt.Printf("Can't open browser: %v\n", err)
	}
	fmt.Println("Server started at http://localhost:" + port)
	http.ListenAndServe(":"+port, nil)
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, _ := upgrader.Upgrade(w, r, nil)
	lock.Lock()
	clients[conn] = true
	lock.Unlock()
	for {
		// השרת מחכה לאות מהלקוח ("כשהוא פנוי")
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		// אם הלקוח שלח בקשה לנתונים (למשל המילה "pull")
		if string(message) == "/pull" {
			broadcastBatch()
		}
	}
}

func broadcastBatch() {
	lock.Lock()
	defer lock.Unlock()
	var batch []PhysicsData

	// "שואבים" את כל ההודעות שמחכות ב-Channel כרגע
	// מבלי להיחסם אם ה-Channel יתרוקן
	collecting := true
	for collecting {
		select {
		case data, ok := <-broadcastChan:
			if !ok {
				collecting = false
			} else {
				batch = append(batch, data)
			}
		default:
			// ה-Channel ריק כרגע, מפסיקים לאסוף ושולחים מה שיש
			collecting = false
		}
	}
	for client := range clients {
		err := client.WriteJSON(batch) // שולח את כל המערך כהודעה אחת
		if err != nil {
			client.Close()
			delete(clients, client)
		}
	}
	select {
	case mes := <-out_mesChan:
		MES := WSMessage{
			Message: mes}
		for client := range clients {
			err := client.WriteJSON(MES) // שולח את כל המערך כהודעה אחת
			if err != nil {
				client.Close()
				delete(clients, client)
			}
		}
	default:
		{
		}
	}
}

func sendCmdHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		CmdArray []int32 `json:"cmdArray"`
	}
	json.NewDecoder(r.Body).Decode(&payload)
	fmt.Println(payload.CmdArray)
	b := make([]byte, len(payload.CmdArray)*4)
	for i, v := range payload.CmdArray {
		binary.LittleEndian.PutUint32(b[i*4:], uint32(v))
	}
	// שליחה לערוץ - הרוטינה של הסריאל כבר תטפל בזה
	cmdChan <- b

	w.WriteHeader(http.StatusOK)
}

func getPortsHandler(w http.ResponseWriter, r *http.Request) {
	// 1. קבלת רשימת הפורטים מהמערכת
	ports, err := serial.GetPortsList()
	if err != nil {
		// במקרה של שגיאה, נחזיר סטטוס 500 ושגיאה בטקסט
		http.Error(w, "Unable to list serial ports: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 2. הגדרת Header שהתוכן הוא JSON
	w.Header().Set("Content-Type", "application/json")

	// 3. אם הרשימה ריקה, נחזיר מערך ריק [] במקום null
	if ports == nil {
		ports = []string{}
	}

	// 4. קידוד ושליחה
	json.NewEncoder(w).Encode(ports)
}

func getNameHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	fmt.Printf("Folder %s opened successfully\n", req.Name)
	lockFile.Lock()
	folderName = fmt.Sprintf("%s_%s", req.Name, time.Now().Format("02_01_06"))
	fileName = fmt.Sprintf("%s_%s.csv", req.Name, time.Now().Format("02_01_06_150405"))
	lockFile.Unlock()
}

func openPortsHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Port string `json:"port"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	fmt.Printf("Port %s opened successfully\n", req.Port)
	port_req <- req.Port

}
func saveImageHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ImageData string `json:"image"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	// הסרת ה-Prefix של Base64 (data:image/png;base64,...)
	b64data := req.ImageData[strings.IndexByte(req.ImageData, ',')+1:]
	data, _ := base64.StdEncoding.DecodeString(b64data)
	lockFile.Lock()
	os.MkdirAll(folderName, os.ModePerm)
	imageName := folderName + time.Now().Format("_150405") + ".png"
	imgPath := filepath.Join(folderName, imageName)
	lockFile.Unlock()
	os.WriteFile(imgPath, data, 0644)
	w.WriteHeader(http.StatusOK)
}

func serialReader(out chan [DataSize]byte, port_req chan string, data_req chan string) {
	buffer := make([]byte, 0)
	temp := make([]byte, PacketSize*BufSize)
	var payload [DataSize]byte
	var s serial.Port
	var err error
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		select {
		case portName := <-port_req:
			{
				buffer = buffer[:0]
				if s != nil {
					s.Close()
				}
				c := &serial.Mode{BaudRate: 2000000}
				s, err = serial.Open(portName, c)
				if err != nil {
					fmt.Println(err)
					out_mesChan <- portName + " unavilable"
				}
				data_req <- "zero"
				if s != nil {
					s.ResetInputBuffer()
					out_mesChan <- "connected " + portName
				}
			}
		default:
			{
			}
		}
		// time.Sleep(1 * time.Millisecond)
		if s != nil {
			select {
			case cmd := <-cmdChan:
				s.Write(cmd)
			default:
				// אם אין פקודה, ממשיכים לקריאה
			}

			n, err := s.Read(temp)
			if err != nil {
				continue
			}
			buffer = append(buffer, temp[:n]...)
			// fmt.Println(len(buffer))
			for len(buffer) > PacketSize {
				if buffer[0] == 170 && buffer[PacketSize] == 170 {
					var sum byte
					for _, b := range buffer[1:(PacketSize - 1)] {
						sum += b
					}
					if sum == buffer[PacketSize-1] {
						// שליחת התוכן שבתוך ה-Frame
						copy(payload[:], buffer[1:PacketSize-1])
						out <- payload
						buffer = buffer[PacketSize:] // ניקוי החבילה שנקראה
						continue
					}
				}
				// אם לא תקין, מוחקים איבר ראשון וממשיכים לחפש
				buffer = buffer[1:]
			}
		}
	}

}

func dataProcessor(in chan [DataSize]byte, out chan PhysicsData, dataReq <-chan string) {
	var (
		t0    uint32
		hasT0 bool
	)

	for {
		select {
		case raw, ok := <-in:
			if !ok {
				return
			}

			ts := binary.LittleEndian.Uint32(raw[0:4])
			if !hasT0 {
				t0 = ts
				hasT0 = true
			}

			var pData PhysicsData
			pData.Time = float32(ts-t0) * 0.00001
			pData.Values[0] = float32(int64(binary.LittleEndian.Uint32(raw[4:8])) - 2147483648)

			for i := 1; i < VarSize; i++ {
				v := binary.LittleEndian.Uint16(raw[(i*2 + 6):(i*2 + 8)])
				pData.Values[i] = float32(int32(v) - 32767)
			}

			pData.Values[12] += 32767

			out <- pData
			broadcastChan <- pData

		case message, ok := <-dataReq:
			if !ok {
				return
			}
			if message == "zero" {
				ClearChannel(in)
				ClearChannel(out)
				ClearChannel(broadcastChan)
				hasT0 = false
				t0 = 0
			}
		}
	}
}
func csvSaver(in chan PhysicsData) {
	var matrix [][]string

	for data := range in {
		// המרה לשורת טקסט עבור ה-CSV
		row := []string{}
		row = append(row, strconv.FormatFloat(float64(data.Time), 'f', 4, 32))
		for _, v := range data.Values {
			row = append(row, strconv.FormatFloat(float64(v), 'f', 4, 32))
		}
		matrix = append(matrix, row)

		if len(matrix) >= 1000 {
			saveToFile(matrix)
			matrix = [][]string{} // איפוס מטריצה
		}
	}
}

func saveToFile(data [][]string) {
	lockFile.Lock()
	os.MkdirAll(folderName, os.ModePerm)
	csvPath := filepath.Join(folderName, fileName)
	lockFile.Unlock()
	f, err := os.OpenFile(csvPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer f.Close()
	fileInfo, err := f.Stat()
	if err == nil && fileInfo.Size() == 0 {
		// כתיבת שורת הכותרות - התאם את השמות לנתונים שלך מה-STM32
		headers := LABELS + "\n"
		_, err = f.WriteString(headers)
		if err != nil {
			fmt.Println("Error writing headers:", err)
		}
	}

	writer := csv.NewWriter(f)
	writer.WriteAll(data)
	writer.Flush()
	fmt.Println("Saved 1000 rows to CSV")
}

func ClearChannel[T any](ch chan T) {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		default:
			return
		}
	}
}
