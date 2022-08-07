package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
)

type FileInfo struct {
	Length      int64  `json:"length"`
	Name        string `json:"name"`
	PieceLength int64  `json:"pieceLength"`
}

type TorrentFile struct {
	Id           string   `json:"id"`
	Server       string   `json:"server"`
	Announce     string   `json:"announce"`
	Comment      string   `json:"comment"`
	CreationDate int64    `json:"creationDate"`
	Info         FileInfo `json:"info"`
}

type TorrentMessage struct {
	Offset     int64
	PartBuffer []byte
}

func NewTorrentMessage(offset int64, partBuffer []byte) *TorrentMessage {
	return &TorrentMessage{offset, partBuffer}
}

var wg = sync.WaitGroup{}
var ctx = context.Background()

const fileChunk = 1 * (1 << 20)

var (
	file    *os.File
	torFile *TorrentFile
	rdb     *redis.Client
)

func init() {
	// set up torrent file to be unmarshalled
	filePath := os.Args[1]
	var err error
	file, err = os.Open(filePath)
	catch(err)

	// torrent file struct
	fileInfo, _ := file.Stat()
	torFileBytes := make([]byte, fileInfo.Size())
	file.Read(torFileBytes)
	torFile = new(TorrentFile)
	json.Unmarshal(torFileBytes, torFile)

	//connect to redis
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func catch(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	err := rdb.Publish(ctx, torrentOpenedChannel(torFile.Id), "hello").Err() // o is offset
	catch(err)

	pubsub := rdb.Subscribe(ctx, torrentDownloadingChannel(torFile.Id))
	ch := pubsub.Channel()

	// create the file
	mediaFile, err := os.Create(torFile.Info.Name)
	catch(err)

	wg.Add(1)
	go func(ch <-chan *redis.Message) {
		defer wg.Done()
		totalParts := uint64(math.Ceil(float64(torFile.Info.Length) / float64(torFile.Info.PieceLength)))
		var count uint64 = 0
		// something wrong with this count shit

		for msg := range ch {
			count++
			tm := new(TorrentMessage)

			err = json.Unmarshal([]byte(msg.Payload), tm)
			catch(err)

			mediaFile.WriteAt(tm.PartBuffer, tm.Offset)

			if count >= totalParts {
				return
			}
		}
	}(ch)
	wg.Wait()

	fmt.Println("done")
	fmt.Println("would you like to continue seeding ? (y/n)")

	reader := bufio.NewReader(os.Stdin)
	message, _ := reader.ReadString('\n')

	if message != "y\n" {
		pubsub.Close()
		return
	}

	wg.Add(1)
	url := fmt.Sprintf("%s/add_peer?fId=%s", torFile.Server, torFile.Id)

	resp, err := http.Get(url)
	catch(err)

	body, err := ioutil.ReadAll(resp.Body)
	catch(err)

	channel := string(body)
	pubsub = rdb.Subscribe(ctx, channel)
	ch = pubsub.Channel()
	go func(ch <-chan *redis.Message) {
		for msg := range ch {
			fmt.Println(msg.Payload, msg.Channel)

			totalParts := uint64(math.Ceil(float64(torFile.Info.Length) / float64(torFile.Info.PieceLength)))
			for i := uint64(0); i < totalParts; i++ {
				var offset = int64(i * fileChunk)
				go sendDataPackets(offset, torFile.Id, mediaFile)
			}
		}
	}(ch)

	wg.Wait()
}

func sendDataPackets(offset int64, fileId string, file *os.File) {
	partBuffer := make([]byte, fileChunk)
	file.ReadAt(partBuffer, offset)
	message := NewTorrentMessage(offset, partBuffer)
	marshal, err := json.Marshal(message)
	catch(err)

	err = rdb.Publish(ctx, torrentDownloadingChannel(fileId), marshal).Err()
	catch(err)
}

func torrentOpenedChannel(id string) string {
	return fmt.Sprintf("torrent:opened:%s", id)
}

func torrentDownloadingChannel(fId string) string {
	return fmt.Sprintf("torrent:downloading:%s", fId)
}
