package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/samber/lo"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const fileChunk = 1 * (1 << 20)

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

func catch(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func NewTorrentMessage(offset int64, partBuffer []byte) *TorrentMessage {
	return &TorrentMessage{offset, partBuffer}
}

func NewTorrent(info os.FileInfo, comment ...string) *TorrentFile {
	return &TorrentFile{
		Id:           utils.UUIDv4(),
		Server:       "http://localhost:4000",
		Announce:     "http://localhost:6379",
		Comment:      comment[0],
		CreationDate: time.Now().UnixMilli(),
		Info: FileInfo{
			Length:      info.Size(),
			Name:        info.Name(),
			PieceLength: fileChunk,
		},
	}
}

var wg = sync.WaitGroup{}
var ctx = context.Background()

var (
	rdb *redis.Client
)

func main() {
	rdb = redis.NewClient(&redis.Options{
		Addr:        "localhost:6379",
		Password:    "", // no password set
		DB:          0,  // use default DB
		PoolTimeout: 1 * time.Hour,
	})

	// going to get the file later by id
	file := openFile()
	defer file.Close()
	pubsub := rdb.PSubscribe(ctx, "torrent:opened:*")

	ch := pubsub.Channel()
	wg.Add(1)
	go receiveMessages(file, ch)

	fileInfo, _ := file.Stat()

	app := fiber.New(fiber.Config{
		AppName: "go-torrent",
	})

	app.Use(cors.New())

	app.Get("/download_torrent", func(c *fiber.Ctx) error {

		// return c.Download("./media/testVideo.mp4", "test.mp4")

		torFile := NewTorrent(fileInfo, "first testing Video")

		marshal, err := json.Marshal(torFile)
		if err != nil {
			return err
		}

		filename := "./internal/media/test_torrent"
		torrent, err := os.Create(filename)

		torrent.Write(marshal)

		return c.Download(filename, "testTorrent.json")
	})

	app.Get("/add_peer", func(c *fiber.Ctx) error {
		fileId := c.Query("fId")
		peer := peerChannel(fileId)
		rdb.SAdd(ctx, "torrent:peers:"+fileId, peer)

		return c.SendString(peer)
	})

	log.Fatal(app.Listen(":4000"))

	wg.Wait()
}

func receiveMessages(file *os.File, ch <-chan *redis.Message) {
	defer wg.Done()
	for msg := range ch {
		fileId := strings.SplitAfterN(msg.Channel, ":", 3)[2]
		// get file by id right here
		peers, err := rdb.SMembers(ctx, "torrent:peers:"+fileId).Result()
		catch(err)

		lo.ForEach(peers, func(peer string, _ int) {
			err = rdb.Publish(ctx, peer, torrentDownloadingChannel(fileId)).Err()
			catch(err)
		})

		fileInfo, _ := file.Stat()
		fileSize := fileInfo.Size()
		totalParts := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

		for i := uint64(0); i < totalParts; i++ {
			var offset = int64(i * fileChunk)
			go sendDataPackets(offset, fileId, file)
		}

	}
}

func openFile() *os.File {
	file, err := os.Open("./internal/media/testVideo.mp4")
	if err != nil {
		log.Fatal(err)
	}

	return file
}

func torrentDownloadingChannel(fId string) string {
	return fmt.Sprintf("torrent:downloading:%s", fId)
}

func peerChannel(fId string) string {
	b := make([]byte, 3)
	rand.Read(b)
	return fmt.Sprintf("torrent:peer:%s:%s", fId, hex.EncodeToString(b))
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

func splitMediaToBuffer(c *fiber.Ctx) {
	file, err := os.Open("./internal/media/testVideo.mp4")
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("recovered")
		}
	}()
	if err != nil {
		panic(err)
		// os.exit
	}

	defer file.Close()

	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()

	fmt.Println(fileChunk)

	totalParts := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Println(totalParts)

	for i := uint64(0); i < totalParts; i++ {
		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		//fileName := "./internal/bucket/some-bigfile_#" + strconv.FormatUint(i, 10)
		//_, err := os.Create(fileName)
		//
		//if err != nil {
		//	fmt.Println(err)
		//	os.Exit(1)
		//}
		//
		//// write/save buffer to disk
		//ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)
		//
		//fmt.Println("Split to : ", fileName)
	}
}

func joinBytesToMedia() {
	newFile, err := os.Create("final.mp4")
	if err != nil {
		log.Fatal(err)
	}

	dirEntries, err := os.ReadDir("./internal/bucket")
	if err != nil {
		log.Fatal(err)
	}

	var offset int64 = 0
	totalParts := 70

	for i := 0; i < totalParts; i++ {
		entry, _ := lo.Find(dirEntries, func(entry os.DirEntry) bool {
			return strings.EqualFold(entry.Name(), "some-bigfile_#"+strconv.Itoa(i))
		})

		if entry == nil {
			continue
		}

		file, err := os.Open("./internal/bucket/" + entry.Name())
		if err != nil {
			log.Fatal(err)
		}

		fileInfo, _ := file.Stat()
		partBytes := make([]byte, fileInfo.Size())

		file.Read(partBytes)

		at, err := newFile.WriteAt(partBytes, offset)
		if err != nil {
			log.Fatal(err)
		}

		offset += int64(at)

		//newVideoInfo, _ := newFile.Stat()

		//fmt.Println(newVideoInfo.Size())

		file.Close()
	}

}
