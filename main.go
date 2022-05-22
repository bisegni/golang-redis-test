package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()
var waitGrp sync.WaitGroup

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		panic(err)
	}
	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := rdb.Get(ctx, "key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}

	for i := 0; i <= 10; i++ {
		newID, perr := produceMessage(
			rdb,
			"testStream",
			map[string]string{"msg": fmt.Sprintf("Message %d", i)},
		)
		if perr != nil {
			panic(perr)
		} else {
			fmt.Println("new id created:", newID)
		}
	}

	createConsumerGroup(rdb, "testStream", "group1")

	consumeEvents(rdb, "testStream", "group1")
}

func createConsumerGroup(c *redis.Client, sName string, gName string) {
	if _, err := c.XGroupCreateMkStream(ctx, sName, gName, "0").Result(); err != nil {
		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			fmt.Printf("Error on create Consumer Group: %v ...\n", gName)
			panic(err)
		}

	}
}

func produceMessage(c *redis.Client, sName string, message interface{}) (string, error) {
	return c.XAdd(ctx,
		&redis.XAddArgs{
			Stream: sName,
			ID:     "*",
			Values: message,
		}).Result()
}

func consumeEvents(c *redis.Client, sName string, gName string) {

	for {
		func() {
			fmt.Println("new round ", time.Now().Format(time.RFC3339))

			streams, err := c.XReadGroup(
				ctx,
				&redis.XReadGroupArgs{
					Streams:  []string{sName, ">"},
					Group:    gName,
					Consumer: "testConsumer",
					Count:    10,
					Block:    0,
				}).Result()

			if err != nil {
				log.Printf("err on consume events: %+v\n", err)
				return
			}

			for _, m := range streams[0].Messages {
				waitGrp.Add(1)
				go processStream(c, sName, gName, m)
			}
			waitGrp.Wait()
		}()
	}

}

func processStream(c *redis.Client, sName string, gName string, m redis.XMessage) {
	defer waitGrp.Done()

	fmt.Println(m.Values)

	//client.XDel(streamName, stream.ID)
	c.XAck(ctx, sName, gName, m.ID)

	//time.Sleep(2 * time.Second)
}
