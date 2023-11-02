
import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

const TestTS = 1679912278000

func TestKafkaClient_ConsumeTopic(t *testing.T) {
	addr := []string{"ip:9092"}
	cfg := sarama.NewConfig()
	//cfg.Consumer.Return.Errors = true
	kc, err := NewKafkaClient(addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := kc.NewTopicConsumers([]string{"topicName"}, TestTS); err != nil {
		t.Fatal(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		kc.ConsumeTopic(GoodsEsP12)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 10)
		kc.StopConsumeTopic(GoodsEsP12)
	}()
	wg.Wait()
}


func TestConsumePartition(t *testing.T) {
	addr := []string{"10.216.10.155:9092"}
	kc, err := NewKafkaClient(addr, sarama.NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	pt := int32(0)
	if _, err := kc.NewPartitionConsumer("topic", pt, -2); err != nil {
		t.Fatal(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := kc.ConsumePartition("topic", pt); err != nil {
			fmt.Println(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 2)
		kc.StopConsumeTopic("topic")
	}()
	wg.Wait()
}
