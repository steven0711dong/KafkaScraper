package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var totalMsg int
var totalBurst int
var totalSendDur int
var totalRestDur int
var s []string
var dups []string
var missingProduced []string

type Statistics struct {
	Host     string
	Received int
	Dups     int
}

var stats struct {
	data map[string]*Statistics
	Mu   sync.Mutex
}

type msg struct {
	data          map[string](map[int](map[string]time.Time))
	Mu            sync.Mutex
	totalMessages int
	timestamp     time.Time
}

var producedMsg struct {
	data          map[string](map[int](map[string]time.Time))
	Mu            sync.Mutex
	totalMessages int
	timestamp     time.Time
}

var receivedMsg struct {
	data          map[string](map[int](map[string]float64))
	Mu            sync.Mutex
	totalMessages int
	dups          int
	timestamp     time.Time
}

var result struct {
	data          map[string](map[int](map[string]time.Duration))
	Mu            sync.Mutex
	totalMessages int
	timestamp     time.Time
}

func main() {
	result.data = make(map[string](map[int](map[string]time.Duration)))
	producedMsg.data = make(map[string](map[int](map[string]time.Time)))
	receivedMsg.data = make(map[string](map[int](map[string]float64)))
	stats.data = make(map[string]*Statistics)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stats" {
			//if r.URL.Query().Get("reset") != "" eg. ur := "https://hello.google.com/stat?name=Branch&products=[Journeys,Email,Universal%20Ads]"
			receivedMsg.Mu.Lock()
			res := fmt.Sprintf("received: %d dups: %d ", receivedMsg.totalMessages, receivedMsg.dups)
			receivedMsg.Mu.Unlock()
			w.Write([]byte(res))
			return
		} else if r.URL.Path == "/reset" {
			receivedMsg.Mu.Lock()
			receivedMsg.data = make(map[string](map[int](map[string]float64)))
			receivedMsg.dups = 0
			receivedMsg.totalMessages = 0
			receivedMsg.Mu.Unlock()
			w.Write([]byte(fmt.Sprintf("Map has been cleared: %d", len(stats.data))))
			return
		}

		m := []string{}
		json.NewDecoder(r.Body).Decode(&m)
		for _, e := range m {
			s := strings.Split(e, "/")
			p, err := strconv.Atoi(s[1])
			if err != nil {
				fmt.Println("An error happened when converting partition to integer")
			}
			appendToData(s[0], p, s[2], 0)
		}

		w.Write([]byte("Scraper has received kafka msg stats from sink."))
	})
	fmt.Print("Listening for events on port 8080\n")
	http.ListenAndServe(":8080", nil)
}

func appendToData(topic string, partition int, offset string, t float64) {
	receivedMsg.Mu.Lock()
	if partitions, found := receivedMsg.data[topic]; found {
		if offsets, ok := partitions[partition]; ok {
			if _, ok := offsets[offset]; ok {
				receivedMsg.dups++
			} else {
				offsets[offset] = t
				receivedMsg.totalMessages++
			}
		} else {
			newSetOfOffsets := make(map[string]float64)
			newSetOfOffsets[offset] = t
			partitions[partition] = newSetOfOffsets
			receivedMsg.totalMessages++
		}
	} else {
		newSetOfOffsets := make(map[string]float64)
		newSetOfOffsets[offset] = t
		newPartitions := make(map[int](map[string]float64))
		newPartitions[partition] = newSetOfOffsets
		receivedMsg.data[topic] = newPartitions
		receivedMsg.totalMessages++
	}
	receivedMsg.Mu.Unlock()
}

func displayReceivedMessages(x string) string {
	res := []string{x}
	for topic, partitions := range receivedMsg.data {
		for partition, offsets := range partitions {
			for offset, dur := range offsets {
				res = append(res, fmt.Sprintf("Topic: %s => Partition: %d => Offset: %+v => e2e delay: %d seconds\n", topic, partition, offset, int(dur)))
			}
		}
	}
	return strings.Join(res[:], "")
}

// func calculateResult() {
// 	m := map[string]map[int]map[string]time.Time{
// 		"topic1": {
// 			1: {
// 				"fakeoffset1":  time.Now().Add(time.Second * 23),
// 				"fakeoffset2":  time.Now().Add(time.Second * 12),
// 				"fakeoffset10": time.Now().Add(time.Second * 2),
// 			},
// 			2: {
// 				"fakeoffset1": time.Now().Add(time.Second * 1),
// 				"fakeoffset2": time.Now().Add(time.Millisecond * 23),
// 				"fakeoffset5": time.Now().Add(time.Second * 12),
// 			},
// 		},
// 	}

// 	producedMsg.Mu.Lock()
// 	for topic, partitions := range m {
// 		for partition, offsets := range partitions {
// 			for offset, timestamp := range offsets {
// 				//for this topic, partition and offset, find it in producedMsg
// 				if _, ok := producedMsg.data[topic]; !ok {
// 					if missingProduced == nil {
// 						missingProduced = []string{fmt.Sprintf("topic:%s-partition:%d-offset:%s", topic, partition, offset)}
// 					} else {
// 						missingProduced = append(missingProduced, fmt.Sprintf("topic:%s-partition:%d-offset:%s", topic, partition, offset))
// 					}
// 					continue
// 				}
// 				if _, ok := producedMsg.data[topic][partition]; !ok {
// 					if missingProduced == nil {
// 						missingProduced = []string{fmt.Sprintf("topic:%s-partition:%d-offset:%s", topic, partition, offset)}
// 					} else {
// 						missingProduced = append(missingProduced, fmt.Sprintf("topic:%s-partition:%d-offset:%s", topic, partition, offset))
// 					}
// 					continue
// 				}
// 				ts, ok := producedMsg.data[topic][partition][offset]
// 				if !ok {
// 					if missingProduced == nil {
// 						missingProduced = []string{fmt.Sprintf("topic:%s-partition:%d-offset:%s", topic, partition, offset)}
// 					} else {
// 						missingProduced = append(missingProduced, fmt.Sprintf("topic:%s-partition:%d-offset:%s", topic, partition, offset))
// 					}
// 					continue
// 				}
// 				duration := timestamp.Sub(ts)
// 				o := map[string]time.Duration{offset: duration}
// 				p := map[int]map[string]time.Duration{partition: o}
// 				if ps, ok := result.data[topic]; ok {
// 					if os, ok := ps[partition]; ok {
// 						if _, ok := os[offset]; ok {
// 							if dups == nil {
// 								dups = []string{fmt.Sprintf("t:%s-p:%d-o:%s|", topic, partition, offset)}
// 							} else {
// 								dups = append(dups, fmt.Sprintf("t:%s-p:%d-o:%s|", topic, partition, offset))
// 							}
// 						} else {
// 							os[offset] = duration
// 						}
// 					} else {
// 						ps[partition] = o
// 					}
// 				} else {
// 					result.data[topic] = p
// 				}
// 			}
// 		}
// 	}
// 	producedMsg.Mu.Unlock()
// }

// func missedMsg() int {
// 	numMsg := 0
// 	for _, partitions := range producedMsg.data {
// 		for _, offsets := range partitions {
// 			numMsg += len(offsets)
// 		}
// 	}
// 	for _, partitions := range result.data {
// 		for _, offsets := range partitions {
// 			numMsg -= len(offsets)
// 		}
// 	}
// 	return numMsg
// }

// func appendToProducedData(topic string, partition int, offset string, timestamp time.Time) {
// 	producedMsg.Mu.Lock()
// 	if partitions, found := producedMsg.data[topic]; found {
// 		if offsets, ok := partitions[partition]; ok {
// 			if _, ok := offsets[offset]; ok {
// 				errStr := fmt.Sprintf("topic:%s-partition:%d-offset:%s", topic, partition, offset)
// 				offsets[errStr] = timestamp
// 			} else {
// 				offsets[offset] = timestamp
// 			}
// 		} else {
// 			newSetOfOffsets := make(map[string]time.Time)
// 			newSetOfOffsets[offset] = timestamp
// 			partitions[partition] = newSetOfOffsets
// 		}
// 	} else {
// 		newSetOfOffsets := make(map[string]time.Time)
// 		newSetOfOffsets[offset] = timestamp
// 		newPartitions := make(map[int](map[string]time.Time))
// 		newPartitions[partition] = newSetOfOffsets
// 		producedMsg.data[topic] = newPartitions
// 	}
// 	producedMsg.timestamp = timestamp
// 	producedMsg.Mu.Unlock()
// }

// func display() string {
// 	format := `
// 	-------
// 	Missed messages: %d
// 	Dups received: %v
// 	Missing produced messages: %v
// 	-------

// 	`
// 	res := []string{fmt.Sprintf(format, missedMsg(), dups, missingProduced)}
// 	for topic, partitions := range result.data {
// 		for partition, offsets := range partitions {
// 			for offset, dur := range offsets {
// 				res = append(res, fmt.Sprintf("Topic: %s => Partition: %d => Offset: %+v => e2e delay: %d\n", topic, partition, offset, int(dur.Seconds())))
// 			}
// 		}
// 	}
// 	return strings.Join(res[:], "")
// }
