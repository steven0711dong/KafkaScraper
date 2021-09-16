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

type PerSource struct {
	data          map[string](map[int](map[string]float64))
	Mu            sync.Mutex
	totalMessages int
	dups          int
	nonTwoHundred int
	timestamp     time.Time
}

var result struct {
	Mu   sync.Mutex
	data map[string]*PerSource
}

func main() {
	result.data = make(map[string]*PerSource)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stats" {
			//if r.URL.Query().Get("reset") != "" eg. ur := "https://hello.google.com/stat?name=Branch&products=[Journeys,Email,Universal%20Ads]"
			result.Mu.Lock()
			res := ""
			for source, stats := range result.data {
				res += fmt.Sprintf("source %s received: %d messages, %d are dups and %d non 200s.\n", source, stats.totalMessages, stats.dups, stats.nonTwoHundred)
			}
			result.Mu.Unlock()
			w.Write([]byte(res))
			return
		} else if r.URL.Path == "/reset" {
			result.Mu.Lock()
			result.data = make(map[string]*PerSource)
			result.Mu.Unlock()
			w.Write([]byte(fmt.Sprintf("Map has been cleared: %d", len(result.data))))
			return
		}

		allRecords := []string{}
		json.NewDecoder(r.Body).Decode(&allRecords)
		for _, e := range allRecords {
			s := strings.Split(e, "/")
			// source/topic/partition/offset/statuscoode
			p, err := strconv.Atoi(s[2])
			if err != nil {
				fmt.Println("An error happened when converting partition to integer")
			}
			so := s[0]
			result.Mu.Lock()
			perSource, _ := result.data[so]
			if perSource == nil {
				perSource = &PerSource{data: make(map[string]map[int]map[string]float64)}
				result.data[so] = perSource
			}
			result.Mu.Unlock()
			appendToData(s[1], p, s[3], 0, perSource, s[4])
		}

		w.Write([]byte("Scraper has received kafka msg stats from sink."))
	})
	fmt.Print("Listening for events on port 8080\n")
	http.ListenAndServe(":8080", nil)
}

func appendToData(topic string, partition int, offset string, t float64, perSource *PerSource, statusCode string) {
	perSource.Mu.Lock()
	if statusCode != "200" {
		perSource.nonTwoHundred++
	}
	if partitions, found := perSource.data[topic]; found {
		if offsets, ok := partitions[partition]; ok {
			if _, ok := offsets[offset]; ok {
				perSource.dups++
			} else {
				offsets[offset] = t
				perSource.totalMessages++
			}
		} else {
			newSetOfOffsets := make(map[string]float64)
			newSetOfOffsets[offset] = t
			partitions[partition] = newSetOfOffsets
			perSource.totalMessages++
		}
	} else {
		newSetOfOffsets := make(map[string]float64)
		newSetOfOffsets[offset] = t
		newPartitions := make(map[int](map[string]float64))
		newPartitions[partition] = newSetOfOffsets
		perSource.data[topic] = newPartitions
		perSource.totalMessages++
	}
	perSource.Mu.Unlock()
}
