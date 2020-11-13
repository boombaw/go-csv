// main.go

package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/parnurzeal/gorequest"
	"github.com/sirupsen/logrus"
)

type Response struct {
	Prov, Kota, Kec, Kel, KodePos string
}

// var totalWorker = os.Getenv("WORKER")

var dataHeaders = make([]string, 0)
var g []Response

func init() {
	_ = godotenv.Load()
}

func main() {
	start := time.Now()
	fmt.Println("Halo")

	csvReader, csvFile, err := openCsvFile()
	if err != nil {
		logrus.Fatal(err.Error())
	}
	defer csvFile.Close()

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorkers(jobs, wg)
	readCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)

	wg.Wait()

	createCsvFile()

	duration := time.Since(start)
	fmt.Println("done in", duration)
}

func createCsvFile() {
	logrus.Info("Create CSV FILE")
	file, err := os.OpenFile(os.Getenv("CSV_EXPORT_PATH"), os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()

	if err != nil {
		os.Exit(1)
	}

	header := []string{"Provinsi", "Kota/Kab", "Kec", "Kel", "Kodepos"}
	csvWriter := csv.NewWriter(file)
	csvWriter.Write(header)
	for _, v := range g {
		str := []string{v.Prov, v.Kota, v.Kec, v.Kel, v.KodePos}
		csvWriter.Write(str)
	}
	csvWriter.Flush()
}

func openCsvFile() (*csv.Reader, *os.File, error) {
	logrus.Info("Open CSV FILE")

	file, err := os.Open(os.Getenv("CSV_PATH"))
	if err != nil {
		logrus.Error(err)
		return nil, nil, err
	}

	reader := csv.NewReader(file)
	return reader, file, nil
}

func dispatchWorkers(jobs <-chan []interface{}, wg *sync.WaitGroup) {
	totalWorker, _ := strconv.ParseInt(os.Getenv("WORKER"), 10, 64)
	for i := 0; i <= int(totalWorker); i++ {
		go func(i int, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				doTheJob(i, counter, job)
				wg.Done()
				counter++
			}
		}(i, jobs, wg)
	}
}

func readCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(dataHeaders) == 0 {
			dataHeaders = row
			continue
		}

		rowOrdered := make([]interface{}, 0)
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}

func doTheJob(i, counter int, values []interface{}) {

	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			lat := values[0].(string)
			long := values[1].(string)
			_, body, _ := gorequest.New().Get(os.Getenv("MAPBOX_URL") + long + "," + lat + ".json?types=poi&access_token=" + os.Getenv("MAPBOX_TOKEN")).End()

			var dat map[string]interface{}
			if err := json.Unmarshal([]byte(body), &dat); err != nil {
				logrus.Errorf("Cannot unmarshal string %v\n", err)
			}
			c := dat["features"].([]interface{})[0].(map[string]interface{})
			ctx := c["context"].([]interface{})

			var r Response
			r.Kel = ctx[0].(map[string]interface{})["text"].(string)
			r.KodePos = ctx[1].(map[string]interface{})["text"].(string)
			r.Kec = ctx[2].(map[string]interface{})["text"].(string)
			r.Kota = ctx[3].(map[string]interface{})["text"].(string)
			r.Prov = ctx[3].(map[string]interface{})["text"].(string)

			g = append(g, r)
			counter++
		}(&outerError)

		if outerError == nil {
			break
		}
	}

	if counter%100 == 0 {
		logrus.Infoln("=> worker", i, "inserted", counter, "data")
	}
}
