package main

// Sample file for test: https://drive.google.com/file/d/1DFkJdX5UTnB_xL7g8xwkkdE8BxdurAhN/view?usp=sharing
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

type Geocode struct {
	Lat  string `json:"lat"`
	Long string `json:"long"`
}

type Response struct {
	Prov, Kota, Kec, Kel, KodePos string
}

var list []Response
var mu sync.Mutex

func init() {
	_ = godotenv.Load()
}

func main() {
	f1, _ := os.Open(os.Getenv("CSV_PATH"))
	defer f1.Close()

	ts := time.Now()
	te := time.Now().Sub(ts)

	ts1 := time.Now()
	concuRSwWP(f1)
	te1 := time.Now().Sub(ts1)

	createCsvFile()

	// Read and Set to a map
	fmt.Println("\nEND Basic: ", te)
	fmt.Println("END Concu: ", te1)
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
	for _, v := range list {
		str := []string{v.Prov, v.Kota, v.Kec, v.Kel, v.KodePos}
		csvWriter.Write(str)
	}
	csvWriter.Flush()
}

// with Worker pools
func concuRSwWP(f *os.File) {
	logrus.Infoln("Opening CSV File")
	fcsv := csv.NewReader(f)
	fcsv.FieldsPerRecord = -1
	rs := make([]*Geocode, 0)
	numWps, _ := strconv.Atoi(os.Getenv("WORKER"))
	jobs := make(chan []string, numWps)
	res := make(chan *Geocode)

	var wg sync.WaitGroup
	worker := func(jobs <-chan []string, results chan<- *Geocode) {
		for {
			select {
			case job, ok := <-jobs: // you must check for readable state of the channel.
				if !ok {
					return
				}
				results <- parseStruct(job)
			}
		}
	}

	// init workers
	for w := 0; w < numWps; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(jobs, res)
		}()
	}

	go func() {
		for {
			rStr, err := fcsv.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("ERROR: ", err.Error())
				break
			}
			jobs <- rStr
		}
		close(jobs) // close jobs to signal workers that no more job are incoming.
	}()

	go func() {
		wg.Wait()
		close(res) // when you close(res) it breaks the below loop.
	}()

	i := 0
	for r := range res {
		if i != 0 {
			rs = append(rs, r)
		}
		i++
	}

	for _, v := range rs {
		_, body, _ := gorequest.New().Get(os.Getenv("MAPBOX_URL") + v.Long + "," + v.Lat + ".json?access_token=" + os.Getenv("MAPBOX_TOKEN")).End()

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

		list = append(list, r)

	}

	fmt.Println("Count data ", len(rs))
}

func parseStruct(data []string) *Geocode {
	return &Geocode{
		Lat:  data[0],
		Long: data[1],
	}
}
