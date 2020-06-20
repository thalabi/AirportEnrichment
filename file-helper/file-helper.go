package filehelper

import (
	"encoding/csv"
	"io"
	"log"
	"net/http"
	"os"
)

// DownloadFile will download a url to a local file. It's efficient because it will
// write as it downloads and not load the whole file into memory.
func DownloadFile(url string, filepath string) error {

	const retryTimes = 10
	var response *http.Response
	// Get the data, retrying 10 times
	retryCount := 0
	for {
		var error error
		response, error = http.Get(url)
		if error == nil {
			break
		}
		if retryCount++; retryCount > retryTimes {
			return error
		}
		log.Printf("Retrying http get %v", url)
	}
	defer response.Body.Close()

	// Create the file
	out, error := os.Create(filepath)
	if error != nil {
		return error
	}
	defer out.Close()

	// Write the body to file
	_, error = io.Copy(out, response.Body)
	return error
}

// ReadCsvFile eads file in [][]string
func ReadCsvFile(filepath string) [][]string {
	file, error := os.Open(filepath)
	if error != nil {
		log.Fatal(error)
	}
	rows, error := csv.NewReader(file).ReadAll()
	file.Close()
	if error != nil {
		log.Fatal(error)
	}
	return rows
}
