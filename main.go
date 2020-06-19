package main

import (
	"encoding/csv"
	"log"
	"os"

	filehelper "github.com/thalabi/AirportEnrichment/file-helper"

	_ "github.com/godror/godror"
	"github.com/magiconair/properties"
	"github.com/thalabi/AirportEnrichment/model"
)

func main() {
	prop := properties.MustLoadFile("application.properties", properties.UTF8)
	model.InitDB(prop)

	log.Println("Downloading file ...")
	filehelper.DownloadFile(prop.GetString("airports-url", ""), prop.GetString("airports-filename", ""))
	log.Println("Reading file ...")
	rows := filehelper.ReadFile(prop.GetString("airports-filename", ""))
	log.Printf("Read %v lines", len(rows))
	columnNameToIndex := buildColumnNameMap(rows)
	log.Println("Clearing airport_enrichment table ...")
	model.ClearRows()
	log.Println("Inserting into airport_enrichment table ...")
	model.PersistRows(columnNameToIndex, rows[1:])
	log.Println("Enriching airport table ...")
	model.UpdateAirportTable()
}

func readAirportFile() [][]string {
	f, err := os.Open("airports.csv")
	if err != nil {
		log.Fatal(err)
	}
	rows, err := csv.NewReader(f).ReadAll()
	f.Close()
	if err != nil {
		log.Fatal(err)
	}
	return rows
}

func buildColumnNameMap(rows [][]string) map[string]int {
	var columnNameToIndex = make(map[string]int)
	for i, colName := range rows[0] {
		columnNameToIndex[colName] = i
	}
	return columnNameToIndex
}
