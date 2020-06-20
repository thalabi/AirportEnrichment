package main

import (
	"log"

	filehelper "github.com/thalabi/AirportEnrichment/file-helper"

	_ "github.com/godror/godror"
	"github.com/magiconair/properties"
	"github.com/thalabi/AirportEnrichment/model"
)

func main() {
	prop := properties.MustLoadFile("application.properties", properties.UTF8)
	airportsURL := prop.GetString("airports-url", "")
	airportsFilename := prop.GetString("airports-filename", "airports.csv")
	model.InitDB(prop)

	log.Println("Downloading file ...")
	filehelper.DownloadFile(airportsURL, airportsFilename)

	log.Println("Reading file ...")
	rows := filehelper.ReadCsvFile(airportsFilename)
	log.Printf("Read %v lines", len(rows))

	columnNameToIndex := buildColumnNameMap(rows)

	log.Println("Clearing airport_enrichment table ...")
	model.ClearRows()

	log.Println("Inserting into airport_enrichment table ...")
	model.PersistRows(columnNameToIndex, rows[1:])

	log.Println("Enriching airport table ...")
	model.UpdateAirportTable()
}

func buildColumnNameMap(rows [][]string) map[string]int {
	var columnNameToIndex = make(map[string]int)
	for i, colName := range rows[0] {
		columnNameToIndex[colName] = i
	}
	return columnNameToIndex
}
