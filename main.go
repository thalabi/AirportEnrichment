package main

import (
	"encoding/csv"
	"fmt"
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

	filehelper.DownloadFile(prop.GetString("airports-url", ""), prop.GetString("airports-filename", ""))
	rows := filehelper.ReadFile(prop.GetString("airports-filename", ""))
	//rows := readAirportFile()
	log.Println("# of columns:", len(rows[0]))
	log.Println("# of rows:", len(rows))
	columnNameToIndex := buildColumnNameMap(rows)
	fmt.Println(len(rows[0]))
	for i, colName := range rows[0] {
		columnNameToIndex[colName] = i
		log.Println(colName)
	}
	for i, row := range rows[1:] {
		if i < 5 {
			log.Println("ident: ", row[columnNameToIndex["ident"]], "name: ", row[columnNameToIndex["name"]], "latitude_deg: ", row[columnNameToIndex["latitude_deg"]], "longitude_deg: ", row[columnNameToIndex["longitude_deg"]], "iso_country: ", row[columnNameToIndex["iso_country"]], "iso_region: ", row[columnNameToIndex["iso_region"]])
		}
	}
	model.ClearRows()
	model.PersistRows(columnNameToIndex, rows[1:])
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
		log.Println(colName)
	}
	return columnNameToIndex
}
