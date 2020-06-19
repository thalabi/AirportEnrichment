package model

import (
	"log"
	"strconv"

	"gopkg.in/guregu/null.v3"
)

// AirportEnrichment represents a row in table airport_enrichment
type AirportEnrichment struct {
	ID               null.Int `db:"ID"`
	Ident            null.String
	Type             null.String
	Name             null.String
	LatitudeDeg      null.Float
	LongitudeDeg     null.Float
	ElevationFt      null.Int
	Continent        null.String
	IsoCountry       null.String
	IsoRegion        null.String
	Municipality     null.String
	ScheduledService null.String
	GpsCode          null.String
	IataCode         null.String
	LocalCode        null.String
	HomeLink         null.String
	WikipediaLink    null.String
	Keywords         null.String
}

// ClearRows clears table rows
func ClearRows() {
	sqlStatement := "truncate table airport_enrichment reuse storage"
	_, error := Db.Exec(sqlStatement)
	if error != nil {
		log.Println(error)
		log.Fatal("Failed to execute sql ", sqlStatement)
	}

}

// PersistRows to table airport_enrichment
func PersistRows(columnNameToIndex map[string]int, rows [][]string) {

	sqlStatement := "insert into airport_enrichment(id, ident, type, name, latitude_deg, longitude_deg, elevation_ft, continent, iso_country, iso_region, municipality, scheduled_service, gps_code, iata_code, local_code, home_link, wikipedia_link, keywords) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18)"
	tx := Db.MustBegin()

	insertCount := 0
	for i, row := range rows {
		airportEnrichment := buildRow(columnNameToIndex, i, row)
		if airportEnrichment == nil {
			continue
		}

		result := tx.MustExec(sqlStatement, airportEnrichment.ID, airportEnrichment.Ident, airportEnrichment.Type, airportEnrichment.Name, airportEnrichment.LatitudeDeg, airportEnrichment.LongitudeDeg, airportEnrichment.ElevationFt, airportEnrichment.Continent, airportEnrichment.IsoCountry, airportEnrichment.IsoRegion, airportEnrichment.Municipality, airportEnrichment.ScheduledService, airportEnrichment.GpsCode, airportEnrichment.IataCode, airportEnrichment.LocalCode, airportEnrichment.HomeLink, airportEnrichment.WikipediaLink, airportEnrichment.Keywords)
		count, error := result.RowsAffected()
		if error != nil {
			log.Println(error)
			log.Println("Failed to execute sql ", sqlStatement)
		}
		if count != 1 {
			log.Println("Expected insert count to be 1, got ", count)
		}

		insertCount++

		if (i+1)%1000 == 0 {
			tx.Commit()
			tx = Db.MustBegin()
			log.Println("Inserted row # ", insertCount)
		}
	}
	tx.Commit()
	log.Println("Inserted row # ", insertCount)
}

// UpdateAirportTable updates airport from airport_enrichment table
func UpdateAirportTable() {
	sqlStatement := `
	merge into airport_2 target
	using airport_enrichment source
	on (target.identifier = source.ident)
		when matched then
			update set target.name = upper(source.name), target.latitude = source.latitude_deg, target.longitude = source.longitude_deg,
						target.city = upper(source.municipality), target.country = upper(source.iso_country),
							target.modified = sysdate, target.version = target.version + 1
			where nvl(target.name,' ') != upper(source.name) or target.latitude != source.latitude_deg or target.longitude != source.longitude_deg or
					nvl(target.city,' ') != upper(source.municipality) or nvl(target.country,' ') != upper(source.iso_country)
		when not matched then
			insert (target.id, target.identifier, target.name, target.latitude, target.longitude,
						target.city, target.country, 
							target.created,	target.modified , target.version)
			values (airport_seq.nextval, source.ident, upper(source.name), source.latitude_deg , source.longitude_deg,
							upper(source.municipality), upper(source.iso_country),
							   sysdate, sysdate, 0)	
	`
	result, error := Db.Exec(sqlStatement)
	if error != nil {
		log.Println(error)
		log.Fatal("Failed to execute sql ", sqlStatement)
	}
	count, _ := result.RowsAffected()
	log.Println("Rows affected: ", count)
}

func buildRow(columnNameToIndex map[string]int, i int, row []string) *AirportEnrichment {
	var airportEnrichment AirportEnrichment
	id, error := strconv.Atoi(row[columnNameToIndex["id"]])
	if error != nil {
		log.Printf("Row index: %v, Unable to parse id: %v ", i, row[columnNameToIndex["id"]])
		return nil
	}
	airportEnrichment.ID = null.IntFrom(int64(id))
	airportEnrichment.Ident = null.StringFrom(row[columnNameToIndex["ident"]])
	airportEnrichment.Type = null.StringFrom(row[columnNameToIndex["type"]])
	airportEnrichment.Name = null.StringFrom(row[columnNameToIndex["name"]])
	latitudeDeg, error := strconv.ParseFloat(row[columnNameToIndex["latitude_deg"]], 64)
	if error != nil {
		if row[columnNameToIndex["latitude_deg"]] != "" {
			log.Printf("Row index: %v, Unable to parse latitude_deg: %v, setting to null ", i, row[columnNameToIndex["latitude_deg"]])
		}
		airportEnrichment.LatitudeDeg = null.FloatFromPtr(nil)
	} else {
		airportEnrichment.LatitudeDeg = null.FloatFrom(latitudeDeg)
	}
	longitudeDeg, error := strconv.ParseFloat(row[columnNameToIndex["longitude_deg"]], 64)
	if error != nil {
		if row[columnNameToIndex["longitude_deg"]] != "" {
			log.Printf("Row index: %v, Unable to parse longitude_deg: %v ", i, row[columnNameToIndex["longitude_deg"]])
		}
		airportEnrichment.LongitudeDeg = null.FloatFromPtr(nil)
	} else {
		airportEnrichment.LongitudeDeg = null.FloatFrom(longitudeDeg)
	}
	elevationFt, error := strconv.Atoi(row[columnNameToIndex["elevation_ft"]])
	if error != nil {
		if row[columnNameToIndex["elevation_ft"]] != "" {
			log.Printf("Row index: %v, Unable to parse int: %v, setting to null", i, row[columnNameToIndex["elevation_ft"]])
		}
		airportEnrichment.ElevationFt = null.IntFromPtr(nil)
	} else {
		airportEnrichment.ElevationFt = null.IntFrom(int64(elevationFt))
	}
	airportEnrichment.Continent = null.StringFrom(row[columnNameToIndex["continent"]])
	airportEnrichment.IsoCountry = null.StringFrom(row[columnNameToIndex["iso_country"]])
	airportEnrichment.IsoRegion = null.StringFrom(row[columnNameToIndex["iso_region"]])
	airportEnrichment.Municipality = null.StringFrom(row[columnNameToIndex["municipality"]])
	airportEnrichment.ScheduledService = null.StringFrom(row[columnNameToIndex["scheduled_service"]])
	airportEnrichment.GpsCode = null.StringFrom(row[columnNameToIndex["gps_code"]])
	airportEnrichment.IataCode = null.StringFrom(row[columnNameToIndex["iata_code"]])
	airportEnrichment.LocalCode = null.StringFrom(row[columnNameToIndex["local_code"]])
	airportEnrichment.HomeLink = null.StringFrom(row[columnNameToIndex["home_link"]])
	airportEnrichment.WikipediaLink = null.StringFrom(row[columnNameToIndex["wikipedia_link"]])
	airportEnrichment.Keywords = null.StringFrom(row[columnNameToIndex["keywords"]])

	return &airportEnrichment
}
