package model

import (
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/magiconair/properties"
)

// Db handle exported
var Db *sqlx.DB

// InitDB func
func InitDB(prop *properties.Properties) {
	var err error
	Db, err = sqlx.Connect("postgres", "user="+prop.GetString("username", "")+" password="+prop.GetString("password", "")+" dbname=postgres sslmode=disable host="+prop.GetString("host", "")+" port="+prop.GetString("port", ""))
	if err != nil {
		log.Panic(err)
	}

	if err = Db.Ping(); err != nil {
		log.Panic(err)
	}
}
