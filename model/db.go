package model

import (
	"log"

	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/magiconair/properties"
)

// Db handle exported
var Db *sqlx.DB

// InitDB func
func InitDB(prop *properties.Properties) {
	var err error
	host := prop.GetString("host", "")
	port := prop.GetString("port", "")
	username := prop.GetString("username", "")
	password := prop.GetString("password", "")
	Db, err = sqlx.Connect("pgx", "postgres://"+username+":"+password+"@"+host+":"+port+"/"+"postgres")
	if err != nil {
		log.Panic(err)
	}

	if err = Db.Ping(); err != nil {
		log.Panic(err)
	}
}
