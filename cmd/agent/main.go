package main

import (
	"flag"
	"log"

	"github.com/techinyus/storiq-agent/internal/db"
	"github.com/techinyus/storiq-agent/internal/ws"
)

const (
	serverURL    = "wss://agent.storiq.com"
	agentVersion = "1.0.0"
)

func main() {
	secret := flag.String("secret", "", "shared secret used to authenticate with the Storiq backend")
	dbURL := flag.String("db-url", "", "PostgreSQL connection string for the local database")
	flag.Parse()

	if *secret == "" {
		log.Fatal("[agent] --secret is required")
	}
	if *dbURL == "" {
		log.Fatal("[agent] --db-url is required")
	}

	database, err := db.Connect(*dbURL)
	if err != nil {
		log.Fatalf("[agent] failed to connect to database: %v", err)
	}
	defer database.Close()

	client := ws.NewClient(serverURL, *secret, agentVersion, database)
	client.Run()
}
