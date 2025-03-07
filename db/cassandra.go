package db

import (
	"log"
	"time"

	"github.com/gocql/gocql"
)

var Session *gocql.Session

func Initialize() error {
	// When running Go app outside Docker, connect to localhost
	host := "localhost"
	port := 9042

	log.Printf("Connecting to Cassandra at %s:%d", host, port)

	// Create cluster config without keyspace first
	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = time.Second * 15
	cluster.ConnectTimeout = time.Second * 30 // Longer timeout for initial connection
	cluster.Port = port
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        time.Second * 10,
		NumRetries: 5,
	}

	// Try to create a session
	initialSession, err := cluster.CreateSession()
	if err != nil {
		log.Printf("Failed to connect to Cassandra: %v", err)
		return err
	}
	defer initialSession.Close()

	// Create the keyspace if it doesn't exist
	keyspaceName := "testdb"
	createKeyspaceQuery := `
	CREATE KEYSPACE IF NOT EXISTS ` + keyspaceName + ` 
	WITH REPLICATION = { 
		'class' : 'SimpleStrategy', 
		'replication_factor' : 1 
	}
	`

	err = initialSession.Query(createKeyspaceQuery).Exec()
	if err != nil {
		log.Printf("Failed to create keyspace: %v", err)
		return err
	}
	log.Printf("Keyspace %s created or already exists", keyspaceName)

	// Now create a new session with the keyspace
	cluster = gocql.NewCluster(host)
	cluster.Consistency = gocql.Quorum
	cluster.Keyspace = keyspaceName
	cluster.Timeout = time.Second * 15
	cluster.ConnectTimeout = time.Second * 10
	cluster.Port = port

	Session, err = cluster.CreateSession()
	if err != nil {
		log.Printf("Failed to create session with keyspace: %v", err)
		return err
	}
	log.Printf("Connected to keyspace %s", keyspaceName)

	return createTodoTable()
}

func createTodoTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS todos (
	id UUID PRIMARY KEY,
	info TEXT,
	created_at TIMESTAMP
	)
	`
	err := Session.Query(query).Exec()
	if err != nil {
		log.Printf("Failed to create todos table: %v", err)
		return err
	}
	log.Printf("Table todos created or already exists")
	return nil
}
