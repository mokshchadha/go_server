// setup_cassandra.go
package main

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	// Step 1: Start Cassandra with Docker Compose
	fmt.Println("Starting Cassandra with Docker Compose...")

	// Step 2: Wait for Cassandra to be ready
	fmt.Println("Waiting for Cassandra to be ready...")
	waitForCassandra()

	// Step 3: Initialize the database
	fmt.Println("Initializing database...")
	initializeDatabase()

	fmt.Println("\n----- Setup Complete -----")
	fmt.Println("You can connect to Cassandra using: docker exec -it cassandra-db cqlsh")
	fmt.Println("To view your todos table: docker exec -it cassandra-db cqlsh -e \"USE test_db; SELECT * FROM todos;\"")
}

func waitForCassandra() {
	// Keep trying to connect until successful or timeout
	maxAttempts := 20
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		cmd := exec.Command("docker", "exec", "cassandra-db", "cqlsh", "-e", "describe keyspaces")
		if err := cmd.Run(); err == nil {
			fmt.Println("Cassandra is ready!")
			return
		}
		fmt.Printf("Attempt %d/%d: Cassandra is not ready yet, waiting 10 seconds...\n", attempt, maxAttempts)
		time.Sleep(10 * time.Second)
	}
	log.Fatal("Timed out waiting for Cassandra to be ready")
}

func initializeDatabase() {
	// Create a cluster config
	cluster := gocql.NewCluster("localhost")
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = time.Second * 10
	cluster.ConnectTimeout = time.Second * 10

	// Connect to Cassandra
	var session *gocql.Session
	var err error

	// Retry connection a few times
	for i := 0; i < 5; i++ {
		session, err = cluster.CreateSession()
		if err == nil {
			break
		}
		fmt.Printf("Retrying connection to Cassandra: %v\n", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	defer session.Close()

	// Create keyspace
	fmt.Println("Creating keyspace 'test_db'...")
	if err := session.Query(`
		CREATE KEYSPACE IF NOT EXISTS test_db 
		WITH REPLICATION = { 
			'class' : 'SimpleStrategy', 
			'replication_factor' : 1 
		}`).Exec(); err != nil {
		log.Fatalf("Failed to create keyspace: %v", err)
	}

	// Use the keyspace
	cluster.Keyspace = "test_db"
	session.Close()

	// Connect to the keyspace
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to keyspace: %v", err)
	}
	defer session.Close()

	// Create todos table
	fmt.Println("Creating todos table...")
	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS todos (
			id UUID PRIMARY KEY,
			title TEXT,
			description TEXT,
			completed BOOLEAN,
			created_at TIMESTAMP,
			updated_at TIMESTAMP
		)`).Exec(); err != nil {
		log.Fatalf("Failed to create todos table: %v", err)
	}

	// Insert sample data
	fmt.Println("Inserting sample data...")
	insertSampleData(session)

	// Query to verify data
	fmt.Println("Verifying data...")
	var count int
	if err := session.Query(`SELECT COUNT(*) FROM todos`).Scan(&count); err != nil {
		log.Fatalf("Failed to query todos: %v", err)
	}
	fmt.Printf("Successfully created %d todo items\n", count)
}

func insertSampleData(session *gocql.Session) {
	ctx := context.Background()

	// Sample todos
	todos := []struct {
		title       string
		description string
		completed   bool
	}{
		{
			title:       "Buy groceries",
			description: "Milk, eggs, and bread",
			completed:   false,
		},
		{
			title:       "Finish project",
			description: "Complete the Go Cassandra setup",
			completed:   false,
		},
		{
			title:       "Call mom",
			description: "Ask about the family reunion",
			completed:   true,
		},
	}

	// Insert todos
	for _, todo := range todos {
		id, _ := gocql.RandomUUID()
		now := time.Now()

		err := session.Query(`
			INSERT INTO todos (id, title, description, completed, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)`,
			id, todo.title, todo.description, todo.completed, now, now,
		).WithContext(ctx).Exec()

		if err != nil {
			log.Fatalf("Failed to insert todo '%s': %v", todo.title, err)
		}
		fmt.Printf("Created todo: %s\n", todo.title)
	}
}
