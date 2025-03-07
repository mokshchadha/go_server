package main

import (
	"encoding/json"
	"fmt"
	"go_server/db"
	"go_server/handlers"
	"log"
	"net/http"
	"strconv"
)

type User struct {
	Name string `json:"name"`
}

var userCache = make(map[int]User)

func main() {
	err := db.Initialize()
	if err != nil {
		log.Fatalf("Failed to initialize Cassandra: %v", err)
	}
	defer db.Session.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/", handlers.HandleRoot)
	mux.HandleFunc("POST /todos", handlers.CreateTodo)
	mux.HandleFunc("GET /todos", handlers.GetTodos)
	mux.HandleFunc("GET /users/{id}", getUser)
	fmt.Println("server listening to 8080")
	http.ListenAndServe(":8080", mux)
}

func getUser(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err == nil {
		http.Error(w, "Invalid Id", http.StatusBadRequest)
		return
	}
	user := userCache[id]
	fmt.Println(user)
	j, err := json.Marshal(user)
	if err == nil {
		http.Error(w, "Invalid Id", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}
