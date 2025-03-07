package handlers

import (
	"encoding/json"
	"go_server/db"
	"go_server/models"
	"net/http"
	"time"

	"github.com/gocql/gocql"
)

func HandleRoot(w http.ResponseWriter, r *http.Request) {
	query := `
	select * from todos;
	`
	db.Session.Query(query).Exec()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	hello := json.RawMessage("Hello world")
	w.Write(hello)
}

func DeleteTodo(w http.ResponseWriter, r *http.Request) {

}

func CreateTodo(w http.ResponseWriter, r *http.Request) {
	var todo models.Todo

	err := json.NewDecoder(r.Body).Decode(&todo)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	if todo.Info == "" {
		http.Error(w, "Cannot submit empty Todo", http.StatusBadRequest)
	}

	id, uuidErr := gocql.RandomUUID()

	if uuidErr != nil {
		http.Error(w, "Failed to generate ID", http.StatusInternalServerError)
	}

	query := `INSERT INTO todos (id, info, created_at) Values ( ?, ? ,?)`

	dbErr := db.Session.Query(query, id, todo.Info, time.Now()).Exec()

	if dbErr != nil {
		http.Error(w, dbErr.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func GetTodos(w http.ResponseWriter, r *http.Request) {

}
