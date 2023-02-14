package main

import (
	"log"

	"github.com/Jxancestral17/LOG_PACKAGE/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
