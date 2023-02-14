package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

//HTTPServer

//Crea un nuovo server htttp
/*
Accetta un inidirizzo su cui eseguire il server

Gestisce due richieste
HTTP POST -> aggiunge i record
HTTP GET -> legge i record
*/
func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()

	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

/*
Registro che il server usa per ogni call di API
*/

/*
POST
*/
type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

/*
GET
*/
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

/*
unmarshalling in una strutura
utilizza della struttura per produrre un logo
Ottenere offset del log che ha memorizzato il record
*/
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {

	var req ProduceRequest

	//Errore nella richiesta
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	//Errore nell aggiungere un record
	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

/*
marshalling in una strutura
utilizza della struttura per produrre un logo
Ottenere offset del log che ha memorizzato il record
*/

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {

	var req ConsumeRequest

	err := json.NewDecoder(r.Body).Decode(&req)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
