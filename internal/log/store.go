package log

/*

Store -> File dove vengono memorizzati i log records

*/

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// Definiamo la condifica
var (
	enc = binary.BigEndian
)

// Numero di Byte utilizzati per memorizzare la lunghezza del record
const (
	lenWidth = 8
)

/*Struttura store*/

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {

	//Prende la dimensione del file corrente evidando di sovrascrivere
	fi, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil

}

/*
Rende persisenti i byte specificati nello store
Cosi da scrivere la lungehzza del record cosi che quando si va a leggere sappiamo quanti byte bisogna leggere
*/
func (s *store) Append(p []byte) (u uint64, pos uint64, err error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	//Miglioriamo le prestazioni bufferizzando direrettamente nel file
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)

	//Restituisce i byte scritti, e la posizione cosi da essere usta quando verra indicizzata
	return uint64(w), pos, nil

}

func (s *store) Read(pos uint64) ([]byte, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	//Svuota il buffer del writer prima che che provi a leggere un recrod ancora non scaricato sul disco
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	//Scopriamo quanti byte vanno letti
	size := make([]byte, lenWidth)

	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	//Legge il file
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

/*
Legge la lunghezza di p byte a partire dall'offset
*/
func (s *store) ReadAt(p []byte, off int64) (int, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)

}

/*
Rende persisenti i buffer memorizzati prima di chiudere il file
*/
func (s *store) Close() error {

	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return nil
	}
	return s.File.Close()

}
