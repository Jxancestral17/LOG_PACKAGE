package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

/*
Definiscono che componodno ciuscuna vode di indice
Definisce il indicie che compre un file presisten e un file mappato in memoria
*/
var (
	offWidth uint64 = 4 // Dichiatato come uint64 ma realamente è un uint32
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

/*
crea un indice per il file e salviamo la dimesione corrente del file
 in modo da tener traccia della qunatita di dati nel file
Cresce il file fino alla dimesione massimo dell'incide
 prima di mappare in memoria il file quindi restituisce l'indice creato
*/

func newIndex(f *os.File, c Config) (*index, error) {

	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil

}

/*
si assicura che il file mapppato in memmoria abbia sincornizzato i suoi dati
con il file persistne ed esso abbia scaricato il suo contenuto in una memoria stabile
tronca il file presistne alla quantita di dati che è effetivamente in esso
e chiude il file

*/

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()

}

/*
Accetta un offset e restituisce la posizione del record associato nell'archivo

offset relativi -> uint32 -> 4 byte più leggeri
offset assoluti -> uint64 -> 8 bytes più pesanti

*/

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {

	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil

}

/*

Aggiugne offset e posizione specifici all'indice

Verifichiamo di avere spazio per la voce
Se ce spazio codifica l'ffset e la posizione vengono scritti nel file mappato in memoria
Incrementiamo la posiione in uci verra effetuta la scrittura successiva

*/

func (i *index) Write(off uint32, pos uint64) error {

	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil

}

/*Ritorna il path del indexe*/
func (i *index) Name() string {
	return i.file.Name()
}
