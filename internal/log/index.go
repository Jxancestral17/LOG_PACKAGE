package log

import (
	"os"

	"github.com/tysontate/gommap"
)

/*
Definiscono che componodno ciuscuna vode di indice
Definisce il indicie che compre un file presisten e un file mappato in memoria
*/
var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	nmap gommap.MMap
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

	if idx.nmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil

}
