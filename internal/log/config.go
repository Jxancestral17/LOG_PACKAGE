package log

/*Configuraimo dimensiome massimo dello store e dell'indice di un segment*/

type Config struct {
	Segment struct {
		MaxStoreBytes  uint64
		MaxIndexBytes  uint64
		InitialOffeset uint64
	}
}
