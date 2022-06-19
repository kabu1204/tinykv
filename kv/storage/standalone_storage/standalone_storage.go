package standalone_storage

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	opts badger.Options
}

type standAloneStorageReader struct {
	db    *badger.DB
	iters []engine_util.DBIterator
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	return &StandAloneStorage{
		db:   nil,
		opts: opts,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	db, err := badger.Open(s.opts)
	if err != nil {
		log.Fatal(err)
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.db.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := standAloneStorageReader{
		db: s.db,
	}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for _, one := range batch {
		if m, ok := one.Data.(storage.Put); ok {
			wb.SetCF(m.Cf, m.Key, m.Value)
		} else if m, ok := one.Data.(storage.Delete); ok {
			wb.DeleteCF(m.Cf, m.Key)
		} else {
			log.Error("[standalone_storage] Failed to type assertion")
			return fmt.Errorf("[standalone_storage] Failed to type assertion")
		}
	}
	return wb.WriteToDB(s.db)
}

func (r *standAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	txn := r.db.NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (r *standAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := r.db.NewTransaction(false)
	it := engine_util.NewCFIterator(cf, txn)
	return it
}

func (r *standAloneStorageReader) Close() {}
