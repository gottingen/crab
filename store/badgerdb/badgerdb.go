package badgerdb



import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gottingen/crab/store"
	"github.com/dgraph-io/badger"
)

// BadgerDB - represents a badger db implementation
type BadgerDB struct {
	badger        *badger.DB
	countersLocks sync.RWMutex
}

// OpenBadger - Opens the specified path
func NewBadgerDB(path string) (*BadgerDB, error) {
	opts := badger.DefaultOptions(path)

	bdb, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	db := new(BadgerDB)
	db.badger = bdb
	db.countersLocks = sync.RWMutex{}

	go (func() {
		for db.badger.RunValueLogGC(0.5) == nil {
			// cleaning ...
		}
	})()

	return db, nil
}

// Close ...
func (db *BadgerDB) Close() {
	db.badger.Close()
}

// Size - returns the size of the database (LSM + ValueLog) in bytes
func (db *BadgerDB) Size() int64 {
	lsm, vlog := db.badger.Size()
	return lsm + vlog
}

// GC - runs the garbage collector
func (db *BadgerDB) GC() error {
	var err error
	for {
		err = db.badger.RunValueLogGC(0.5)
		if err != nil {
			break
		}
	}
	return err
}

// Incr - increment the key by the specified value
func (db *BadgerDB) Incr(k string, by int64) (int64, error) {
	db.countersLocks.Lock()
	defer db.countersLocks.Unlock()

	val, err := db.Get(k)
	if err != nil {
		val = ""
	}

	valFloat, _ := strconv.ParseInt(val, 10, 64)
	valFloat += by

	err = db.Set(k, fmt.Sprintf("%d", valFloat), -1)
	if err != nil {
		return 0, err
	}

	return valFloat, nil
}

// Set - sets a key with the specified value and optional ttl
func (db *BadgerDB) Set(k, v string, ttl int) error {
	return db.badger.Update(func(txn *badger.Txn) (err error) {
		if ttl < 1 {
			err = txn.Set([]byte(k), []byte(v))
		} else {
			enry := badger.NewEntry([]byte(k), []byte(v))
			enry.WithTTL(time.Duration(ttl)*time.Millisecond)
			err = txn.SetEntry(enry)
		}

		return err
	})
}

// MSet - sets multiple key-value pairs
func (db *BadgerDB) MSet(data map[string]string) error {
	return db.badger.Update(func(txn *badger.Txn) (err error) {
		for k, v := range data {
			txn.Set([]byte(k), []byte(v))
		}
		return nil
	})
}

// Get - fetches the value of the specified k
func (db *BadgerDB) Get(k string) (string, error) {
	var data string

	err := db.badger.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(k))
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		data = string(val)

		return nil
	})

	return data, err
}

// MGet - fetch multiple values of the specified keys
func (db *BadgerDB) MGet(keys []string) (data []string) {
	db.badger.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get([]byte(key))
			if err != nil {
				data = append(data, "")
				continue
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				data = append(data, "")
				continue
			}
			data = append(data, string(val))
		}
		return nil
	})

	return data
}

// TTL - returns the time to live of the specified key's value
func (db *BadgerDB) TTL(key string) int64 {
	var expires int64

	db.badger.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			expires = -2
			return nil
		}

		exp := item.ExpiresAt()
		if exp == 0 {
			expires = -1
			return nil
		}

		expires = int64(exp)

		return nil
	})

	if expires == -2 {
		return -2
	}

	if expires == -1 {
		return -1
	}

	now := time.Now().Unix()

	if now >= expires {
		return -2
	}

	return (expires - now)
}

// Del - removes key(s) from the store
func (db *BadgerDB) Del(keys []string) error {
	return db.badger.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			txn.Delete([]byte(key))
		}

		return nil
	})
}

// Scan - iterate over the whole store using the handler function
func (db *BadgerDB) Scan(scannerOpt store.ScannerOptions) error {
	return db.badger.View(func(txn *badger.Txn) error {
		iteratorOpts := badger.DefaultIteratorOptions
		iteratorOpts.PrefetchValues = scannerOpt.FetchValues

		it := txn.NewIterator(iteratorOpts)
		defer it.Close()

		start := func(it *badger.Iterator) {
			if scannerOpt.Offset == "" {
				it.Rewind()
			} else {
				it.Seek([]byte(scannerOpt.Offset))
				if !scannerOpt.IncludeOffset && it.Valid() {
					it.Next()
				}
			}
		}

		valid := func(it *badger.Iterator) bool {
			if !it.Valid() {
				return false
			}

			if scannerOpt.Prefix != "" && !it.ValidForPrefix([]byte(scannerOpt.Prefix)) {
				return false
			}

			return true
		}

		for start(it); valid(it); it.Next() {
			var k, v []byte

			item := it.Item()
			k = item.KeyCopy(nil)

			if scannerOpt.FetchValues {
				v, _ = item.ValueCopy(nil)
			}

			if !scannerOpt.Handler(string(k), string(v)) {
				break
			}
		}

		return nil
	})
}

