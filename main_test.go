package main

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v2"
	"github.com/google/uuid"
)

func TestBadgerTransactions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badger.db")
	opt := badger.DefaultOptions(path)

	db, err := badger.Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	setAndCommit := func(wg *sync.WaitGroup, txn *badger.Txn, key, val []byte) {
		defer wg.Done()
		err := txn.Set(key, val)
		if err != nil {
			t.Fatal(err)
		}
		err = txn.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 100; i++ {
		t.Run("run", func(t *testing.T) {
			key := []byte(uuid.NewString())
			val1 := []byte("foo")
			val2 := []byte("bar")

			txn1 := db.NewTransaction(true)
			txn2 := db.NewTransaction(true)
			defer txn1.Discard()
			defer txn2.Discard()

			var wg sync.WaitGroup
			wg.Add(2)
			go setAndCommit(&wg, txn1, key, val1)
			go setAndCommit(&wg, txn2, key, val2)
			wg.Wait()

			// I would expect one of the transactions to fail but actually both succeed
			// and the result is non-deterministic.
			var got []byte
			err = db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				err = item.Value(func(val []byte) error {
					got = val
					return nil
				})
				return err
			})
			if err != nil {
				t.Fatal(err)
			}

			if string(got) != string(val1) {
				t.Fatal("got did not match val1")
			}
		})
	}
}
