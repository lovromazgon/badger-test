package main

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
)

func TestBadgerSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badger.db")
	opt := badger.DefaultOptions(path)

	db, err := badger.Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	setAndCommit := func(t *testing.T, wg *sync.WaitGroup, txn *badger.Txn, key, val []byte) {
		defer wg.Done()

		// NOTE: uncomment next lines to get consistent failures
		// when a key is read in a transaction badger will make sure it was not
		// modified in the meantime when committing

		// // first read the value saved for this key, it should fail, there is no value
		// item, err := txn.Get(key)
		// if err == nil {
		// 	// there is something set under this key?! read it
		// 	var got []byte
		// 	err = item.Value(func(val []byte) error {
		// 		got = val
		// 		return nil
		// 	})
		// 	t.Fatalf("expected no key at this point, got %s", got)
		// }

		err = txn.Set(key, val)
		if err != nil {
			t.Fatal(err)
		}
		err = txn.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}

	// run test 100 times to get some failures and prove non-deterministic behavior
	for i := 0; i < 100; i++ {
		t.Run("run", func(t *testing.T) {
			key := []byte(uuid.NewString())
			val1 := []byte("foo")
			val2 := []byte("bar")

			// both transaction start at the same time
			txn1 := db.NewTransaction(true)
			txn2 := db.NewTransaction(true)
			defer txn1.Discard()
			defer txn2.Discard()

			var wg sync.WaitGroup
			wg.Add(2)
			// we try to set the same key and commit the transaction in two
			// separate go routines, two separate transactions
			go setAndCommit(t, &wg, txn1, key, val1)
			go setAndCommit(t, &wg, txn2, key, val2)
			wg.Wait()

			// if we made it this far the test didn't fail yet - why? shouldn't
			// one of the transactions fail?!
			// let's check the value that's in the DB
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

			// most of the time the value will match val1, but in some runs it
			// does not, result is non-deterministic
			if string(got) != string(val1) {
				t.Fatal("got did not match val1")
			}
		})
	}
}
