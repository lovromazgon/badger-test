package main

import (
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v2"
)

// TestBadgerIteratorTransactionV2 creates a transaction and writes 2 values in
// that transaction. Before committing it, it tries to read a value and
// succeeds, iterating through values also works. After committing the
// transaction the test creates a new transaction and tries iterating through
// values again, that works as well.
func TestBadgerIteratorTransactionV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badger.db")
	opt := badger.DefaultOptions(path)

	db, err := badger.Open(opt)
	if err != nil {
		t.Fatal(err)
	}

	txn := db.NewTransaction(true)
	defer txn.Discard()

	// let's set 2 keys
	err = txn.Set([]byte("key-1"), []byte("val-1"))
	if err != nil {
		t.Fatal(err)
	}
	err = txn.Set([]byte("key-2"), []byte("val-2"))
	if err != nil {
		t.Fatal(err)
	}

	// we can get one key
	_, err = txn.Get([]byte("key-1"))
	if err != nil {
		t.Fatal(err)
	}

	// we now try to iterate through the keys, we would expect to see 2 keys
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	var results [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		results = append(results, it.Item().Key())
	}
	it.Close()

	// NOTE: iterator finds 2 keys, badger v2 works as expected
	if len(results) != 2 {
		t.Errorf("expected 2 keys, got %d", len(results))
	}

	// iterate again after committing the transaction
	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	txn = db.NewTransaction(false)
	defer txn.Discard()

	it = txn.NewIterator(badger.DefaultIteratorOptions)
	results = nil
	for it.Rewind(); it.Valid(); it.Next() {
		results = append(results, it.Item().Key())
	}
	it.Close()

	// iterator still finds 2 keys
	if len(results) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(results))
	}
}
