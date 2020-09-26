// Copyright 2019 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package boltstore implements the blob.Store interface using bbolt.
package boltstore

import (
	"context"
	"os"
	"strings"

	"github.com/creachadair/ffs/blob"
	"go.etcd.io/bbolt"
)

// Store implements the blob.Store interface using a bbolt database.
type Store struct {
	db     *bbolt.DB
	bucket []byte
}

// Opener constructs a filestore from an address comprising a path, for use
// with the store package.
func Opener(_ context.Context, addr string) (blob.Store, error) {
	// TODO: Parse other options out of the address string somehow.
	return Open(addr, 0600, &bbolt.Options{
		NoFreelistSync: true,
		FreelistType:   bbolt.FreelistMapType,
	})
}

// Open creates a Store by opening the bbolt database specified by opts.
// If path has the form name@path, name is used as the bucket label.
func Open(path string, mode os.FileMode, opts *bbolt.Options) (*Store, error) {
	bucket := "0"
	if i := strings.Index(path, "@"); i > 0 {
		bucket, path = path[:i], path[i+1:]
	}
	db, err := bbolt.Open(path, mode, opts)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, bucket: []byte(bucket)}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(s.bucket)
		return err
	}); err != nil {
		return nil, err
	}
	return s, nil
}

// Close implements the io.Closer interface. It closes the underlying database
// instance and reports its result.
func (s *Store) Close() error { return s.db.Close() }

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) (data []byte, err error) {
	if key == "" {
		return nil, blob.ErrKeyNotFound // bolt does not store empty keys
	}
	err = s.db.View(func(tx *bbolt.Tx) error {
		data = tx.Bucket(s.bucket).Get([]byte(key))
		if data == nil {
			return blob.ErrKeyNotFound
		}
		return nil
	})
	return
}

// Put implements part of blob.Store. A successful Put linearizes to the point
// at which the rename of the write temporary succeeds; a Put that fails due to
// an existing key linearizes to the point when the key path stat succeeds.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	key := []byte(opts.Key)
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if !opts.Replace && b.Get(key) != nil {
			return blob.ErrKeyExists
		}
		return b.Put(key, opts.Data)
	})
}

// Size implements part of blob.Store.
func (s *Store) Size(_ context.Context, key string) (size int64, err error) {
	if key == "" {
		return 0, blob.ErrKeyNotFound // badger cannot store empty keys
	}
	err = s.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(s.bucket).Get([]byte(key))
		if data == nil {
			return blob.ErrKeyNotFound
		}
		size = int64(len(data))
		return nil
	})
	return
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	if key == "" {
		return blob.ErrKeyNotFound // badger cannot store empty keys
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		key := []byte(key)
		b := tx.Bucket(s.bucket)
		if b.Get(key) == nil {
			return blob.ErrKeyNotFound
		}
		return b.Delete(key)
	})
}

// List implements part of blob.Store.
func (s *Store) List(_ context.Context, start string, f func(string) error) error {
	err := s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(s.bucket).Cursor()

		k, _ := c.Seek([]byte(start))
		for k != nil {
			if err := f(string(k)); err != nil {
				return err
			}
			k, _ = c.Next()
		}
		return nil
	})
	if err == blob.ErrStopListing {
		return nil
	}
	return err
}

// Len implements part of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) {
	var count int64
	err := s.List(ctx, "", func(string) error {
		count++
		return nil
	})
	return count, err
}
