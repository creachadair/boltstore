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

// Opener constructs a Store from an address comprising a path, for use with
// the store package. If addr has the form name@path, the name is used as the
// bucket label.
func Opener(_ context.Context, addr string) (blob.Store, error) {
	// TODO: Parse other options out of the address string somehow.
	path, bucket := addr, ""
	if i := strings.Index(addr, "@"); i > 0 {
		path, bucket = path[i+1:], path[:i]
	}
	return Open(path, &Options{Bucket: bucket})
}

// Open creates a Store by opening the bbolt database specified by opts.
func Open(path string, opts *Options) (*Store, error) {
	db, err := bbolt.Open(path, opts.fileMode(), opts.boltOptions())
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, bucket: []byte(opts.bucket("blobs"))}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(s.bucket)
		return err
	}); err != nil {
		return nil, err
	}
	return s, nil
}

// Options provides options for opening a bbolt database.
type Options struct {
	Mode        os.FileMode
	Bucket      string
	BoltOptions *bbolt.Options
}

func (o *Options) fileMode() os.FileMode {
	if o == nil || o.Mode == 0 {
		return 0600
	}
	return o.Mode
}

func (o *Options) bucket(fallback string) string {
	if o == nil || o.Bucket == "" {
		return fallback
	}
	return o.Bucket
}

func (o *Options) boltOptions() *bbolt.Options {
	if o == nil || o.BoltOptions == nil {
		return &bbolt.Options{
			NoFreelistSync: true,
			FreelistType:   bbolt.FreelistMapType,
		}
	}
	return o.BoltOptions
}

// Close implements part of the blob.Store interface. It closes the underlying
// database instance and reports its result.
func (s *Store) Close(_ context.Context) error { return s.db.Close() }

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) (data []byte, err error) {
	if key == "" {
		return nil, blob.KeyNotFound(key) // bolt does not store empty keys
	}
	err = s.db.View(func(tx *bbolt.Tx) error {
		value := tx.Bucket(s.bucket).Get([]byte(key))
		if value == nil {
			return blob.KeyNotFound(key)
		}
		data = copyOf(value)
		return nil
	})
	return
}

func copyOf(data []byte) []byte {
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp
}

// Put implements part of blob.Store.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	key := []byte(opts.Key)
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if !opts.Replace && b.Get(key) != nil {
			return blob.KeyExists(opts.Key)
		}
		return b.Put(key, opts.Data)
	})
}

// Size implements part of blob.Store.
func (s *Store) Size(_ context.Context, key string) (size int64, err error) {
	if key == "" {
		return 0, blob.KeyNotFound(key) // bolt cannot store empty keys
	}
	err = s.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(s.bucket).Get([]byte(key))
		if data == nil {
			return blob.KeyNotFound(key)
		}
		size = int64(len(data))
		return nil
	})
	return
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	if key == "" {
		return blob.KeyNotFound(key) // bolt cannot store empty keys
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		byteKey := []byte(key)
		b := tx.Bucket(s.bucket)
		if b.Get(byteKey) == nil {
			return blob.KeyNotFound(key)
		}
		return b.Delete(byteKey)
	})
}

// List implements part of blob.Store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	err := s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(s.bucket).Cursor()

		k, _ := c.Seek([]byte(start))
		for k != nil {
			if err := f(string(k)); err != nil {
				return err
			} else if err := ctx.Err(); err != nil {
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
