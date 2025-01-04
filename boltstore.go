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

// Package boltstore implements the [blob.KV] interface using bbolt.
package boltstore

import (
	"context"
	"errors"
	"os"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
	"go.etcd.io/bbolt"
)

// Store implements the [blob.StoreCloser] interface using a bbolt database.
type Store struct {
	*monitor.M[*bbolt.DB, KV]
}

func (s Store) Close(context.Context) error { return s.M.DB.Close() }

// KV implements the [blob.KV] interface using a bbolt database.
type KV struct {
	db     *bbolt.DB
	bucket []byte
}

// Opener constructs a [KV] from an address comprising a path, for use with the
// store package.
func Opener(_ context.Context, addr string) (blob.StoreCloser, error) { return Open(addr, nil) }

// Open creates a [KV] by opening the bbolt database specified by opts.
func Open(path string, opts *Options) (blob.StoreCloser, error) {
	db, err := bbolt.Open(path, opts.fileMode(), opts.boltOptions())
	if err != nil {
		return nil, err
	}
	rootPrefix := opts.prefix()
	s := Store{M: monitor.New(monitor.Config[*bbolt.DB, KV]{
		DB:     db,
		Prefix: rootPrefix,
		NewKV: func(_ context.Context, db *bbolt.DB, pfx dbkey.Prefix, _ string) (KV, error) {
			out := KV{db: db, bucket: []byte(pfx)}
			if err := db.Update(func(tx *bbolt.Tx) error {
				_, err := tx.CreateBucketIfNotExists(out.bucket)
				return err
			}); err != nil {
				return KV{}, err
			}
			return out, nil
		},
	})}
	if rootPrefix != "" {
		if err := db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(rootPrefix))
			return err
		}); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Options provides options for opening a bbolt database.
type Options struct {
	Mode        os.FileMode
	Prefix      string
	BoltOptions *bbolt.Options
}

func (o *Options) fileMode() os.FileMode {
	if o == nil || o.Mode == 0 {
		return 0600
	}
	return o.Mode
}

func (o *Options) prefix() dbkey.Prefix {
	if o == nil {
		return ""
	}
	return dbkey.Prefix(o.Prefix)
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

// Get implements part of [blob.KV].
func (s KV) Get(_ context.Context, key string) (data []byte, err error) {
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

// Has implements part of [blob.KV].
func (s KV) Has(_ context.Context, keys ...string) (blob.KeySet, error) {
	var out blob.KeySet
	err := s.db.View(func(tx *bbolt.Tx) error {
		for _, key := range keys {
			value := tx.Bucket(s.bucket).Get([]byte(key))
			if value != nil {
				out.Add(key)
			}
		}
		return nil
	})
	return out, err
}

func copyOf(data []byte) []byte {
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp
}

// Put implements part of [blob.KV].
func (s KV) Put(_ context.Context, opts blob.PutOptions) error {
	key := []byte(opts.Key)
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if !opts.Replace && b.Get(key) != nil {
			return blob.KeyExists(opts.Key)
		}
		return b.Put(key, opts.Data)
	})
}

// Delete implements part of [blob.KV].
func (s KV) Delete(_ context.Context, key string) error {
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

// List implements part of [blob.KV].
func (s KV) List(ctx context.Context, start string, f func(string) error) error {
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
	if errors.Is(err, blob.ErrStopListing) {
		return nil
	}
	return err
}

// Len implements part of [blob.KV].
func (s KV) Len(ctx context.Context) (int64, error) {
	var count int64
	err := s.List(ctx, "", func(string) error {
		count++
		return nil
	})
	return count, err
}
