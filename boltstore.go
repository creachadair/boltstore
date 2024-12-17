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
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"go.etcd.io/bbolt"
)

// Store implements the [blob.StoreCloser] interface using a bbolt database.
type Store struct {
	*dbMonitor
}

type dbMonitor struct {
	db     *bbolt.DB
	prefix dbkey.Prefix

	μ    sync.Mutex
	subs map[string]*dbMonitor
	kvs  map[string]KV
}

// Keyspace implements part of the [blob.Store] interface.
// The result of a successful call has concrete type [KV].
func (d *dbMonitor) Keyspace(ctx context.Context, name string) (blob.KV, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	kv, ok := d.kvs[name]
	if !ok {
		pfx := d.prefix.Keyspace(name)
		kv = KV{db: d.db, bucket: []byte(pfx)}
		if err := d.db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(kv.bucket)
			return err
		}); err != nil {
			return nil, err
		}
		d.kvs[name] = kv
	}
	return kv, nil
}

// Sub implements part of the [blob.Store] interface.
func (d *dbMonitor) Sub(ctx context.Context, name string) (blob.Store, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	sub, ok := d.subs[name]
	if !ok {
		sub = &dbMonitor{
			db:     d.db,
			prefix: d.prefix.Sub(name),
			subs:   make(map[string]*dbMonitor),
			kvs:    make(map[string]KV),
		}
		// N.B. We don't need to create a bucket in the DB for this, as it does
		// not contain any keys of its own.
		d.subs[name] = sub
	}
	return sub, nil
}

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(_ context.Context) error { return s.db.Close() }

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
	s := Store{dbMonitor: &dbMonitor{
		db:     db,
		prefix: opts.prefix(),
		subs:   make(map[string]*dbMonitor),
		kvs:    make(map[string]KV),
	}}
	if s.prefix != "" {
		if err := db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(s.prefix))
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

// Close implements part of the [blob.KV] interface. It closes the underlying
// database instance and reports its result.
func (s KV) Close(_ context.Context) error { return s.db.Close() }

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
