package kv

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

func NewEncodedKV(bucket nats.KeyValue, k KeyCodec, v ValueCodec) nats.KeyValue {
	return &encodedKV{bucket, k, v}
}

type KeyCodec interface {
	Encode(key string) (string, error)
	Decode(key string) (string, error)
	EncodeRange(keys string) (string, error)
}

type ValueCodec interface {
	Encode(src []byte, dst io.Writer) error
	Decode(src io.Reader, dst io.Writer) error
}

type encodedKV struct {
	bucket     nats.KeyValue
	keyCodec   KeyCodec
	valueCodec ValueCodec
}

type watcher struct {
	watcher    nats.KeyWatcher
	keyCodec   KeyCodec
	valueCodec ValueCodec
	updates    chan nats.KeyValueEntry
	ctx        context.Context
	cancel     context.CancelFunc
}

type entry struct {
	keyCodec   KeyCodec
	valueCodec ValueCodec
	entry      nats.KeyValueEntry
}

func (e *entry) Key() string {
	dk, err := e.keyCodec.Decode(e.entry.Key())
	// TODO abort?
	if err != nil {
		return ""
	}

	return dk
}

func (e *entry) Bucket() string { return e.entry.Bucket() }
func (e *entry) Value() []byte {
	buf := new(bytes.Buffer)
	if err := e.valueCodec.Decode(bytes.NewBuffer(e.entry.Value()), buf); err != nil {
		// TODO abort?
	}
	return buf.Bytes()
}
func (e *entry) Revision() uint64           { return e.entry.Revision() }
func (e *entry) Created() time.Time         { return e.entry.Created() }
func (e *entry) Delta() uint64              { return e.entry.Delta() }
func (e *entry) Operation() nats.KeyValueOp { return e.entry.Operation() }

func (w *watcher) Updates() <-chan nats.KeyValueEntry { return w.updates }
func (w *watcher) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}

	return w.watcher.Stop()
}

func (e *encodedKV) newWatcher(w nats.KeyWatcher) nats.KeyWatcher {
	watch := &watcher{watcher: w, keyCodec: e.keyCodec, valueCodec: e.valueCodec, updates: make(chan nats.KeyValueEntry, 32)}
	watch.ctx, watch.cancel = context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case ent := <-w.Updates():
				if ent == nil {
					watch.updates <- nil
					continue
				}

				watch.updates <- &entry{
					keyCodec:   e.keyCodec,
					valueCodec: e.valueCodec,
					entry:      ent,
				}
			case <-watch.ctx.Done():
				return
			}
		}
	}()

	return watch
}

func (e *encodedKV) Get(key string) (nats.KeyValueEntry, error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return nil, err
	}

	ent, err := e.bucket.Get(ek)
	if err != nil {
		return nil, err
	}

	return &entry{
		keyCodec:   e.keyCodec,
		valueCodec: e.valueCodec,
		entry:      ent,
	}, nil
}

func (e *encodedKV) Put(key string, value []byte) (revision uint64, err error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.valueCodec.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	return e.bucket.Put(ek, buf.Bytes())
}

func (e *encodedKV) Create(key string, value []byte) (revision uint64, err error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.valueCodec.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	return e.bucket.Create(ek, buf.Bytes())
}

func (e *encodedKV) Update(key string, value []byte, last uint64) (revision uint64, err error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.valueCodec.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	return e.bucket.Update(ek, buf.Bytes(), last)
}

func (e *encodedKV) Delete(key string) error {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return err
	}

	return e.bucket.Delete(ek)
}

func (e *encodedKV) Purge(key string) error {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return err
	}

	return e.bucket.Purge(ek)
}

func (e *encodedKV) Watch(keys string, opts ...nats.WatchOpt) (nats.KeyWatcher, error) {
	ek, err := e.keyCodec.EncodeRange(keys)
	logrus.Debugf("watching [%s]", ek)
	if err != nil {
		return nil, err
	}

	nw, err := e.bucket.Watch(ek, opts...)
	if err != nil {
		return nil, err
	}

	return e.newWatcher(nw), err
}

func (e *encodedKV) History(key string, opts ...nats.WatchOpt) ([]nats.KeyValueEntry, error) {
	ek, err := e.keyCodec.Encode(key)
	if err != nil {
		return nil, err
	}

	var res []nats.KeyValueEntry
	hist, err := e.bucket.History(ek, opts...)
	if err != nil {
		return nil, err
	}

	for _, ent := range hist {
		res = append(res, &entry{e.keyCodec, e.valueCodec, ent})
	}

	return res, nil
}

func (e *encodedKV) PutString(key string, value string) (revision uint64, err error) {
	return e.Put(key, []byte(value))
}
func (e *encodedKV) WatchAll(opts ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return e.bucket.WatchAll(opts...)
}
func (e *encodedKV) Keys(opts ...nats.WatchOpt) ([]string, error) {
	keys, err := e.bucket.Keys(opts...)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, key := range keys {
		dk, err := e.keyCodec.Decode(key)
		if err != nil {
			// should not happen
			logrus.Warnf("error decoding %s: %v", key, err)
		}
		res = append(res, dk)
	}

	return res, nil
}

func (e *encodedKV) Bucket() string                           { return e.bucket.Bucket() }
func (e *encodedKV) PurgeDeletes(opts ...nats.WatchOpt) error { return e.bucket.PurgeDeletes(opts...) }
func (e *encodedKV) Status() (nats.KeyValueStatus, error)     { return e.bucket.Status() }
