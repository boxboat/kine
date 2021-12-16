package jetstream

import (
	"context"
	"encoding/json"
	"github.com/k3s-io/kine/pkg/drivers/jetstream/kv"
	"github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/nats-io/nats.go"
)

const (
	kineBucket    = "kine"
	kineTtlBucket = "kineTTL"
	revHistory    = 1000
	ttl           = 10 * time.Minute
)

type Jetstream struct {
	kvBucket  nats.KeyValue
	jetStream nats.JetStreamContext
	server.Backend
	keyWatchCache map[string]*keyWatcherCache
	mutex         *sync.Mutex
}

type present struct{}
type keyWatcherCache struct {
	watcher nats.KeyWatcher
	timeout *time.Timer
	keys    map[string]present
	mutex   *sync.Mutex
}

// New get the JetStream Backend, establish connection to NATS JetStream.
func New(ctx context.Context, connection string) (server.Backend, error) {
	logrus.Infof("connecting to %s", connection)
	conn, err := nats.Connect(connection)
	if err != nil {
		return nil, err
	}

	js, err := conn.JetStream()

	if err != nil {
		return nil, err
	}

	bucket, err := js.KeyValue(kineBucket)
	if err != nil && err == nats.ErrBucketNotFound {
		bucket, err = js.CreateKeyValue(
			&nats.KeyValueConfig{
				Bucket:      kineBucket,
				Description: "Holds kine key/values",
				History:     64,
			})
	}

	kvB := kv.NewEncodedKV(bucket, &kv.EtcdCodec{})

	//kvB, err := js.KeyValue(kineBucket)
	//if err != nil && err == nats.ErrBucketNotFound {
	//	kvB, err = js.CreateKeyValue(
	//		&nats.KeyValueConfig{
	//			Bucket:      kineBucket,
	//			Description: "Holds kine key/values",
	//			// TODO this needs to be 1000?
	//			History: 64,
	//		})
	//}

	if err != nil {
		return nil, err
	}

	// hack around int8 history limit to adjust underlying stream
	//status, err := kvB.Status()
	//if err != nil {
	//	return nil, err
	//}
	//
	//streamInfo := status.(*nats.KeyValueBucketStatus).StreamInfo()
	//cfg := &streamInfo.Config
	//cfg.MaxMsgsPerSubject = 1000
	//_, err = js.UpdateStream(cfg)
	//
	//if err != nil {
	//	return nil, err
	//}

	return &Jetstream{
		kvBucket:      kvB,
		jetStream:     js,
		keyWatchCache: make(map[string]*keyWatcherCache),
		mutex:         &sync.Mutex{},
	}, nil
}

func (j *Jetstream) Start(ctx context.Context) error {
	// TODO
	return nil
}

func (j *Jetstream) isKeyExpiredRetrieveValue(ctx context.Context, key string) (bool, error) {

	entry, err := j.kvBucket.Get(key)
	if err != nil {
		return false, err
	}
	val, err := decode(entry.Value())
	if err != nil {
		return false, err
	}

	return j.isKeyExpired(ctx, entry.Created(), &val), err
}

func (j *Jetstream) isKeyExpired(_ context.Context, createTime time.Time, event *server.Event) bool {

	requestTime := time.Now()
	expired := false
	if event.KV.Lease > 0 {
		if requestTime.After(createTime.Add(time.Second * time.Duration(event.KV.Lease))) {
			expired = true
			if err := j.kvBucket.Delete(event.KV.Key); err != nil {
				logrus.Warnf("problem deleting expired key=%s, error=%v", event.KV.Key, err)
			}
		}
	}

	return expired
}

// Get returns the associated server.KeyValue
func (j *Jetstream) Get(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	logrus.Tracef("GET %s, rev=%d", key, revision)
	defer func() {
		if kvRet != nil {
			logrus.Tracef("GET %s, rev=%d => revRet=%d, kv=%v, size=%d, err=%v", key, revision, revRet, kvRet != nil, len(kvRet.Value), errRet)
		} else {
			logrus.Tracef("GET %s, rev=%d => revRet=%d, kv=%v, err=%v", key, revision, revRet, kvRet != nil, errRet)
		}
	}()

	currentRev, err := j.currentRevision()
	if err != nil {
		return currentRev, nil, err
	}
	rev, event, err := j.get(ctx, key, revision, false)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return currentRev, nil, nil
		} else {
			return rev, nil, err
		}
	}
	if event != nil {
		return rev, event.KV, err
	} else {
		return rev, nil, err
	}
}

func (j *Jetstream) get(ctx context.Context, key string, revision int64, includeDeletes bool) (int64, *server.Event, error) {
	logrus.Tracef("get %s, revision=%d, includeDeletes=%v", key, revision, includeDeletes)
	// Get latest revision
	if revision <= 0 {
		if entry, err := j.kvBucket.Get(key); err == nil {

			if entry.Operation() == nats.KeyValueDelete && !includeDeletes {
				return 0, nil, nil
			}
			val, err := decode(entry.Value())
			if err != nil {
				return 0, nil, err
			}
			if j.isKeyExpired(ctx, entry.Created(), &val) {
				return 0, nil, nats.ErrKeyNotFound
			}
			val.KV.ModRevision = int64(entry.Revision())
			if entry.Operation() == nats.KeyValueDelete {
				val.Delete = true
			}
			return int64(entry.Revision()), &val, nil
		} else if err == nats.ErrKeyNotFound {
			return 0, nil, err
		} else {
			return 0, nil, err
		}
	}

	// Find a particular version
	entries, err := j.kvBucket.History(key, nats.IncludeHistory())
	if err != nil {
		return 0, nil, err
	}
	for _, entry := range entries {
		if entry.Revision() == uint64(revision) {
			if entry.Operation() == nats.KeyValueDelete && !includeDeletes {
				return 0, nil, nil
			}
			val, err := decode(entry.Value())
			if err != nil {
				return 0, nil, err
			}
			if j.isKeyExpired(ctx, entry.Created(), &val) {
				return 0, nil, nats.ErrKeyNotFound
			}
			val.KV.ModRevision = revision
			if entry.Operation() == nats.KeyValueDelete {
				val.Delete = true
			}
			return revision, &val, nil
		}
	}
	return revision, nil, err
}

// Create
func (j *Jetstream) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	logrus.Tracef("CREATE %s, size=%d, lease=%d", key, len(value), lease)
	defer func() {
		logrus.Tracef("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, revRet, errRet)
	}()

	// check if key exists already
	rev, prevEvent, err := j.get(ctx, key, 0, true)
	if err != nil && err != nats.ErrKeyNotFound {
		return 0, err
	}

	if prevEvent != nil && !prevEvent.Delete {
		return 0, server.ErrKeyExists
	}

	createEvent := server.Event{
		Delete: false,
		Create: true,
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: 0,
			ModRevision:    0,
			Value:          value,
			Lease:          lease,
		},
		PrevKV: &server.KeyValue{
			ModRevision: rev,
		},
	}
	if prevEvent != nil {
		createEvent.PrevKV = prevEvent.KV
	}

	event, err := encode(createEvent)
	if err != nil {
		return 0, err
	}

	seq, err := j.kvBucket.Create(key, event)
	if err != nil {
		return 0, err
	}

	return int64(seq), nil
}

func (j *Jetstream) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	logrus.Tracef("DELETE %s, rev=%d", key)
	defer func() {
		logrus.Tracef("DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v", key, revision, revRet, kvRet != nil, deletedRet, errRet)
	}()

	rev, kv, err := j.Get(ctx, key, 0)
	if err != nil {
		return 0, nil, false, err
	}

	if kv == nil {
		return rev, nil, true, nil
	}

	if revision != 0 && kv.ModRevision != revision {
		return rev, kv, false, nil
	}

	err = j.kvBucket.Delete(key)
	if err != nil {
		return rev, kv, false, nil
	}

	return rev, kv, true, nil
}

func (j *Jetstream) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	logrus.Tracef("LIST %s, start=%s, limit=%d, rev=%d", prefix, startKey, limit, revision)
	defer func() {
		logrus.Tracef("LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v", prefix, startKey, limit, revision, revRet, len(kvRet), errRet)
	}()

	//keys, err := j.kvBucket.Keys()
	keys, err := j.getKeys(ctx, prefix)

	if err != nil {
		return 0, nil, err
	}

	rev, err := j.currentRevision()
	if err != nil {
		return 0, nil, err
	}

	if revision == 0 && len(keys) == 0 {
		if err != nil {
			return 0, nil, err
		}

		return j.List(ctx, prefix, startKey, limit, rev)
	} else if revision != 0 {
		rev = revision
	}
	//sort.Strings(keys)
	var count int64 = 0
	kvs := make([]*server.KeyValue, 0)
	for _, key := range keys {
		//if strings.HasPrefix(key, prefix) {
		if count < limit || limit == 0 {
			if _, entry, err := j.Get(ctx, key, 0); err == nil {
				kvs = append(kvs, entry)
				count++
			}
		} else {
			break
		}
		//}
	}
	return rev, kvs, nil
}

func (j *Jetstream) list(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, eventRet []*server.Event, errRet error) {
	logrus.Tracef("list %s, start=%s, limit=%d, rev=%d", prefix, startKey, limit, revision)
	//keys, err := j.kvBucket.Keys()
	keys, err := j.getKeys(ctx, prefix)

	if err != nil {
		return 0, nil, err
	}

	rev, err := j.currentRevision()
	if err != nil {
		return 0, nil, err
	}

	if revision == 0 && len(keys) == 0 {
		if err != nil {
			return 0, nil, err
		}

		return j.list(ctx, prefix, startKey, limit, rev)
	} else if revision != 0 {
		rev = revision
	}
	//sort.Strings(keys)
	var count int64 = 0
	events := make([]*server.Event, 0)
	for _, key := range keys {
		//if strings.HasPrefix(key, prefix) {
		if count < limit || limit == 0 {
			if _, entry, err := j.get(ctx, key, 0, false); err == nil {
				events = append(events, entry)
				count++
			}
		} else {
			break
		}
		//}
	}
	return rev, events, nil
}

// Count returns an exact count of the number of matching keys and the current revision of the database
func (j *Jetstream) Count(ctx context.Context, prefix string) (revRet int64, count int64, err error) {
	logrus.Tracef("COUNT %s", prefix)
	defer func() {
		logrus.Tracef("COUNT %s => rev=%d, count=%d, err=%v", prefix, revRet, count, err)
	}()

	//keys, err := j.kvBucket.Keys()
	keys, err := j.getKeys(ctx, prefix)
	if err != nil {
		return 0, 0, err
	}
	// current revision
	currentRev, err := j.currentRevision()
	if err != nil {
		return 0, 0, err
	}
	//sort.Strings(keys)
	var total int64 = 0
	for _, key := range keys {
		// TODO scan keys for TTL expiration or continue to check just in time?
		if expired, err := j.isKeyExpiredRetrieveValue(ctx, key); err == nil && !expired {
			total++
		}
	}
	return currentRev, int64(len(keys)), nil
}

func (j *Jetstream) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	logrus.Tracef("UPDATE %s, value=%d, rev=%d, lease=%v", key, len(value), revision, lease)
	defer func() {
		kvRev := int64(0)
		if kvRet != nil {
			kvRev = kvRet.ModRevision
		}
		logrus.Tracef("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v", key, len(value), revision, lease, revRet, kvRev, updateRet, errRet)
	}()

	rev, event, err := j.get(ctx, key, 0, false)

	if err != nil {
		return 0, nil, false, err
	}

	if event == nil {
		return 0, nil, false, nil
	}

	if event.KV.ModRevision != revision {
		return rev, event.KV, false, nil
	}

	updateEvent := server.Event{
		Delete: false,
		Create: false,
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: event.KV.CreateRevision,
			Value:          value,
			Lease:          lease,
		},
		PrevKV: event.KV,
	}
	if event.KV.CreateRevision == 0 {
		updateEvent.KV.CreateRevision = rev
	}

	eventBytes, err := encode(updateEvent)
	if err != nil {
		return 0, nil, false, err
	}

	seq, err := j.kvBucket.Put(key, eventBytes)
	if err != nil {
		return 0, nil, false, err
	}

	event.KV.ModRevision = int64(seq)

	return int64(seq), event.KV, true, err

}

func (j *Jetstream) Watch(ctx context.Context, key string, revision int64) <-chan []*server.Event {
	logrus.Tracef("WATCH %s, rev=%d", key, revision)

	_, events, err := j.list(ctx, key, "", 0, 0)

	watcher, err := j.kvBucket.Watch(key)

	if err != nil {
		logrus.Errorf("failed to create watcher %s for revision %d", key, revision)
	}

	result := make(chan []*server.Event, 100)

	go func() {

		if len(events) > 0 {
			result <- events
		}

		for {
			select {
			case i := <-watcher.Updates():
				if i != nil {
					//logrus.Debugf("update %v", i)
					events := make([]*server.Event, 1)
					rev, event, err := j.get(ctx, i.Key(), int64(i.Revision()), true)
					if err != nil {
						logrus.Warnf("error decoding event %v", err)
						continue
					}
					events[0] = event
					events[0].KV.ModRevision = rev
					if i.Operation() == nats.KeyValueDelete {
						events[0].Delete = true
					} else if i.Operation() == nats.KeyValuePut {
						events[0].Create = true
					}
					result <- events
				}
			case <-ctx.Done():
				if err := watcher.Stop(); err != nil {
					logrus.Warnf("error stopping %s watcher: %v", key, err)
				} else {
					logrus.Debugf("stopped %s watcher", key)
				}
				return
			}
		}
	}()
	return result
}

// DbSize get the kineBucket size from JetStream.
func (j *Jetstream) DbSize(ctx context.Context) (int64, error) {
	keySize, err := j.bucketSize(ctx, kineBucket)
	if err != nil {
		return -1, err
	}
	ttlSize, err := j.bucketSize(ctx, kineTtlBucket)
	if err != nil {
		return -1, err
	}
	return keySize + ttlSize, nil
}

func (j *Jetstream) bucketSize(ctx context.Context, bucket string) (int64, error) {
	os, err := j.jetStream.ObjectStore(bucket)
	if err != nil {
		return -1, err
	}
	s, err := os.Status()
	if err != nil {
		return -1, err
	}
	return int64(s.Size()), nil
}

func encode(event server.Event) ([]byte, error) {
	buf, err := json.Marshal(event)
	return buf, err
}

func decode(b []byte) (server.Event, error) {
	event := server.Event{}
	err := json.Unmarshal(b, &event)
	return event, err
}

func (j *Jetstream) currentRevision() (int64, error) {
	status, err := j.kvBucket.Status()
	if err != nil {
		return 0, err
	}
	return int64(status.(*nats.KeyValueBucketStatus).StreamInfo().State.LastSeq), nil
}

func (j *Jetstream) getKeys(ctx context.Context, prefix string) ([]string, error) {
	logrus.Debugf("getKeys %s", prefix)

	j.mutex.Lock()
	cache, ok := j.keyWatchCache[prefix]
	j.mutex.Unlock()

	if ok && cache.timeout.Stop() {
		cache.mutex.Lock()
		keys := make([]string, 0)
		for k := range cache.keys {
			keys = append(keys, k)
		}
		cache.mutex.Unlock()
		cache.timeout.Reset(ttl)
		return keys, nil
	} else {

		watcher, err := j.kvBucket.Watch(prefix, nats.MetaOnly())
		if err != nil {
			return nil, err
		}
		cache := &keyWatcherCache{
			watcher: watcher,
			timeout: time.NewTimer(ttl),
			keys:    make(map[string]present),
			mutex:   &sync.Mutex{},
		}
		j.mutex.Lock()
		j.keyWatchCache[prefix] = cache
		j.mutex.Unlock()

		var keys []string
		// grab all matching keys immediately
		for entry := range watcher.Updates() {
			if entry == nil {
				break
			}
			if entry.Operation() != nats.KeyValueDelete || entry.Operation() != nats.KeyValuePurge {
				cache.keys[entry.Key()] = present{}
				keys = append(keys, entry.Key())
			}
		}

		// start goroutine to watch for updates to the watched prefix
		go func() {
			for {
				select {
				case entry := <-watcher.Updates():
					if entry != nil {
						if entry.Operation() == nats.KeyValueDelete || entry.Operation() == nats.KeyValuePurge {
							cache.mutex.Lock()
							logrus.Debugf("deleting cache entry %s", entry.Key())
							delete(cache.keys, entry.Key())
							cache.mutex.Unlock()
						} else {
							cache.mutex.Lock()
							cache.keys[entry.Key()] = present{}
							cache.mutex.Unlock()
						}
					}
				case <-cache.timeout.C:
					logrus.Debugf("removing %s watcher", prefix)
					// TODO MUST FIX watcher.Stop()
					if err := watcher.Stop(); err != nil {
						logrus.Warnf("failed to stop watcher %s", prefix)
					}
					j.mutex.Lock()
					if c, ok := j.keyWatchCache[prefix]; ok && c == cache {
						delete(j.keyWatchCache, prefix)
					}
					j.mutex.Unlock()
				}
			}
		}()

		return keys, nil
	}

}
