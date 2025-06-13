package gcs_filelock

import (
	"errors"
	"main/gcs"
	"path/filepath"
	"time"

	"github.com/rivian/delta-go/lock"
	"github.com/rivian/delta-go/lock/filelock"
	"github.com/rivian/delta-go/storage"
)

type GCSFileLockStore struct {
	Store   *gcs.GCSStore
	baseURI storage.Path
	key     string
	opts    filelock.Options
}

// Compile time check that GCSFileLock implements lock.Locker
var _ lock.Locker = (*GCSFileLockStore)(nil)

const (
	defaultTTL time.Duration = 60 * time.Second
)

func setOptionsDefaults(opts *filelock.Options) {
	if opts.TTL == 0 {
		opts.TTL = defaultTTL
	}
}

// New creates a new FileLock instance.
func New(baseURI storage.Path, key string, opts filelock.Options) *GCSFileLockStore {
	setOptionsDefaults(&opts)

	l := new(GCSFileLockStore)
	l.baseURI = baseURI
	l.key = key
	l.opts = opts

	return l
}

// NewLock creates a new FileLock instance using an existing FileLock instance.
func (l *GCSFileLockStore) NewLock(key string) (lock.Locker, error) {
	nl := new(GCSFileLockStore)
	nl.baseURI = l.baseURI
	nl.key = key
	nl.opts = l.opts

	return nl, nil
}

// TryLock attempts to acquire a file lock.
func (l *GCSFileLockStore) TryLock() (acquiredLock bool, returnErr error) {
	lockPath := filepath.Join(l.baseURI.Raw, l.Store.Prefix(), l.key)
	lockObjPath := storage.NewPath(lockPath)

	// Tenta criar o arquivo de lock na GCS. Se já existe, não obtém o lock.
	err := l.Store.PutIfNotExists(lockObjPath, []byte("lock"))
	if err != nil {
		if errors.Is(err, storage.ErrObjectAlreadyExists) {
			return false, errors.Join(lock.ErrLockNotObtained, err)
		}
		return false, err
	}

	// TTL: desbloqueia após o tempo, se configurado
	if l.opts.TTL > 0 {
		go func() {
			time.Sleep(l.opts.TTL)
			_ = l.Unlock()
		}()
	}

	return true, nil
}

// Unlock releases a file lock.
func (l *GCSFileLockStore) Unlock() error {
	lockPath := filepath.Join(l.baseURI.Raw, l.key)
	lockObjPath := storage.NewPath(lockPath)
	err := l.Store.Delete(lockObjPath)
	if err != nil {
		return errors.Join(lock.ErrUnableToUnlock, err)
	}
	return nil
}
