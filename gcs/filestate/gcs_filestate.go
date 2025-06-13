package gcs_filestate

import (
	"encoding/json"
	"errors"
	"main/gcs"
	"path/filepath"

	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/storage"
)

// Store stores a table's commit state in a file system.
type GCSFilestateStore struct {
	Store   *gcs.GCSStore
	BaseURI storage.Path
	Key     string
}

// Compile time check that FileStateStore implements state.StateStore
var _ state.Store = (*GCSFilestateStore)(nil)

// New creates a new Store instance.
func New(baseURI storage.Path, key string) *GCSFilestateStore {
	fs := new(GCSFilestateStore)
	fs.BaseURI = baseURI
	fs.Key = key
	return fs
}

// Get retrieves a state store's commit state.
func (s *GCSFilestateStore) Get() (state.CommitState, error) {
	getPath := filepath.Join(s.BaseURI.Raw, s.Key)
	var commitState state.CommitState
	data, err := s.Store.Get(storage.NewPath(getPath))
	if err != nil {
		return commitState, errors.Join(state.ErrorCanNotReadState, err)
	}
	if len(data) == 0 {
		return commitState, errors.Join(state.ErrorStateIsEmpty, err)
	}

	err = json.Unmarshal(data, &commitState)
	if err != nil {
		return commitState, errors.Join(state.ErrorCanNotReadState, err)
	}

	return commitState, nil
}

// Put sets a state store's current commit state.
func (s *GCSFilestateStore) Put(commitState state.CommitState) error {
	data, err := json.Marshal(commitState)
	if err != nil {
		return errors.Join(state.ErrorCanNotWriteState, err)
	}
	// Usa o GCSStore para gravar o estado no bucket
	if s.Store == nil {
		return errors.New("gcstore is not set in GCSFilestateStore, remember to set it pls")
	}
	putPath := storage.Path{Raw: filepath.Join(s.BaseURI.Raw, s.Key)}
	err = s.Store.Put(putPath, data)
	if err != nil {
		return errors.Join(state.ErrorCanNotWriteState, err)
	}
	return nil
}
