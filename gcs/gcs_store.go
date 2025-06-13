package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	gstorage "cloud.google.com/go/storage"
	"github.com/rivian/delta-go/storage"
	"google.golang.org/api/option"
)

// GCS PACKAGE
type GCSStore struct {
	client     *gstorage.Client
	bucketName string
	prefix     string
}

func NewGCSStore(ctx context.Context, prefix string, bucketName string, credentialsPath ...string) (*GCSStore, error) {
	var client *gstorage.Client
	var err error

	if len(credentialsPath) > 0 && credentialsPath[0] != "" {
		client, err = gstorage.NewClient(ctx, option.WithCredentialsFile(credentialsPath[0]))
	} else {
		client, err = gstorage.NewClient(ctx)
	}

	if err != nil {
		fmt.Printf("failed to create GCS client: %v", err)
		return nil, err
	}
	return &GCSStore{
		client:     client,
		bucketName: bucketName,
		prefix:     prefix,
	}, nil
}
func (gcsStore *GCSStore) Prefix() string {
	return gcsStore.prefix
}
func (gcsStore *GCSStore) Put(location storage.Path, bytes []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	obj := gcsStore.client.Bucket(gcsStore.bucketName).Object(gcsStore.prefix + "/" + location.Raw)
	w := obj.NewWriter(ctx)

	_, err := w.Write(bytes)
	if err != nil {
		fmt.Printf("failed to write on GCS: %v", err)
		return err
	}
	return w.Close()
}

func (g *GCSStore) GetWriter(ctx context.Context, path string) io.Writer {
	obj := g.client.Bucket(g.bucketName).Object(g.prefix + "/" + path)
	return obj.NewWriter(ctx)
}

// TODO:
func (gcsStore *GCSStore) Get(location storage.Path) ([]byte, error) {
	ctx := context.Background()
	objectPath := gcsStore.prefix + "/" + location.Raw
	if len(gcsStore.prefix) > 0 && len(location.Raw) >= len(gcsStore.prefix) && location.Raw[:len(gcsStore.prefix)] == gcsStore.prefix {
		objectPath = location.Raw
	}
	obj := gcsStore.client.Bucket(gcsStore.bucketName).Object(objectPath)
	// attrs, _ := obj.Attrs(ctx)
	// fmt.Printf("%d\n", attrs.Size)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		if errors.Is(err, gstorage.ErrObjectNotExist) {
			return nil, storage.ErrObjectDoesNotExist
		}
		return nil, fmt.Errorf("failed to create object reader: %w", err)
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
func (gcsStore *GCSStore) Head(location storage.Path) (storage.ObjectMeta, error) {
	var m storage.ObjectMeta
	ctx := context.Background()
	objectPath := path.Join(gcsStore.prefix, location.Raw)
	if len(gcsStore.prefix) > 0 && len(location.Raw) >= len(gcsStore.prefix) && location.Raw[:len(gcsStore.prefix)] == gcsStore.prefix {
		objectPath = location.Raw
	}
	obj := gcsStore.client.Bucket(gcsStore.bucketName).Object(objectPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return m, storage.ErrObjectDoesNotExist
	}
	m.Location = location
	m.LastModified = attrs.Updated
	m.Size = attrs.Size
	return m, nil
}

func (g *GCSStore) Delete(location storage.Path) error {
	ctx := context.Background()
	objectPath := path.Join(g.prefix, location.Raw)
	if len(g.prefix) > 0 && len(location.Raw) >= len(g.prefix) && location.Raw[:len(g.prefix)] == g.prefix {
		objectPath = location.Raw
	}
	obj := g.client.Bucket(g.bucketName).Object(objectPath)
	// attr, _ := obj.Attrs(ctx)
	// fmt.Printf("%d", attr.Size)
	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}
	return nil
}

func (g *GCSStore) DeleteFolder(location storage.Path) error {
	return nil
}

func (g *GCSStore) List(prefix storage.Path, previousResult *storage.ListResult) (storage.ListResult, error) {
	return storage.ListResult{}, nil
}

func (g *GCSStore) ListAll(prefix storage.Path) (storage.ListResult, error) {
	return storage.ListResult{}, nil
}

func (g *GCSStore) IsListOrdered() bool {
	return false
}

func (g *GCSStore) Rename(from storage.Path, to storage.Path) error {
	ctx := context.Background()
	bucket := g.client.Bucket(g.bucketName)

	fromObj := bucket.Object(path.Join(g.prefix, from.Raw))
	toObj := bucket.Object(path.Join(g.prefix, to.Raw))

	// Copy source to destination
	_, err := toObj.CopierFrom(fromObj).Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to copy object: %w", err)
	}

	// Delete source object
	if err := fromObj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete source object after copy: %w", err)
	}

	return nil
}

func (g *GCSStore) RenameIfNotExists(from storage.Path, to storage.Path) error {
	// return ErrObjectAlreadyExists if the destination file exists
	_, err := g.Head(to)
	if !errors.Is(err, gstorage.ErrObjectNotExist) && err == nil {
		return errors.Join(storage.ErrObjectAlreadyExists, fmt.Errorf("object at location %s already exists", to.Raw))
	}

	err = g.Rename(from, to)
	if err != nil {
		return err
	}
	return nil
}

func (g *GCSStore) ReadAt(location storage.Path, p []byte, off int64, max int64) (n int, err error) {
	ctx := context.Background()

	obj := g.client.Bucket(g.bucketName).Object(location.Raw)

	length := max - off + 1
	if length < 0 {
		return 0, fmt.Errorf("invalid range: max < off")
	}

	reader, err := obj.NewRangeReader(ctx, off, length)
	if err != nil {
		if errors.Is(err, gstorage.ErrObjectNotExist) {
			return 0, fmt.Errorf("read at object does not exist: %w", err)
		}
		return 0, fmt.Errorf("failed to create range reader: %w", err)
	}
	defer reader.Close()

	return io.ReadFull(reader, p)
}

func (g *GCSStore) SupportsWriter() bool {
	return true
}

func (g *GCSStore) Writer(to storage.Path, flag int) (io.Writer, func() error, error) {
	return nil, func() error { return nil }, nil
}

func (gcsStore *GCSStore) BaseURI() storage.Path {
	return storage.Path{
		Raw: "",
	}
}

// PutStream uploads a stream of data to GCS at the specified path.
func (g *GCSStore) PutStream(ctx context.Context, path string, reader io.Reader) error {
	obj := g.client.Bucket(g.bucketName).Object(g.prefix + "/" + path)
	writer := obj.NewWriter(ctx)

	writer.ContentType = "application/octet-stream"

	if _, err := io.Copy(writer, reader); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write to GCS: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	return nil
}

func (g *GCSStore) Close() error {
	return g.client.Close()
}

func (s *GCSStore) PutIfNotExists(path storage.Path, data []byte) error {
	ctx := context.Background()
	bucket := s.client.Bucket(s.bucketName)
	obj := bucket.Object(path.Raw)

	// Check if the object already exists
	_, err := obj.Attrs(ctx)
	if err == nil {
		return storage.ErrObjectAlreadyExists
	}

	// Create new object
	writer := obj.NewWriter(ctx)
	_, err = writer.Write(data)
	if err != nil {
		return err
	}
	return writer.Close()
}
