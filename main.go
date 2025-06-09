package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"time"

	gstorage "cloud.google.com/go/storage"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/google/uuid"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/lock/filelock"
	"github.com/rivian/delta-go/state/filestate"
	"github.com/rivian/delta-go/storage"
	"google.golang.org/api/option"
)

func main() {

	fCPU, err := os.Create("recorder_cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer fCPU.Close() // error handling omitted for example
	if err := pprof.StartCPUProfile(fCPU); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	dir := "delta"
	gcs, err := NewGCSStore(context.Background(), "data", "kanastra-deltago-test", "application_default_credentials.json")
	if err != nil {
		fmt.Printf("error creating gcs store: %v\n", err)
		return
	}

	tmpPath := storage.NewPath(dir)
	// state := filestate.New(tmpPath, "_delta_log/_commit.state")
	// lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})
	// table := delta.NewTable(gcs, lock, state)

	// First write
	fileName := fmt.Sprintf("part-%s.parquet", uuid.New().String())
	writingPath := filepath.Join(tmpPath.Raw, fileName)
	// filePath := filepath.Join("data/", tmpPath.Raw, fileName)

	// Generate some data
	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "salary", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		},
		nil,
	)
	data := generateRecord(arrowSchema)

	// writing parquet
	if err := writeParquetToGCS(data, gcs, writingPath, arrowSchema); err != nil {
		fmt.Printf("error writing parquets: %v\n", err)
		return
	}

	// creating delta schema
	// schema := delta.SchemaTypeStruct{
	// 	Fields: []delta.SchemaField{
	// 		{Name: "id", Type: delta.Integer, Nullable: false, Metadata: make(map[string]any)},
	// 		{Name: "name", Type: delta.String, Nullable: false, Metadata: make(map[string]any)},
	// 		{Name: "age", Type: delta.Integer, Nullable: false, Metadata: make(map[string]any)},
	// 		{Name: "salary", Type: delta.Float, Nullable: false, Metadata: make(map[string]any)},
	// 		{Name: "active", Type: delta.Boolean, Nullable: false, Metadata: make(map[string]any)},
	// 	},
	// }

	// add, _, err := delta.NewAdd(gcs, storage.NewPath(filePath), make(map[string]string))
	// if err != nil {
	// 	fmt.Printf("error in delta add: %v\n", err)
	// 	return
	// }

	// metadata := delta.NewTableMetaData("Test Table", "test description", new(delta.Format).Default(), schema, []string{}, make(map[string]string))
	// err = table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{*add})
	// if err != nil {
	// 	fmt.Printf("error in table create: %v\n", err)
	// 	return
	// }

	numThreads := 100
	wg := new(sync.WaitGroup)
	for i := 0; i < numThreads; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			wait := rand.Int63n(int64(10 * time.Millisecond))
			time.Sleep(time.Duration(wait))

			// store := filestore.New(tmpPath)
			state := filestate.New(storage.NewPath(dir), "_delta_log/_commit.state")
			lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})

			//Lock needs to be instantiated for each worker because it is passed by reference, so if it is not created different instances of tables would share the same lock
			table := delta.NewTable(gcs, lock, state)
			transaction := table.CreateTransaction(delta.NewTransactionOptions())

			//Make some data
			data := generateRecord(arrowSchema)
			fileName := fmt.Sprintf("part-%s.parquet", uuid.New().String())
			filePath := filepath.Join("data/", tmpPath.Raw, fileName)
			writingPath := filepath.Join(tmpPath.Raw, fileName)
			if err := writeParquetToGCS(data, gcs, writingPath, arrowSchema); err != nil {
				fmt.Printf("error writing parquet: %v\n", err)
			}
			// criando copia pra teste de mais de uma action por transaction
			fileName1 := "copia-" + fileName
			filePath1 := filepath.Join("data/", tmpPath.Raw, fileName1)
			writingPath1 := filepath.Join(tmpPath.Raw, fileName1)
			if err := writeParquetToGCS(data, gcs, writingPath1, arrowSchema); err != nil {
				fmt.Printf("error writing parquet: %v\n", err)
			}

			add, _, err := delta.NewAdd(gcs, storage.NewPath(filePath), make(map[string]string))
			if err != nil {
				fmt.Printf("error in delta add: %v\n", err)
			}
			add1, _, err := delta.NewAdd(gcs, storage.NewPath(filePath1), make(map[string]string))
			if err != nil {
				fmt.Printf("error in delta add: %v\n", err)
			}

			transaction.AddAction(add)
			transaction.AddAction(add1)
			operation := delta.Write{Mode: delta.Append}
			appMetaData := make(map[string]any)
			appMetaData["test"] = 123

			transaction.SetOperation(operation)
			transaction.SetAppMetadata(appMetaData)
			v, err := transaction.Commit()
			if err != nil {
				fmt.Printf("error commiting transaction: %v", err)
			}

			if rand.Intn(20) == 0 {
				// We don't actually need a lock here since we are only writing a checkpoint for a version that this process has committed
				checkpointLock := filelock.New(tmpPath, "_delta_log/_checkpoint.lock", filelock.Options{})
				checkpointed, err := table.CreateCheckpoint(checkpointLock, delta.NewCheckpointConfiguration(), v)
				if err != nil {
					fmt.Printf("error creating checkpoint: %v", err)
				}
				fmt.Printf("checkpoint created for version %d: %v", v, checkpointed)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	fMEM, err := os.Create("recorder_mem.prof")
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer fMEM.Close() // error handling omitted for example

	if err := pprof.WriteHeapProfile(fMEM); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}

func generateRecord(schema *arrow.Schema) arrow.Record {

	mem := memory.NewGoAllocator()

	// Configurações para geração
	batchSize := 10000

	// Listas para geração aleatória
	firstNames := []string{
		"Ana", "João", "Maria", "Pedro", "Carla", "Lucas", "Fernanda", "Rafael", "Juliana", "Diego",
		"Camila", "Thiago", "Patrícia", "Felipe", "Mariana", "André", "Beatriz", "Gabriel", "Larissa", "Bruno",
		"Amanda", "Ricardo", "Vanessa", "Rodrigo", "Priscila", "Marcelo", "Renata", "Gustavo", "Caroline", "Leonardo",
		"Sabrina", "Fábio", "Daniela", "Vinícius", "Natália", "Alexandre", "Bruna", "Henrique", "Letícia", "Márcio",
		"Cristina", "Paulo", "Tatiana", "Sérgio", "Michele", "Roberto", "Adriana", "Fernando", "Karina", "Carlos",
	}

	lastNames := []string{
		"Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira", "Lima", "Gomes",
		"Costa", "Ribeiro", "Martins", "Carvalho", "Almeida", "Lopes", "Soares", "Fernandes", "Vieira", "Barbosa",
		"Rocha", "Dias", "Monteiro", "Mendes", "Cardoso", "Reis", "Araújo", "Nascimento", "Freitas", "Nunes",
		"Moreira", "Teixeira", "Miranda", "Pinto", "Fonseca", "Ramos", "Borges", "Campos", "Castro", "Correia",
	}

	totalWritten := 0

	// Criar builders para este batch
	idBuilder := array.NewInt64Builder(mem)
	nameBuilder := array.NewStringBuilder(mem)
	ageBuilder := array.NewInt32Builder(mem)
	salaryBuilder := array.NewFloat64Builder(mem)
	activeBuilder := array.NewBooleanBuilder(mem)

	// Gerar dados para este batch
	for i := 0; i < batchSize; i++ {
		id := int64(totalWritten + i + 1)
		firstName := firstNames[rand.Intn(len(firstNames))]
		lastName := lastNames[rand.Intn(len(lastNames))]
		name := firstName + " " + lastName
		age := int32(18 + rand.Intn(50)) // Idade entre 18 e 67 anos
		active := rand.Float32() > 0.1   // 90% chance de estar ativo

		idBuilder.Append(id)
		nameBuilder.Append(name)
		ageBuilder.Append(age)
		activeBuilder.Append(active)

		// Salary pode ser null (10% de chance)
		if rand.Float32() < 0.1 {
			salaryBuilder.Append(1000)
		} else {
			// Salário entre 2000 e 15000
			salary := 2000.0 + rand.Float64()*13000.0
			salaryBuilder.Append(salary)
		}
	}

	// Construir arrays
	idArray := idBuilder.NewArray()
	nameArray := nameBuilder.NewArray()
	ageArray := ageBuilder.NewArray()
	salaryArray := salaryBuilder.NewArray()
	activeArray := activeBuilder.NewArray()

	// Criar record
	record := array.NewRecord(schema, []arrow.Array{
		idArray, nameArray, ageArray, salaryArray, activeArray,
	}, int64(batchSize))

	return record
}

// func writeParquet(data arrow.Record, filename string, schema *arrow.Schema) error {
// 	file, err := os.Create(filename)
// 	if err != nil {
// 		fmt.Printf("error creating file with filename: %v\n", err)
// 		return err
// 	}

// 	parquetProps := parquet.NewWriterProperties()
// 	arrowProps := pqarrow.NewArrowWriterProperties()

// 	// Escreve diretamente no arquivo, sem buffer intermediário
// 	writer, err := pqarrow.NewFileWriter(schema, file, parquetProps, arrowProps)
// 	if err != nil {
// 		fmt.Printf("error creating pqarrow file writer: %v\n", err)
// 		file.Close()
// 		return err
// 	}

// 	if err := writer.Write(data); err != nil {
// 		fmt.Printf("error writing record to file: %v\n", err)
// 		writer.Close()
// 		file.Close()
// 		return err
// 	}

// 	// Close the writer to flush all data and write the footer
// 	if err := writer.Close(); err != nil {
// 		fmt.Printf("error closing parquet writer: %v\n", err)
// 		file.Close()
// 		return err
// 	}

// 	return nil
// }

// writeParquetToGCS escreve um arquivo parquet diretamente no GCS
func writeParquetToGCS(data arrow.Record, gcs *GCSStore, path string, schema *arrow.Schema) error {
	ctx := context.Background()
	writer := gcs.GetWriter(ctx, path)

	parquetProps := parquet.NewWriterProperties()
	arrowProps := pqarrow.NewArrowWriterProperties()

	pqWriter, err := pqarrow.NewFileWriter(schema, writer, parquetProps, arrowProps)
	if err != nil {
		return fmt.Errorf("error creating pqarrow file writer: %w", err)
	}

	if err := pqWriter.Write(data); err != nil {
		pqWriter.Close()
		return fmt.Errorf("error writing record to GCS: %w", err)
	}

	if err := pqWriter.Close(); err != nil {
		return fmt.Errorf("error closing parquet writer: %w", err)
	}

	return nil
}

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
		fmt.Errorf("Failed to create GCS client: %v", err)
		return nil, err
	}
	return &GCSStore{
		client:     client,
		bucketName: bucketName,
		prefix:     prefix,
	}, nil
}

func (gcsStore *GCSStore) Put(location storage.Path, bytes []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	obj := gcsStore.client.Bucket(gcsStore.bucketName).Object(gcsStore.prefix + "/" + location.Raw)
	w := obj.NewWriter(ctx)

	_, err := w.Write(bytes)
	if err != nil {
		fmt.Errorf("Failed to write on GCS: %v", err)
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
	return []byte("De sorte que haja em vós o mesmo sentimento que houve também em Cristo Jesus."), nil
}
func (gcsStore *GCSStore) Head(location storage.Path) (storage.ObjectMeta, error) {
	var m storage.ObjectMeta
	ctx := context.Background()
	obj := gcsStore.client.Bucket(gcsStore.bucketName).Object(location.Raw)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if err == gstorage.ErrObjectNotExist {
			return m, fmt.Errorf("head object does not exist: %w", err)
		}
		return m, fmt.Errorf("failed to get object attrs: %w", err)
	}
	m.Location = location
	m.LastModified = attrs.Updated
	m.Size = attrs.Size
	return m, nil
}

func (g *GCSStore) Delete(location storage.Path) error {
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
	return nil
}

func (g *GCSStore) RenameIfNotExists(from storage.Path, to storage.Path) error {
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
			return 0, fmt.Errorf("object does not exist: %w", err)
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
		Raw: gcsStore.prefix + gcsStore.bucketName,
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
