package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
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
	"github.com/rivian/delta-go/storage/filestore"
	"google.golang.org/api/option"
)

func main() {
	dir := "table"
	os.MkdirAll(dir, 0766)

	tmpPath := storage.NewPath(dir)
	// store := filestore.New(tmpPath)
	// state := filestate.New(tmpPath, "_delta_log/_commit.state")
	// lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})
	// table := delta.NewTable(store, lock, state)

	// First write
	fileName := fmt.Sprintf("part-%s.parquet", uuid.New().String())
	filePath := filepath.Join(tmpPath.Raw, fileName)

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
	if _, err := writeParquet(data, filePath, arrowSchema); err != nil {
		fmt.Printf("error writing parquets go")
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

	// add, _, err := delta.NewAdd(store, storage.NewPath(fileName), make(map[string]string))
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

			store := filestore.New(tmpPath)
			state := filestate.New(storage.NewPath(dir), "_delta_log/_commit.state")
			lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})

			//Lock needs to be instantiated for each worker because it is passed by reference, so if it is not created different instances of tables would share the same lock
			table := delta.NewTable(store, lock, state)
			transaction := table.CreateTransaction(delta.NewTransactionOptions())

			//Make some data
			// data := generateRecord(arrowSchema)
			// fileName := fmt.Sprintf("part-%s.parquet", uuid.New().String())
			// filePath := filepath.Join(tmpPath.Raw, fileName)
			// if _, err := writeParquet(data, filePath, arrowSchema); err != nil {
			// 	fmt.Printf("error writing parquet: %v\n", err)
			// }
			// criando copia pra teste de mais de uma action por transaction
			// fileName1 := "copia-" + fileName
			// filePath1 := filepath.Join(tmpPath.Raw, fileName1)
			// if _, err := writeParquet(data, filePath1, arrowSchema); err != nil {
			// 	fmt.Printf("error writing parquet: %v\n", err)
			// }

			// add, _, err := delta.NewAdd(store, storage.NewPath(fileName), make(map[string]string))
			// if err != nil {
			// 	fmt.Printf("error in delta add: %v\n", err)
			// }
			// add1, _, err := delta.NewAdd(store, storage.NewPath(fileName1), make(map[string]string))
			// if err != nil {
			// 	fmt.Printf("error in delta add: %v\n", err)
			// }
			ts := int64(20250606)
			remove := delta.Remove{
				Path:                 "part-cf6cbb62-0b18-4e07-9e0f-0798537ad718.parquet",
				DeletionTimestamp:    &ts,
				DataChange:           true,
				ExtendedFileMetadata: false,
			}

			// transaction.AddAction(add)
			// transaction.AddAction(add1)
			transaction.AddAction(remove)
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
		}()
	}
	wg.Wait()
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

type payload struct {
	File *os.File
	Size int64
}

func writeParquet(data arrow.Record, filename string, schema *arrow.Schema) (*payload, error) {
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("error creating file with filename: %v", err)
		return nil, err
	}

	parquetProps := parquet.NewWriterProperties()
	arrowProps := pqarrow.NewArrowWriterProperties()
	buf := new(bytes.Buffer)
	writer, err := pqarrow.NewFileWriter(schema, buf, parquetProps, arrowProps)
	if err != nil {
		fmt.Printf("error creating pqarrow file writer: %v\n", err)
		return nil, err
	}

	if err := writer.Write(data); err != nil {
		fmt.Printf("error writing record to buffer: %v\n", err)
		return nil, err
	}

	// Close the writer to flush all data and write the footer
	if err := writer.Close(); err != nil {
		fmt.Printf("error closing parquet writer: %v\n", err)
		return nil, err
	}

	if _, err := file.Write(buf.Bytes()); err != nil {
		fmt.Printf("error writing buffer data into file: %v\n", err)
		return nil, err
	}

	info, _ := file.Stat()
	p := &payload{
		File: file,
		Size: info.Size(),
	}

	if err := file.Close(); err != nil {
		return nil, err
	}
	return p, nil
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

func (gcsStore *GCSStore) Put(ctx context.Context, path string, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	obj := gcsStore.client.Bucket(gcsStore.bucketName).Object(gcsStore.prefix + "/" + path)
	w := obj.NewWriter(ctx)

	_, err := w.Write(data)
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

