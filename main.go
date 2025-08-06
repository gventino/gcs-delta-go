package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"time"

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
)

func main() {
	// cpu profiling
	fCPU, err := os.Create("recorder_cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer fCPU.Close()
	if err := pprof.StartCPUProfile(fCPU); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	// delta table location (locally)
	dir := "local_delta"
	// giving writing permissions to the process
	os.MkdirAll(dir, 0766)

	// setting up path, store, state and lock
	tmpPath := storage.NewPath(dir)
	store := filestore.New(tmpPath)
	state := filestate.New(tmpPath, "_delta_log/_commit.state")
	lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})
	table := delta.NewTable(store, lock, state)

	// first write
	fileName := fmt.Sprintf("part-%s.parquet", uuid.New().String())
	filePath := filepath.Join(tmpPath.Raw, fileName)

	// parquet schema v
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
	// generating first parquet's data
	data := generateRecord(arrowSchema)

	// writing parquet
	if err := writeParquet(data, filePath, arrowSchema); err != nil {
		fmt.Printf("error writing parquets: %v\n", err)
		return
	}

	// creating delta schema
	// yep, u actually need a schema for the parquet and an equivalent schema to the delta table :/
	schema := delta.SchemaTypeStruct{
		Fields: []delta.SchemaField{
			{Name: "id", Type: delta.Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "name", Type: delta.String, Nullable: false, Metadata: make(map[string]any)},
			{Name: "age", Type: delta.Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "salary", Type: delta.Float, Nullable: false, Metadata: make(map[string]any)},
			{Name: "active", Type: delta.Boolean, Nullable: false, Metadata: make(map[string]any)},
		},
	}

	// here we're adding a record to the table during it's creation
	add, _, err := delta.NewAdd(store, storage.NewPath(fileName), make(map[string]string))
	if err != nil {
		fmt.Printf("error in delta add: %v\n", err)
		return
	}

	// delta table metadata and creation :D
	metadata := delta.NewTableMetaData("Test Table", "test description", new(delta.Format).Default(), schema, []string{}, make(map[string]string))
	err = table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{*add})
	if err != nil {
		fmt.Printf("error in table create: %v\n", err)
		return
	}

	// dumb multithread parquet writing to the delta table
	numThreads := 8
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

			// lock needs to be instantiated for each worker because it is passed by reference.
			// this is a very bad func name, we're creating a pointer to an existing table, not creating a new one
			table := delta.NewTable(store, lock, state)
			transaction := table.CreateTransaction(delta.NewTransactionOptions())

			/* 	DELTA WORKFLOW:
			* 1. get some data, here we're generating it, but u can get it from wherever u want (files, db, api, etc)
			* 2. create the parquet file with that data 
			 		 		(there are more optimal approaches, we're doing naive batches way here)
			* 3. create the add action
			* 4. add the action to the current transaction
			* 5. commit transaction
			*/
			var data arrow.Record
			var fileName, filePath string
			var err error
			for range 10 {
				data = generateRecord(arrowSchema)
				fileName = fmt.Sprintf("part-%s.parquet", uuid.New().String())
				filePath = filepath.Join(tmpPath.Raw, fileName)
				if err = writeParquet(data, filePath, arrowSchema); err != nil {
					fmt.Printf("error writing parquet: %v\n", err)
				}
				add, _, err := delta.NewAdd(store, storage.NewPath(fileName), make(map[string]string))
				if err != nil {
					fmt.Printf("error in delta add: %v\n", err)
				}
				transaction.AddAction(add)		
			}

			operation := delta.Write{Mode: delta.Append}
			appMetaData := make(map[string]any)
			appMetaData["test"] = 123

			transaction.SetOperation(operation)
			transaction.SetAppMetadata(appMetaData)
			v, err := transaction.Commit()
			if err != nil {
				fmt.Printf("error commiting transaction: %v", err)
			}

			// random checkpoints :P
			if rand.Intn(2) == 0 {
				checkpointLock := filelock.New(tmpPath, "_delta_log/_checkpoint.lock", filelock.Options{})
				checkpointed, err := table.CreateCheckpoint(checkpointLock, delta.NewCheckpointConfiguration(), v)
				if err != nil {
					fmt.Printf("\nerror creating checkpoint: %v", err)
				}
				fmt.Printf("\ncheckpoint created for version %d: %v", v, checkpointed)
			}
		}()
	}
	wg.Wait()

	// memory profiling
	fMEM, err := os.Create("recorder_mem.prof")
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer fMEM.Close()
	if err := pprof.WriteHeapProfile(fMEM); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}


/*
* this guy right here generate an arrow record
* with some random user data.
*/
func generateRecord(schema *arrow.Schema) arrow.Record {
	mem := memory.NewGoAllocator()

	// batches size
	batchSize := 1000000

	// random common data
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

	// creating builders for this batch
	idBuilder := array.NewInt64Builder(mem)
	nameBuilder := array.NewStringBuilder(mem)
	ageBuilder := array.NewInt32Builder(mem)
	salaryBuilder := array.NewFloat64Builder(mem)
	activeBuilder := array.NewBooleanBuilder(mem)

	// generating entries for this batch
	for i := 0; i < batchSize; i++ {
		id := int64(totalWritten + i + 1)
		firstName := firstNames[rand.Intn(len(firstNames))]
		lastName := lastNames[rand.Intn(len(lastNames))]
		name := firstName + " " + lastName
		age := int32(18 + rand.Intn(50))
		active := rand.Float32() > 0.1

		idBuilder.Append(id)
		nameBuilder.Append(name)
		ageBuilder.Append(age)
		activeBuilder.Append(active)

		if rand.Float32() < 0.1 {
			salaryBuilder.Append(1000)
		} else {
			salary := 2000.0 + rand.Float64()*13000.0
			salaryBuilder.Append(salary)
		}
	}

	// building the arrays
	idArray := idBuilder.NewArray()
	nameArray := nameBuilder.NewArray()
	ageArray := ageBuilder.NewArray()
	salaryArray := salaryBuilder.NewArray()
	activeArray := activeBuilder.NewArray()

	// creating the record
	record := array.NewRecord(schema, []arrow.Array{
		idArray, nameArray, ageArray, salaryArray, activeArray,
	}, int64(batchSize))

	return record
}

/*
* this one creates a parquet file from a arrow record
*/
func writeParquet(data arrow.Record, filename string, schema *arrow.Schema) error {
	// creating the file that will be used to the parquet building
	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("error creating file with filename: %v\n", err)
		return err
	}

	// default properties
	parquetProps := parquet.NewWriterProperties()
	arrowProps := pqarrow.NewArrowWriterProperties()

	// creating file writer
	writer, err := pqarrow.NewFileWriter(schema, file, parquetProps, arrowProps)
	if err != nil {
		fmt.Printf("error creating pqarrow file writer: %v\n", err)
		file.Close()
		return err
	}

	// it writes directly to the file, without bufferized writing
	if err := writer.Write(data); err != nil {
		fmt.Printf("error writing record to file: %v\n", err)
		writer.Close()
		file.Close()
		return err
	}

	// close the writer to flush all data and write the footer
	if err := writer.Close(); err != nil {
		fmt.Printf("error closing parquet writer: %v\n", err)
		file.Close()
		return err
	}
	return nil
}
