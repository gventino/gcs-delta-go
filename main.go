package main

import (
	"context"
	"fmt"
	"log"
	"main/gcs"
	gcs_filelock "main/gcs/filelock"
	gcs_filestate "main/gcs/filestate"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/google/uuid"
	"github.com/rivian/delta-go"

	"github.com/rivian/delta-go/lock/filelock"
	"github.com/rivian/delta-go/storage"
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
	gcs, err := gcs.NewGCSStore(context.Background(), "data", "kanastra-deltago-test", "application_default_credentials.json")
	if err != nil {
		fmt.Printf("error creating gcs store: %v\n", err)
		return
	}

	tmpPath := storage.NewPath(dir)
	baseURI := gcs.BaseURI()
	state := gcs_filestate.New(baseURI, "_delta_log/_commit.state")
	state.Store = gcs // always remember to set this guy

	lock := gcs_filelock.New(baseURI, "_delta_log/_commit.lock", filelock.Options{})
	lock.Store = gcs

	table := delta.NewTable(gcs, lock, state)

	// First write
	fileName := fmt.Sprintf("part-%s.parquet", uuid.New().String())
	writingPath := filepath.Join(tmpPath.Raw, fileName)
	filePath := filepath.Join("data/", tmpPath.Raw, fileName)

	// Generate some data
	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "salary", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			// {Name: "sex", Type: arrow.BinaryTypes.String, Nullable: true},
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
	schema := delta.SchemaTypeStruct{
		Fields: []delta.SchemaField{
			{Name: "id", Type: delta.Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "name", Type: delta.String, Nullable: false, Metadata: make(map[string]any)},
			{Name: "age", Type: delta.Integer, Nullable: false, Metadata: make(map[string]any)},
			{Name: "salary", Type: delta.Float, Nullable: false, Metadata: make(map[string]any)},
			{Name: "active", Type: delta.Boolean, Nullable: false, Metadata: make(map[string]any)},
			// {Name: "sex", Type: delta.String, Nullable: true, Metadata: make(map[string]any)},
		},
	}

	add, _, err := delta.NewAdd(gcs, storage.NewPath(filePath), make(map[string]string))
	if err != nil {
		fmt.Printf("error in delta add: %v\n", err)
		return
	}

	metadata := delta.NewTableMetaData("Test Table", "test description", new(delta.Format).Default(), schema, []string{}, make(map[string]string))
	err = table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{*add})
	if err != nil {
		fmt.Printf("error in table create: %v\n", err)
		return
	}

	// numThreads := 2
	// wg := new(sync.WaitGroup)
	// for i := 0; i < numThreads; i++ {
	// wg.Add(1)

	// go func() {
	// 	defer wg.Done()

	// store := filestore.New(tmpPath)
	baseURI = gcs.BaseURI()
	state = gcs_filestate.New(baseURI, "_delta_log/_commit.state")
	state.Store = gcs // always remember to set this guy

	lock = gcs_filelock.New(baseURI, "_delta_log/_commit.lock", filelock.Options{})
	lock.Store = gcs

	//Lock needs to be instantiated for each worker because it is passed by reference, so if it is not created different instances of tables would share the same lock
	table, err = delta.OpenTable(gcs, lock, state)
	if err != nil {
		fmt.Printf("error opening table: %v\n", err)
		return
	}
	table.State.CurrentMetadata.Schema = schema
	transaction := table.CreateTransaction(delta.NewTransactionOptions())

	//Make some data
	data = generateRecord(arrowSchema)
	fileName = fmt.Sprintf("teste-%s.parquet", uuid.New().String())
	filePath = filepath.Join("data/", tmpPath.Raw, fileName)
	writingPath = filepath.Join(tmpPath.Raw, fileName)
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

	add, _, err = delta.NewAdd(gcs, storage.NewPath(filePath), make(map[string]string))
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

	if true {
		// We don't actually need a lock here since we are only writing a checkpoint for a version that this process has committed
		checkpointLock := gcs_filelock.New(baseURI, "_delta_log/_commit.lock", filelock.Options{})
		checkpointLock.Store = gcs
		checkpointed, err := table.CreateCheckpoint(checkpointLock, delta.NewCheckpointConfiguration(), v)
		if err != nil {
			fmt.Printf("error creating checkpoint: %v", err)
		}
		fmt.Printf("checkpoint created for version %d: %v", v, checkpointed)
	}

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

// writeParquetToGCS escreve um arquivo parquet diretamente no GCS
func writeParquetToGCS(data arrow.Record, gcs *gcs.GCSStore, path string, schema *arrow.Schema) error {
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
