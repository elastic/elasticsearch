# Deep Dive: Arrow-Native Parquet Reading (TP-06)

## 1. Current Parquet Reader

### Library: parquet-mr (via `parquet-hadoop-bundle`)

The current `ParquetFormatReader` uses **Apache parquet-mr**, specifically the `parquet-hadoop-bundle` artifact at **version 1.16.0**.

**Dependency declaration:**
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/build.gradle`
- Lines 11-14 (version), line 36 (dependency):
```groovy
versions << [
  'hadoop'  : '3.4.2',
  'parquet' : '1.16.0',
]
// ...
implementation("org.apache.parquet:parquet-hadoop-bundle:${versions.parquet}")
```
- Hadoop dependencies are also required at runtime (lines 45-46):
```groovy
implementation('org.apache.hadoop:hadoop-client-api:3.4.2')
implementation('org.apache.hadoop:hadoop-client-runtime:3.4.2')
```

### Reading approach: Row-by-row via `GroupRecordConverter`

- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`
- The reader uses **parquet-mr's example API** (the least efficient API available):
  - Line 13: `import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;`
  - Line 18: `import org.apache.parquet.io.RecordReader;`
  - Line 218: `recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(parquetSchema));`
  - Lines 236-241: Reads one `Group` object per row:
    ```java
    for (int i = 0; i < rowsToRead; i++) {
        Group group = recordReader.read();
        if (group != null) {
            batch.add(group);
            rowsRemainingInGroup--;
        }
    }
    ```
  - Lines 255-269: Then iterates each column across all rows to build blocks:
    ```java
    private Page convertToPage(List<Group> batch) {
        int rowCount = batch.size();
        Block[] blocks = new Block[attributes.size()];
        for (int col = 0; col < attributes.size(); col++) {
            Attribute attribute = attributes.get(col);
            String fieldName = attribute.name();
            DataType dataType = attribute.dataType();
            blocks[col] = createBlock(batch, fieldName, dataType, rowCount);
        }
        return new Page(blocks);
    }
    ```

### Key inefficiencies:

1. **Object materialization**: Every Parquet row is materialized as a `Group` object (line 238). This is parquet-mr's "example" API, intended for demos, not production.
2. **Row-oriented intermediate**: Even though Parquet is columnar and ESQL Blocks are columnar, the code goes through a row-oriented intermediate (`List<Group>`).
3. **Double traversal**: Data is first read row-by-row into `Group` objects, then traversed column-by-column to create Blocks.
4. **String copies**: Line 370-371: `String value = group.getString(fieldName, 0); byte[] bytes = value.getBytes(StandardCharsets.UTF_8);` -- creates a Java String from Parquet binary, then re-encodes to bytes.
5. **No column projection at Parquet level**: While `projectedColumns` is accepted (line 89), the actual Parquet file still reads ALL columns in each row group (no `requestedSchema` set on the reader). The projection only happens when building blocks.
6. **ByteBuffer copy in SeekableInputStream**: File `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetStorageObjectAdapter.java`, lines 191-205: The `read(ByteBuffer)` implementation allocates a temporary `byte[]` for every read, then copies into the ByteBuffer.

---

## 2. Arrow Java in the Dependency Tree

Apache Arrow Java **is** in the dependency tree at **version 18.3.0**, used by three modules:

### Arrow module (core ESQL arrow library)
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/arrow/build.gradle`
- Lines 16-21:
```groovy
implementation('org.apache.arrow:arrow-vector:18.3.0')
implementation('org.apache.arrow:arrow-format:18.3.0')
implementation('org.apache.arrow:arrow-memory-core:18.3.0')
runtimeOnly('org.apache.arrow:arrow-memory-unsafe:18.3.0')
```
- This module provides `BlockConverter` (Block -> Arrow) and `ArrowToBlockConverter` (Arrow -> Block).

### gRPC/Flight plugin
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/build.gradle`
- Lines 31-47:
```groovy
implementation('org.apache.arrow:flight-core:18.3.0')
implementation('org.apache.arrow:arrow-vector:18.3.0')
implementation('org.apache.arrow:arrow-memory-core:18.3.0')
runtimeOnly('org.apache.arrow:arrow-memory-unsafe:18.3.0')
```

### Iceberg plugin (compileOnly)
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/build.gradle`
- Lines 115-116:
```groovy
compileOnly("org.apache.arrow:arrow-vector:${versions.arrow}")
compileOnly("org.apache.arrow:arrow-memory-core:${versions.arrow}")
```
- The `iceberg-arrow` artifact (line 81) provides `org.apache.iceberg.arrow.vectorized.ArrowReader` but excludes Arrow transitive deps (line 93: `exclude group: 'org.apache.arrow'`).

### NOT in the tree:
- `arrow-dataset` -- not present anywhere in the repository.
- `arrow-c-data` -- not present.

---

## 3. Iceberg's ArrowReader

### Source: `org.apache.iceberg.arrow.vectorized.ArrowReader`

- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java`
- Line 18: `import org.apache.iceberg.arrow.vectorized.ArrowReader;`
- This comes from the **`iceberg-arrow`** artifact (version 1.10.1, per `build-tools-internal/version.properties` line 27).

### How it works:

1. **Construction** (line 164):
   ```java
   ArrowReader arrowReader = new ArrowReader(scan, pageSize, /* reuseContainers */ false);
   ```
   - Takes an Iceberg `TableScan`, batch size, and reuse flag.

2. **Iteration** (line 170):
   ```java
   CloseableIterator<ColumnarBatch> batchIterator = arrowReader.open(tasks);
   ```
   - Returns `ColumnarBatch` objects (Iceberg's wrapper around Arrow vectors).

3. **ColumnarBatch -> VectorSchemaRoot conversion** (lines 242-258):
   ```java
   private VectorSchemaRoot convertColumnarBatchToVectorSchemaRoot(ColumnarBatch batch) {
       int numRows = batch.numRows();
       int numColumns = batch.numCols();
       List<FieldVector> fieldVectors = new ArrayList<>(numColumns);
       for (int col = 0; col < numColumns; col++) {
           ColumnVector columnVector = batch.column(col);
           FieldVector fieldVector = columnVector.getFieldVector();
           fieldVectors.add(fieldVector);
       }
       return new VectorSchemaRoot(fieldVectors);
   }
   ```
   - Each `ColumnVector` wraps an Arrow `FieldVector` -- the underlying Arrow vectors are directly extracted.

### What it produces:

- **`VectorSchemaRoot`** (Arrow's batch format) containing `FieldVector` instances.
- Under the hood, Iceberg's ArrowReader uses **parquet-mr's vectorized column reader** to read Parquet column chunks directly into Arrow memory buffers. This is the key -- it reads columnar data into columnar Arrow buffers without materializing row objects.

### Current status: NOT WIRED for actual data reading

- Line 99-106: The `get(DriverContext)` method throws `UnsupportedOperationException`:
  ```java
  throw new UnsupportedOperationException(
      "Direct Iceberg source operator creation is not yet supported. "
          + "Use the generic async operator factory via OperatorFactoryRegistry."
  );
  ```
- The `IcebergDataSourcePlugin` (file `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergDataSourcePlugin.java`) only provides `TableCatalog` for metadata -- no `operatorFactory()`.
- The `createIcebergTableReader()` method (lines 127-173) is implemented but never called from the actual compute pipeline.

---

## 4. ArrowToBlockConverter

- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowToBlockConverter.java`
- 299 lines, handles 9 Arrow types.

### Conversion approach: **Element-by-element with builder API**

Every converter follows the same pattern -- iterate each value index and append to a BlockBuilder:

```java
// Example: FromFloat64 (lines 113-130)
public Block convert(FieldVector vector, BlockFactory factory) {
    Float8Vector f8v = (Float8Vector) vector;
    int valueCount = f8v.getValueCount();
    try (DoubleBlock.Builder builder = factory.newDoubleBlockBuilder(valueCount)) {
        for (int i = 0; i < valueCount; i++) {
            if (f8v.isNull(i)) {
                builder.appendNull();
            } else {
                builder.appendDouble(f8v.get(i));
            }
        }
        return builder.build();
    }
}
```

### Types handled (lines 70-83):
| Arrow Type | Converter Class | ESQL Block Type |
|---|---|---|
| FLOAT4 | `FromFloat32` | DoubleBlock (widened) |
| FLOAT8 | `FromFloat64` | DoubleBlock |
| BIGINT | `FromInt64` | LongBlock |
| INT | `FromInt32` | IntBlock |
| BIT | `FromBoolean` | BooleanBlock |
| VARCHAR | `FromVarChar` | BytesRefBlock |
| VARBINARY | `FromVarBinary` | BytesRefBlock |
| TIMESTAMPMICRO | `FromTimestampMicro` | LongBlock (micros/1000) |
| TIMESTAMPMICROTZ | `FromTimestampMicroTZ` | LongBlock (micros/1000) |

### Is it vectorized? **No.**

Every converter reads values one at a time via `vector.get(i)` and appends to a builder. This is **not** a bulk memory copy. The underlying ESQL blocks are backed by flat arrays (confirmed: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/main/generated-src/org/elasticsearch/compute/data/LongArrayVector.java` line 37: `private final long[] values;`), so a zero-copy or bulk-copy path is theoretically possible for fixed-width types.

### Where it's used:

The `ArrowToBlockConverter` is in the `arrow` module. It is NOT directly used by `FlightTypeMapping` (which has its own conversion code). It exists as a utility for any code that works with Arrow vectors. Currently its primary consumer would be the Iceberg path (once wired).

---

## 5. FlightTypeMapping

- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightTypeMapping.java`
- 144 lines.

### How Flight converts Arrow data: **Element-by-element, duplicated logic**

`FlightTypeMapping.toBlock()` (lines 54-124) converts Arrow `FieldVector` to ESQL `Block` using the same element-by-element pattern as `ArrowToBlockConverter`, but with **completely duplicated code** using `instanceof` checks rather than the converter factory:

```java
static Block toBlock(FieldVector vector, int rowCount, BlockFactory blockFactory) {
    if (vector instanceof IntVector intVec) { ... }
    else if (vector instanceof BigIntVector bigIntVec) { ... }
    else if (vector instanceof Float8Vector float8Vec) { ... }
    else if (vector instanceof VarCharVector varCharVec) { ... }
    else if (vector instanceof BitVector bitVec) { ... }
    else if (vector instanceof TimeStampMilliVector tsVec) { ... }
    throw new IllegalArgumentException(...);
}
```

### Differences from ArrowToBlockConverter:
1. **No Float4 support** -- FlightTypeMapping only handles Float8, not Float4.
2. **TimeStampMilli** -- Flight uses millisecond timestamps (line 111: `TimeStampMilliVector`), whereas `ArrowToBlockConverter` handles microsecond timestamps (`TimeStampMicroVector`, `TimeStampMicroTZVector`).
3. **No VARBINARY** -- Flight doesn't handle binary data.
4. **Not using ArrowToBlockConverter** -- This is duplicated code that could be unified.

### Usage in FlightResultCursor:
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightResultCursor.java`
- Lines 47-54:
```java
public Page next() {
    VectorSchemaRoot root = stream.getRoot();
    int rowCount = root.getRowCount();
    Block[] blocks = new Block[attributes.size()];
    for (int col = 0; col < attributes.size(); col++) {
        blocks[col] = FlightTypeMapping.toBlock(root.getVector(col), rowCount, blockFactory);
    }
    hasNextBatch = advance();
    return new Page(rowCount, blocks);
}
```

---

## 6. iceberg-parquet Dependency

### Present in the dependency tree

- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/build.gradle`
- Lines 64-79:
```groovy
implementation("org.apache.iceberg:iceberg-parquet:${versions.iceberg}") {
    exclude group: 'org.apache.parquet', module: 'parquet-hadoop'
    exclude group: 'org.apache.parquet', module: 'parquet-column'
    // ... many exclusions
}
```

### What it provides:

`iceberg-parquet` (version 1.10.1) provides Iceberg's Parquet integration layer:
- `org.apache.iceberg.parquet.Parquet` -- factory for Parquet read/write builders
- `org.apache.iceberg.parquet.ParquetReader` -- Iceberg's own Parquet reader
- Internal integration with parquet-mr's column readers

### Vectorized reading via `iceberg-arrow`:

The **vectorized reading** comes from the separate `iceberg-arrow` artifact (lines 81-98):
```groovy
implementation("org.apache.iceberg:iceberg-arrow:${versions.iceberg}") {
    exclude group: 'org.apache.arrow'
    // ...
}
```

This provides:
- `org.apache.iceberg.arrow.vectorized.ArrowReader` -- the vectorized batch reader
- `org.apache.iceberg.arrow.vectorized.ColumnarBatch` -- batch container
- `org.apache.iceberg.arrow.vectorized.ColumnVector` -- column wrapper with `getFieldVector()`

Under the hood, `iceberg-arrow`'s ArrowReader uses `iceberg-parquet`'s internal column reader infrastructure but reads directly into Arrow memory buffers (via `ArrowBuf`), bypassing parquet-mr's `Group` materialization entirely.

---

## 7. What Would an Arrow-Native Parquet Reader Look Like?

### Option A: Use Iceberg's ArrowReader standalone (without Iceberg metadata)

**Feasibility: Possible but awkward.**

Iceberg's `ArrowReader` constructor requires a `TableScan` and `CombinedScanTask`, which are Iceberg-specific concepts. You cannot just point it at a bare Parquet file URL. It would require:
1. Creating a fake Iceberg table from a Parquet file (wrapping the file as if it were an Iceberg table with one data file).
2. Providing a `FileIO` that delegates to `StorageObject`/`StorageProvider`.

This is the approach partially shown in `IcebergSourceOperatorFactory.createIcebergTableReader()` (lines 127-173), but it creates a full Iceberg `Table` with `S3FileIO`, `StaticTableOperations`, etc. Heavy overhead for a bare Parquet file.

**Verdict: Not recommended for bare Parquet files.**

### Option B: Use parquet-mr's `VectorizedParquetRecordReader` / `ColumnReader` API

parquet-mr (1.16.0) includes a `VectorizedParquetRecordReader` (used internally by Spark) but it's not a public API and produces Spark-specific `ColumnarBatch` objects, not Arrow vectors.

However, parquet-mr 1.16.0 does provide lower-level `ColumnReader` APIs that read column chunks as typed arrays. A custom reader could:
1. Use `ParquetFileReader` (already used) to get `PageReadStore` per row group.
2. Use `ColumnReadStoreImpl` instead of `GroupRecordConverter` to read individual columns directly.
3. Copy column data directly into ESQL block arrays.

This avoids the `Group` materialization overhead while staying within parquet-mr.

**Verdict: Medium effort, significant improvement, no new dependencies.**

### Option C: Use `arrow-dataset` module (Apache Arrow Java)

The `arrow-dataset` module (part of Arrow Java 18.3.0) provides `ParquetDatasetFactory` which can read Parquet files directly into Arrow `VectorSchemaRoot` batches. This is the most direct path:

```java
// Conceptual API:
DatasetFactory factory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
    FileFormat.PARQUET, "s3://bucket/data.parquet");
Dataset dataset = factory.finish();
Scanner scanner = dataset.newScan(scanOptions);
ArrowReader reader = scanner.scanBatches();
while (reader.loadNextBatch()) {
    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    // convert to ESQL blocks
}
```

**Problem**: `arrow-dataset` requires Arrow's JNI/C++ library (`libarrow_dataset_jni.so`), which is a native library that must be shipped for each platform. This adds significant packaging complexity (Linux x86_64, Linux aarch64, macOS x86_64, macOS aarch64, Windows).

**Verdict: Most performant, but hardest to package. Not practical for initial implementation.**

### Option D: Write a custom Arrow-native Parquet reader using parquet-mr column APIs + direct Arrow buffer population

This is the most likely practical approach:

1. Use `ParquetFileReader` + `PageReadStore` (already in the codebase).
2. Replace `GroupRecordConverter` + `RecordReader<Group>` with direct column chunk reading.
3. For each column in the projected schema:
   - Get the `ColumnChunkPageReader` from `PageReadStore`.
   - Read pages and decode values directly into ESQL block arrays.
4. For fixed-width types (INT32, INT64, FLOAT, DOUBLE, BOOLEAN): read decoded values directly into the backing `long[]`/`int[]`/`double[]` arrays.
5. For variable-width types (BINARY/STRING): read into `BytesRefBlock.Builder` with minimal copies.

### Recommended implementation path:

**Phase 1 (MVP TP):** Replace `GroupRecordConverter` with column-oriented reading using parquet-mr's `ColumnReader` API.
- Eliminates `Group` object allocation (one per row).
- Eliminates row-to-column transposition.
- Eliminates `String` intermediate for binary fields.
- Adds real column projection at the Parquet level (set `requestedSchema`).
- Stays within existing parquet-mr 1.16.0 dependency.
- Expected improvement: 2-5x for wide schemas, significant memory reduction.

**Phase 2 (MVP GA or Post-MVP):** Either:
- Wire Iceberg's ArrowReader for Iceberg tables (it already produces Arrow vectors).
- Consider `arrow-dataset` JNI for bare Parquet files if native library packaging is solved.

---

## 8. Performance Implications

### Current inefficiencies (quantified by data path):

```
Parquet column pages on disk
  -> parquet-mr page decoder (decompresses, decodes encoding)
    -> GroupRecordConverter assembles Group objects (1 per row)          [WASTE: object allocation]
      -> Group stored in List<Group> (accumulates batch)                [WASTE: row-oriented buffer]
        -> iterate rows per column to call group.getLong(name, 0)       [WASTE: field lookup by name per value]
          -> BlockBuilder.appendLong()                                  [copy into block array]
            -> Block (flat long[])
```

**Unnecessary copies and allocations:**

1. **Group object allocation**: One `Group` object per row, each containing a `Map<String, List<Object>>` structure internally. For 1M rows, that's 1M Group objects + millions of boxed primitives.

2. **Row-to-column transposition**: Data is read column-by-column from Parquet (good), materialized as rows (bad), then iterated column-by-column again (redundant).

3. **Field lookup by name**: `group.getBoolean(fieldName, 0)` does a name-to-index lookup on every value access (line 309: `group.getBoolean(fieldName, 0)`). For N rows x M columns, that's N*M hash lookups.

4. **String encoding roundtrip**: For BINARY/STRING columns (lines 370-371): Parquet bytes -> Java String -> UTF-8 bytes -> BytesRef. The Parquet binary is already UTF-8 bytes; creating a Java String is pure waste.

5. **ByteBuffer temp array**: In `ParquetStorageObjectAdapter.StorageObjectSeekableInputStream.read(ByteBuffer)` (lines 191-205), every `read(ByteBuffer)` allocates a `byte[]` temp buffer and copies through it. Parquet-mr's page reader calls `readFully(ByteBuffer)` extensively.

### What Arrow-native reading eliminates:

```
Parquet column pages on disk
  -> decoder reads directly into Arrow ArrowBuf (off-heap)
    -> ArrowToBlockConverter copies into Block (flat long[])
      -> Block (flat long[])
```

Or with the recommended parquet-mr column reader approach:

```
Parquet column pages on disk
  -> parquet-mr page decoder (decompresses, decodes encoding)
    -> ColumnReader reads typed values
      -> directly populate Block array (flat long[])
        -> Block (flat long[])
```

### Estimated savings by inefficiency:

| Inefficiency | Current Cost | After Fix |
|---|---|---|
| Group object allocation | ~200 bytes/row overhead | Eliminated |
| Row-to-column transposition | O(rows * cols) with poor cache locality | O(rows) per column, sequential access |
| Field lookup by name | Hash lookup per value access | Index-based direct access |
| String encoding roundtrip | 2 copies per string value | 1 copy (or zero with Arrow) |
| ByteBuffer temp array | 1 extra copy per page read | Direct buffer read |
| Overall throughput | Baseline | Estimated 2-5x improvement for wide schemas, 1.5-3x for narrow schemas |

### Memory implications:

- **Current**: Peak memory = batch of Group objects + block arrays being built. Groups hold all columns even for projected queries.
- **Arrow-native**: Peak memory = one column's Arrow buffer + one block being built. Columns processed one at a time.
- **Column reader**: Peak memory = column decoder state + block array. Minimal overhead.

### Note on zero-copy potential:

ESQL blocks use **on-heap** `long[]`/`int[]`/`double[]` arrays (confirmed in `LongArrayVector`, line 37). Arrow vectors use **off-heap** `ArrowBuf` memory. True zero-copy between Arrow and ESQL is not possible without either:
1. Making ESQL blocks support off-heap memory (major compute engine change).
2. Using Arrow vectors directly in the compute pipeline (even larger change).

For MVP, the practical goal is **single-copy**: decode Parquet column data once, copy once into ESQL block arrays. The current implementation does **at least 3 copies** for most types.

---

## Summary Table

| Component | File | Status |
|---|---|---|
| Current Parquet reader | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java` | Row-by-row via GroupRecordConverter, very inefficient |
| parquet-mr dependency | `parquet-hadoop-bundle:1.16.0` declared in `esql-datasource-parquet/build.gradle` line 36 | Present, used for current reader |
| Arrow Java 18.3.0 | `esql/arrow/build.gradle` lines 16-21 | Present in arrow module, gRPC plugin, Iceberg plugin |
| ArrowToBlockConverter | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/ArrowToBlockConverter.java` | Element-by-element, 9 types, not vectorized |
| FlightTypeMapping | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightTypeMapping.java` | Duplicated Arrow->Block logic, 6 types |
| Iceberg ArrowReader | Used in `IcebergSourceOperatorFactory` (line 164), from `iceberg-arrow:1.10.1` | Implemented but NOT wired -- `get()` throws UnsupportedOperationException |
| iceberg-parquet | `iceberg-parquet:1.10.1` in `esql-datasource-iceberg/build.gradle` line 64 | Present, provides Parquet integration for Iceberg |
| arrow-dataset | Not in dependency tree | Would require JNI native library |
| BlockConverter (Block->Arrow) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/arrow/src/main/java/org/elasticsearch/xpack/esql/arrow/BlockConverter.java` | Element-by-element, well-structured with TODO comments about bulk copy |
