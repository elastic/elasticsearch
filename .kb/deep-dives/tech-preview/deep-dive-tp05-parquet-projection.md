# Deep Dive: Parquet Column Projection (TP-05)

## Executive Summary

**The Parquet format reader does NOT push column projection down to the parquet-mr library.** It reads ALL columns from every row group and then filters at the ESQL level by only building blocks for the requested columns. This is a significant performance gap: for wide Parquet files (50+ columns) where only a few columns are queried, the reader deserializes far more data than necessary.

ORC, by contrast, correctly wires column projection into the ORC library's `Reader.Options.include()` mechanism.

---

## 1. ParquetFormatReader Analysis

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`

### The `read()` method (lines 88-122)

```java
@Override
public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
    // ...
    ParquetReadOptions options = ParquetReadOptions.builder().build();  // line 94 - NO projection config
    ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options);  // line 97

    // Get the FULL schema
    MessageType parquetSchema = fileMetaData.getSchema();  // line 101 - FULL schema, not projected
    List<Attribute> attributes = convertParquetSchemaToAttributes(parquetSchema);  // line 102

    // Filter attributes based on projection (lines 104-119)
    // This only filters the ESQL Attribute list - does NOT tell parquet-mr to skip columns
    List<Attribute> projectedAttributes;
    if (projectedColumns == null || projectedColumns.isEmpty()) {
        projectedAttributes = attributes;
    } else {
        // ... builds projectedAttributes from the column name list
    }

    return new ParquetPageIterator(reader, parquetSchema, projectedAttributes, batchSize, blockFactory);
    //                                     ^^^^^^^^^^^^^ FULL schema passed, not a projected sub-schema
}
```

**Key problem**: The `ParquetFileReader` is opened with the full schema (`parquetSchema`). The `projectedColumns` parameter is used only to filter the ESQL `Attribute` list, not to tell parquet-mr which columns to actually read from disk.

### ParquetPageIterator (inner class, lines 173-383)

```java
ParquetPageIterator(
    ParquetFileReader reader,
    MessageType parquetSchema,      // FULL schema
    List<Attribute> attributes,      // Projected attributes (ESQL level only)
    int batchSize,
    BlockFactory blockFactory
) {
    // ...
    this.parquetSchema = parquetSchema;                              // line 194 - stores FULL schema
    this.columnIO = new ColumnIOFactory().getColumnIO(parquetSchema); // line 197 - ColumnIO for ALL columns
}
```

At line 212, `readNextRowGroup()` is called:
```java
currentRowGroup = reader.readNextRowGroup();  // line 212 - reads ALL columns in the row group
```

At line 218, the `RecordReader` is created with the full schema:
```java
recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(parquetSchema));
// line 218 - reads ALL columns into Group objects
```

The Group-based API then materializes ALL column values into memory. Only at block creation time (lines 255-268 in `convertToPage`) does it select only the projected attributes:

```java
private Page convertToPage(List<Group> batch) {
    Block[] blocks = new Block[attributes.size()];  // attributes = projected only
    for (int col = 0; col < attributes.size(); col++) {
        Attribute attribute = attributes.get(col);
        String fieldName = attribute.name();
        blocks[col] = createBlock(batch, fieldName, dataType, rowCount);
    }
    return new Page(blocks);
}
```

**Bottom line**: All column data is deserialized into `Group` objects (line 238: `Group group = recordReader.read()`), then only the requested columns are extracted into ESQL Blocks. The non-projected column data is simply discarded.

---

## 2. ORC Comparison - Correct Projection Implementation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/src/main/java/org/elasticsearch/xpack/esql/datasource/orc/OrcFormatReader.java`

### The `read()` method (lines 87-136)

ORC correctly wires column projection at the library level:

```java
@Override
public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
    // ...
    TypeDescription schema = reader.getSchema();
    List<Attribute> attributes = convertOrcSchemaToAttributes(schema);

    List<Attribute> projectedAttributes;
    boolean[] include = null;           // <-- ORC include array
    if (projectedColumns == null || projectedColumns.isEmpty()) {
        projectedAttributes = attributes;
    } else {
        projectedAttributes = new ArrayList<>();
        // ... build name-to-index map ...

        // ORC include array: index 0 is the root struct, then 1..N for each field
        include = new boolean[schema.getMaximumId() + 1];  // line 108
        include[0] = true;                                  // line 109 - root struct
        // ... for each projected column:
        TypeDescription child = schema.getChildren().get(idx);
        include[child.getId()] = true;  // line 121 - mark column as included
    }

    Reader.Options readOptions = reader.options().rowBatchSize(batchSize);
    if (include != null) {
        readOptions.include(include);  // line 131 - PUSH PROJECTION INTO ORC LIBRARY
    }
    RecordReader rows = reader.rows(readOptions);  // line 133 - ORC only reads included columns
```

**Key difference**: ORC builds a `boolean[] include` array and passes it to `readOptions.include(include)` at line 131. The ORC library then only reads and deserializes the requested columns from disk. Non-projected columns are never touched.

The OrcPageIterator at line 256 then reads `batch.cols[fieldIndex]` which only contains vectors for the included columns (or null/zero vectors for excluded ones). This is fundamentally more efficient than the Parquet approach.

---

## 3. NDJSON Comparison

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-ndjson/src/main/java/org/elasticsearch/xpack/esql/datasource/ndjson/NdJsonPageDecoder.java`

NDJSON handles projection by skipping non-projected fields during JSON parsing (lines 222-229):

```java
while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
    // ...
    var childDecoder = this.children == null ? null : this.children.get(parser.currentName());
    parser.nextToken();
    if (childDecoder == null) {
        parser.skipChildren();  // line 226 - SKIP non-projected fields efficiently
    } else {
        childDecoder.decodeValue(parser);  // line 228 - only decode projected fields
    }
}
```

The `prepareSchema()` method (line 151) at the `NdJsonPageDecoder` constructor builds a tree of decoders only for the projected columns. Unknown/non-projected fields are skipped via `parser.skipChildren()`, which means the JSON parser advances past them without allocating memory for their values. This is efficient for NDJSON since it is a row-oriented format - you must scan all bytes regardless, but you avoid parsing and allocating non-projected field values.

---

## 4. projectedColumns Flow: End-to-End Trace

### Step 1: Logical Plan

`ExternalRelation` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java`) has full schema in `output`. Column pruning in the logical optimizer (`PruneColumns` at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/PruneColumns.java`) does NOT have a case for `ExternalRelation` - it falls through to the `default -> p` case at line 91. This means ExternalRelation always retains its full schema through logical optimization.

However, ESQL's standard `Project` nodes above the `ExternalRelation` will get pruned, and when converting logical to physical, the `ExternalSourceExec.attributes` list reflects only the columns needed by the query plan output.

### Step 2: Physical Plan

`ExternalSourceExec` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/physical/ExternalSourceExec.java`) stores `attributes` (line 60) which represent the projected output columns.

### Step 3: LocalExecutionPlanner

In `planExternalSource()` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/LocalExecutionPlanner.java`, lines 1223-1279):

```java
List<String> projectedColumns = new ArrayList<>();
for (Attribute attr : externalSource.output()) {   // line 1238
    projectedColumns.add(attr.name());              // line 1239
}
// ...
SourceOperatorContext operatorContext = SourceOperatorContext.builder()
    .projectedColumns(projectedColumns)   // line 1263 - set on context
    .attributes(externalSource.output())  // line 1264
    // ...
    .build();
```

### Step 4: OperatorFactoryRegistry

In `factory()` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/OperatorFactoryRegistry.java`, line 58), the `SourceOperatorContext` is passed to the factory. For file-based sources, this goes to `FileSourceFactory.operatorFactory()`.

### Step 5: FileSourceFactory

In `operatorFactory()` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java`, lines 118-151), the context's `attributes()` (NOT `projectedColumns()`) is passed to the `AsyncExternalSourceOperatorFactory`:

```java
return new AsyncExternalSourceOperatorFactory(
    storage, format, path,
    context.attributes(),    // line 141 - attributes (projected columns as Attribute objects)
    context.batchSize(),
    context.maxBufferSize(),
    context.executor(),
    // ...
);
```

**Note**: `context.projectedColumns()` is set on the `SourceOperatorContext` (step 3) but `FileSourceFactory` does NOT use it directly. Instead, `AsyncExternalSourceOperatorFactory` derives projectedColumns from `attributes` at runtime.

### Step 6: AsyncExternalSourceOperatorFactory

In `get()` (file: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperatorFactory.java`, lines 169-191):

```java
public SourceOperator get(DriverContext driverContext) {
    // ...
    VirtualColumnInjector injector = buildInjector(driverContext);
    List<String> projectedColumns = projectedColumns(injector);   // line 181
    // projectedColumns() at line 200: extracts names from attributes
    // ...
    formatReader.read(storageObject, projectedColumns, batchSize);  // line 303 - projectedColumns passed
}
```

### Step 7: FormatReader.read()

The `projectedColumns` list finally arrives at the `FormatReader.read()` method:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java`, line 58

```java
CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException;
```

### The Chain Break Point

**The chain does NOT break.** The `projectedColumns` list correctly arrives at `ParquetFormatReader.read()` (line 89). The problem is entirely within `ParquetFormatReader` itself: it receives the correct `projectedColumns` list but only uses it to filter the ESQL `Attribute` list - it never passes the projection information to the parquet-mr library.

---

## 5. Parquet-MR Column Projection API

The parquet-mr library supports column projection through **projected schemas**. The key mechanism:

### MessageType Schema Projection

`MessageType` (extends `GroupType`) supports creating a sub-schema containing only the desired columns. The approach used by tools like Spark and Hive:

1. **Build a projected MessageType** containing only the needed columns
2. **Pass it to `ParquetFileReader.setRequestedSchema(projectedSchema)`** or use it in `ColumnIOFactory.getColumnIO(projectedSchema, fileSchema)`

The specific API path:

```java
// Current code (reads ALL columns):
MessageType parquetSchema = fileMetaData.getSchema();
this.columnIO = new ColumnIOFactory().getColumnIO(parquetSchema);
currentRowGroup = reader.readNextRowGroup();
recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(parquetSchema));

// What it SHOULD do (read only projected columns):
MessageType fileSchema = fileMetaData.getSchema();
MessageType projectedSchema = new MessageType(fileSchema.getName(),
    projectedColumns.stream()
        .filter(fileSchema::containsField)
        .map(fileSchema::getType)
        .toList()
);
this.columnIO = new ColumnIOFactory().getColumnIO(projectedSchema, fileSchema);
//                                                ^^^^^^^^^^^^^^^^  ^^^^^^^^^^
//                                                requested schema  file schema
currentRowGroup = reader.readNextFilteredRowGroup();  // or readNextRowGroup()
recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(projectedSchema));
//                                                                               ^^^^^^^^^^^^^^^^
```

The key API is `ColumnIOFactory.getColumnIO(MessageType requestedSchema, MessageType fileSchema)` - this two-argument overload tells parquet-mr to only read the columns present in `requestedSchema`, while understanding the file's actual column layout from `fileSchema`.

Additionally, `readNextRowGroup()` reads all column chunks in the row group. For true I/O-level projection, `readNextFilteredRowGroup()` can be used, but the ColumnIO-based approach already ensures only the requested columns' data is decoded.

---

## 6. What Exactly Needs to Change

### Change 1: Build a projected MessageType in `ParquetFormatReader.read()` (lines 89-122)

**Current** (line 100-102):
```java
MessageType parquetSchema = fileMetaData.getSchema();
List<Attribute> attributes = convertParquetSchemaToAttributes(parquetSchema);
```

**Should become** (after the attribute filtering at line 119):
```java
MessageType fileSchema = fileMetaData.getSchema();
// ... existing attribute filtering logic ...

// Build projected schema for parquet-mr
MessageType projectedSchema;
if (projectedColumns == null || projectedColumns.isEmpty()) {
    projectedSchema = fileSchema;
} else {
    List<Type> projectedTypes = new ArrayList<>();
    for (String col : projectedColumns) {
        if (fileSchema.containsField(col)) {
            projectedTypes.add(fileSchema.getType(col));
        }
    }
    projectedSchema = new MessageType(fileSchema.getName(), projectedTypes);
}

return new ParquetPageIterator(reader, fileSchema, projectedSchema, projectedAttributes, batchSize, blockFactory);
```

### Change 2: Update ParquetPageIterator constructor (lines 186-199)

Add a `projectedSchema` field alongside the existing `parquetSchema` (which should be renamed to `fileSchema`):

```java
ParquetPageIterator(
    ParquetFileReader reader,
    MessageType fileSchema,         // full file schema
    MessageType projectedSchema,    // projected schema (only requested columns)
    List<Attribute> attributes,
    int batchSize,
    BlockFactory blockFactory
) {
    this.reader = reader;
    this.parquetSchema = projectedSchema;  // use projected schema for column IO
    this.attributes = attributes;
    this.batchSize = batchSize;
    // Use the two-arg overload to read only projected columns
    this.columnIO = new ColumnIOFactory().getColumnIO(projectedSchema, fileSchema);
    this.blockFactory = blockFactory;
}
```

### Change 3: Update readNextRowGroup consumer (lines 211-219)

The `GroupRecordConverter` should use the projected schema:

```java
recordReader = columnIO.getRecordReader(
    currentRowGroup,
    new GroupRecordConverter(projectedSchema)  // was: parquetSchema (full)
);
```

With the projected `columnIO`, the `readNextRowGroup()` call will still read all column chunks from the row group, but the `RecordReader` will only decode the projected columns. This is the standard parquet-mr approach.

For even better I/O efficiency (skipping reading non-projected column chunks entirely), the `ParquetFileReader` should be configured with a projected schema before reading:

```java
reader.setRequestedSchema(projectedSchema);  // Tell parquet-mr to skip non-projected column chunks
```

This should be called before the first `readNextRowGroup()`, ideally right after opening the reader.

### Summary of Changes

| Location | Line(s) | Current Behavior | Required Change |
|----------|---------|-------------------|-----------------|
| `ParquetFormatReader.read()` | 94-97 | Opens reader with no projection config | Build `projectedSchema` MessageType |
| `ParquetFormatReader.read()` | 121 | Passes full `parquetSchema` to iterator | Pass both `fileSchema` and `projectedSchema` |
| `ParquetPageIterator` constructor | 197 | `getColumnIO(parquetSchema)` (1-arg) | `getColumnIO(projectedSchema, fileSchema)` (2-arg) |
| `ParquetPageIterator.hasNext()` | 218 | `GroupRecordConverter(parquetSchema)` with full schema | `GroupRecordConverter(projectedSchema)` |
| `ParquetPageIterator` (new) | after line 97 | N/A | `reader.setRequestedSchema(projectedSchema)` for I/O-level skip |

### Impact Estimate

For a Parquet file with 100 columns where a query projects only 3:
- **Current**: Reads and deserializes all 100 columns per row group, discards 97
- **After fix**: Reads and deserializes only 3 columns per row group
- **Expected speedup**: Roughly proportional to column selectivity; for this example, ~30x less data deserialized per row group

---

## 7. Format Reader Projection Comparison Matrix

| Format | Receives projectedColumns? | Pushes projection to library? | Mechanism |
|--------|---------------------------|-------------------------------|-----------|
| **Parquet** | Yes (line 89) | **NO** - reads all columns | Filters at ESQL Attribute level only |
| **ORC** | Yes (line 87) | **YES** (line 131) | `Reader.Options.include(boolean[])` |
| **NDJSON** | Yes (line 43) | **YES** (line 226) | `parser.skipChildren()` for non-projected |
| **CSV** | Yes (line 95) | Partial | Reads all columns, builds blocks only for projected indices |

---

## 8. Additional Notes

### PruneColumns Does Not Handle ExternalRelation

The logical optimizer rule `PruneColumns` (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/PruneColumns.java`) has no case for `ExternalRelation` in its switch statement (line 80-92). It falls through to `default -> p`, meaning the `ExternalRelation`'s output attribute list is never pruned at the logical level.

However, this does not cause the column projection issue. The physical plan's `ExternalSourceExec.attributes` list is already correctly pruned to only the needed columns (the optimizer's `Project` nodes above handle this). The `projectedColumns` list that arrives at `FormatReader.read()` is correct -- the problem is purely in the Parquet reader not using it at the parquet-mr level.

### Test Coverage

The existing test `testReadWithColumnProjection` at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/test/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReaderTests.java` (line 172) verifies that the correct columns appear in the output Page, but it does NOT verify that non-projected columns were skipped at the I/O level. The test would still pass with or without library-level projection, since the end result (correct Page with projected columns) is the same -- only performance differs.
