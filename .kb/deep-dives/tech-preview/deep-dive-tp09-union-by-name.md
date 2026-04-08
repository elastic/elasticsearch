# Deep Dive: Schema Inference - Union-by-Name (TP-09)

## 1. SchemaResolution Enum

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java`
**Lines**: 37-44

```java
enum SchemaResolution {
    /** Use the schema from the first file; ignore differences in subsequent files. */
    FIRST_FILE_WINS,
    // TODO: implement strict schema validation across files
    STRICT,
    // TODO: implement union-by-name schema merging across files
    UNION_BY_NAME
}
```

**Findings**:
- The enum has three values: `FIRST_FILE_WINS`, `STRICT`, and `UNION_BY_NAME`.
- `STRICT` and `UNION_BY_NAME` are **TODO stubs only** -- they are declared but never used anywhere in the codebase. There is no code that reads or branches on these values.
- The default method at line 46-48 always returns `FIRST_FILE_WINS`:

```java
default SchemaResolution defaultSchemaResolution() {
    return SchemaResolution.FIRST_FILE_WINS;
}
```

- No `FormatReader` implementation overrides `defaultSchemaResolution()`.
- No code anywhere calls `defaultSchemaResolution()` except the test at `ExternalSourceResolverTests.java:235` which asserts the default is `FIRST_FILE_WINS`.

---

## 2. ExternalSourceResolver.resolveMultiFileSource()

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`
**Lines**: 144-182

```java
private ExternalSourceResolution.ResolvedSource resolveMultiFileSource(
    String path,
    Map<String, Object> config,
    @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
    boolean hivePartitioning
) throws Exception {
    StoragePath storagePath = StoragePath.of(path);
    StorageProviderRegistry registry = dataSourceModule.storageProviderRegistry();

    StorageProvider provider;
    if (config != null && config.isEmpty() == false) {
        provider = registry.createProvider(storagePath.scheme(), settings, config);
    } else {
        provider = registry.provider(storagePath);
    }

    FileSet fileSet;
    if (path.indexOf(',') >= 0) {
        fileSet = GlobExpander.expandCommaSeparated(path, provider, hints, hivePartitioning);
    } else {
        fileSet = GlobExpander.expandGlob(path, provider, hints, hivePartitioning);
    }

    if (fileSet.isEmpty()) {
        throw new IllegalArgumentException("Glob pattern matched no files: " + path);
    }

    // *** KEY LINE: Only reads schema from FIRST file ***
    StoragePath firstFile = fileSet.files().get(0).path();
    SourceMetadata metadata = resolveSingleSource(firstFile.toString(), config);

    ExternalSourceMetadata extMetadata = wrapAsExternalSourceMetadata(metadata, config);

    PartitionMetadata partitionMetadata = fileSet.partitionMetadata();
    if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
        extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
    }

    return new ExternalSourceResolution.ResolvedSource(extMetadata, fileSet);
}
```

**Exact flow for multi-file schema resolution**:
1. Expand the glob/comma-separated path into a `FileSet` of matched files (line 161-165).
2. If no files matched, throw (line 167-169).
3. **Take `fileSet.files().get(0).path()`** -- the first file only (line 171).
4. Call `resolveSingleSource(firstFile.toString(), config)` to get its metadata (line 172).
5. Wrap metadata and optionally enrich with Hive partition columns (lines 174-179).
6. Return -- **no other files' schemas are examined**.

The method does **not** consult `FormatReader.defaultSchemaResolution()` at all. The `SchemaResolution` enum is completely disconnected from the actual resolution code.

---

## 3. ExternalSourceResolver.resolveSingleSource()

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`
**Lines**: 195-235

```java
private SourceMetadata resolveSingleSource(String path, Map<String, Object> config) {
    // Early scheme validation
    try {
        StoragePath parsed = StoragePath.of(path);
        DataSourceCapabilities capabilities = dataSourceModule.capabilities();
        if (capabilities != null && capabilities.supportsScheme(parsed.scheme()) == false) {
            throw new UnsupportedSchemeException(...);
        }
    } catch (UnsupportedSchemeException e) {
        throw e;
    } catch (IllegalArgumentException e) {
        // Path parsing failed -- let the factory iteration handle it
    }

    Exception lastFailure = null;
    for (ExternalSourceFactory factory : dataSourceModule.sourceFactories().values()) {
        if (factory.canHandle(path)) {
            try {
                return factory.resolveMetadata(path, config);
            } catch (Exception e) {
                LOGGER.debug("Factory [{}] claimed path [{}] but failed: {}", factory.type(), path, e.getMessage());
                lastFailure = e;
            }
        }
    }
    // ... error handling
}
```

**Flow**:
1. Validate scheme is supported.
2. Iterate all registered `ExternalSourceFactory` instances.
3. Find the first that `canHandle(path)` returns true.
4. Call `factory.resolveMetadata(path, config)` -- this delegates to the format-specific reader.

For file-based sources, `FileSourceFactory.resolveMetadata()` is called, which at line 92-110 of `FileSourceFactory.java`:
```java
public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
    StoragePath storagePath = StoragePath.of(location);
    StorageProvider provider = ...;
    StorageObject storageObject = provider.newObject(storagePath);
    FormatReader reader = formatRegistry.byExtension(storagePath.objectName());
    return reader.metadata(storageObject);
}
```

This calls `FormatReader.metadata(StorageObject)` which returns a `SourceMetadata` containing the schema.

---

## 4. NdJsonSchemaInferrer

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-ndjson/src/main/java/org/elasticsearch/xpack/esql/datasource/ndjson/NdJsonSchemaInferrer.java`

### Type Coercion / Widening Logic (lines 201-227)

```java
DataType resolveType() {
    if (types.isEmpty()) {
        return DataType.UNSUPPORTED;
    }
    if (types.size() == 1) {
        return types.iterator().next();
    }
    // Multiple types - for now, use the widest type
    // TODO: Create MultiTypeEsField for proper union type support
    if (types.contains(DataType.DATETIME)) {
        return DataType.DATETIME;
    }
    if (types.contains(DataType.KEYWORD)) {
        return DataType.KEYWORD;
    }
    if (types.contains(DataType.DOUBLE)) {
        return DataType.DOUBLE;
    }
    if (types.contains(DataType.LONG)) {
        return DataType.LONG;
    }
    if (types.contains(DataType.INTEGER)) {
        return DataType.INTEGER;
    }
    return types.iterator().next();
}
```

**Priority order** (highest priority = wins when present):
1. `DATETIME` (if any value parsed as ISO-8601 date)
2. `KEYWORD` (if any value was a string that didn't parse as date)
3. `DOUBLE` (float values)
4. `LONG` (integers exceeding int range)
5. `INTEGER` (small integers)
6. Fallback: first element of the `EnumSet`

**Note on the widening logic**: The priority list is unusual. `DATETIME` beats everything because string values that look like ISO-8601 timestamps get `DATETIME`, and strings that do not get `KEYWORD`. When both `DATETIME` and `KEYWORD` are present (some strings are dates, some are not), `DATETIME` wins -- which will cause parse failures at read time. The `TODO` at line 210 acknowledges this: "Create MultiTypeEsField for proper union type support."

### Sampling (lines 55-57, 62-96)
- Default sample size: 100 lines (`DEFAULT_SAMPLE_SIZE = 100`, line 50).
- Opens the stream, reads up to `maxLines` JSON objects.
- Each field accumulates an `EnumSet<DataType>` of all types observed across all sampled lines.
- Malformed lines are skipped gracefully (lines 72-77, 80-85).

### FieldInfo structure (lines 184-228)
- Tree structure with `children` map for nested objects.
- `isArray` flag for multi-value fields.
- `types` is an `EnumSet<DataType>` -- accumulates all types seen for this field.
- Nested objects use dot notation (e.g., `user.name`).

---

## 5. Parquet Schema Discovery

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`

### metadata() method (lines 67-70)
```java
public SourceMetadata metadata(StorageObject object) throws IOException {
    List<Attribute> schema = readSchema(object);
    return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
}
```

### readSchema() method (lines 72-86)
```java
private List<Attribute> readSchema(StorageObject object) throws IOException {
    org.apache.parquet.io.InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);

    // Build ParquetReadOptions with SKIP_ROW_GROUPS to only read schema metadata
    ParquetReadOptions options = ParquetReadOptions.builder()
        .withMetadataFilter(ParquetMetadataConverter.SKIP_ROW_GROUPS)
        .build();

    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options)) {
        org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData = reader.getFileMetaData();
        MessageType parquetSchema = fileMetaData.getSchema();
        return convertParquetSchemaToAttributes(parquetSchema);
    }
}
```

**Key details**:
- Uses `ParquetMetadataConverter.SKIP_ROW_GROUPS` (line 77) to read **only the file footer metadata** -- no actual row data is read. This is very efficient.
- The Parquet footer contains the full schema in the `MessageType` object.
- The `ParquetStorageObjectAdapter` adapts the `StorageObject` to Parquet's `InputFile` interface.
- Schema is deterministic per file (embedded in the file format), unlike NDJSON which requires sampling.

### Type mapping (lines 149-171)
```java
private DataType convertParquetTypeToEsql(Type parquetType) {
    if (parquetType.isPrimitive() == false) {
        return DataType.UNSUPPORTED; // Complex types not yet supported
    }
    PrimitiveType primitive = parquetType.asPrimitiveType();
    LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();

    return switch (primitive.getPrimitiveTypeName()) {
        case BOOLEAN -> DataType.BOOLEAN;
        case INT32 -> logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation ? DataType.DATETIME : DataType.INTEGER;
        case INT64 -> logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ? DataType.DATETIME : DataType.LONG;
        case FLOAT, DOUBLE -> DataType.DOUBLE;
        case BINARY, FIXED_LEN_BYTE_ARRAY -> {
            if (logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                yield DataType.KEYWORD;
            }
            yield DataType.KEYWORD;
        }
        default -> DataType.UNSUPPORTED;
    };
}
```

---

## 6. CSV Schema Discovery

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-csv/src/main/java/org/elasticsearch/xpack/esql/datasource/csv/CsvFormatReader.java`

### readSchema() method (lines 75-91)
```java
private List<Attribute> readSchema(StorageObject object) throws IOException {
    try (
        InputStream stream = object.newStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
    ) {
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("//")) {
                continue;
            }
            // First non-comment line is the schema
            return parseSchema(line);
        }
        throw new IOException("CSV file has no schema line");
    }
}
```

**Key details**:
- CSV uses an **explicit typed header** format: `column_name:type_name,...` (line 88).
- The first non-comment, non-empty line defines the schema.
- No inference is performed -- types are declared explicitly.
- Supported types (lines 141-150): `INTEGER`, `LONG`, `DOUBLE`, `KEYWORD`, `TEXT`, `BOOLEAN`, `DATETIME`, `NULL`.

This is fundamentally different from NDJSON (inference-based) and Parquet/ORC (embedded metadata). CSV schema is always deterministic per file because it's declared in the header.

---

## 7. ORC Schema Discovery

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-orc/src/main/java/org/elasticsearch/xpack/esql/datasource/orc/OrcFormatReader.java`
**Lines**: 71-84

```java
public SourceMetadata metadata(StorageObject object) throws IOException {
    List<Attribute> schema = readSchema(object);
    return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
}

private static List<Attribute> readSchema(StorageObject object) throws IOException {
    OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
    Path path = new Path(object.path().toString());
    OrcFile.ReaderOptions options = OrcFile.readerOptions(new Configuration(false)).filesystem(fs);
    try (Reader reader = OrcFile.createReader(path, options)) {
        TypeDescription schema = reader.getSchema();
        return convertOrcSchemaToAttributes(schema);
    }
}
```

Like Parquet, ORC embeds schema in file metadata (the footer/stripe metadata). Schema is read without loading row data.

---

## 8. SourceMetadata Interface

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceMetadata.java`

```java
public interface SourceMetadata {
    List<Attribute> schema();           // resolved schema as ESQL attributes
    String sourceType();                // "parquet", "iceberg", "csv", etc.
    String location();                  // URI or path
    default Optional<SourceStatistics> statistics();    // row counts, etc.
    default Optional<List<String>> partitionColumns();  // partition column names
    default Map<String, Object> sourceMetadata();       // opaque source-specific metadata
    default Map<String, Object> config();               // configuration map
}
```

The schema is carried as `List<Attribute>` -- a flat ordered list of column names and their ESQL data types. There is no concept of "per-file schema" or "schema variant" in this interface.

**SimpleSourceMetadata** (at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SimpleSourceMetadata.java`) is the standard implementation -- an immutable record of these fields.

**ExternalSourceMetadata** (at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceMetadata.java`) extends `SourceMetadata` with backward-compatible `tablePath()` and `attributes()` methods.

---

## 9. What Exactly Needs to Change for UNION_BY_NAME

### Current behavior summary

When a glob pattern matches N files, the system:
1. Lists all matching files (via `GlobExpander`).
2. Reads schema from **file[0] only** (`ExternalSourceResolver.resolveMultiFileSource()`, line 171-172).
3. Uses that single schema for all N files.
4. At read time (`AsyncExternalSourceOperatorFactory.startMultiFileRead()`), iterates all files with the same projected column list. If file[i] has a column missing, the `FormatReader.read()` call either returns nulls or errors depending on the format reader's behavior.

### What union-by-name requires

#### A. Schema sampling -- read more than one file

Currently `resolveMultiFileSource()` reads schema from one file. For union-by-name, it must sample multiple files.

**Design choices**:
- **How many files to sample**: Not all -- could be thousands. Options:
  - All files (accurate but O(N) I/O calls at plan time).
  - A configurable sample (e.g., first K files, or random K files). Default could be 10-20.
  - For Parquet/ORC: cheap (footer reads only). For NDJSON: moderate (first 100 lines per file).
  - For CSV: trivially cheap (one header line per file).
- **Parallelism**: Schema reads should happen in parallel (they're independent I/O operations).

#### B. Schema merging logic

A new utility class (e.g., `SchemaMerger` or `SchemaUnion`) is needed to merge `List<Attribute>` from multiple files.

**Union-by-name semantics**:
- The merged schema is the **union of all column names** across all sampled files.
- Column ordering: columns appear in discovery order (first file defines initial order, new columns from subsequent files are appended).
- When the same column name appears in multiple files:
  - **Same type**: keep as-is.
  - **Compatible types**: widen using the same priority as `NdJsonSchemaInferrer.resolveType()`, i.e.:
    - `INTEGER` + `LONG` -> `LONG`
    - `INTEGER` + `DOUBLE` -> `DOUBLE`
    - `LONG` + `DOUBLE` -> `DOUBLE`
    - `KEYWORD` + anything else -> `KEYWORD` (string is the universal fallback)
  - **Incompatible types** (e.g., `BOOLEAN` + `LONG`): widen to `KEYWORD` or raise an error, depending on policy.
- Columns present in some files but not others: marked nullable (`Nullability.TRUE`).

**Existing analog**: Elasticsearch already handles this for multi-index queries. `InvalidMappedField` (at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/InvalidMappedField.java`) tracks fields mapped to different types across indices. `MultiTypeEsField` (at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/MultiTypeEsField.java`) resolves type conflicts by carrying per-index conversion expressions. However, these are index-oriented (keyed by index name) and tightly coupled to `IndexResolver`. External sources would need an analogous mechanism keyed by file path.

#### C. Per-file schema adaptation at read time

Currently `AsyncExternalSourceOperatorFactory.startMultiFileRead()` (line 252-276) passes the same `projectedColumns` list to every file:

```java
for (StorageEntry entry : fileSet.files()) {
    StorageObject obj = storageProvider.newObject(entry.path(), entry.length(), entry.lastModified());
    try (CloseableIterator<Page> pages = formatReader.read(obj, projectedColumns, batchSize)) {
        drainPages(pages, buffer, injector);
    }
}
```

With union-by-name, different files may lack certain columns. What needs to happen:
1. The operator must know which columns each file has (or at least which it does NOT have).
2. For missing columns: inject null-valued blocks. This is analogous to what `VirtualColumnInjector` already does for partition columns -- it injects constant-value blocks into pages.
3. For type mismatches: insert type conversion (e.g., INT -> LONG widening) between the raw page and the output page.

**Options**:
- **Option A (simple)**: Don't track per-file schema. Just request all merged columns. Let each `FormatReader.read()` return nulls for missing columns (Parquet already does this -- `createBlock()` at `ParquetFormatReader.java:276` returns `blockFactory.newConstantNullBlock(rowCount)` for unknown fields). For type mismatches, rely on the format reader returning the file's native type and add a conversion wrapper.
- **Option B (complete)**: Store per-file schema at plan time and pass it to the operator factory. The factory adapts each file's pages to the merged schema.

Option A is simpler and works well for Parquet/ORC (which already handle missing columns gracefully). NDJSON also naturally handles this since fields absent from a document come back as null. CSV is the only format where this could fail if a projected column name does not exist in the file's header -- `CsvBatchIterator.computeProjectedIndices()` at line 296-297 throws `EsqlIllegalArgumentException("Column not found in CSV schema")`.

#### D. Changes to ExternalSourceResolver.resolveMultiFileSource()

Pseudo-code for the new flow:

```java
private ExternalSourceResolution.ResolvedSource resolveMultiFileSource(...) throws Exception {
    // ... existing glob expansion ...

    // Determine schema resolution strategy
    FormatReader reader = formatRegistry.byExtension(firstFile.objectName());
    SchemaResolution strategy = reader.defaultSchemaResolution();
    // Or: let the user specify via WITH clause (e.g., schema_resolution='union_by_name')

    List<Attribute> mergedSchema;
    if (strategy == SchemaResolution.UNION_BY_NAME) {
        // Sample up to N files
        int sampleSize = Math.min(fileSet.size(), MAX_SCHEMA_SAMPLE);
        List<List<Attribute>> schemas = new ArrayList<>();
        for (int i = 0; i < sampleSize; i++) {
            SourceMetadata meta = resolveSingleSource(fileSet.files().get(i).path().toString(), config);
            schemas.add(meta.schema());
        }
        mergedSchema = SchemaMerger.unionByName(schemas);
    } else {
        // Existing behavior: first file wins
        SourceMetadata metadata = resolveSingleSource(firstFile.toString(), config);
        mergedSchema = metadata.schema();
    }

    // ... rest of the method
}
```

#### E. New classes needed

1. **`SchemaMerger`** (new class in `org.elasticsearch.xpack.esql.datasources`):
   - Static method `unionByName(List<List<Attribute>> schemas) -> List<Attribute>`
   - Static method `strict(List<List<Attribute>> schemas) -> List<Attribute>` (validates all schemas are identical)
   - Contains the type widening priority table
   - Marks columns found in only some schemas as nullable

2. **`SchemaAdaptingIterator`** (new class in `org.elasticsearch.xpack.esql.datasources`):
   - Wraps a `CloseableIterator<Page>` and adapts pages from a file-specific schema to the merged schema.
   - Injects null blocks for missing columns.
   - Applies type widening (e.g., INT -> LONG block conversion).
   - Similar pattern to existing `InjectingIterator` (at `AsyncExternalSourceOperatorFactory.java:345-368`).

3. No changes needed to `SourceMetadata`, `ExternalRelation`, or `ExternalSourceExec` -- the merged schema is just a `List<Attribute>` and flows through existing paths.

#### F. Configuration surface

The `SchemaResolution` strategy should be controllable:
- Per-format default: `FormatReader.defaultSchemaResolution()` already exists.
- Per-query override: via `WITH schema_resolution = 'union_by_name'` in the ESQL query.
- The parser/config-map plumbing for `WITH` clauses already exists.

#### G. Edge cases

1. **Zero files match glob**: Already handled (throws `IllegalArgumentException`).
2. **All files have identical schema**: Union-by-name degenerates to first-file-wins. No extra cost.
3. **Completely disjoint schemas**: Every column gets marked nullable. Potentially useful but surprising. May want a warning.
4. **Very large number of files**: Sampling limits prevent O(N) schema reads. Unseen files may have novel columns that are silently missing from the merged schema.
5. **Type widening ambiguity**: What if file A has `score:INTEGER` and file B has `score:BOOLEAN`? No natural widening. Must decide: error, or `KEYWORD` as universal fallback.
6. **NDJSON specifics**: NDJSON schema is already a union across sampled lines *within* a single file. Cross-file union is a natural extension of the same logic.

---

## 10. Summary of Verified Facts

| Aspect | Current State | What the Code Says |
|--------|--------------|-------------------|
| `SchemaResolution` enum | 3 values defined | `STRICT` and `UNION_BY_NAME` are TODO stubs only (FormatReader.java:40-43) |
| `defaultSchemaResolution()` | Returns `FIRST_FILE_WINS` | Never called by production code (FormatReader.java:46-48) |
| Multi-file resolution | First file only | `resolveMultiFileSource()` takes `fileSet.files().get(0)` (ExternalSourceResolver.java:171) |
| NDJSON type widening | DATETIME > KEYWORD > DOUBLE > LONG > INTEGER | `FieldInfo.resolveType()` (NdJsonSchemaInferrer.java:201-227) |
| MultiTypeEsField TODO | Acknowledged in code | NdJsonSchemaInferrer.java:210 comment |
| Parquet schema read | Footer only, SKIP_ROW_GROUPS | ParquetFormatReader.java:77 |
| CSV schema read | First non-comment line, explicit types | CsvFormatReader.java:88 |
| ORC schema read | File metadata via OrcFile.createReader | OrcFormatReader.java:78-83 |
| Per-file adaptation at read time | None | `startMultiFileRead()` passes same columns to all files (AsyncExternalSourceOperatorFactory.java:260-265) |
| Parquet missing column handling | Returns null block | ParquetFormatReader.java:276 returns `newConstantNullBlock` |
| CSV missing column handling | Throws exception | CsvFormatReader.java:296-297 throws `EsqlIllegalArgumentException` |
| Existing type-conflict analog | `InvalidMappedField` + `MultiTypeEsField` | Handles multi-index type conflicts, keyed by index name |
