# Deep Dive: User-Declared Schema / Manifest

## 1. Current Schema Resolution Flow

The schema resolution flow has 5 stages, from parser to analyzer:

### Stage 1: Parser creates `UnresolvedExternalRelation`

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`
**Lines 781-789:**

```java
public LogicalPlan visitExternalCommand(EsqlBaseParser.ExternalCommandContext ctx) {
    Source source = source(ctx);
    Expression tablePath = expression(ctx.stringOrParameter());
    MapExpression options = visitCommandNamedParameters(ctx.commandNamedParameters());
    Map<String, Expression> params = options != null ? options.keyFoldedMap() : Map.of();
    return new UnresolvedExternalRelation(source, tablePath, params);
}
```

The `params` map comes from the `WITH` clause (e.g., `WITH access_key = "...", secret_key = "..."`). These are `Map<String, Expression>` where values are `Literal` instances.

### Stage 2: PreAnalyzer collects external source paths

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/PreAnalyzer.java`
**Lines 73-82:**

```java
List<String> icebergPaths = new ArrayList<>();
plan.forEachUp(UnresolvedExternalRelation.class, p -> {
    if (p.tablePath() instanceof Literal literal && literal.value() != null) {
        String path = org.elasticsearch.common.lucene.BytesRefs.toString(literal.value());
        icebergPaths.add(path);
    }
});
```

Despite the name `icebergPaths`, this collects ALL external source paths (Parquet, CSV, NDJSON, Iceberg, Flight, etc.).

### Stage 3: EsqlSession calls ExternalSourceResolver

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java`
**Lines 851-871:**

```java
private void preAnalyzeExternalSources(...) {
    if (preAnalysis.icebergPaths().isEmpty()) {
        listener.onResponse(result);
        return;
    }
    Map<String, Map<String, Expression>> pathParams = extractIcebergParams(plan);
    var filterHints = PartitionFilterHintExtractor.extract(plan);
    externalSourceResolver.resolve(
        preAnalysis.icebergPaths(),
        pathParams,
        filterHints.isEmpty() ? null : filterHints,
        listener.map(result::withExternalSourceResolution)
    );
}
```

### Stage 4: ExternalSourceResolver dispatches to factory chain

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`
**Lines 195-235 (`resolveSingleSource` method):**

This is the critical method. The flow is:

1. Parse the path into `StoragePath` and validate the scheme (line 198-209)
2. Iterate all registered `ExternalSourceFactory` instances (line 212)
3. For each factory, call `canHandle(path)` (line 213)
4. First factory that claims the path calls `factory.resolveMetadata(path, config)` (line 215)
5. This returns a `SourceMetadata` which contains the schema as `List<Attribute>`

**This is the I/O hot path.** For file-based sources, `resolveMetadata` opens a network connection to the remote storage, downloads file header/metadata, and parses it to discover the schema. For NDJSON, it reads the first N lines to infer types. For Parquet, it reads the file footer.

### Stage 5: Analyzer resolves the plan node

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Analyzer.java`
**Lines 456-489 (`ResolveExternalRelations` rule):**

```java
protected LogicalPlan rule(UnresolvedExternalRelation plan, AnalyzerContext context) {
    String tablePath = extractTablePath(plan.tablePath());
    var resolvedSource = context.externalSourceResolution().resolvedSource(tablePath);
    if (resolvedSource == null) {
        return plan; // Still unresolved
    }
    var metadata = resolvedSource.metadata();
    return new ExternalRelation(plan.source(), tablePath, metadata, metadata.schema(), resolvedSource.fileSet());
}
```

### Short-circuit insertion point

A declared schema bypass would go in **Stage 4** (`ExternalSourceResolver.resolveSingleSource` or `resolveSource`). Specifically, before entering the factory chain at line 212, the resolver would check if a pre-declared schema exists for this path. If so, it constructs a `SimpleSourceMetadata` from the declared schema and returns immediately, skipping all I/O.

For multi-file paths (glob patterns), the bypass would go in `resolveMultiFileSource` (lines 144-182), before `resolveSingleSource` is called at line 172. The glob expansion (file listing) would still need to happen, but the schema resolution from the first file would be skipped.

---

## 2. SourceMetadata Construction

### SimpleSourceMetadata.Builder

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SimpleSourceMetadata.java`
**Lines 154-201:**

```java
public static class Builder {
    private List<Attribute> schema;
    private String sourceType;
    private String location;
    private SourceStatistics statistics;
    private List<String> partitionColumns;
    private Map<String, Object> sourceMetadata;
    private Map<String, Object> config;

    public Builder schema(List<Attribute> schema) { ... }
    public Builder sourceType(String sourceType) { ... }
    public Builder location(String location) { ... }
    public Builder statistics(SourceStatistics statistics) { ... }
    public Builder partitionColumns(List<String> partitionColumns) { ... }
    public Builder sourceMetadata(Map<String, Object> sourceMetadata) { ... }
    public Builder config(Map<String, Object> config) { ... }

    public SimpleSourceMetadata build() {
        return new SimpleSourceMetadata(schema, sourceType, location, statistics, partitionColumns, sourceMetadata, config);
    }
}
```

**Yes, you can construct a `SourceMetadata` from a list of field names and types without reading any file.** The builder only requires:
- `schema` (List<Attribute>) -- cannot be null (validated at line 63)
- `sourceType` (String) -- cannot be null (validated at line 66)
- `location` (String) -- cannot be null (validated at line 69)

Example construction:

```java
List<Attribute> schema = List.of(
    new ReferenceAttribute(Source.EMPTY, "timestamp", DataType.DATETIME),
    new ReferenceAttribute(Source.EMPTY, "message", DataType.KEYWORD),
    new ReferenceAttribute(Source.EMPTY, "level", DataType.INTEGER)
);

SourceMetadata metadata = SimpleSourceMetadata.builder()
    .schema(schema)
    .sourceType("declared")  // or "parquet", "ndjson", etc.
    .location("s3://bucket/logs/*.ndjson.gz")
    .build();
```

This is already done implicitly in existing code:
- **ParquetFormatReader** (line 69): `new SimpleSourceMetadata(schema, formatName(), object.path().toString())`
- **CsvFormatReader** (line 72): `new SimpleSourceMetadata(schema, formatName(), objectPath.toString())`
- **NdJsonFormatReader** (line 39): `new SimpleSourceMetadata(schema, formatName(), object.path().toString())`
- **FlightConnectorFactory** (line 60): `new SimpleSourceMetadata(attributes, "flight", location, null, null, null, resolvedConfig)`

---

## 3. ESQL Type System

### SourceMetadata uses ESQL DataType, not ES mapping types

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceMetadata.java`
**Line 40:**

```java
List<Attribute> schema();
```

The `Attribute` objects carry `DataType` (from `org.elasticsearch.xpack.esql.core.type.DataType`), which is ESQL's own type enum, not ES mapping types.

### DataType enum values

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/DataType.java`
**Lines 171-370:**

The types relevant for external sources (i.e., the ones that format readers actually produce):

| DataType | typeName | esType | Notes |
|----------|----------|--------|-------|
| `BOOLEAN` | `boolean` | `boolean` | Line 185 |
| `LONG` | `long` | `long` | Line 218 |
| `INTEGER` | `integer` | `integer` | Line 222-224 |
| `DOUBLE` | `double` | `double` | Line 232-234 |
| `KEYWORD` | `keyword` | `keyword` | Line 289 |
| `TEXT` | `text` | `text` | Line 297 |
| `DATETIME` | `DATETIME` | `date` | Line 301 (note: typeName is DATETIME, esType is date) |
| `DATE_NANOS` | `date_nanos` | `date_nanos` | Line 305 |
| `IP` | `ip` | `ip` | Line 316 |
| `VERSION` | `version` | `version` | Line 321 |
| `UNSIGNED_LONG` | `unsigned_long` | `unsigned_long` | Line 228 |
| `NULL` | `null` | `null` | Line 181 |
| `GEO_POINT` | `geo_point` | `geo_point` | Line 327 |
| `GEO_SHAPE` | `geo_shape` | `geo_shape` | Line 331 |

### Type resolution methods (user-supplied type names)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/DataType.java`

Three lookup methods:

1. **`fromTypeName(String name)`** (line 546-548): Uses `NAME_TO_TYPE` map. Case-insensitive. Keys are `typeName()` values plus "date" -> DATETIME.

2. **`fromEs(String name)`** (line 550-557): Uses `ES_TO_TYPE` map. Maps ES field type names to DataType. Includes aliases like "match_only_text" -> TEXT, "shape" -> CARTESIAN_SHAPE, "semantic_text" -> TEXT.

3. **`fromNameOrAlias(String typeName)`** (line 921-924): Uses `NAME_OR_ALIAS_TO_TYPE` map. Includes all of `fromTypeName` plus aliases: "bool" -> BOOLEAN, "int" -> INTEGER, "string" -> KEYWORD, "date" -> DATETIME. **This is the best method for user-facing type parsing.**

The CSV format reader already has its own type parsing at line 141-151 of `CsvFormatReader.java` that maps strings like `"INTEGER"`, `"INT"`, `"I"`, `"LONG"`, `"L"`, etc. to DataType values. A declared schema feature should use `DataType.fromNameOrAlias()` for consistency with the `::type` cast syntax.

### Attribute types used by format readers

Format readers use two different Attribute subclasses:
- **`ReferenceAttribute`**: Used by ParquetFormatReader (line 144) and enrichSchemaWithPartitionColumns (line 253). Simple: just name + DataType.
  - **File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/expression/ReferenceAttribute.java`
- **`FieldAttribute`**: Used by CsvFormatReader (line 134) and NdJsonSchemaInferrer. Requires an `EsField` wrapper.

For a declared schema, `ReferenceAttribute` is the right choice -- it is simpler and does not require constructing an `EsField` object. Constructor:

```java
new ReferenceAttribute(Source.EMPTY, name, dataType)  // deprecated but works (line 38)
new ReferenceAttribute(Source.EMPTY, null, name, dataType)  // preferred (line 42)
```

---

## 4. Config/Params Flow -- Where Would a Declared Schema Be Stored?

### Current config flow

The `WITH` clause parameters flow through:

1. **Parser** (`LogicalPlanBuilder.java` line 785-786): `Map<String, Expression> params` stored in `UnresolvedExternalRelation`
2. **EsqlSession** (`EsqlSession.java` line 878-886): `extractIcebergParams` extracts `Map<String, Map<String, Expression>> pathParams` keyed by path
3. **ExternalSourceResolver** (`ExternalSourceResolver.java` line 286-305): `paramsToConfigMap` converts `Expression` literals to `Map<String, Object>` string values
4. **Factory** (`FileSourceFactory.resolveMetadata` line 92-110): receives `Map<String, Object> config`

### Option A: WITH params at query time

Schema declaration could be passed as a WITH parameter:

```
EXTERNAL "s3://bucket/logs/*.ndjson.gz"
WITH schema = "timestamp:datetime, message:keyword, level:integer"
```

This flows naturally through the existing `params` map. The `ExternalSourceResolver.resolveSource` method would check for a `schema` key in the config map before calling `resolveSingleSource`. This is the simplest option and requires no new storage.

**Where the bypass goes:** In `ExternalSourceResolver.resolveSource` (line 127), before calling `resolveSingleSource`:

```java
private ExternalSourceResolution.ResolvedSource resolveSource(
    String path, Map<String, Object> config, ...) throws Exception {

    // Check for declared schema
    Object declaredSchema = config != null ? config.get("schema") : null;
    if (declaredSchema != null) {
        SourceMetadata metadata = parseDeclaredSchema(declaredSchema.toString(), path);
        ExternalSourceMetadata extMetadata = wrapAsExternalSourceMetadata(metadata, config);
        // Still need file set for multi-file
        if (GlobExpander.isMultiFile(path)) {
            FileSet fileSet = expandFiles(path, config);
            return new ExternalSourceResolution.ResolvedSource(extMetadata, fileSet);
        }
        return new ExternalSourceResolution.ResolvedSource(extMetadata, FileSet.UNRESOLVED);
    }
    // ... existing flow
}
```

### Option B: CRUD API / cluster state

There is **no existing CRUD API** for datasource configs. No cluster state storage for datasource definitions exists on main. A CRUD API would store schema alongside credentials in cluster state (similar to how `EnrichPolicy` is stored). The schema would be retrieved in the resolver before calling factories.

This is a separate work item (the CRUD API) and the declared schema feature should be designed to work with either approach -- the resolver checks for a declared schema regardless of where it came from (WITH params or CRUD config).

### Option C: Hybrid

The CRUD API config would store both credentials and schema. At query time, the resolver would merge CRUD config with WITH params (WITH taking precedence, as already happens at line 316-325 of ExternalSourceResolver). The schema from either source would be checked before I/O.

---

## 5. FormatReader.resolveMetadata -- Can It Be Bypassed?

### FormatReader.metadata() interface

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java`
**Line 52:**

```java
SourceMetadata metadata(StorageObject object) throws IOException;
```

This method requires a `StorageObject`, which requires a `StorageProvider`, which requires a network connection. The method:

1. Opens a stream to the remote object (Parquet: reads footer, CSV: reads first line, NDJSON: reads first N lines)
2. Parses format-specific metadata
3. Constructs `List<Attribute>` from discovered types
4. Returns `SimpleSourceMetadata(schema, formatName, location)`

### Can it be bypassed entirely?

**Yes.** The `FormatReader.metadata()` is only called indirectly through `FileSourceFactory.resolveMetadata()` (line 106):

```java
public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
    // ... create StorageProvider, StorageObject
    FormatReader reader = formatRegistry.byExtension(storagePath.objectName());
    return reader.metadata(storageObject);  // <-- THIS is the I/O call
}
```

And `FileSourceFactory.resolveMetadata` is only called from `ExternalSourceResolver.resolveSingleSource` (line 215):

```java
return factory.resolveMetadata(path, config);
```

If the bypass is placed in `ExternalSourceResolver.resolveSource` (before `resolveSingleSource` is called), then `FormatReader.metadata()` is never invoked. No StorageProvider, no StorageObject, no network I/O at all for schema resolution.

### What about the format-specific data reading?

The `FormatReader.read()` method (line 58) does NOT depend on `metadata()` having been called first. The Parquet reader re-reads the schema inside `read()` (line 100-102 of ParquetFormatReader). The CSV reader re-parses the schema line inside `readNextBatch()` (line 218-226 of CsvFormatReader). The NDJSON reader infers schema from data during reading.

So skipping `metadata()` does NOT break data reading. The format readers are stateless with respect to schema discovery vs data reading.

### Caveat: sourceType for operator factory dispatch

When a declared schema is used, the `sourceType` field of `SourceMetadata` matters because `OperatorFactoryRegistry` uses it to dispatch to the correct operator factory. The declared schema must preserve the correct sourceType (e.g., "parquet", "ndjson", "csv") so that the right format reader is used for data reading. This could be inferred from the file extension, or explicitly specified in the declaration.

---

## 6. What Exactly Needs to Change

### Change 1: Parse declared schema string into List<Attribute>

**New utility method** in `ExternalSourceResolver` or a new `DeclaredSchemaParser` class:

```java
static List<Attribute> parseDeclaredSchema(String schemaString) {
    // Parse "field1:type1, field2:type2, ..."
    String[] fields = schemaString.split(",");
    List<Attribute> attrs = new ArrayList<>(fields.length);
    for (String field : fields) {
        String[] parts = field.trim().split(":");
        String name = parts[0].trim();
        DataType type = DataType.fromNameOrAlias(parts[1].trim());
        if (type == DataType.UNSUPPORTED) {
            throw new IllegalArgumentException("Unsupported type: " + parts[1]);
        }
        attrs.add(new ReferenceAttribute(Source.EMPTY, null, name, type));
    }
    return attrs;
}
```

**Type validation:** Use `DataType.fromNameOrAlias()` (line 921-924 of DataType.java). This accepts: all type names, plus aliases "bool", "int", "string", "date". Returns `UNSUPPORTED` for unknown types, which should be rejected.

### Change 2: Short-circuit in ExternalSourceResolver

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`

**In `resolveSource` (line 127):** Check for declared schema before any file I/O:

```java
private ExternalSourceResolution.ResolvedSource resolveSource(
    String path, Map<String, Object> config, ...) throws Exception {

    // New: check for declared schema
    String declaredSchemaStr = config != null ? (String) config.get("schema") : null;
    if (declaredSchemaStr != null) {
        List<Attribute> schema = parseDeclaredSchema(declaredSchemaStr);
        String sourceType = inferSourceType(path);  // from extension
        SourceMetadata metadata = new SimpleSourceMetadata(schema, sourceType, path);
        ExternalSourceMetadata extMetadata = wrapAsExternalSourceMetadata(metadata, config);

        if (GlobExpander.isMultiFile(path)) {
            // Still need to expand the glob for the file set
            return resolveMultiFileSourceWithDeclaredSchema(path, config, hints, hivePartitioning, extMetadata);
        }
        return new ExternalSourceResolution.ResolvedSource(extMetadata, FileSet.UNRESOLVED);
    }

    // Existing flow...
}
```

**In `resolveMultiFileSource` (line 144):** For the multi-file case, the glob expansion still needs to happen (to discover which files exist), but the schema resolution from the first file (line 172) is skipped:

```java
private ExternalSourceResolution.ResolvedSource resolveMultiFileSourceWithDeclaredSchema(
    String path, Map<String, Object> config, ..., ExternalSourceMetadata extMetadata) throws Exception {
    // Still expand the glob to get the file set
    StoragePath storagePath = StoragePath.of(path);
    StorageProvider provider = storageRegistry.createProvider(storagePath.scheme(), settings, config);
    FileSet fileSet = GlobExpander.expandGlob(path, provider, hints, hivePartitioning);

    if (fileSet.isEmpty()) {
        throw new IllegalArgumentException("Glob pattern matched no files: " + path);
    }

    // Skip: SourceMetadata metadata = resolveSingleSource(firstFile.toString(), config);
    // Use the declared schema directly

    PartitionMetadata partitionMetadata = fileSet.partitionMetadata();
    if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
        extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
    }

    return new ExternalSourceResolution.ResolvedSource(extMetadata, fileSet);
}
```

### Change 3: Infer sourceType from file extension

**New utility method:** To get the correct `sourceType` when schema is declared:

```java
private String inferSourceType(String path) {
    StoragePath storagePath = StoragePath.of(path);
    String objectName = storagePath.objectName();
    // Remove glob characters for extension detection
    objectName = objectName.replace("*", "").replace("?", "");
    if (objectName.endsWith(".parquet") || objectName.endsWith(".parq")) return "parquet";
    if (objectName.endsWith(".ndjson") || objectName.endsWith(".jsonl")) return "ndjson";
    if (objectName.endsWith(".ndjson.gz") || objectName.endsWith(".jsonl.gz")) return "ndjson";
    if (objectName.endsWith(".csv") || objectName.endsWith(".tsv")) return "csv";
    if (objectName.endsWith(".orc")) return "orc";
    return "file";  // generic fallback
}
```

Or better, delegate to the `FormatReaderRegistry.byExtension()` method and call `formatName()` on the result, without actually reading any data.

### Change 4: Config key constant

Add to `PartitionConfig` or a new constants class:

```java
public static final String CONFIG_SCHEMA = "schema";
```

### Change 5: Validation

Required validations:
1. Each field must have exactly two parts (name:type) -- reject malformed declarations
2. Type must resolve via `DataType.fromNameOrAlias()` -- reject unsupported types
3. Field names must be non-empty and unique -- reject duplicates
4. Schema string must not be empty -- reject empty declarations
5. When both declared schema and format-discovered schema exist (e.g., CRUD config + file-based source), the declared schema takes precedence (no merge)

### What does NOT need to change

- **Parser**: The `WITH schema = "..."` syntax already works -- it is just another parameter in the generic `Map<String, Expression>`.
- **Analyzer**: `ResolveExternalRelations` does not care where the schema came from -- it just reads `metadata.schema()`.
- **Physical planning / operator factories**: They consume `SourceMetadata` generically.
- **Format readers**: They are only called for data reading (which still happens at execution time). The `metadata()` method is skipped but `read()` is not affected.
- **ExternalRelation / ExternalSourceExec**: Plan nodes are schema-agnostic -- they carry `List<Attribute>` regardless of source.

---

## Summary of Code Locations

| Component | File | Key Lines |
|-----------|------|-----------|
| Parser (WITH params) | `.../parser/LogicalPlanBuilder.java` | 781-789 |
| UnresolvedExternalRelation | `.../plan/logical/UnresolvedExternalRelation.java` | 43-48 |
| PreAnalyzer (path collection) | `.../analysis/PreAnalyzer.java` | 73-82 |
| EsqlSession (invoke resolver) | `.../session/EsqlSession.java` | 851-871 |
| ExternalSourceResolver (BYPASS HERE) | `.../datasources/ExternalSourceResolver.java` | 127-142 (resolveSource), 195-235 (resolveSingleSource) |
| ExternalSourceResolver (multi-file) | `.../datasources/ExternalSourceResolver.java` | 144-182 (resolveMultiFileSource) |
| FileSourceFactory (file I/O) | `.../datasources/FileSourceFactory.java` | 92-110 (resolveMetadata) |
| FormatReader.metadata (skip this) | `.../datasources/spi/FormatReader.java` | 52 |
| SimpleSourceMetadata.Builder | `.../datasources/spi/SimpleSourceMetadata.java` | 154-201 |
| SourceMetadata interface | `.../datasources/spi/SourceMetadata.java` | 32-119 |
| DataType.fromNameOrAlias (type parsing) | `.../core/type/DataType.java` | 921-924 |
| DataType NAME_OR_ALIAS_TO_TYPE | `.../core/type/DataType.java` | 523-530 |
| ReferenceAttribute (schema attrs) | `.../core/expression/ReferenceAttribute.java` | 38-44 |
| Analyzer (consumes metadata) | `.../analysis/Analyzer.java` | 456-489 |
| ExternalRelation (plan node) | `.../plan/logical/ExternalRelation.java` | 53-68 |
| ExternalSourceResolution (result record) | `.../datasources/ExternalSourceResolution.java` | 16-35 |
| ExternalSourceMetadata (wrapper) | `.../datasources/ExternalSourceMetadata.java` | 31-78 |
| PartitionConfig (config keys) | `.../datasources/PartitionConfig.java` | 27-29 |

All paths are under `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/`.

---

## Effort Estimate

- **Schema parsing utility**: ~30 lines of new code
- **ExternalSourceResolver bypass**: ~40 lines modified/added in `resolveSource` and `resolveMultiFileSource`
- **sourceType inference**: ~15 lines
- **Tests**: ~100-150 lines (unit tests for schema parsing, integration test for end-to-end bypass)
- **Total**: ~200-250 lines of production code, ~150 lines of test code

This is a small, well-contained change. The insertion point is clean, the data structures already support it, and no downstream components need modification.
