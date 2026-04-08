# Deep Dive: Schema Discovery API for ES|QL External Data Sources

## 1. The _field_caps API: How It Works

### REST Endpoint

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/rest/action/RestFieldCapabilitiesAction.java`

Routes:
```
GET  /_field_caps
POST /_field_caps
GET  /{index}/_field_caps
POST /{index}/_field_caps
```

Query parameters: `fields`, `include_unmapped`, `include_empty_fields`, `filters`, `types`

Request body supports: `fields`, `index_filter` (DSL query to narrow indices), `runtime_mappings`

### What _field_caps Returns

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/fieldcaps/FieldCapabilitiesResponse.java`

Response structure (serialized as JSON):
```json
{
  "indices": ["index-1", "index-2", ...],
  "fields": {
    "field_name": {
      "keyword": {
        "type": "keyword",
        "metadata_field": false,
        "searchable": true,
        "aggregatable": true,
        "time_series_dimension": false,
        "indices": ["index-1"],           // only present on type conflicts
        "non_searchable_indices": null,
        "non_aggregatable_indices": null,
        "meta": { "key": ["value1", "value2"] }
      },
      "text": {
        "type": "text",
        "searchable": true,
        "aggregatable": false,
        "indices": ["index-2"]
      }
    }
  },
  "failed_indices": 0,
  "failures": []
}
```

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/fieldcaps/FieldCapabilities.java`

Per-field, per-type information:
- `name` - field name
- `type` - ES type string (keyword, long, double, date, etc.)
- `isMetadataField` - whether this is a metadata field
- `isSearchable` - whether indexed for search
- `isAggregatable` - whether aggregatable
- `isDimension` - time series dimension flag
- `metricType` - time series metric type (counter, gauge, etc.)
- `indices` - which indices have this field with this type (null = all)
- `nonSearchableIndices` - indices where not searchable (null = all searchable)
- `nonAggregatableIndices` - indices where not aggregatable (null = all aggregatable)
- `nonDimensionIndices` - indices where not a dimension
- `metricConflictsIndices` - indices with conflicting metric types
- `meta` - merged metadata map (key -> set of values from all indices)

### Transport Action Pattern

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/fieldcaps/TransportFieldCapabilitiesAction.java`

- Extends `HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse>`
- Action name: `"indices:data/read/field_caps"`
- Pattern: coordinate on one node, fan out to all shards, merge results
- Uses `FieldCapabilities.Builder` to merge per-index responses
- Merge runs on `SEARCH_COORDINATION` thread pool

### How It Handles Multiple Indices with Conflicting Types

When the same field has different types across indices:
1. Each type gets its own entry in the field's map: `"field_name": { "keyword": {...}, "text": {...} }`
2. The `indices` array on each type entry lists ONLY the indices where that type applies
3. If all indices agree on the type, `indices` is null (omitted in JSON)
4. The `nonSearchableIndices` / `nonAggregatableIndices` arrays track per-index capability differences within a single type

In ESQL's IndexResolver, conflicting types produce `InvalidMappedField`, which causes the field to be unsupported for queries spanning those indices.

### ESQL's Fork: EsqlResolveFieldsAction

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlResolveFieldsAction.java`

ESQL uses its own action (`indices:data/read/esql/resolve_fields`) that wraps `TransportFieldCapabilitiesAction`. This is a fork-in-progress: currently delegates to field_caps but is being gradually decoupled. It returns `EsqlResolveFieldsResponse` which wraps `FieldCapabilitiesResponse`.

### ESQL's IndexResolver: How field_caps Becomes an EsIndex

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/IndexResolver.java`

The `mergedMappings()` method converts `FieldCapabilitiesResponse` into `IndexResolution` (containing `EsIndex`):
1. Collects per-field capabilities from all index responses
2. Builds hierarchical `EsField` tree (parent fields contain children)
3. Conflicting types produce `InvalidMappedField`
4. Unsupported ES types produce `UnsupportedEsField`
5. Time series dimension/metric conflicts are tracked
6. Result is `EsIndex` with field name -> `EsField` mapping

---

## 2. Current Schema Resolution for External Sources

### Resolution Flow

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java`

External source resolution runs in parallel with index resolution during the pre-analysis phase:
```
preAnalyzeLookupIndices(...)
  .andThen(preAnalyzeExternalSources(...))    // <-- here
  .andThen(resolveEnrichPolicies(...))
```

### ExternalSourceResolver

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`

Resolution algorithm:
1. For each path in the query, iterate registered `ExternalSourceFactory` instances
2. First factory where `canHandle(path)` returns true wins
3. Call `factory.resolveMetadata(path, config)` -> `SourceMetadata`
4. For multi-file (glob) paths: expand glob, resolve schema from first file, enrich with partition columns
5. Wrap result as `ExternalSourceMetadata` with merged config
6. Return `ExternalSourceResolution` containing all resolved sources

### SourceMetadata Interface

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceMetadata.java`

Returns:
- `schema()` - `List<Attribute>` (ESQL attributes with name + DataType)
- `sourceType()` - string like "parquet", "iceberg", "csv", "flight"
- `location()` - URI/path string
- `statistics()` - optional `SourceStatistics` (row count, size, column stats)
- `partitionColumns()` - optional list of partition column names
- `sourceMetadata()` - opaque `Map<String, Object>` for source-specific data
- `config()` - configuration map (credentials, endpoint, etc.)

### SourceStatistics Interface

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceStatistics.java`

```java
interface SourceStatistics {
    OptionalLong rowCount();
    OptionalLong sizeInBytes();
    Optional<Map<String, ColumnStatistics>> columnStatistics();

    interface ColumnStatistics {
        OptionalLong nullCount();
        OptionalLong distinctCount();
        Optional<Object> minValue();
        Optional<Object> maxValue();
    }
}
```

### SimpleSourceMetadata

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SimpleSourceMetadata.java`

Standard immutable implementation with a Builder pattern. Used by Parquet, CSV, NDJSON format readers.

### How Different Sources Resolve Metadata

**Parquet** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`):
- Opens file with `SKIP_ROW_GROUPS` (metadata only, no data read)
- Reads Parquet MessageType schema
- Converts each Parquet field to ESQL `ReferenceAttribute` with appropriate DataType
- Returns `SimpleSourceMetadata(schema, "parquet", path)`

**Flight** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnectorFactory.java`):
- Connects to Flight server via gRPC
- Calls `client.getSchema(FlightDescriptor.path(target))`
- Converts Arrow Schema to ESQL attributes
- Returns `SimpleSourceMetadata` with resolved endpoint/target in config

**Iceberg** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java`):
- Reads Iceberg table metadata from S3
- Converts Iceberg `Schema` columns to ESQL attributes (via `IcebergTableMetadata`)
- Preserves native Iceberg schema in `IcebergTableMetadata.icebergSchema()`
- Returns `IcebergSourceMetadata` wrapping `IcebergTableMetadata`

### Multi-file Schema Resolution

For glob patterns like `s3://bucket/*.parquet`:
1. Expand glob to get file list
2. Resolve schema from the **first file only** (current behavior: `FIRST_FILE_WINS`)
3. Hive-style partition columns are extracted from directory paths
4. Partition columns are appended to the schema
5. `FormatReader.SchemaResolution` enum defines: `FIRST_FILE_WINS`, `STRICT` (TODO), `UNION_BY_NAME` (TODO)

### What Information is NOT Currently Returned

Compared to `_field_caps`, current external source schema resolution does NOT provide:
- Per-field searchability/aggregatability flags
- Cross-file type conflict detection (it's first-file-wins)
- Metadata about which files contain which fields
- Schema evolution/drift information
- Caching (every query re-reads the schema from the source)

---

## 3. What the External Source Schema Discovery API Should Look Like

### Endpoint Design

Recommended: `GET /_esql/datasource/{name}/_schema` or `POST /_esql/datasource/{name}/_schema`

Rationale for `_schema` over `_field_caps`:
- External sources don't have "capabilities" like searchable/aggregatable
- The API returns schema information (field names, types, partitioning)
- Clearer intent for Kibana and other consumers
- Leaves room for `_field_caps` semantics if ES indices and external sources are ever unified

Alternative if we want 1:1 symmetry with `_field_caps`:
```
GET /_esql/datasource/{name}/_field_caps
POST /_esql/datasource/{name}/_field_caps
```

### Request Format

```json
POST /_esql/datasource/my_s3_logs/_schema
{
  "fields": ["*"],           // Optional: glob pattern for field name filtering
  "include_statistics": true, // Optional: include column-level statistics
  "include_unmapped": false,  // Optional: include fields with unknown types
  "sample_files": 5,          // Optional: number of files to sample for union-by-name
  "schema_resolution": "first_file"  // "first_file" | "union_by_name" | "strict"
}
```

For ad-hoc paths (no registered datasource):
```json
POST /_esql/_schema
{
  "location": "s3://my-bucket/logs/*.parquet",
  "fields": ["*"],
  "config": {
    "access_key": "...",
    "secret_key": "..."
  }
}
```

### Response Format

Modeled after `_field_caps` but adapted for external sources:

```json
{
  "source": "my_s3_logs",
  "source_type": "parquet",
  "location": "s3://my-bucket/logs/*.parquet",
  "files_matched": 42,
  "schema_resolution": "first_file",
  "fields": {
    "@timestamp": {
      "datetime": {
        "type": "datetime",
        "esql_type": "date",
        "native_type": "INT64/TIMESTAMP_MILLIS",
        "nullable": true,
        "partition_column": false,
        "files": null
      }
    },
    "message": {
      "keyword": {
        "type": "keyword",
        "esql_type": "keyword",
        "native_type": "BINARY/STRING",
        "nullable": true,
        "partition_column": false,
        "files": null
      }
    },
    "status_code": {
      "integer": {
        "type": "integer",
        "esql_type": "integer",
        "native_type": "INT32",
        "nullable": true,
        "partition_column": false,
        "files": null
      },
      "keyword": {
        "type": "keyword",
        "esql_type": "keyword",
        "native_type": "BINARY/STRING",
        "nullable": true,
        "partition_column": false,
        "files": ["s3://my-bucket/logs/old-format.parquet"]
      }
    },
    "year": {
      "keyword": {
        "type": "keyword",
        "esql_type": "keyword",
        "native_type": "hive_partition",
        "nullable": false,
        "partition_column": true,
        "files": null
      }
    }
  },
  "statistics": {
    "total_row_count": 1250000,
    "total_size_bytes": 524288000,
    "columns": {
      "@timestamp": {
        "null_count": 0,
        "min_value": "2024-01-01T00:00:00Z",
        "max_value": "2024-12-31T23:59:59Z"
      }
    }
  },
  "partition_columns": ["year"],
  "failures": []
}
```

### Key Design Decisions

**1. Type conflict reporting (like _field_caps):**
When `schema_resolution: "union_by_name"` and multiple files have different types for the same field, the response uses the same pattern as `_field_caps`: each conflicting type gets its own entry, with `files` listing which files have that type.

**2. Native type exposure:**
Unlike `_field_caps` which only returns ES types, the schema API exposes the `native_type` from the source format (e.g., Parquet's `INT64/TIMESTAMP_MILLIS`). This helps users understand type conversion and debug issues.

**3. Partition column annotation:**
Partition columns are annotated with `partition_column: true` so Kibana can show them differently (e.g., with a partition icon) and know they support efficient filtering.

**4. ESQL type vs source type:**
Both the ESQL type (used for queries) and the ES-compatible type name (for Kibana field formatting) are returned.

### Schema Inference vs Declared Schema

The API should support both modes:

**Inferred schema** (default): Read schema from the source itself
- Parquet: from file footer metadata
- Iceberg: from table metadata
- CSV/NDJSON: from sampling + type inference
- Flight: from `getSchema()` RPC

**Declared schema** (via CRUD API): Schema stored in Elasticsearch cluster state
- User creates a datasource definition with explicit field types
- Declared schema overrides inferred schema
- Useful for CSV/NDJSON where inference is unreliable
- The `_schema` API should indicate source: `"schema_source": "declared"` vs `"schema_source": "inferred"`

**Merged schema**: When declared schema exists but is partial
- Declared fields take precedence
- Inferred fields fill gaps
- `"schema_source": "merged"` in response
- Each field indicates `"declared": true/false`

### Caching Strategy

The API should cache aggressively since schema discovery requires I/O:

**Cache key**: `(datasource_name, location_pattern, schema_resolution_mode)`

**Cache invalidation**:
- TTL-based: configurable, default 5 minutes for file-based, 30 minutes for catalog-based
- Manual: `POST /_esql/datasource/{name}/_schema?refresh=true`
- On datasource CRUD update: invalidate immediately

**Cache location**: Node-local `ConcurrentHashMap` with TTL eviction (same pattern as `_field_caps` internal caching). No cluster-state persistence for the cache itself.

**For Kibana**: The cached response means repeated Kibana page loads don't re-scan S3/GCS. The TTL ensures schema changes are eventually picked up.

### Transport Action Design

```java
public class TransportExternalSchemaAction
    extends HandledTransportAction<ExternalSchemaRequest, ExternalSchemaResponse> {

    public static final String NAME = "cluster:admin/esql/datasource/schema";
    public static final ActionType<ExternalSchemaResponse> TYPE = new ActionType<>(NAME);
}
```

This should be a coordinator-only action (no fan-out to data nodes) since external source metadata resolution is centralized. The action:
1. Looks up the datasource definition (from cluster state if registered, or from request params if ad-hoc)
2. Calls `ExternalSourceResolver.resolve()` with the appropriate factory
3. For union-by-name: samples N files and merges schemas using `FieldCapabilities.Builder`-like logic
4. Applies declared schema overrides if present
5. Caches the result
6. Returns `ExternalSchemaResponse`

---

## 4. Kibana Integration

### How Kibana Uses _field_caps Today

Kibana's primary consumption of `_field_caps`:

1. **Data View creation/refresh**: When a user creates a data view (formerly index pattern) or navigates to any page using one, Kibana calls `_field_caps` to get the full field list.

2. **Field list sidebar**: The left sidebar in Discover, Lens, Maps all show available fields. This comes from `_field_caps`.

3. **Autocomplete/suggestions**: When typing in KQL or ES|QL, field name suggestions come from the cached field list.

4. **Field formatting**: Kibana uses the field type to determine how to display values (dates get formatted, IPs get special treatment, etc.).

5. **Filter building**: When clicking on a field value to filter, Kibana needs to know if the field is searchable and aggregatable.

### What Kibana Needs from the External Source Equivalent

For the ES|QL editor's autocomplete, Kibana needs:

1. **Field names and types** - for autocomplete suggestions and type-aware function argument completion
2. **Field type (ESQL type)** - Kibana's ES|QL autocomplete uses ESQL types (keyword, date, integer, etc.) to filter compatible functions
3. **Partition vs data columns** - for showing partition columns with different affordances (e.g., suggesting them in WHERE clauses for efficient filtering)

The ES|QL editor already has its own validation/autocomplete engine that works on the client side. It needs a way to fetch the field list for an external source, similar to how it fetches fields for ES indices.

### Proposed Kibana Integration Flow

```
User types: FROM datasource::my_s3_logs | WHERE ...
                                              ^
                                              cursor here

1. Kibana sees "datasource::my_s3_logs" in the FROM clause
2. Kibana calls GET /_esql/datasource/my_s3_logs/_schema?fields=*
3. Response provides field names + ESQL types
4. Autocomplete engine suggests fields compatible with WHERE context
5. Response is cached client-side (same as data view field caching)
```

For ad-hoc external sources (not registered via CRUD API):
```
User types: FROM "s3://bucket/logs/*.parquet" | WHERE ...

1. Kibana sees a quoted URI in FROM clause
2. Kibana calls POST /_esql/_schema with { "location": "s3://bucket/logs/*.parquet" }
3. Same response format, same caching
```

### Compatibility with Existing Data View Infrastructure

The schema response format is intentionally similar to `_field_caps` so that Kibana can reuse its existing field formatting and type mapping infrastructure. The key differences:

| _field_caps property | External schema equivalent | Notes |
|---|---|---|
| `type` | `type` | Same ES type strings |
| `searchable` | N/A | Always true for external sources |
| `aggregatable` | N/A | Always true for external sources |
| `metadata_field` | `partition_column` | Partition columns are the closest analog |
| `indices` | `files` | Which files have this type (only on conflicts) |
| `meta` | `meta` | Same metadata map structure |
| N/A | `native_type` | New: source format's native type |
| N/A | `esql_type` | New: ESQL-specific type name |

---

## 5. GitHub Issues and PRs

### Existing Related Issues

No specific GitHub issue exists for "schema discovery API for external data sources" in the elasticsearch repository. The closest related issues are:

- **[#122122] [ES|QL] Return data streams and indices that have data for a query** - Proposes an API to check if data exists for a query, used for Kibana onboarding UX decisions.

- **[#76509] Understanding Field capabilities API performance limitations** - Discusses performance issues with `_field_caps` when matching many indices with many fields. Relevant because the external source schema API must avoid the same pitfalls.

### Relevant PRs for Context

- **[#102510] ESQL: Make fieldcaps calls lighter** - Optimized field_caps to only request the fields ESQL actually needs, not `*`. The external schema API should support the same `fields` filter parameter.

- **[#118063] Smarter field caps with subscribable listener** - Refactored EsqlSession to use async chaining for field_caps calls. The external schema resolution already uses this pattern (runs in `preAnalyzeExternalSources` as an async step).

- **[#121918] ESQL: use field_caps native nested fields filtering** - Used field_caps filtering to exclude nested fields. External sources don't have nested fields, but the filtering pattern is relevant.

### Kibana Issues

- **[#167595] [DataView] Improve fields performance** - Kibana optimizing data view field fetching to reduce redundant `_field_caps` calls. The external schema API should be designed to be cache-friendly to avoid similar performance issues.

- **[#176082] Upgrade the Dataview API request to filter out "Empty fields"** - Kibana wants to exclude empty fields. The external schema API's `include_empty_fields` parameter serves the same purpose.

---

## 6. Implementation Recommendations

### Phase 1: MVP Tech Preview

1. **Internal-only API**: Add `ExternalSchemaAction` transport action, no REST endpoint yet
2. **First-file-wins only**: No union-by-name or strict mode
3. **No caching**: Re-resolve on every call (acceptable for TP)
4. **Simple response**: Field names + ESQL types + partition flag
5. **Kibana integration**: Kibana calls the internal action when it sees an external source in the FROM clause

### Phase 2: MVP GA

1. **REST endpoint**: `GET/POST /_esql/datasource/{name}/_schema`
2. **Ad-hoc endpoint**: `POST /_esql/_schema` for unregistered sources
3. **Caching**: TTL-based node-local cache with manual refresh
4. **Union-by-name**: Sample N files, merge schemas, report conflicts
5. **Declared schema support**: Override/merge with user-declared types
6. **Statistics**: Optional column-level statistics from Parquet/Iceberg metadata

### Phase 3: Post-MVP

1. **Schema evolution tracking**: Detect and report schema changes over time
2. **Schema compatibility checks**: Warn when schema drift breaks queries
3. **Async schema resolution**: For very large glob patterns, return a task ID
4. **Cross-cluster schema discovery**: Support CCS for external sources

### Key Files to Modify

| File | Change |
|---|---|
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java` | Add `resolveSchema()` method with union-by-name support |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java` | Implement `UNION_BY_NAME` schema resolution |
| New: `ExternalSchemaAction.java` | Transport action for schema discovery |
| New: `ExternalSchemaRequest.java` | Request with location, fields filter, options |
| New: `ExternalSchemaResponse.java` | Response modeled after `FieldCapabilitiesResponse` |
| New: `RestExternalSchemaAction.java` | REST handler for `/_esql/datasource/{name}/_schema` |
| New: `SchemaCache.java` | TTL-based cache for resolved schemas |
