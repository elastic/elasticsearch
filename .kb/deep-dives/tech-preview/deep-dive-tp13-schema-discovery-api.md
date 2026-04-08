# Deep Dive: Schema Discovery API for External Data Sources

## Summary

This document investigates the current state of schema resolution for external data sources in ES|QL and analyzes what is needed to build a dedicated REST endpoint for schema discovery (e.g., `GET _datasource/{name}/_fields`). The goal is to enable Kibana to discover fields in external data sources without running a full ES|QL query, analogous to how the `_field_caps` API enables Kibana field discovery for Elasticsearch indices.

---

## 1. resolveMetadata() in ExternalSourceResolver

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`

### How it works

`ExternalSourceResolver.resolve()` (line 85-125) is the entry point. It takes:
- `List<String> paths` -- external source locations (e.g., `s3://bucket/file.parquet`)
- `Map<String, Map<String, Expression>> pathParams` -- per-path configuration from WITH clauses
- `ActionListener<ExternalSourceResolution> listener` -- async callback

Resolution runs on a dedicated executor (line 96) to avoid blocking. For each path:

1. **Glob check** (line 135-137): If the path contains `*`, `,`, or other multi-file patterns, `resolveMultiFileSource()` expands the glob, reads metadata from the first file, then enriches the schema with Hive-style partition columns.
2. **Single source resolution** (line 195-235): Iterates `DataSourceModule.sourceFactories()` to find the first `ExternalSourceFactory` whose `canHandle(path)` returns true, then calls `factory.resolveMetadata(path, config)`.
3. **Result wrapping** (line 307-353): The `SourceMetadata` returned by the factory is wrapped into `ExternalSourceMetadata` (which extends `SourceMetadata`) with merged config from query-level WITH params.

### The critical call: `factory.resolveMetadata(path, config)`

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSourceFactory.java` (line 26)

```java
SourceMetadata resolveMetadata(String location, Map<String, Object> config);
```

This is the SPI contract that every external source factory implements. It returns a `SourceMetadata` containing the schema.

### How each factory implements resolveMetadata:

- **FileSourceFactory** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java`, line 92-110): Opens a `StorageObject` via the storage provider, gets a `FormatReader` by file extension, calls `reader.metadata(storageObject)` which reads the file header (e.g., Parquet footer) and returns `SourceMetadata`.
- **LazyTableCatalogWrapper** (in DataSourceModule.java, line 463-464): Delegates to `TableCatalog.metadata(location, config)` which resolves from the catalog system (e.g., Iceberg metadata).
- **LazyConnectorFactory** (in DataSourceModule.java, line 360-361): Delegates to the resolved `ConnectorFactory.resolveMetadata(location, config)`.

---

## 2. SourceMetadata Structure

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceMetadata.java`

### Interface definition (lines 32-119):

```java
public interface SourceMetadata {
    List<Attribute> schema();          // line 40 -- ESQL Attribute list (columns)
    String sourceType();               // line 48 -- e.g., "parquet", "iceberg", "csv"
    String location();                 // line 56 -- URI or path
    Optional<SourceStatistics> statistics();        // line 64 -- row count, size
    Optional<List<String>> partitionColumns();      // line 75 -- partition column names
    Map<String, Object> sourceMetadata();           // line 95 -- opaque source-specific data
    Map<String, Object> config();                   // line 116 -- credentials/config for operators
}
```

### Key insight: Schema uses ESQL types, not ES types

The `schema()` method returns `List<Attribute>` where each `Attribute` carries:
- **Name**: `attribute.name()` -- the column name (inherited from `NamedExpression`)
- **DataType**: `attribute.dataType()` -- an ESQL `DataType` enum value, NOT an Elasticsearch field type

The `DataType` enum (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/DataType.java`, line 171+) uses ES-style type names as keys (e.g., `"keyword"`, `"long"`, `"boolean"`, `"date"`, `"double"`, `"integer"`, `"ip"`, `"geo_point"`) but represents the ESQL compute engine's type system. Each enum value has an `esType()` string and a `typeName()`.

### Concrete implementation: SimpleSourceMetadata

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SimpleSourceMetadata.java` (lines 21-202)

Fields (lines 23-29):
```java
private final List<Attribute> schema;
private final String sourceType;
private final String location;
private final SourceStatistics statistics;
private final List<String> partitionColumns;
private final Map<String, Object> sourceMetadata;
private final Map<String, Object> config;
```

Includes a builder pattern (lines 154-201).

### ExternalSourceMetadata -- legacy bridge

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceMetadata.java` (lines 31-78)

Extends `SourceMetadata` with deprecated aliases: `tablePath()` -> `location()`, `attributes()` -> `schema()`. New code should use `SourceMetadata` directly.

### SourceStatistics

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceStatistics.java` (lines 19-71)

```java
public interface SourceStatistics {
    OptionalLong rowCount();
    OptionalLong sizeInBytes();
    Optional<Map<String, ColumnStatistics>> columnStatistics();  // per-column stats

    interface ColumnStatistics {
        OptionalLong nullCount();
        OptionalLong distinctCount();
        Optional<Object> minValue();
        Optional<Object> maxValue();
    }
}
```

### Attribute representation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/expression/Attribute.java` (lines 41-271)

Abstract class with key properties:
- `name()` -- column name (from `NamedExpression`)
- `dataType()` -- ESQL `DataType` enum
- `nullable()` -- `Nullability` enum (TRUE, FALSE, UNKNOWN)
- `qualifier()` -- optional namespace qualifier
- `id()` -- unique `NameId` for plan identity

External sources use `ReferenceAttribute` as the concrete type:

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/expression/ReferenceAttribute.java` (lines 25-132)

```java
public class ReferenceAttribute extends TypedAttribute {
    // Constructed as:
    new ReferenceAttribute(Source.EMPTY, null, name, type, Nullability.TRUE, null, true)
}
```

This is what `ExternalSourceResolver.enrichSchemaWithPartitionColumns()` creates for partition columns (line 253), and what FormatReaders typically produce.

---

## 3. Field Capabilities API Pattern (Reference)

### REST Handler

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/rest/action/RestFieldCapabilitiesAction.java` (lines 34-119)

Routes (lines 46-51):
```java
new Route(GET, "/_field_caps"),
new Route(POST, "/_field_caps"),
new Route(GET, "/{index}/_field_caps"),
new Route(POST, "/{index}/_field_caps")
```

Parameters: `fields` (comma-separated or body), `include_unmapped`, `filters`, `types`, `index_filter` (body), `runtime_mappings` (body).

Creates `FieldCapabilitiesRequest`, dispatches via `NodeClient.fieldCaps()`.

### Transport Action

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/fieldcaps/TransportFieldCapabilitiesAction.java` (lines 95-146)

```java
public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    public static final String NAME = "indices:data/read/field_caps";
    // ...
    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, ActionListener<FieldCapabilitiesResponse> listener)
}
```

Pattern: `HandledTransportAction` with `ActionType`, `@Inject` constructor, delegates execution.

### Response

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/fieldcaps/FieldCapabilitiesResponse.java` (lines 38-80)

```java
public class FieldCapabilitiesResponse extends ActionResponse implements ChunkedToXContentObject {
    private final String[] indices;
    private final Map<String, Map<String, FieldCapabilities>> fields;  // field name -> type -> capabilities
    private final List<FieldCapabilitiesFailure> failures;
}
```

The response is a nested map: field name -> ES type name -> capabilities (searchable, aggregatable, etc.).

### ESQL-specific fork: EsqlResolveFieldsAction

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlResolveFieldsAction.java` (lines 33-94)

ES|QL has its own fork of field_caps at action name `"indices:data/read/esql/resolve_fields"`. It wraps `TransportFieldCapabilitiesAction` but returns `EsqlResolveFieldsResponse` instead of `FieldCapabilitiesResponse`. This is registered in `EsqlPlugin.getActions()` (line 334).

This demonstrates the precedent for ES|QL-specific field resolution endpoints.

---

## 4. Existing REST Endpoints in ESQL

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/EsqlPlugin.java`

### REST handlers registered (lines 366-379):

| Handler Class | Route | Method |
|---|---|---|
| `RestEsqlQueryAction` | `/_query` | POST |
| `RestEsqlAsyncQueryAction` | `/_query/async` | POST |
| `RestEsqlGetAsyncResultAction` | `/_query/async/{id}` | GET |
| `RestEsqlStopAsyncAction` | `/_query/async/{id}/_stop` | POST |
| `RestEsqlDeleteAsyncResultAction` | `/_query/async/{id}` | DELETE |
| `RestEsqlListQueriesAction` | `/_query/queries`, `/_query/queries/{id}` | GET |

When `ESQL_VIEWS_FEATURE_FLAG` is enabled (lines 374-378):

| Handler Class | Route | Method |
|---|---|---|
| `RestPutViewAction` | `/_query/view/{name}` | PUT |
| `RestDeleteViewAction` | `/_query/view/{name}` | DELETE |
| `RestGetViewAction` | `/_query/view/{name}`, `/_query/view` | GET |

### Transport actions registered (lines 327-351):

| Action | Transport Class |
|---|---|
| `EsqlQueryAction` | `TransportEsqlQueryAction` |
| `EsqlAsyncGetResultAction` | `TransportEsqlAsyncGetResultsAction` |
| `EsqlStatsAction` | `TransportEsqlStatsAction` |
| `EsqlResolveFieldsAction` | `EsqlResolveFieldsAction` (self-contained) |
| `EsqlSearchShardsAction` | `EsqlSearchShardsAction` (self-contained) |
| `EsqlAsyncStopAction` | `TransportEsqlAsyncStopAction` |
| `EsqlListQueriesAction` | `TransportEsqlListQueriesAction` |
| `EsqlGetQueryAction` | `TransportEsqlGetQueryAction` |
| Views: `PutViewAction`, `DeleteViewAction`, `GetViewAction` | `TransportPut/Delete/GetViewAction` |

**No existing endpoint for external data source schema discovery.** The `EsqlResolveFieldsAction` is for ES index fields only (delegates to field_caps).

---

## 5. EsqlPlugin Registration Pattern

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/EsqlPlugin.java`

### Pattern summary:

1. **REST handlers**: Created in `getRestHandlers()` (line 355). Returns `List<RestHandler>`. Each handler extends `BaseRestHandler`, defines `routes()`, `getName()`, and `prepareRequest()`.

2. **Transport actions**: Registered in `getActions()` (line 327). Returns `List<ActionHandler>` where each entry binds an `ActionType<Response>` to a transport action class.

3. **Components**: Created in `createComponents()` (line 206). Returns injected objects. `DataSourceModule` is created here (line 242) and added to the component list (line 268). `PlanExecutor` receives it (line 259).

### How DataSourceModule flows to ExternalSourceResolver:

- `EsqlPlugin.createComponents()` creates `DataSourceModule` (line 242-248)
- `DataSourceModule` is passed to `PlanExecutor` (line 259) and also registered as a component (line 268)
- `PlanExecutor` uses it to create `ExternalSourceResolver` for query execution
- A schema discovery endpoint would need access to `DataSourceModule` to call `resolveMetadata` directly

---

## 6. What Needs to Be Built

### Proposed endpoint: `GET /_query/datasource/_fields`

A REST endpoint that accepts a data source location (and optional config), resolves its schema without executing a query, and returns the field list.

### Classes needed:

#### A. Request class: `EsqlDataSourceFieldsRequest`

Similar to `FieldCapabilitiesRequest` but for external sources:
```java
public class EsqlDataSourceFieldsRequest extends ActionRequest {
    private String location;          // e.g., "s3://bucket/data.parquet"
    private Map<String, String> config;  // optional credentials/settings (access_key, etc.)
}
```

#### B. Response class: `EsqlDataSourceFieldsResponse`

```java
public class EsqlDataSourceFieldsResponse extends ActionResponse implements ToXContentObject {
    private String location;
    private String sourceType;        // "parquet", "iceberg", "csv", etc.
    private List<FieldInfo> fields;   // name + type pairs
    // Optional:
    private SourceStatistics statistics;
    private List<String> partitionColumns;
}
```

Where `FieldInfo` contains:
- `name` (String) -- column name from `Attribute.name()`
- `type` (String) -- ESQL type name from `DataType.typeName()` or `DataType.esType()` (e.g., "keyword", "long", "double", "boolean", "datetime")
- `nullable` (boolean) -- from `Attribute.nullable()`

The response would serialize to JSON like:
```json
{
  "location": "s3://bucket/data.parquet",
  "source_type": "parquet",
  "fields": [
    { "name": "timestamp", "type": "datetime" },
    { "name": "message", "type": "keyword" },
    { "name": "status_code", "type": "integer" },
    { "name": "duration_ms", "type": "double" }
  ],
  "partition_columns": ["year", "month"],
  "statistics": {
    "row_count": 1000000,
    "size_in_bytes": 52428800
  }
}
```

#### C. REST handler: `RestEsqlDataSourceFieldsAction`

```java
@ServerlessScope(Scope.PUBLIC)
public class RestEsqlDataSourceFieldsAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_query/datasource/_fields"),
            new Route(POST, "/_query/datasource/_fields")
        );
    }
    // POST for body with config/credentials, GET for simple cases
}
```

#### D. Transport action: `TransportEsqlDataSourceFieldsAction`

This is the core logic. It needs:
1. Access to `DataSourceModule` (injected as a component)
2. A way to call `ExternalSourceResolver.resolve()` or directly iterate factories

The simplest implementation:
```java
public class TransportEsqlDataSourceFieldsAction
    extends HandledTransportAction<EsqlDataSourceFieldsRequest, EsqlDataSourceFieldsResponse> {

    private final DataSourceModule dataSourceModule;
    private final ThreadPool threadPool;

    @Inject
    public TransportEsqlDataSourceFieldsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        DataSourceModule dataSourceModule,
        ThreadPool threadPool
    ) { ... }

    @Override
    protected void doExecute(Task task, EsqlDataSourceFieldsRequest request,
                            ActionListener<EsqlDataSourceFieldsResponse> listener) {
        // Run on GENERIC thread pool to avoid blocking transport threads
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                // Iterate source factories, find handler, resolve metadata
                for (ExternalSourceFactory factory : dataSourceModule.sourceFactories().values()) {
                    if (factory.canHandle(request.location())) {
                        SourceMetadata metadata = factory.resolveMetadata(
                            request.location(), request.configAsMap());
                        listener.onResponse(toResponse(metadata));
                        return;
                    }
                }
                listener.onFailure(new IllegalArgumentException("No handler for: " + request.location()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
```

#### E. Action type: `EsqlDataSourceFieldsAction`

```java
public class EsqlDataSourceFieldsAction {
    public static final String NAME = "cluster:monitor/esql/datasource/fields";
    public static final ActionType<EsqlDataSourceFieldsResponse> INSTANCE = new ActionType<>(NAME);
}
```

#### F. Registration in EsqlPlugin

In `getActions()` (line 327):
```java
new ActionHandler(EsqlDataSourceFieldsAction.INSTANCE, TransportEsqlDataSourceFieldsAction.class)
```

In `getRestHandlers()` (line 355):
```java
new RestEsqlDataSourceFieldsAction()
```

### Total new files needed: ~4-5

1. `EsqlDataSourceFieldsAction.java` (action type + request + response, can be one file or split)
2. `TransportEsqlDataSourceFieldsAction.java` (transport action)
3. `RestEsqlDataSourceFieldsAction.java` (REST handler)
4. Tests

### Key design decisions:

1. **Reuse ExternalSourceResolver vs. direct factory call**: The transport action can either reuse `ExternalSourceResolver` (which handles glob expansion, partition enrichment, config merging) or call factories directly. Reusing the resolver is better because it handles multi-file schema discovery correctly.

2. **Type representation in response**: The schema comes as ESQL `DataType` enum values. The response should expose these as their `esType()` strings (e.g., "keyword", "long", "double") since that is what Kibana already understands from `_field_caps`. The `DataType.typeName()` gives the ESQL display name (e.g., "DATETIME" for `date`, "KEYWORD" for `keyword`) which could be returned as a secondary field.

3. **Authentication/credentials**: The request needs to carry config (access_key, secret_key, endpoint, etc.) for sources that require authentication. These should flow through the same `config` map that `resolveMetadata` already accepts. This is a security-sensitive area -- credentials in REST requests need proper handling (HTTPS-only, no logging, etc.).

4. **Caching**: Schema resolution involves I/O (reading Parquet footers, Iceberg metadata). The roadmap mentions schema caching as a separate work item. The discovery API should be designed to work with a cache but not require it for the initial implementation.

5. **Error handling**: When a source is unreachable or the format is unrecognized, the endpoint should return structured errors (4xx) rather than 500s. The existing `ExternalSourceResolver` already throws structured exceptions (line 201: `UnsupportedSchemeException`, line 225: `UnsupportedOperationException`).

---

## Appendix: Relevant File Index

| Component | Absolute Path | Key Lines |
|---|---|---|
| ExternalSourceResolver | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java` | 85-125 (resolve), 195-235 (resolveSingleSource) |
| SourceMetadata (SPI) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceMetadata.java` | 32-119 (full interface) |
| SimpleSourceMetadata | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SimpleSourceMetadata.java` | 21-202 |
| ExternalSourceMetadata | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceMetadata.java` | 31-78 |
| ExternalSourceFactory (SPI) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSourceFactory.java` | 20-39 |
| ConnectorFactory (SPI) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ConnectorFactory.java` | 17-26 |
| TableCatalog (SPI) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/TableCatalog.java` | 32-77 |
| FormatReader (SPI) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java` | 32-85 |
| SourceStatistics | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceStatistics.java` | 19-71 |
| FileSourceFactory | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java` | 92-110 (resolveMetadata) |
| DataSourceModule | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/DataSourceModule.java` | 48-502 |
| ExternalSourceResolution | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolution.java` | 16-35 |
| ExternalRelation | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java` | 46-159 |
| Attribute | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/expression/Attribute.java` | 41-271 |
| ReferenceAttribute | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/expression/ReferenceAttribute.java` | 25-132 |
| DataType enum | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/DataType.java` | 171+ (enum values) |
| EsqlPlugin | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/EsqlPlugin.java` | 118-459 |
| RestEsqlQueryAction | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/RestEsqlQueryAction.java` | 28-74 |
| RestEsqlListQueriesAction | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/RestEsqlListQueriesAction.java` | 25-52 |
| EsqlResolveFieldsAction | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlResolveFieldsAction.java` | 33-94 |
| RestFieldCapabilitiesAction | `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/rest/action/RestFieldCapabilitiesAction.java` | 34-119 |
| TransportFieldCapabilitiesAction | `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/fieldcaps/TransportFieldCapabilitiesAction.java` | 95-146 |
| FieldCapabilitiesResponse | `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/fieldcaps/FieldCapabilitiesResponse.java` | 38-80 |
| GetViewAction (reference pattern) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/view/GetViewAction.java` | 34-140 |
| RestGetViewAction (reference pattern) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/view/RestGetViewAction.java` | 25-57 |
| TransportGetViewAction (reference pattern) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/view/TransportGetViewAction.java` | 35-96 |
