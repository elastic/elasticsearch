# Deep Dive #31: PromQL Connector -- Querying Prometheus-Compatible Metrics Systems

## Executive Summary

**The situation is fundamentally different from what the roadmap assumed.** ES|QL already has a complete, native PromQL implementation that parses PromQL syntax, builds a PromQL-specific logical plan AST, and translates it into ESQL time series aggregates. This executes against Elasticsearch's own time-series indices (TSDB). The question of a "PromQL connector" to query external Prometheus/Thanos/Mimir/VictoriaMetrics is therefore **not about translating ESQL to PromQL** -- it is about **whether to use the existing Connector SPI to proxy queries to an external Prometheus HTTP API, or whether to extend the existing native PromQL parser to target external sources**.

There are two completely separate integration vectors:
1. **Native PromQL on local TSDB data** -- already implemented and shipping
2. **Querying external Prometheus-compatible systems** -- not implemented, requires connector work

This document focuses on vector 2: what it would take to build a connector that queries an external Prometheus HTTP API endpoint.

---

## 1. Connector SPI -- The Full Contract

### Source Files (all under `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/`)

**`ExternalSourceFactory.java`** (lines 20-39) -- Base interface for all external source factories:
- `type()` -- returns source type string (e.g., "flight", "prometheus")
- `canHandle(String location)` -- URI-matching predicate
- `resolveMetadata(String location, Map<String,Object> config)` -- returns `SourceMetadata` (schema, location, stats)
- `filterPushdownSupport()` -- optional, returns `FilterPushdownSupport` for predicate pushdown
- `operatorFactory()` -- optional, returns `SourceOperatorFactoryProvider` for custom operators
- `splitProvider()` -- defaults to `SplitProvider.SINGLE` (no parallelism)

**`ConnectorFactory.java`** (lines 17-26) -- Extends `ExternalSourceFactory`, adds:
- `open(Map<String,Object> config)` -- creates a live `Connector` instance

**`Connector.java`** (lines 22-29) -- Live connection, `Closeable`:
- `execute(QueryRequest request, Split split)` -- returns `ResultCursor`
- `execute(QueryRequest request, ExternalSplit split)` -- default delegates to `Split.SINGLE`

**`QueryRequest.java`** (lines 20-32) -- Immutable record:
- `target` -- the query target string
- `projectedColumns` -- column names to return
- `attributes` -- ESQL attributes with types
- `config` -- opaque config map
- `batchSize` -- page size hint
- `blockFactory` -- for building ESQL Blocks

**`ResultCursor.java`** (lines 17-20) -- Extends `CloseableIterator<Page>`:
- `hasNext()` / `next()` -- yields ESQL `Page` objects (columnar blocks)
- `cancel()` -- optional cancellation

**`SourceMetadata.java`** (lines 32-119) -- Schema + metadata:
- `schema()` -- `List<Attribute>` columns
- `sourceType()` -- e.g., "prometheus"
- `location()` -- original URI
- `statistics()` -- optional row count, size
- `sourceMetadata()` -- opaque plugin-specific data
- `config()` -- operator config

**`FilterPushdownSupport.java`** (lines 34-128) -- Filter pushdown:
- `pushFilters(List<Expression> filters)` -- returns `PushdownResult(Object pushedFilter, List<Expression> remainder)`
- `canPush(Expression expr)` -- `YES` / `NO` / `RECHECK`

**`DataSourcePlugin.java`** (lines 39-128) -- Plugin registration:
- `supportedConnectorSchemes()` -- URI schemes (e.g., `"prometheus"`)
- `connectors(Settings)` -- map of type to `ConnectorFactory`
- `namedWriteables()` -- for split serialization (if distributed)

### How a Connector Produces Pages

The dispatch chain in `OperatorFactoryRegistry.factory()` (lines 58-95 of `OperatorFactoryRegistry.java`):
1. Registry looks up `ExternalSourceFactory` by `sourceType`
2. If it's a `ConnectorFactory`, calls `cf.open(config)` to get a `Connector`
3. Builds a `QueryRequest` from `SourceOperatorContext`
4. Creates `AsyncConnectorSourceOperatorFactory(connector, request, maxBufferSize, executor)`

`AsyncConnectorSourceOperatorFactory.get(DriverContext)` (lines 64-110):
1. Binds `BlockFactory` to the request
2. Creates `AsyncExternalSourceBuffer`
3. Submits background task: calls `connector.execute(request, Split.SINGLE)`
4. Drains `ResultCursor` pages into buffer via `ExternalSourceDrainUtils.drainPages()`
5. Returns `AsyncExternalSourceOperator(buffer)` -- driver polls buffer for pages

---

## 2. Flight Connector as Template

### Plugin Structure

**Plugin class:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/GrpcDataSourcePlugin.java`
- Extends `Plugin implements DataSourcePlugin`
- `supportedConnectorSchemes()` returns `Set.of("flight", "grpc")`
- `connectors(Settings)` returns `Map.of("flight", new FlightConnectorFactory())`
- `getNamedWriteables()` registers `FlightSplit.ENTRY`

**Build file:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/build.gradle`
- `esplugin.classname = 'org.elasticsearch.xpack.esql.datasource.grpc.GrpcDataSourcePlugin'`
- `esplugin.extendedPlugins = ['x-pack-esql']`
- Dependencies: `compileOnly project(xpackModule('esql'))`, plus domain-specific libs

### Factory Implementation

**`FlightConnectorFactory.java`** (lines 31-90):
```java
class FlightConnectorFactory implements ConnectorFactory {
    String type() { return "flight"; }
    boolean canHandle(String location) { return location.startsWith("flight://") || location.startsWith("grpc://"); }

    SourceMetadata resolveMetadata(String location, Map<String,Object> config) {
        // Parse URI, connect to Flight server, call getSchema()
        // Convert Arrow Schema -> List<Attribute> via FlightTypeMapping
        // Return SimpleSourceMetadata with resolved config (endpoint, target)
    }

    Connector open(Map<String,Object> config) {
        return new FlightConnector((String) config.get("endpoint"));
    }

    SplitProvider splitProvider() {
        return new FlightSplitProvider(); // multi-endpoint discovery
    }
}
```

### Connector Implementation

**`FlightConnector.java`** (lines 39-143):
- Maintains `FlightClient` for default endpoint
- `ConcurrentHashMap<String, FlightClient>` for multi-endpoint splits
- `execute(QueryRequest, Split)` -- calls `getInfo()` + `getStream()`, returns `FlightResultCursor`
- `execute(QueryRequest, ExternalSplit)` -- dispatches to endpoint-specific client

### ResultCursor Implementation

**`FlightResultCursor.java`** (lines 26-77):
- Wraps `FlightStream` as `ResultCursor` (implements `CloseableIterator<Page>`)
- `next()`: reads `VectorSchemaRoot`, converts each column via `FlightTypeMapping.toBlock()`:
  - Iterates rows, builds `Block` using `BlockFactory` builders
  - `new Page(rowCount, blocks)` -- one Block per column

### Type Mapping

**`FlightTypeMapping.java`** (lines 41-144):
- Arrow `Int(<=32)` -> `DataType.INTEGER`, `Int(64)` -> `DataType.LONG`
- Arrow `FloatingPoint` -> `DataType.DOUBLE`
- Arrow `Utf8` -> `DataType.KEYWORD`
- Arrow `Bool` -> `DataType.BOOLEAN`
- Arrow `Timestamp` -> `DataType.DATETIME` (stored as `LongBlock`, epoch millis)

### Split Implementation

**`FlightSplit.java`** (lines 30-121):
- Implements `ExternalSplit extends NamedWriteable`
- Fields: `ticketBytes`, `location`, `estimatedRows`
- Serialization: `StreamInput`/`StreamOutput` + `getWriteableName()`

**`FlightSplitProvider.java`** (lines 32-70):
- Calls `getInfo()`, maps each `FlightEndpoint` to a `FlightSplit`

### Pattern Summary for PromQL Connector

A PromQL connector would follow exactly this pattern:
1. `PromqlConnectorFactory implements ConnectorFactory`
2. `PromqlConnector implements Connector`
3. `PromqlResultCursor implements ResultCursor`
4. `PromqlDataSourcePlugin extends Plugin implements DataSourcePlugin`

---

## 3. HTTP Client Infrastructure in Elasticsearch

### Three Existing Patterns

**Pattern A: Apache HttpClient 4.x (sync) -- Watcher**
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/common/http/HttpClient.java`
- Uses `CloseableHttpClient` from Apache HttpClient 4.x
- Synchronous, blocking calls
- Full SSL/TLS support via `SSLService`
- Watcher-specific: tied to `CryptoService`, allowlist filtering

**Pattern B: Apache HttpAsyncClient (async) -- Inference**
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/external/http/HttpClient.java`
- Uses `CloseableHttpAsyncClient` with `FutureCallback`
- Non-blocking, thread-pool-based response handling
- Connection pooling via `PoolingNHttpClientConnectionManager`
- Most modern pattern for making HTTP calls from plugins

**Pattern C: JDK HttpClient (Java 21) -- HTTP Data Source Plugin**
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-http/src/main/java/org/elasticsearch/xpack/esql/datasource/http/HttpStorageProvider.java`
- Uses `java.net.http.HttpClient` (built-in since Java 11, `AutoCloseable` since Java 21)
- Simplest approach, no external dependencies
- Already used in the ESQL data sources framework
- Configurable: `connectTimeout`, `followRedirects`, async via `ExecutorService`

### Recommendation for PromQL Connector

**Use Pattern C (JDK HttpClient)** -- it is already the pattern used by the ESQL data sources HTTP plugin, has zero external dependencies, and the connector SPI already executes on a background thread (via `AsyncConnectorSourceOperatorFactory`), so sync HTTP calls are perfectly fine. The connector's `execute()` runs off the driver thread already.

The HTTP data source plugin (`HttpDataSourcePlugin`) shows the executor injection pattern:
```java
// From HttpDataSourcePlugin.java, line 45
public Map<String, StorageProviderFactory> storageProviders(Settings settings, ExecutorService executor) {
    return Map.of("http", s -> new HttpStorageProvider(HttpConfiguration.defaults(), executor));
}
```

For connectors, the `AsyncConnectorSourceOperatorFactory` already handles background execution (line 94 of `AsyncConnectorSourceOperatorFactory.java`):
```java
executor.execute(() -> {
    try (ResultCursor cursor = connector.execute(request, Split.SINGLE)) {
        ExternalSourceDrainUtils.drainPages(cursor, buffer);
        buffer.finish(false);
    } catch (Exception e) {
        buffer.onFailure(e);
    } finally { ... }
});
```

So the connector can use blocking HTTP calls internally.

---

## 4. Prometheus HTTP API -- Data Model Mapping

### API Endpoints

The Prometheus HTTP API provides:
- `GET /api/v1/query` -- instant query at a single timestamp
- `GET /api/v1/query_range` -- range query with start/end/step
- `GET /api/v1/series` -- metadata/label discovery
- `GET /api/v1/labels` -- list label names
- `GET /api/v1/label/<name>/values` -- list label values
- `GET /api/v1/metadata` -- metric metadata (type, help, unit)

### Range Query Response Structure

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "instance": "host1", "job": "node"},
        "values": [[1704067200, "0.85"], [1704067260, "0.87"], [1704067320, "0.82"]]
      },
      {
        "metric": {"__name__": "cpu_usage", "instance": "host2", "job": "node"},
        "values": [[1704067200, "0.42"], [1704067260, "0.44"], [1704067320, "0.41"]]
      }
    ]
  }
}
```

### ESQL Type Mapping

| Prometheus Field | ESQL DataType | Block Type | Notes |
|---|---|---|---|
| `values[][0]` (timestamp) | `DataType.DATETIME` | `LongBlock` | Unix epoch seconds -> millis conversion |
| `values[][1]` (value) | `DataType.DOUBLE` | `DoubleBlock` | String -> double parse |
| `metric.__name__` | `DataType.KEYWORD` | `BytesRefBlock` | Metric name |
| `metric.<label>` | `DataType.KEYWORD` | `BytesRefBlock` | Each label becomes a column |

### Schema Discovery

The schema for a PromQL query is dynamic -- different metrics have different label sets. Two approaches:

**Approach A: Fixed schema (recommended for MVP)**
- All PromQL results mapped to: `@timestamp` (DATETIME), `value` (DOUBLE), `__name__` (KEYWORD), plus a dynamic set of label columns
- During `resolveMetadata()`, query `/api/v1/metadata` + `/api/v1/labels` to discover all labels
- Each label becomes a KEYWORD column; rows without that label get null

**Approach B: Query-derived schema**
- Execute a small range of the query during `resolveMetadata()`
- Inspect the result to discover actual labels present
- More precise but adds latency to planning

### Row Flattening

Prometheus returns nested time series; ESQL needs flat rows. Each `(timestamp, value)` pair in each series becomes one row:

```
@timestamp          | value | __name__  | instance | job
2024-01-01T00:00:00 | 0.85  | cpu_usage | host1    | node
2024-01-01T00:01:00 | 0.87  | cpu_usage | host1    | node
2024-01-01T00:02:00 | 0.82  | cpu_usage | host1    | node
2024-01-01T00:00:00 | 0.42  | cpu_usage | host2    | node
2024-01-01T00:01:00 | 0.44  | cpu_usage | host2    | node
2024-01-01T00:02:00 | 0.41  | cpu_usage | host2    | node
```

This is the natural ESQL row model and matches what downstream `STATS`, `WHERE`, `SORT` etc. expect.

---

## 5. PromQL Query Generation -- Translation Model

### Critical Discovery: ESQL Already Has Native PromQL

ES|QL has a **complete native PromQL implementation** that operates on local TSDB data:

**Parser:** ANTLR4 grammar at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/PromqlBaseParser.g4`
- Full PromQL expression grammar: selectors, aggregations, functions, binary ops, subqueries

**AST:** `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/promql/`
- `PromqlCommand` -- container plan node
- `InstantSelector`, `RangeSelector`, `LiteralSelector` -- metric selectors
- `AcrossSeriesAggregate`, `WithinSeriesAggregate` -- aggregation nodes
- `VectorBinaryOperator`, `VectorBinaryComparison` -- binary ops
- `PromqlFunctionCall`, `ScalarFunction` -- function calls

**Translation:** `TranslatePromqlToEsqlPlan.java` (882 lines) -- converts PromQL AST to standard ESQL plan nodes:
- `InstantSelector` -> `LastOverTime` aggregate
- `AcrossSeriesAggregate` -> `TimeSeriesAggregate` or `Aggregate`
- `PromqlFunctionCall` -> ESQL aggregate/scalar functions via `PromqlFunctionRegistry`
- Label matchers -> `Filter` with `Equals`, `In`, `StartsWith`, `EndsWith`, `RLike`

**Function registry:** `PromqlFunctionRegistry.java` -- maps PromQL functions to ESQL equivalents:
- `rate()`, `irate()`, `increase()`, `delta()`, `idelta()`, `deriv()`
- `sum_over_time()`, `avg_over_time()`, `min_over_time()`, `max_over_time()`, `count_over_time()`
- `ceil()`, `floor()`, `abs()`, `sqrt()`, `exp()`, `log2()`, `log10()`
- `sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`
- `clamp()`, `clamp_min()`, `clamp_max()`

**ES|QL syntax for PromQL:**
```
PROMQL start "2025-01-01T00:00:00Z" end "2025-01-01T01:00:00Z" step 5m (rate(http_requests_total[5m]))
```

**Capabilities flags:** `EsqlCapabilities.java` defines `PROMQL_COMMAND_V0`, `PROMQL_MATH_V0`, `PROMQL_BINARY_COMPARISON_V0`, `PROMQL_TIME`, `PROMQL_BUCKETS_PARAMETER`, `PROMQL_NESTED_AGGREGATES`, `PROMQL_SCALAR`, `PROMQL_IMPLICIT_RANGE_SELECTOR`, etc.

### Implications for External PromQL Connector

This means there are **two fundamentally different integration modes**:

**Mode 1: ESQL-native PromQL on external metrics (extend existing)**
- Use the existing PromQL parser + translator
- Replace the `EsRelation` (Elasticsearch index) with an external source that fetches raw metric data
- The PromQL translation runs locally, pushing only time-range + label filters to the external system
- The external system returns raw time series data; aggregation happens in ESQL
- Advantage: full PromQL compatibility, leverages ESQL optimizer
- Disadvantage: fetches raw data, potentially large data transfer

**Mode 2: Connector that passes PromQL through to external Prometheus (proxy)**
- Use the Connector SPI to send the PromQL string to the external Prometheus HTTP API
- Prometheus evaluates the query server-side
- Connector only receives results (timestamp/value/labels)
- Advantage: efficient -- aggregation happens at source, minimal data transfer
- Disadvantage: limited pushdown control, can't compose with ESQL pipe operations

**Recommended approach: Mode 2 (proxy) for external systems.** The existing native PromQL is for local TSDB data. For external Prometheus/Thanos/Mimir, the right model is to send the PromQL query to the remote system and consume the results. The Connector SPI is designed exactly for this.

### Query Mapping (Mode 2 -- Proxy)

For a proxy connector, the ESQL query would look like:
```
FROM prometheus://thanos.example.com:9090/api/v1/query_range?query=rate(http_requests_total[5m])
| WHERE instance == "host1"
| STATS avg_value = AVG(value) BY instance
```

Or with a dedicated syntax:
```
EXTERNAL "prometheus://thanos.example.com:9090" QUERY "rate(http_requests_total[5m])" START "2025-01-01T00:00:00Z" END "2025-01-01T01:00:00Z" STEP "5m"
| WHERE instance == "host1"
```

The connector would:
1. Parse the URI to extract endpoint + query parameters
2. Build the HTTP request: `GET {endpoint}/api/v1/query_range?query={query}&start={start}&end={end}&step={step}`
3. Parse the JSON response
4. Flatten time series into ESQL rows (Page objects)

### Filter Pushdown Opportunities

For Mode 2, limited but valuable pushdown is possible:

| ESQL Filter | PromQL Translation | Pushable? |
|---|---|---|
| `WHERE instance == "host1"` | Add `{instance="host1"}` to PromQL selector | YES (if query is simple metric name) |
| `WHERE @timestamp >= X AND @timestamp < Y` | Set `start` and `end` params | YES |
| `WHERE value > 0.5` | Can't express in PromQL query params | NO (post-filter) |
| `LIMIT 100` | Set `limit` param (Thanos/Mimir only) | PARTIAL |

The `FilterPushdownSupport` interface (lines 34-128 of `FilterPushdownSupport.java`) provides exactly the right abstraction:
- `canPush()` returns `YES` for label equality filters and timestamp ranges
- `canPush()` returns `NO` for value-based filters
- `pushFilters()` splits expressions into pushed (HTTP params) and remainder (ESQL FilterExec)

---

## 6. ResultCursor for JSON -- Prometheus Response Parsing

### Response Parsing Strategy

The Prometheus JSON response needs to be parsed into ESQL `Page` objects. The approach:

1. **Parse JSON** using `java.io.InputStream` + Jackson `JsonParser` (already available in ES via `x-content`)
   - Alternative: ES's `XContentParser` from `org.elasticsearch.xcontent.json.JsonXContent`
   - Located at: `/Users/oleglvovitch/github/root/elasticsearch/libs/x-content/src/main/java/org/elasticsearch/xcontent/json/JsonXContent.java`

2. **Streaming parse**: Process each time series incrementally to bound memory

3. **Build Pages** with BlockFactory builders, same pattern as `FlightResultCursor`:

```java
class PromqlResultCursor implements ResultCursor {
    private final JsonParser parser;
    private final List<Attribute> attributes;
    private final BlockFactory blockFactory;
    private final int batchSize;

    // Pre-parsed: list of (metric labels, values array iterator) pairs
    // Iterator over time series results

    @Override
    public Page next() {
        // Build blocks for: @timestamp (LongBlock), value (DoubleBlock),
        //                    __name__ (BytesRefBlock), label1..labelN (BytesRefBlock)
        LongBlock.Builder tsBuilder = blockFactory.newLongBlockBuilder(batchSize);
        DoubleBlock.Builder valBuilder = blockFactory.newDoubleBlockBuilder(batchSize);
        BytesRefBlock.Builder[] labelBuilders = ...;

        int rows = 0;
        while (rows < batchSize && hasMoreDataPoints()) {
            // Get next (timestamp, value) from current or next time series
            tsBuilder.appendLong(timestamp * 1000L); // seconds -> millis
            valBuilder.appendDouble(Double.parseDouble(value));
            // Append labels for this series (same for all points in a series)
            for (int i = 0; i < labelColumns.length; i++) {
                String labelValue = currentSeries.metric.get(labelColumns[i]);
                if (labelValue != null) {
                    labelBuilders[i].appendBytesRef(new BytesRef(labelValue));
                } else {
                    labelBuilders[i].appendNull();
                }
            }
            rows++;
        }

        Block[] blocks = new Block[2 + labelColumns.length];
        blocks[0] = tsBuilder.build();
        blocks[1] = valBuilder.build();
        for (int i = 0; i < labelColumns.length; i++) {
            blocks[2 + i] = labelBuilders[i].build();
        }
        return new Page(rows, blocks);
    }
}
```

### Memory Considerations

- Prometheus range queries can return large result sets (1000s of time series x 1000s of data points)
- Streaming parse is essential -- don't buffer the entire JSON response
- The `batchSize` from `QueryRequest` controls page size (default 1000 rows)
- `AsyncExternalSourceBuffer` has bounded capacity (`maxBufferSize` pages), providing backpressure

---

## 7. Existing Prometheus/Metrics Integration in Elasticsearch

### Prometheus Remote Write Plugin

**Location:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/prometheus/`

This is a **write-only** integration -- Prometheus pushes metrics to ES via Remote Write protocol:
- REST endpoint: `POST /_prometheus/api/v1/write` (protobuf body)
- `PrometheusRemoteWriteRestAction.java` -- handles Remote Write requests
- `PrometheusRemoteWriteTransportAction.java` -- processes protobuf, indexes to data streams
- `PrometheusIndexTemplateRegistry.java` -- manages index templates for metrics
- Feature-flagged: `PROMETHEUS_FEATURE_FLAG` + `XPackSettings.PROMETHEUS_ENABLED`

**No read/query API is implemented.** The Prometheus plugin only handles ingest (write path).

### Native PromQL in ES|QL

As described in section 5, ES|QL has a complete PromQL parser and translator:
- **50+ files** in `x-pack/plugin/esql/src/main/java/.../promql/`
- Full ANTLR4 grammar for PromQL syntax
- Translates to ESQL `TimeSeriesAggregate` nodes
- Operates on **local Elasticsearch TSDB indices**
- Supports: instant selectors, range selectors, aggregations (sum/avg/min/max/count/stddev), rate, irate, increase, delta, binary operators, comparison operators, scalar functions (abs, ceil, floor, sqrt, etc.), nested aggregations

### Metrics Monitoring

ES has its own metrics collection (not PromQL-compatible):
- `x-pack/plugin/monitoring/` -- Monitoring plugin (exports to ES indices)
- Apm/OTel-based metrics collection
- No Prometheus query federation

---

## 8. Plugin Structure -- What Would Be Needed

### Directory Layout

Following the Flight connector pattern (`x-pack/plugin/esql-datasource-grpc/`):

```
x-pack/plugin/esql-datasource-prometheus/
  build.gradle
  src/main/java/org/elasticsearch/xpack/esql/datasource/prometheus/
    PromqlDataSourcePlugin.java          -- Plugin registration
    PromqlConnectorFactory.java          -- ConnectorFactory impl
    PromqlConnector.java                 -- Connector impl (HTTP client)
    PromqlResultCursor.java              -- ResultCursor (JSON -> Pages)
    PromqlTypeMapping.java               -- Prometheus -> ESQL type mapping
    PromqlConfiguration.java             -- Endpoint config, auth, timeouts
    PromqlFilterPushdown.java            -- Optional: label/time filter pushdown
  src/test/java/org/elasticsearch/xpack/esql/datasource/prometheus/
    PromqlConnectorFactoryTests.java
    PromqlConnectorTests.java
    PromqlResultCursorTests.java
    PromqlTypeMappingTests.java
    PromqlFilterPushdownTests.java
  src/test/resources/
    sample_range_response.json
    sample_instant_response.json
```

### Build File

```groovy
apply plugin: 'elasticsearch.internal-es-plugin'

esplugin {
    name = 'esql-datasource-prometheus'
    description = 'PromQL connector for ESQL external data sources'
    classname = 'org.elasticsearch.xpack.esql.datasource.prometheus.PromqlDataSourcePlugin'
    extendedPlugins = ['x-pack-esql']
}

dependencies {
    compileOnly project(path: xpackModule('esql'))
    compileOnly project(path: xpackModule('esql-core'))
    compileOnly project(path: xpackModule('core'))
    compileOnly project(':server')
    compileOnly project(xpackModule('esql:compute'))
    // No external deps needed -- uses JDK HttpClient + ES xcontent for JSON
    testImplementation project(':test:framework')
}
```

### Plugin Class

```java
public class PromqlDataSourcePlugin extends Plugin implements DataSourcePlugin {
    @Override
    public Set<String> supportedConnectorSchemes() {
        return Set.of("prometheus", "prom");
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of("prometheus", new PromqlConnectorFactory());
    }
}
```

### ConnectorFactory

```java
class PromqlConnectorFactory implements ConnectorFactory {
    @Override
    public String type() { return "prometheus"; }

    @Override
    public boolean canHandle(String location) {
        return location.startsWith("prometheus://") || location.startsWith("prom://");
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String,Object> config) {
        // Parse URI: prometheus://host:9090/query?query=rate(foo[5m])
        // Call /api/v1/labels to discover label names
        // Build schema: @timestamp (DATETIME), value (DOUBLE), label1..N (KEYWORD)
        // Return SimpleSourceMetadata
    }

    @Override
    public Connector open(Map<String,Object> config) {
        return new PromqlConnector(config);
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        return new PromqlFilterPushdown(); // time range + label equality
    }
}
```

### Key Dependencies (all already in ES)

- `java.net.http.HttpClient` -- HTTP calls (zero external deps)
- `org.elasticsearch.xcontent.json.JsonXContent` -- JSON parsing
- `org.elasticsearch.compute.data.BlockFactory` -- building ESQL Blocks/Pages
- `org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute` -- schema attributes
- `org.elasticsearch.xpack.esql.core.type.DataType` -- DATETIME, DOUBLE, KEYWORD

---

## 9. Complexity and Effort Estimate

### Minimal Viable Connector (Mode 2 -- Proxy)

| Component | Effort | Notes |
|---|---|---|
| PromqlDataSourcePlugin | 0.5d | Boilerplate, follows Flight pattern |
| PromqlConnectorFactory | 1d | URI parsing, metadata resolution via HTTP |
| PromqlConnector | 1d | JDK HttpClient, builds query URL |
| PromqlResultCursor | 2d | JSON streaming parse -> Page, row flattening |
| PromqlTypeMapping | 0.5d | Fixed: DATETIME + DOUBLE + KEYWORD columns |
| PromqlFilterPushdown | 1d | Time range params, label equality matchers |
| Tests | 2d | Unit tests, mock Prometheus server |
| Build/config/docs | 0.5d | build.gradle, settings, licensing |
| **Total** | **~8.5d** | ~2 weeks with one engineer |

### Open Design Questions

1. **Syntax**: `EXTERNAL "prometheus://host:9090" QUERY "rate(foo[5m])"` vs. `FROM prometheus://host:9090/query_range?query=rate(foo[5m])` vs. future `FROM prometheus::metric_name`?

2. **Schema discovery**: Query `/api/v1/labels` for all labels, or execute a sample query during `resolveMetadata()`? Former is broader but may include irrelevant labels; latter is precise but slower.

3. **Authentication**: Prometheus API supports Bearer token, Basic auth, mTLS. Configuration via CRUD API (same as other external sources) or plugin settings?

4. **Compatibility**: Should the connector work with Prometheus, Thanos, Mimir, VictoriaMetrics, Cortex? The HTTP API is standard across all; Thanos/Mimir add `limit` and `partial_response` params.

5. **Integration with native PromQL**: Could the PROMQL command be extended to target external Prometheus? E.g., `PROMQL index="prometheus://host:9090" (rate(http_requests[5m]))` -- this would send the inner PromQL directly to the external system. The `index` parameter already exists in the PromQL command grammar (see `LogicalPlanBuilder.java` line 1400).

6. **Result size limits**: Prometheus has a default `--query.max-samples` limit (50M). Large range queries can hit this. The connector should handle `"error"` responses and propagate them as ESQL errors.

---

## 10. Key File References

### Connector SPI
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ConnectorFactory.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/Connector.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ResultCursor.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSourceFactory.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/QueryRequest.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FilterPushdownSupport.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/DataSourcePlugin.java`

### Operator Dispatch
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/OperatorFactoryRegistry.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncConnectorSourceOperatorFactory.java`

### Flight Connector (template)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/GrpcDataSourcePlugin.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnectorFactory.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnector.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightResultCursor.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightTypeMapping.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightSplit.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightSplitProvider.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/build.gradle`

### Native PromQL in ES|QL
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/PromqlBaseParser.g4`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/promql/PromqlCommand.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/promql/TranslatePromqlToEsqlPlan.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/promql/function/PromqlFunctionRegistry.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/promql/PromqlLogicalPlanBuilder.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlCapabilities.java` (lines 1899-1966)

### Prometheus Plugin (write-only)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/prometheus/src/main/java/org/elasticsearch/xpack/prometheus/PrometheusPlugin.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/prometheus/src/main/java/org/elasticsearch/xpack/prometheus/rest/PrometheusRemoteWriteRestAction.java`

### HTTP Client Infrastructure
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-http/src/main/java/org/elasticsearch/xpack/esql/datasource/http/HttpStorageProvider.java` (JDK HttpClient pattern)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/external/http/HttpClient.java` (Apache async pattern)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/common/http/HttpClient.java` (Apache sync pattern)

### ES|QL Parser Integration
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java` (lines 1295-1370: PromQL command parsing, including `index` parameter at line 1400)
