<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Source hash: 2335753018fcaa8b02e77e0ce2dd1eb7eb0a54a3 -->
<!-- Generator: Claude Code (claude-opus-4-5-20251101) -->
<!-- Tracked files:
     - action/RestEsqlQueryAction.java
     - action/RestEsqlAsyncQueryAction.java
     - action/RequestXContent.java
     - action/EsqlQueryRequest.java
     - action/EsqlQueryTask.java
     - plugin/TransportEsqlQueryAction.java
     - execution/PlanExecutor.java
     - session/Result.java
     - action/EsqlQueryResponse.java
     - action/EsqlResponseListener.java
     - action/EsqlExecutionInfo.java
-->

# Chapter I: ES|QL Request/Response Lifecycle

This chapter describes how ES|QL queries enter the system, flow through coordination, and return results. The planner and compute engine are treated as black boxes—this focuses on I/O and control flow. For APIs that manage async queries and monitor running queries, see [Chapter II: Query Management & Monitoring](./chapter-02-monitoring.md).

---

## API Endpoints Overview

ES|QL exposes two endpoints for query execution:

| Method | Path | Handler | Description |
|--------|------|---------|-------------|
| `POST` | `/_query` | [`RestEsqlQueryAction`](#1-synchronous-query-post-_query) | Execute a synchronous query |
| `POST` | `/_query/async` | [`RestEsqlAsyncQueryAction`](#2-asynchronous-query-post-_queryasync) | Execute an asynchronous query |

For async query management (get results, delete, stop) and query monitoring endpoints, see [Chapter II](./chapter-02-monitoring.md).

---

## High-Level Overview

When a client sends an ES|QL query to Elasticsearch, the request enters through the REST layer at `/_query`. The `RestEsqlQueryAction` handler receives the HTTP request, parses the JSON body into an [`EsqlQueryRequest`](#3-request-parsing) object, and hands it off to the transport layer for execution on the coordinator node.

The [`TransportEsqlQueryAction`](#4-transport-action) is the coordinator's orchestrator. It determines whether this is a synchronous or asynchronous query, sets up the execution context (session ID, execution metadata, cluster settings), and creates a bridge to the compute service. Rather than calling the planner and compute engine directly, it delegates to [`PlanExecutor`](#5-plan-executor), which acts as a factory for [`EsqlSession`](#6-session-execution) instances.

[`PlanExecutor`](#5-plan-executor) creates a fresh [`EsqlSession`](#6-session-execution) for each query, wiring together all the dependencies needed for planning: the index resolver for schema discovery, the function registry, the analyzer, the mapper, and the verifier. It then invokes the session's `execute()` method, which orchestrates the entire planning pipeline—parsing the query string, resolving field types against index mappings, optimizing the logical plan, and mapping it to a physical plan.

Once planning completes, the session calls the [`PlanRunner`](#planrunner-bridge-to-compute)—a callback injected by [`TransportEsqlQueryAction`](#4-transport-action) that wraps `ComputeService.execute()`. The compute service executes the physical plan across the cluster's data nodes and collects the results into a [`Result`](#7-result-object) object containing the schema (column names and types) and the data (as columnar `Page` objects).

Control then returns to [`TransportEsqlQueryAction`](#4-transport-action), which converts the [`Result`](#7-result-object) into an [`EsqlQueryResponse`](#9-response-rendering). This response object knows how to serialize itself to JSON (or CSV, or Arrow format) via the `toXContentChunked()` method. Finally, the [`EsqlResponseListener`](#10-response-listener) takes this response, determines the appropriate output format based on the client's `Accept` header, and streams the result back over HTTP.

Throughout this flow, [`EsqlExecutionInfo`](#11-execution-metadata) tracks metadata about the query execution—timing, cluster status for cross-cluster searches, and whether results are partial due to failures.

---

## Component Interaction Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              HTTP Layer                                      │
│  POST /_query  ───────────────────────────────────────►  HTTP Response      │
└───────────┬─────────────────────────────────────────────────────▲───────────┘
            │                                                     │
            ▼                                                     │
┌───────────────────────┐                           ┌─────────────────────────┐
│  RestEsqlQueryAction  │                           │  EsqlResponseListener   │
│  ───────────────────  │                           │  ─────────────────────  │
│  • Parse JSON body    │                           │  • Format selection     │
│  • Create request obj │                           │  • JSON/CSV/Arrow       │
│  • Extract URL params │                           │  • Stream to HTTP       │
└───────────┬───────────┘                           └─────────────▲───────────┘
            │                                                     │
            │ EsqlQueryRequest                                    │ RestResponse
            ▼                                                     │
┌─────────────────────────────────────────────────────────────────┴───────────┐
│                        TransportEsqlQueryAction                              │
│  ────────────────────────────────────────────────────────────────────────── │
│  • Sync/async routing         • Session ID generation                       │
│  • Cluster settings           • Create PlanRunner (→ ComputeService)        │
│  • EsqlExecutionInfo setup    • Result → EsqlQueryResponse conversion       │
└───────────┬─────────────────────────────────────────────────────▲───────────┘
            │                                                     │
            │ EsqlQueryRequest                                    │ Result
            ▼                                                     │
┌───────────────────────┐                                         │
│     PlanExecutor      │                                         │
│  ───────────────────  │                                         │
│  • Session factory    │                                         │
│  • Metrics collection │                                         │
│  • Telemetry          │                                         │
└───────────┬───────────┘                                         │
            │                                                     │
            │ Creates & invokes                                   │
            ▼                                                     │
┌───────────────────────┐         PlanRunner          ┌───────────────────────┐
│     EsqlSession       │ ──────────────────────────► │    ComputeService     │
│  ───────────────────  │        (callback)           │  ───────────────────  │
│  ┌─────────────────┐  │                             │  ┌─────────────────┐  │
│  │ PLANNER         │  │                             │  │ COMPUTE ENGINE  │  │
│  │ Parse→Analyze→  │  │                             │  │ Execute plan    │  │
│  │ Optimize→Map    │  │                             │  │ across cluster  │  │
│  └─────────────────┘  │                             │  └─────────────────┘  │
└───────────────────────┘                             └───────────┬───────────┘
                                                                  │
                                                                  │ Result
                                                                  ▼
                                                      ┌───────────────────────┐
                                                      │        Result         │
                                                      │  ───────────────────  │
                                                      │  • Schema (columns)   │
                                                      │  • Pages (data)       │
                                                      │  • Completion info    │
                                                      └───────────────────────┘
```

---

## 1. Synchronous Query: `POST /_query`

**File**: `action/RestEsqlQueryAction.java` (74 LOC)

The REST handler is the entry point for all synchronous ES|QL queries arriving over HTTP. It is deliberately minimal—its only job is to translate the HTTP request into the internal representation and hand off to the transport layer.

When a request arrives at `POST /_query`, the handler uses [`RequestXContent`](#3-request-parsing)`.parseSync()` to deserialize the JSON body into an [`EsqlQueryRequest`](#3-request-parsing) object. This parsing step validates the JSON structure and extracts all query parameters, pragmas, and options. The handler also checks for the `allow_partial_results` URL parameter, which controls whether the query should return partial results if some shards or clusters fail.

The handler wraps the execution in a `RestCancellableNodeClient`, which allows the query to be cancelled if the HTTP connection is closed. This is important for long-running queries—if the user closes their browser or the connection times out, the query doesn't continue consuming cluster resources.

```
POST /_query
Content-Type: application/json

{
  "query": "FROM logs | WHERE status >= 400 | LIMIT 100"
}
```

**Key Code**:
```java
@Override
public List<Route> routes() {
    return List.of(new Route(POST, "/_query"));
}

@Override
protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    try (XContentParser parser = request.contentOrSourceParamParser()) {
        return restChannelConsumer(RequestXContent.parseSync(parser), request, client);
    }
}
```

---

## 2. Asynchronous Query: `POST /_query/async`

**File**: `action/RestEsqlAsyncQueryAction.java` (57 LOC)

The async query endpoint allows clients to submit long-running queries without blocking. Instead of waiting for results, the client receives an execution ID immediately and can poll for results later using the [get async results API](./chapter-02-monitoring.md#1-get-async-results).

This handler is nearly identical to the sync handler, but uses [`RequestXContent`](#3-request-parsing)`.parseAsync()` which sets `async: true` on the request and parses async-specific fields like `keep_on_completion`, `wait_for_completion_timeout`, and `keep_alive`.

```
POST /_query/async
Content-Type: application/json

{
  "query": "FROM logs | WHERE status >= 400 | STATS count(*) BY host",
  "wait_for_completion_timeout": "5s",
  "keep_on_completion": true,
  "keep_alive": "1d"
}
```

**Response (if query completes within timeout)**:
```json
{
  "id": "FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUToxMDE=",
  "is_running": false,
  "took": 42,
  "columns": [...],
  "values": [...]
}
```

**Response (if query still running)**:
```json
{
  "id": "FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUToxMDE=",
  "is_running": true
}
```

### Async Result Storage

When [`TransportEsqlQueryAction`](#4-transport-action) receives an async query, it delegates to `AsyncTaskManagementService.asyncExecute()`. This creates an [`EsqlQueryTask`](#esqlquerytask)—a subclass of `StoredAsyncTask<EsqlQueryResponse>`—that manages the query's lifecycle and result persistence.

The async framework stores results in the [`.async-search`](#async-search-index) system index (defined as `XPackPlugin.ASYNC_RESULTS_INDEX`). This is a hidden, system-managed index that:

- Stores query results as documents keyed by the [`AsyncExecutionId`](#asyncexecutionid)
- Automatically handles TTL-based expiration via the `keep_alive` parameter
- Supports result retrieval even if the coordinating node fails (results are persisted to the cluster)
- Provides security isolation—only the user who submitted the query can retrieve results

<a id="esqlquerytask"></a>
### EsqlQueryTask

**File**: `action/EsqlQueryTask.java` (65 LOC)

```java
public class EsqlQueryTask extends StoredAsyncTask<EsqlQueryResponse> {
    private EsqlExecutionInfo executionInfo;

    public EsqlQueryTask(
        long id, String type, String action, String description,
        TaskId parentTaskId, Map<String, String> headers,
        Map<String, String> originHeaders, AsyncExecutionId asyncExecutionId,
        TimeValue keepAlive
    ) {
        super(id, type, action, description, parentTaskId, headers,
              originHeaders, asyncExecutionId, keepAlive);
    }

    @Override
    public EsqlQueryResponse getCurrentResult() {
        // Returns partial results while query is running
        return new EsqlQueryResponse(
            List.of(), List.of(), 0, 0, null, false,
            getExecutionId().getEncoded(), true, true, ZoneOffset.UTC,
            getStartTime(), getExpirationTimeMillis(), executionInfo
        );
    }
}
```

<a id="asyncexecutionid"></a>
### AsyncExecutionId

The `AsyncExecutionId` encapsulates both the task ID (for task management operations) and the document ID (for retrieving stored results from [`.async-search`](#async-search-index)). When `keep_on_completion` is true, the final [`EsqlQueryResponse`](#9-response-rendering) is serialized and stored in this index, allowing retrieval long after the query task has completed.

<a id="async-search-index"></a>
### .async-search Index

The `.async-search` system index is where async query results are persisted. It's managed by the `AsyncTaskIndexService` and provides:

- Document-based storage keyed by [`AsyncExecutionId`](#asyncexecutionid)
- Automatic cleanup based on `keep_alive` TTL
- Security context preservation for result retrieval

For APIs to retrieve, delete, or stop async queries, see [Chapter II: Query Management & Monitoring](./chapter-02-monitoring.md).

---

## 3. Request Parsing

**File**: `action/RequestXContent.java`

The request parsing layer transforms raw JSON into a strongly-typed [`EsqlQueryRequest`](#3-request-parsing) object using Elasticsearch's `ObjectParser` framework. This provides automatic validation, type coercion, and clear error messages for malformed requests.

The parser handles the full complexity of ES|QL requests: the query string itself, optional QueryDSL filters that get pushed down to the source, query parameters for parameterized queries, inline tables for lookup operations, timezone and locale settings for date/time handling, and various execution options.

**Supported Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `query` | String | **Required**. The ES|QL query string |
| `columnar` | Boolean | Return results in columnar format (default: false) |
| `filter` | QueryDSL | Additional filter applied to source data |
| `params` | Array | Query parameters (named or positional) |
| `tables` | Object | Inline table data for lookup |
| `time_zone` | String | Timezone for date/time operations |
| `locale` | String | Locale for formatting |
| `profile` | Boolean | Include profiling information |
| `pragma` | Object | Query hints/pragmas |

**Async-specific fields**:

| Field | Type | Description |
|-------|------|-------------|
| `keep_on_completion` | Boolean | Keep results after completion |
| `wait_for_completion_timeout` | Duration | How long to wait before returning async ID |
| `keep_alive` | Duration | How long to keep results |

Query parameters support type detection from JSON literals. When you pass `{"params": [100, "error", true]}`, the parser automatically infers INTEGER, STRING, and BOOLEAN types. Parameters can be classified as VALUE (for literals in expressions), IDENTIFIER (for column names), or PATTERN (for index patterns), allowing safe parameterization of different parts of the query.

**Parameter Types**:
```java
// Supported parameter types (auto-detected from JSON)
STRING, INTEGER, LONG, DOUBLE, BOOLEAN, NULL

// Parameter classifications
VALUE       // Literal value: WHERE status = ?
IDENTIFIER  // Column/field name: FROM ?
PATTERN     // Index pattern: FROM ?
```

---

## 4. Transport Action

**File**: `plugin/TransportEsqlQueryAction.java` (524 LOC)

The transport action is the coordinator node's central orchestrator. It receives the parsed request from the REST layer and manages the entire query lifecycle: setting up execution context, routing to sync or async execution paths, invoking the planner, bridging to compute, and constructing the final response.

One of its first responsibilities is determining the execution mode. If the request specifies `async: true` or uses the `/_query/async` endpoint, the transport action delegates to `AsyncTaskManagementService`, which creates a background [`EsqlQueryTask`](#esqlquerytask) and returns an [`AsyncExecutionId`](#asyncexecutionid) immediately. For synchronous queries, execution proceeds inline.

The transport action creates the [`EsqlExecutionInfo`](#11-execution-metadata) object that tracks metadata throughout execution. This includes timing information, per-cluster status for cross-cluster searches, and whether the results are partial due to failures. For cross-cluster queries, it consults `RemoteClusterService` to determine which clusters should be queried and how to handle failures.

A key design pattern here is the [`PlanRunner`](#planrunner-bridge-to-compute) callback. Rather than having the transport action call the compute service directly, it creates a lambda that wraps `ComputeService.execute()` and passes this to [`PlanExecutor`](#5-plan-executor). This inversion of control allows the session to complete all planning work before execution begins, and keeps the compute service decoupled from the planning layer.

### Execution Flow

```java
@Override
protected void doExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
    // Execute on SEARCH thread pool (workaround for #97916)
    requestExecutor.execute(ActionRunnable.wrap(listener, l -> doExecuteForked(task, request, l)));
}

private void doExecuteForked(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
    if (requestIsAsync(request)) {
        asyncTaskManagementService.asyncExecute(request, ...);
    } else {
        innerExecute(task, request, listener);
    }
}
```

### Session Setup

```java
private void innerExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
    // 1. Apply defaults
    if (request.allowPartialResults() == null) {
        request.allowPartialResults(defaultAllowPartialResults);
    }

    // 2. Create session ID (unique per query, based on task ID)
    String sessionId = new TaskId(clusterService.localNode().getId(), task.getId()).toString();

    // 3. Create execution metadata tracker
    EsqlExecutionInfo executionInfo = createEsqlExecutionInfo(request);

    // 4. Create plan runner (bridges to compute service)
    PlanRunner planRunner = (plan, config, foldCtx, profile, resultListener) ->
        computeService.execute(sessionId, task, flags, plan, config, ...);

    // 5. Execute through PlanExecutor
    planExecutor.esql(request, sessionId, ..., planRunner, listener);
}
```

---

## 5. Plan Executor

**File**: `execution/PlanExecutor.java` (147 LOC)

The `PlanExecutor` serves as a factory and coordination point for query execution. It owns the long-lived components that are shared across queries—the index resolver, function registry, mapper, and verifier—and creates fresh [`EsqlSession`](#6-session-execution) instances for each query.

This separation of concerns is important: the session is stateful and tied to a single query's lifecycle, while the executor manages shared resources and collects metrics across all queries. The executor tracks telemetry about query patterns, function usage, and success/failure rates, publishing this data to the metrics system.

When `esql()` is called, the executor wires together all dependencies the session needs. The `IndexResolver` provides schema discovery through the FieldCaps API. The `EsqlFunctionRegistry` contains all built-in functions. The `Mapper` translates logical plans to physical plans. The `Verifier` performs semantic validation after analysis. All of these are injected into a new session instance.

After creating the session, the executor wraps the listener to handle success and failure cases. On success, it publishes telemetry and logs the query. On failure, it records the failure in metrics and ensures proper cleanup.

```java
public void esql(EsqlQueryRequest request, String sessionId, ..., ActionListener<Versioned<Result>> listener) {
    // Create telemetry tracker
    final PlanTelemetry planTelemetry = new PlanTelemetry(functionRegistry);

    // Create session with all dependencies
    final var session = new EsqlSession(
        sessionId,
        localClusterMinimumVersion,
        analyzerSettings,
        indexResolver,
        enrichPolicyResolver,
        viewResolver,
        preAnalyzer,
        functionRegistry,
        mapper,
        verifier,
        planTelemetry,
        indicesExpressionGrouper,
        projectMetadata,
        services
    );

    // Track metrics
    metrics.total(clientId);

    // Execute and handle success/failure
    ActionListener.run(executeListener, l -> session.execute(request, executionInfo, planRunner, l));
}
```

---

## 6. Session Execution

**File**: `session/EsqlSession.java`

The `EsqlSession` orchestrates the complete query planning lifecycle. From the perspective of [`TransportEsqlQueryAction`](#4-transport-action), it's a black box that accepts a request and eventually produces a [`Result`](#7-result-object). Internally, it coordinates parsing, analysis, optimization, and physical planning before invoking the compute layer.

The session's `execute()` method is the entry point. It first parses the query string into an abstract syntax tree, then resolves any view references (named queries that expand inline). Next comes analysis: the analyzer resolves field names against index mappings retrieved via the `IndexResolver`, validates types, and builds a fully-typed logical plan.

After analysis, the plan passes through multiple optimization phases. The pre-optimizer handles operations that may require async work (like ML inference). The logical optimizer applies rewrites like predicate pushdown, constant folding, and projection pruning. Finally, the mapper translates the optimized logical plan into a physical plan with concrete execution strategies.

Once physical planning completes, the session invokes the [`PlanRunner`](#planrunner-bridge-to-compute) callback that was injected by [`TransportEsqlQueryAction`](#4-transport-action). This triggers actual execution in the compute layer, which returns a [`Result`](#7-result-object) containing the query output.

```java
public void execute(
    EsqlQueryRequest request,
    EsqlExecutionInfo executionInfo,
    PlanRunner planRunner,
    ActionListener<Versioned<Result>> listener
) {
    // [BLACK BOX: Planner]
    // 1. Parse query string
    // 2. Resolve views
    // 3. Analyze (schema resolution, type checking)
    // 4. Pre-optimize
    // 5. Optimize logical plan
    // 6. Map to physical plan
    // 7. Optimize physical plan

    // Then call planRunner to execute
    planRunner.run(physicalPlan, configuration, foldCtx, planTimeProfile, resultListener);
}
```

The [`PlanRunner`](#planrunner-bridge-to-compute) is the bridge to compute—it's injected by [`TransportEsqlQueryAction`](#4-transport-action) and wraps `ComputeService.execute()`.

---

## 7. Result Object

**File**: `session/Result.java` (37 LOC)

The `Result` record is the output contract between compute and the response layer. It encapsulates everything needed to construct the final response: the schema describing each column, the actual data as columnar pages, the [`Configuration`](#configuration) used during execution, and metadata about how the query ran.

The schema is a list of `Attribute` objects, each describing a column's name and data type. This allows the response layer to generate proper column headers and apply type-appropriate formatting (like rendering dates in the configured timezone).

The pages contain the actual query results in columnar format. Each `Page` holds multiple `Block` objects—one per column—with the actual values. This columnar layout is efficient for both execution (SIMD operations) and serialization (type-homogeneous compression).

The `DriverCompletionInfo` captures execution statistics: how many documents were scanned, how many values were loaded, and detailed profiling information if profiling was enabled. The [`EsqlExecutionInfo`](#11-execution-metadata) tracks cross-cluster metadata for CCS queries.

```java
public record Result(
    List<Attribute> schema,     // Column names and types
    List<Page> pages,           // Columnar data blocks
    Configuration configuration, // Query configuration used
    DriverCompletionInfo completionInfo,  // Execution stats
    @Nullable EsqlExecutionInfo executionInfo  // CCS metadata
) {}
```

| Field | Description |
|-------|-------------|
| `schema` | List of `Attribute` describing each column (name, data type) |
| `pages` | List of `Page` objects, each containing columnar `Block`s |
| `configuration` | Timezone, locale, pragmas used during execution |
| `completionInfo` | Documents found, values loaded, driver profiles |
| `executionInfo` | Cross-cluster search metadata, timing |

---

## 8. Response Construction

**File**: `plugin/TransportEsqlQueryAction.java` → `toResponse()`

Once compute returns a [`Result`](#7-result-object), the [`TransportEsqlQueryAction`](#4-transport-action) transforms it into an [`EsqlQueryResponse`](#9-response-rendering) suitable for serialization. This transformation extracts the schema into `ColumnInfoImpl` objects (which know their output type names for JSON), carries over the data pages unchanged, and packages up profiling and execution metadata.

The conversion handles edge cases like `UnsupportedAttribute`—columns with types that ES|QL can't fully process. These retain their original type names so clients can see what the underlying field type was, even if ES|QL treats them as opaque.

For async queries that specify `keepOnCompletion`, the response includes the [`AsyncExecutionId`](#asyncexecutionid) and expiration time, allowing clients to retrieve results later via the [get async results API](./chapter-02-monitoring.md#1-get-async-results).

```java
private EsqlQueryResponse toResponse(Task task, EsqlQueryRequest request, boolean profileEnabled, Versioned<Result> result) {
    var innerResult = result.inner();

    // Convert schema to column info
    List<ColumnInfoImpl> columns = innerResult.schema().stream().map(c -> {
        List<String> originalTypes = (c instanceof UnsupportedAttribute ua)
            ? new ArrayList<>(ua.originalTypes())
            : null;
        return new ColumnInfoImpl(c.name(), c.dataType().outputType(), originalTypes);
    }).toList();

    // Build profile if requested
    Profile profile = profileEnabled
        ? new Profile(completionInfo.driverProfiles(), completionInfo.planProfiles(), ...)
        : null;

    return new EsqlQueryResponse(
        columns,
        innerResult.pages(),
        innerResult.completionInfo().documentsFound(),
        innerResult.completionInfo().valuesLoaded(),
        profile,
        request.columnar(),
        request.async(),
        configuration.zoneId(),
        ...
    );
}
```

---

## 9. Response Rendering

**File**: `action/EsqlQueryResponse.java`

The `EsqlQueryResponse` implements `ChunkedToXContentObject`, which enables streaming serialization to JSON. Rather than building the entire response in memory, it produces an iterator of content chunks that can be written incrementally. This is important for large result sets—the response can start flowing to the client before all data is ready.

The serialization order is carefully designed for client usability: async metadata (if applicable) comes first so clients can detect running queries, then timing and partial-result status, then the column schema (so clients know the structure before data arrives), and finally the values array which may be very large.

The `columnar` option controls data layout in the JSON. When false (default), values are row-oriented: `[["a", 1], ["b", 2]]`. When true, values are column-oriented: `[["a", "b"], [1, 2]]`. The columnar format is more compact when columns have homogeneous types and compresses better.

```java
@Override
public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
    var content = new ArrayList<Iterator<? extends ToXContent>>();

    content.add(ChunkedToXContentHelper.startObject());

    // Async metadata (if async query)
    if (isAsync) {
        // "id": "...", "is_running": true/false
    }

    // Timing and status
    // "took": 123, "is_partial": false

    // Column schema
    // "columns": [{"name": "host", "type": "keyword"}, ...]

    // Values (chunked for streaming)
    content.add(ChunkedToXContentHelper.array("values",
        ResponseXContentUtils.columnValues(columns, pages, columnar, ...)));

    // CCS metadata (if cross-cluster)
    // "_clusters": { ... }

    // Profile (if requested)
    // "profile": { "drivers": [...], "plans": [...] }

    content.add(ChunkedToXContentHelper.endObject());

    return Iterators.concat(content.toArray(Iterator[]::new));
}
```

**JSON Response Structure**:
```json
{
  "took": 42,
  "is_partial": false,
  "documents_found": 1000,
  "values_loaded": 3000,
  "columns": [
    {"name": "host", "type": "keyword"},
    {"name": "status", "type": "integer"},
    {"name": "@timestamp", "type": "date"}
  ],
  "values": [
    ["server1", 200, "2024-01-15T10:30:00Z"],
    ["server2", 500, "2024-01-15T10:31:00Z"]
  ]
}
```

---

## 10. Response Listener

**File**: `action/EsqlResponseListener.java` (257 LOC)

The `EsqlResponseListener` is the final link in the chain, responsible for taking the [`EsqlQueryResponse`](#9-response-rendering) and sending it back over HTTP in the format the client requested. It examines the `Accept` header (or `format` query parameter) to determine whether to produce JSON, CSV, TSV, plain text, or Apache Arrow binary output.

For JSON output, it delegates to the response's `toXContentChunked()` method. For text formats like CSV, it uses `TextFormat` formatters that iterate over the result pages and produce delimited output. For Arrow, it wraps the pages in an `ArrowResponse` that serializes the columnar data in Arrow's streaming IPC format.

The listener also handles logging: it logs partial failures from any clusters that returned errors, and at debug level logs query completion with timing information. This is valuable for operational monitoring without requiring explicit profiling.

```java
private RestResponse buildResponse(EsqlQueryResponse esqlResponse) {
    if (mediaType instanceof TextFormat format) {
        // CSV, TSV, plain text
        return RestResponse.chunked(RestStatus.OK,
            ChunkedRestResponseBodyPart.fromTextChunks(
                format.contentType(restRequest),
                format.format(restRequest, esqlResponse)));

    } else if (mediaType == ArrowFormat.INSTANCE) {
        // Apache Arrow binary format
        ArrowResponse arrowResponse = new ArrowResponse(columns, pages);
        return RestResponse.chunked(RestStatus.OK, arrowResponse, ...);

    } else {
        // JSON (default)
        return RestResponse.chunked(RestStatus.OK,
            ChunkedRestResponseBodyPart.fromXContent(esqlResponse, ...));
    }
}
```

**Supported Output Formats**:

| Format | Content-Type | Description |
|--------|--------------|-------------|
| JSON | `application/json` | Default, structured output |
| CSV | `text/csv` | Comma-separated values |
| TSV | `text/tab-separated-values` | Tab-separated values |
| Plain | `text/plain` | Human-readable text |
| Arrow | `application/vnd.apache.arrow.stream` | Binary columnar format |

**HTTP Headers Added**:
- `Took-nanos`: Execution time in nanoseconds

---

## 11. Execution Metadata

**File**: `action/EsqlExecutionInfo.java`

The `EsqlExecutionInfo` class tracks metadata about query execution, with particular focus on cross-cluster search scenarios. It maintains per-cluster status, aggregates timing information, and tracks whether results are partial due to cluster or shard failures.

For cross-cluster searches, each remote cluster has its own status tracking: RUNNING while the query executes, SUCCESSFUL on completion, SKIPPED if the cluster was unavailable and partial results are enabled, or FAILED if the cluster returned an error. This granular tracking allows clients to understand exactly which parts of a federated query succeeded.

The `IncludeExecutionMetadata` enum controls when this information appears in responses. ALWAYS includes it for every query (useful for debugging), CCS_ONLY includes it only when multiple clusters are involved (the common case), and NEVER omits it entirely (for minimal response size).

```java
public class EsqlExecutionInfo {

    enum IncludeExecutionMetadata {
        ALWAYS,     // Always include in response
        CCS_ONLY,   // Include only for cross-cluster searches
        NEVER       // Never include
    }

    // Per-cluster status tracking
    ConcurrentMap<String, Cluster> clusterInfo;

    // Cluster status values
    enum Status { RUNNING, SUCCESSFUL, SKIPPED, FAILED }

    // Query timing
    EsqlQueryProfile queryProfile;

    // Is result partial (some clusters skipped)?
    boolean isPartial;
}
```

---

## File Summary

| File | LOC | Purpose |
|------|-----|---------|
| [`RestEsqlQueryAction.java`](#1-synchronous-query-post-_query) | 74 | Sync query endpoint |
| [`RestEsqlAsyncQueryAction.java`](#2-asynchronous-query-post-_queryasync) | 57 | Async query endpoint |
| [`RequestXContent.java`](#3-request-parsing) | ~200 | Request JSON parsing |
| [`EsqlQueryRequest.java`](#3-request-parsing) | ~400 | Request object |
| [`EsqlQueryTask.java`](#esqlquerytask) | 65 | Async task with stored results |
| [`TransportEsqlQueryAction.java`](#4-transport-action) | 524 | Main coordinator orchestration |
| [`PlanExecutor.java`](#5-plan-executor) | 147 | Session factory, metrics |
| [`EsqlSession.java`](#6-session-execution) | ~800 | Query lifecycle (planner wrapper) |
| [`Result.java`](#7-result-object) | 37 | Execution result record |
| [`EsqlQueryResponse.java`](#9-response-rendering) | ~450 | Response object, JSON rendering |
| [`EsqlResponseListener.java`](#10-response-listener) | 257 | HTTP response formatting |
| [`EsqlExecutionInfo.java`](#11-execution-metadata) | ~400 | Execution metadata, CCS tracking |

---

## Key Interfaces

<a id="planrunner-bridge-to-compute"></a>
### PlanRunner (bridge to compute)

The `PlanRunner` functional interface defines the contract between planning and execution. It's implemented as a lambda in [`TransportEsqlQueryAction`](#4-transport-action) that wraps `ComputeService.execute()`, allowing the session to remain decoupled from the compute layer.

```java
@FunctionalInterface
public interface PlanRunner {
    void run(
        PhysicalPlan physicalPlan,
        Configuration configuration,
        FoldContext foldContext,
        EsqlPlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    );
}
```

<a id="configuration"></a>
### Configuration

The `Configuration` record carries all runtime settings through the planning and execution pipeline. It includes time-related settings (timezone, query start time), formatting settings (locale), resource limits (result truncation), and query-specific options (pragmas, inline tables).

```java
public Configuration(
    ZoneId zoneId,
    Instant now,
    Locale locale,
    String username,
    String clusterName,
    QueryPragmas pragmas,
    int resultTruncationMaxSize,
    int resultTruncationDefaultSize,
    String query,
    boolean profile,
    Map<String, Map<String, Column>> tables,
    long queryStartTimeNanos,
    boolean allowPartialResults,
    ...
)
```
