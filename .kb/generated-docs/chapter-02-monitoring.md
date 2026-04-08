<!-- GENERATED DOC - DO NOT EDIT MANUALLY -->
<!-- Generated: 2026-02-05 -->
<!-- Source hash: 5cad86c58738b1e36536b61703b13a1cfc0ce510 -->
<!-- Generator: Claude Code (claude-opus-4-5-20251101) -->
<!-- Tracked files:
     - action/RestEsqlListQueriesAction.java
     - action/RestEsqlGetAsyncResultAction.java
     - action/RestEsqlDeleteAsyncResultAction.java
     - action/RestEsqlStopAsyncAction.java
     - plugin/TransportEsqlListQueriesAction.java
     - plugin/TransportEsqlGetQueryAction.java
     - plugin/TransportEsqlAsyncGetResultsAction.java
     - plugin/TransportEsqlAsyncStopAction.java
     - plugin/EsqlQueryStatus.java
     - compute/operator/DriverStatus.java
-->

# Chapter II: Query Management & Monitoring

This chapter covers the ES|QL APIs for managing async queries (retrieving results, deleting, stopping) and monitoring running queries. These endpoints interact with the [`.async-search` index](./chapter-01-request-response.md#async-search-index) for stored results and the Task Management API for execution visibility.

For query submission (sync and async), see [Chapter I: Request/Response Lifecycle](./chapter-01-request-response.md).

---

## API Endpoints Overview

| Method | Path | Handler | Description |
|--------|------|---------|-------------|
| `GET` | `/_query/async/{id}` | [`RestEsqlGetAsyncResultAction`](#1-get-async-results) | [Retrieve async query results](#1-get-async-results) |
| `DELETE` | `/_query/async/{id}` | [`RestEsqlDeleteAsyncResultAction`](#2-delete-async-results) | [Delete async query results](#2-delete-async-results) |
| `POST` | `/_query/async/{id}/stop` | [`RestEsqlStopAsyncAction`](#3-stop-async-query) | [Stop a running async query](#3-stop-async-query) |
| `GET` | `/_query/queries` | [`RestEsqlListQueriesAction`](#4-list-running-queries) | [List all running queries](#4-list-running-queries) |
| `GET` | `/_query/queries/{id}` | [`RestEsqlListQueriesAction`](#5-get-query-details) | [Get details of a specific query](#5-get-query-details) |

---

## Architecture Overview

The management and monitoring APIs operate at two levels:

1. **Async Result Management** (GET/DELETE/STOP async): Interact with the [`.async-search` index](./chapter-01-request-response.md#async-search-index) where async query results are stored, and coordinate with the task management system for running queries.

2. **Query Monitoring** (list/get queries): Query the cluster's task management API to discover running ES|QL tasks and extract execution statistics from compute drivers.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            REST Layer                                        │
│  GET/DELETE/STOP /_query/async/{id} ───► Async Result Management            │
│  GET /_query/queries[/{id}] ───────────► Query Monitoring                   │
└───────────┬─────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Transport Layer                                     │
│  ┌────────────────────────┐  ┌────────────────────────┐  ┌────────────────┐ │
│  │ TransportEsqlAsync     │  │ TransportEsqlAsync     │  │ TransportEsql  │ │
│  │ GetResultsAction       │  │ StopAction             │  │ ListQueries    │ │
│  │ ────────────────────── │  │ ────────────────────── │  │ Action         │ │
│  │ • Fetch from           │  │ • Stop exchange early  │  │ ────────────── │ │
│  │   .async-search        │  │ • Return partial       │  │ • Query task   │ │
│  │ • Long-poll option     │  │   results              │  │   API          │ │
│  └────────────────────────┘  └────────────────────────┘  └────────────────┘ │
└───────────┬───────────────────────────────┬─────────────────────┬───────────┘
            │                               │                     │
            ▼                               ▼                     ▼
┌───────────────────────────┐  ┌───────────────────────┐  ┌───────────────────┐
│   .async-search Index     │  │   ExchangeService     │  │ Task Management   │
│   (XPackPlugin.ASYNC_     │  │   (finishSessionEarly)│  │ API               │
│   RESULTS_INDEX)          │  │                       │  │ (ListTasksAction) │
└───────────────────────────┘  └───────────────────────┘  └───────────────────┘
```

---

## Part A: Async Query Management

These APIs manage the lifecycle of async queries after submission. For submitting async queries, see [Chapter I: Asynchronous Query](./chapter-01-request-response.md#2-asynchronous-query-post-_queryasync).

---

<a id="1-get-async-results"></a>
## 1. Get Async Results: `GET /_query/async/{id}`

**REST Handler**: `action/RestEsqlGetAsyncResultAction.java` (52 LOC)

**Transport Action**: `plugin/TransportEsqlAsyncGetResultsAction.java` (127 LOC)

This endpoint retrieves results for a previously submitted async query. Clients poll this endpoint until `is_running` becomes `false`, then the full results are available.

### Request

```
GET /_query/async/FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUToxMDE=
```

**Optional Parameters**:
- `wait_for_completion_timeout`: How long to wait for results before returning (enables [long-polling](#long-polling))
- `keep_alive`: Extend the result retention period

### Response (query running)

```json
{
  "id": "FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUToxMDE=",
  "is_running": true
}
```

### Response (query complete)

```json
{
  "id": "FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUToxMDE=",
  "is_running": false,
  "took": 42,
  "columns": [
    {"name": "host", "type": "keyword"},
    {"name": "count", "type": "long"}
  ],
  "values": [
    ["server1", 1500],
    ["server2", 2300]
  ]
}
```

### How It Works

The REST handler extracts the [`AsyncExecutionId`](./chapter-01-request-response.md#asyncexecutionid) and optional parameters:

```java
@Override
protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    GetAsyncResultRequest get = new GetAsyncResultRequest(request.param("id"));
    if (request.hasParam("wait_for_completion_timeout")) {
        get.setWaitForCompletionTimeout(request.paramAsTime("wait_for_completion_timeout", ...));
    }
    if (request.hasParam("keep_alive")) {
        get.setKeepAlive(request.paramAsTime("keep_alive", ...));
    }
    return channel -> client.execute(EsqlAsyncGetResultAction.INSTANCE, get,
        new EsqlResponseListener(channel, request));
}
```

The [`TransportEsqlAsyncGetResultsAction`](#1-get-async-results) extends `AbstractTransportQlAsyncGetResultsAction`, which:

1. Decodes the [`AsyncExecutionId`](./chapter-01-request-response.md#asyncexecutionid) from the request
2. Checks if the query task is still running
3. If running and `wait_for_completion_timeout` specified, waits for completion
4. Retrieves the stored result from [`.async-search` index](./chapter-01-request-response.md#async-search-index)
5. Adds response headers (`X-Async-Execution-Id`, `X-Async-Execution-Is-Running`)

```java
public class TransportEsqlAsyncGetResultsAction
    extends AbstractTransportQlAsyncGetResultsAction<EsqlQueryResponse, EsqlQueryTask> {

    @Override
    public Writeable.Reader<EsqlQueryResponse> responseReader() {
        return EsqlQueryResponse.reader(blockFactory);
    }

    // Unwraps exceptions to match sync API behavior
    ActionListener<EsqlQueryResponse> unwrapListener(String asyncId, ActionListener<EsqlQueryResponse> listener) {
        return new ActionListener<>() {
            @Override
            public void onResponse(EsqlQueryResponse response) {
                // Add async headers
                threadPool.getThreadContext()
                    .addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_IS_RUNNING_HEADER,
                        response.isRunning() ? "?1" : "?0");
                threadPool.getThreadContext()
                    .addResponseHeader(AsyncExecutionId.ASYNC_EXECUTION_ID_HEADER, asyncId);
                listener.onResponse(response);
            }
            // ...
        };
    }
}
```

<a id="long-polling"></a>
### Long-Polling

The `wait_for_completion_timeout` parameter enables long-polling:

```
GET /_query/async/{id}?wait_for_completion_timeout=30s
```

Instead of returning immediately with `is_running: true`, the server waits up to 30 seconds for the query to complete. This reduces polling overhead for clients.

---

<a id="2-delete-async-results"></a>
## 2. Delete Async Results: `DELETE /_query/async/{id}`

**REST Handler**: `action/RestEsqlDeleteAsyncResultAction.java` (41 LOC)

**Transport Action**: Uses generic `TransportDeleteAsyncResultAction`

This endpoint deletes stored async query results before they naturally expire. If the query is still running, it also cancels the execution.

### Request

```
DELETE /_query/async/FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUToxMDE=
```

### Response

```json
{
  "acknowledged": true
}
```

### How It Works

```java
@Override
protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    DeleteAsyncResultRequest delete = new DeleteAsyncResultRequest(request.param("id"));
    return channel -> client.execute(TransportDeleteAsyncResultAction.TYPE, delete,
        new RestToXContentListener<>(channel));
}
```

The generic `TransportDeleteAsyncResultAction` from the async search framework:

1. Decodes the [`AsyncExecutionId`](./chapter-01-request-response.md#asyncexecutionid)
2. If the query task is running, cancels it
3. Deletes the document from [`.async-search` index](./chapter-01-request-response.md#async-search-index)
4. Returns acknowledgment

### Use Cases

- **Cleanup after processing**: Delete results once your application has consumed them
- **Cancel long-running queries**: Deleting a running query cancels it immediately
- **Free cluster resources**: Remove results before the `keep_alive` expiration

---

<a id="3-stop-async-query"></a>
## 3. Stop Async Query: `POST /_query/async/{id}/stop`

**REST Handler**: `action/RestEsqlStopAsyncAction.java` (46 LOC)

**Transport Action**: `plugin/TransportEsqlAsyncStopAction.java` (139 LOC)

This endpoint stops a running async query and returns whatever partial results have been computed. Unlike [DELETE](#2-delete-async-results), it preserves the results—it just stops further computation.

### Request

```
POST /_query/async/FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUToxMDE=/stop
```

### Response

```json
{
  "id": "FmNJRUZ1...",
  "is_running": false,
  "is_partial": true,
  "took": 1234,
  "columns": [
    {"name": "host", "type": "keyword"},
    {"name": "count", "type": "long"}
  ],
  "values": [
    ["server1", 500],
    ["server2", 800]
  ]
}
```

Note `is_partial: true` indicates the results are incomplete.

### How It Works

The [`TransportEsqlAsyncStopAction`](#3-stop-async-query) is more complex than delete because it needs to:
1. Gracefully terminate the compute pipeline
2. Collect partial results
3. Store and return those results

```java
public class TransportEsqlAsyncStopAction extends HandledTransportAction<AsyncStopRequest, EsqlQueryResponse> {

    private final ExchangeService exchangeService;
    private final TransportEsqlAsyncGetResultsAction getResultsAction;

    @Override
    protected void doExecute(Task task, AsyncStopRequest request, ActionListener<EsqlQueryResponse> listener) {
        AsyncExecutionId searchId = AsyncExecutionId.decode(request.getId());

        // Route to the node that owns this query
        DiscoveryNode node = clusterService.state().nodes().get(searchId.getTaskId().getNodeId());
        if (clusterService.localNode().getId().equals(searchId.getTaskId().getNodeId()) || node == null) {
            stopQueryAndReturnResult(task, searchId, listener);
        } else {
            // Forward to owning node
            transportService.sendRequest(node, EsqlAsyncStopAction.NAME, request, ...);
        }
    }

    private void stopQueryAndReturnResult(Task task, AsyncExecutionId asyncId,
                                          ActionListener<EsqlQueryResponse> listener) {
        EsqlQueryTask asyncTask = getEsqlQueryTask(asyncId);
        GetAsyncResultRequest getRequest = new GetAsyncResultRequest(asyncId.getEncoded());

        if (asyncTask == null) {
            // Query already finished - just return results
            getResultsAction.execute(task, getRequest, listener);
            return;
        }

        // Mark execution as stopped
        EsqlExecutionInfo executionInfo = asyncTask.executionInfo();
        if (executionInfo != null) {
            executionInfo.markAsStopped();
        }

        // Signal compute to finish early
        exchangeService.finishSessionEarly(sessionID(asyncId), ActionListener.running(() -> {
            // Wait for completion, then return results
            if (asyncTask.addCompletionListener(() -> getResults()) == false) {
                getResults();
            }
        }));
    }
}
```

<a id="exchange-service"></a>
### Key Mechanism: ExchangeService

The [`ExchangeService`](#exchange-service)`.finishSessionEarly()` method signals the compute engine to stop processing:

1. Data exchange channels are closed
2. Operators drain their remaining data
3. Partial aggregations are finalized
4. Results are stored to [`.async-search`](./chapter-01-request-response.md#async-search-index)

This allows for graceful shutdown with partial results, rather than abrupt cancellation with no results.

---

## Part B: Query Monitoring

These APIs provide visibility into currently running queries.

---

<a id="4-list-running-queries"></a>
## 4. List Running Queries: `GET /_query/queries`

**REST Handler**: `action/RestEsqlListQueriesAction.java` (52 LOC)

**Transport Action**: `plugin/TransportEsqlListQueriesAction.java` (81 LOC)

This endpoint lists all currently running ES|QL queries across the cluster.

### Request

```
GET /_query/queries
```

### Response

```json
{
  "queries": [
    {
      "id": "node1:12345",
      "start_time_millis": 1707134400000,
      "running_time_nanos": 5000000000,
      "query": "FROM logs | STATS count(*) BY host"
    },
    {
      "id": "node1:12346",
      "start_time_millis": 1707134401000,
      "running_time_nanos": 3000000000,
      "query": "FROM metrics | WHERE value > 100"
    }
  ]
}
```

### How It Works

The [`TransportEsqlListQueriesAction`](#4-list-running-queries) queries the cluster's task management system for all tasks matching ES|QL query actions:

```java
@Override
protected void doExecute(Task task, EsqlListQueriesRequest request,
                         ActionListener<EsqlListQueriesResponse> listener) {
    ClientHelper.executeAsyncWithOrigin(
        nodeClient,
        ESQL_ORIGIN,
        TransportListTasksAction.TYPE,
        new ListTasksRequest()
            .setActions(
                EsqlQueryAction.NAME,  // "indices:data/read/esql"
                EsqlQueryAction.NAME + AsyncTaskManagementService.ASYNC_ACTION_SUFFIX  // "[async]"
            )
            .setDetailed(true),
        new ActionListener<>() {
            @Override
            public void onResponse(ListTasksResponse response) {
                List<Query> queries = response.getTasks()
                    .stream()
                    .map(TransportEsqlListQueriesAction::toQuery)
                    .toList();
                listener.onResponse(new EsqlListQueriesResponse(queries));
            }
        });
}
```

The `setDetailed(true)` parameter ensures the task's `Status` object (i.e., [`EsqlQueryStatus`](#esqlquerystatus)) is included, allowing extraction of the [`AsyncExecutionId`](./chapter-01-request-response.md#asyncexecutionid).

### Task to Query Conversion

See [Task Hierarchy](#task-hierarchy) for the relationship between query tasks and driver tasks.

```java
private static EsqlListQueriesResponse.Query toQuery(TaskInfo taskInfo) {
    return new EsqlListQueriesResponse.Query(
        ((EsqlQueryStatus) taskInfo.status()).id(),  // AsyncExecutionId from EsqlQueryStatus
        taskInfo.startTime(),                         // When query started (epoch millis)
        taskInfo.runningTimeNanos(),                  // How long it's been running
        taskInfo.description()                        // The query string
    );
}
```

---

<a id="5-get-query-details"></a>
## 5. Get Query Details: `GET /_query/queries/{id}`

**REST Handler**: `action/RestEsqlListQueriesAction.java` (same handler, different route)

**Transport Action**: `plugin/TransportEsqlGetQueryAction.java` (115 LOC)

This endpoint retrieves detailed information about a specific running query, including aggregated execution statistics from all participating compute drivers.

### Request

```
GET /_query/queries/node1:12345
```

### Response

```json
{
  "id": "node1:12345",
  "start_time_millis": 1707134400000,
  "running_time_nanos": 5000000000,
  "documents_found": 1500000,
  "values_loaded": 4500000,
  "query": "FROM logs | STATS count(*) BY host"
}
```

### Two-Phase Task Lookup

The [`TransportEsqlGetQueryAction`](#5-get-query-details) performs two nested task API calls:

**Phase 1**: Fetch the main query task to validate it's an ES|QL query

```java
@Override
protected void doExecute(Task task, EsqlGetQueryRequest request,
                         ActionListener<EsqlGetQueryResponse> listener) {
    ClientHelper.executeAsyncWithOrigin(
        nodeClient, ESQL_ORIGIN, TransportGetTaskAction.TYPE,
        new GetTaskRequest().setTaskId(request.id().getTaskId()),
        new ActionListener<>() {
            @Override
            public void onResponse(GetTaskResponse response) {
                TaskInfo task = response.getTask().getTask();

                // Validate this is an ESQL query task
                if (task.action().startsWith(EsqlQueryAction.NAME) == false) {
                    listener.onFailure(new IllegalArgumentException(
                        "Task [" + request.id() + "] is not an ESQL query task"));
                    return;
                }

                // Phase 2: Get driver tasks...
            }
        });
}
```

**Phase 2**: List all child driver tasks and aggregate their statistics

```java
ClientHelper.executeAsyncWithOrigin(
    nodeClient, ESQL_ORIGIN, TransportListTasksAction.TYPE,
    new ListTasksRequest()
        .setDetailed(true)
        .setActions(DriverTaskRunner.ACTION_NAME)  // "cluster:internal/data/read/esql/compute"
        .setTargetParentTaskId(request.id().getTaskId()),  // Only children of this query
    new ActionListener<>() {
        @Override
        public void onResponse(ListTasksResponse response) {
            listener.onResponse(new EsqlGetQueryResponse(toDetailedQuery(task, response)));
        }
    });
```

### Aggregating Driver Statistics

The [`TransportEsqlGetQueryAction`](#5-get-query-details) aggregates statistics from all active [`DriverStatus`](#driverstatus) objects:

```java
private static EsqlGetQueryResponse.DetailedQuery toDetailedQuery(TaskInfo main,
                                                                   ListTasksResponse sub) {
    String query = main.description();

    // TODO include completed drivers in documentsFound and valuesLoaded
    long documentsFound = 0;
    long valuesLoaded = 0;
    Set<String> dataNodes = new TreeSet<>();

    for (TaskInfo info : sub.getTasks()) {
        DriverStatus status = (DriverStatus) info.status();
        documentsFound += status.documentsFound();
        valuesLoaded += status.valuesLoaded();
        dataNodes.add(info.node());
    }

    return new EsqlGetQueryResponse.DetailedQuery(
        main.taskId(),
        main.startTime(),
        main.runningTimeNanos(),
        documentsFound,
        valuesLoaded,
        query
    );
}
```

---

<a id="task-hierarchy"></a>
## Task Hierarchy

When an ES|QL query executes, it creates a hierarchy of tasks:

```
EsqlQueryTask (main query task)
├── action: "indices:data/read/esql" or "indices:data/read/esql[async]"
├── status: EsqlQueryStatus { asyncExecutionId }
│
└── Driver Tasks (one per driver on each data node)
    ├── action: "cluster:internal/data/read/esql/compute"
    └── status: DriverStatus { cpuNanos, documentsFound, valuesLoaded, ... }
```

The main query task is an [`EsqlQueryTask`](./chapter-01-request-response.md#esqlquerytask) for async queries. Driver tasks are created by `DriverTaskRunner` when the compute engine distributes work across data nodes.

---

<a id="driverstatus"></a>
## DriverStatus: Execution Statistics

**File**: `compute/operator/DriverStatus.java` (194 LOC)

The `DriverStatus` record is the `Task.Status` reported by each compute driver. It provides detailed visibility into driver execution.

### Structure

```java
public record DriverStatus(
    String sessionId,              // Query session ID
    String description,            // Human-readable description
    String clusterName,            // Cluster this driver runs on
    String nodeName,               // Node this driver runs on
    long started,                  // When driver started (epoch millis)
    long lastUpdated,              // When status was generated
    long cpuNanos,                 // CPU time consumed (excludes async/wait)
    long iterations,               // Number of page processing cycles
    Status status,                 // QUEUED, STARTING, RUNNING, ASYNC, WAITING, DONE
    List<OperatorStatus> completedOperators,  // Finished operators
    List<OperatorStatus> activeOperators,     // Currently running operators
    DriverSleeps sleeps            // Sleep/yield statistics
) implements Task.Status
```

### Driver Status States

```java
public enum Status {
    QUEUED,    // Waiting in thread pool queue
    STARTING,  // Being initialized
    RUNNING,   // Actively processing pages
    ASYNC,     // Waiting for async operation (e.g., network I/O)
    WAITING,   // Waiting for upstream data
    DONE;      // Completed
}
```

### Statistics Aggregation

The `documentsFound()` and `valuesLoaded()` methods aggregate across all operators:

```java
public long documentsFound() {
    long documentsFound = 0;
    for (OperatorStatus s : completedOperators) {
        documentsFound += s.documentsFound();
    }
    for (OperatorStatus s : activeOperators) {
        documentsFound += s.documentsFound();
    }
    return documentsFound;
}
```

---

<a id="esqlquerystatus"></a>
## EsqlQueryStatus

**File**: `plugin/EsqlQueryStatus.java` (44 LOC)

A simple `Task.Status` implementation that wraps the [`AsyncExecutionId`](./chapter-01-request-response.md#asyncexecutionid) for the main query task:

```java
public record EsqlQueryStatus(AsyncExecutionId id) implements Task.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Task.Status.class,
        "EsqlDocIdStatus",
        EsqlQueryStatus::new
    );

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("request_id", id.getEncoded()).endObject();
    }
}
```

---

## Limitations and Considerations

### Completed Drivers Not Tracked

The current implementation only counts statistics from active [driver tasks](#task-hierarchy). Once a driver completes, its task is removed from the task management system, and its [`DriverStatus`](#driverstatus) statistics are lost. This means:

- `documentsFound` and `valuesLoaded` may be undercounted for queries where some drivers have already finished
- The TODO comment in `TransportEsqlGetQueryAction.toDetailedQuery()` acknowledges this limitation

### No Historical Data

These APIs only show currently running queries. For historical query analysis, you would need:
- ES|QL's profiling feature (`"profile": true` in the request)
- Slow query logging
- External monitoring solutions

### Task API Overhead

Each monitoring API call results in cluster-wide task discovery. For clusters with many nodes, frequent polling could add overhead.

---

## File Summary

| File | LOC | Purpose |
|------|-----|---------|
| [`RestEsqlGetAsyncResultAction.java`](#1-get-async-results) | 52 | [Get async results](#1-get-async-results) REST endpoint |
| [`RestEsqlDeleteAsyncResultAction.java`](#2-delete-async-results) | 41 | [Delete async results](#2-delete-async-results) REST endpoint |
| [`RestEsqlStopAsyncAction.java`](#3-stop-async-query) | 46 | [Stop async query](#3-stop-async-query) REST endpoint |
| [`RestEsqlListQueriesAction.java`](#4-list-running-queries) | 52 | [List](#4-list-running-queries)/[get](#5-get-query-details) queries REST endpoint |
| [`TransportEsqlAsyncGetResultsAction.java`](#1-get-async-results) | 127 | [Get async results](#1-get-async-results) transport action |
| [`TransportEsqlAsyncStopAction.java`](#3-stop-async-query) | 139 | [Stop async query](#3-stop-async-query) transport action |
| [`TransportEsqlListQueriesAction.java`](#4-list-running-queries) | 81 | [List queries](#4-list-running-queries) transport action |
| [`TransportEsqlGetQueryAction.java`](#5-get-query-details) | 115 | [Get query details](#5-get-query-details) transport action |
| [`EsqlQueryStatus.java`](#esqlquerystatus) | 44 | Task.Status for main query task |
| [`DriverStatus.java`](#driverstatus) | 194 | Task.Status for compute drivers |

---

## Related APIs

- **Task Management API**: `GET /_tasks` - Low-level task visibility
- **Profile API**: Include `"profile": true` in query request for detailed execution breakdown
- **Slow Query Log**: Configure `logger.org.elasticsearch.xpack.esql` for query logging

---

## Cross-References

- [Chapter I: Request/Response Lifecycle](./chapter-01-request-response.md) - Query submission
- [`EsqlQueryTask`](./chapter-01-request-response.md#esqlquerytask) - Async task implementation
- [`AsyncExecutionId`](./chapter-01-request-response.md#asyncexecutionid) - ID encoding for async queries
- [`.async-search` index](./chapter-01-request-response.md#async-search-index) - Result storage
- [`EsqlExecutionInfo`](./chapter-01-request-response.md#11-execution-metadata) - Execution metadata tracking
