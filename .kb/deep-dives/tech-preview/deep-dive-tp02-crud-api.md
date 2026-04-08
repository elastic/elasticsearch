# Deep Dive: Datasource CRUD API — TP-02

## 1. Does Any Datasource CRUD Infrastructure Exist?

**Answer: No.** There is no existing datasource CRUD infrastructure anywhere in the codebase.

### Search results:
- `DataSourceMetadata` — **0 matches** (the name `ExternalSourceMetadata` exists at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceMetadata.java` but this is a query-time schema interface, not cluster-state metadata)
- `DataSourceConfig` — **0 matches**
- `DataSourceService` — **0 matches**
- `PutDataSourceAction` — **0 matches**
- The only hit for "DataSource" in a JDBC context is a test file: `x-pack/plugin/sql/jdbc/src/test/java/org/elasticsearch/xpack/sql/jdbc/JdbcConfigurationDataSourceTests.java` — unrelated to ES|QL.

**Current state:** External sources get their configuration entirely from query-time parameters (the `WITH` clause in `EXTERNAL "uri" WITH access_key="...", secret_key="..."`) — there is no persisted config anywhere. The `ExternalSourceResolver` at line 286-305 converts `Map<String, Expression>` query params into a `Map<String, Object>` config map at resolve time.

---

## 2. IngestPipeline Pattern (Reference)

This is the canonical pattern for CRUD config in cluster state. Four layers: REST handler -> Transport action -> Service -> Cluster state.

### 2a. IngestMetadata implements Metadata.ProjectCustom

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/ingest/IngestMetadata.java`

```java
// Line 37
public final class IngestMetadata implements Metadata.ProjectCustom {
    public static final String TYPE = "ingest";
    // Line 52 — stores pipelines as a Map<String, PipelineConfiguration>
    private final Map<String, PipelineConfiguration> pipelines;
```

Key implementation points:
- **Line 37:** Implements `Metadata.ProjectCustom` (project-scoped, not cluster-scoped)
- **Line 39:** TYPE = "ingest" — used as the key in ProjectMetadata.customs
- **Line 52:** Immutable `Map<String, PipelineConfiguration>` — the config entries
- **Line 59-61:** `getWriteableName()` returns TYPE
- **Line 72-88:** Binary serialization via StreamInput/StreamOutput (`readVInt`, loop, `readFrom`)
- **Line 90-97:** `fromXContent()` parsing via ObjectParser
- **Line 99-102:** `toXContentChunked()` for JSON serialization
- **Line 109-112:** `diff()` returns `IngestMetadataDiff`
- **Line 118-154:** `IngestMetadataDiff` uses `DiffableUtils.diff()` / `DiffableUtils.readJdkMapDiff()` for efficient delta serialization

### 2b. PipelineConfiguration serialization

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/ingest/PipelineConfiguration.java`

```java
// Line 43
public final class PipelineConfiguration implements SimpleDiffable<PipelineConfiguration>, ToXContentObject {
    private final String id;           // line 79
    private final Map<String, Object> config;  // line 80
```

Key points:
- **Line 43:** Implements `SimpleDiffable` (enables diff-based cluster state updates) and `ToXContentObject`
- **Line 79-80:** Just an id + a loosely-typed config map
- **Line 170-174:** `readFrom(StreamInput)` — reads id string + generic map
- **Line 187-192:** `writeTo(StreamOutput)` — writes id string + generic map
- **Line 162-168:** `toXContent()` — writes `{"id": "...", "config": {...}}`
- **Line 82-85:** Constructor does a deep copy of the config map for immutability

### 2c. PUT REST handler

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/rest/action/ingest/RestPutPipelineAction.java`

```java
// Line 35
public class RestPutPipelineAction extends BaseRestHandler {
    // Line 38-40
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_ingest/pipeline/{id}"));
    }
    // Line 48-76 — prepareRequest:
    //   1. Extract id from path param
    //   2. Parse body as raw BytesReference
    //   3. Create PutPipelineRequest
    //   4. client.execute(PutPipelineTransportAction.TYPE, request, ...)
```

### 2d. Transport action + cluster state update

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/ingest/PutPipelineTransportAction.java`

```java
// Line 32
public class PutPipelineTransportAction extends AcknowledgedTransportMasterNodeAction<PutPipelineRequest> {
    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("cluster:admin/ingest/pipeline/put");
    // Line 59-61 — masterOperation delegates to IngestService:
    protected void masterOperation(...) {
        ingestService.putPipeline(projectResolver.getProjectId(), request, listener);
    }
```

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/ingest/IngestService.java`

```java
// Line 138 — task queue declaration
private final MasterServiceTaskQueue<PipelineClusterStateUpdateTask> taskQueue;

// Line 268 — task queue creation in constructor
this.taskQueue = clusterService.createTaskQueue("ingest-pipelines", Priority.NORMAL, PIPELINE_TASK_EXECUTOR);

// Line 574-591 — putPipeline method
public void putPipeline(ProjectId projectId, PutPipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
    // Check for no-op update (line 576)
    // Validate pipeline on all nodes (line 582-583)
    // Submit cluster state update task (line 585-589)
    taskQueue.submitTask("put-pipeline-" + request.getId(),
        new PutPipelineClusterStateUpdateTask(projectId, l, request),
        request.masterNodeTimeout());
}

// Line 181-212 — PIPELINE_TASK_EXECUTOR (batched)
static final ClusterStateTaskExecutor<PipelineClusterStateUpdateTask> PIPELINE_TASK_EXECUTOR = batchExecutionContext -> {
    // For each task:
    //   1. Get ProjectMetadata.Builder
    //   2. Get current IngestMetadata from builder
    //   3. Execute task to produce new IngestMetadata
    //   4. If changed, putCustom(IngestMetadata.TYPE, newIngestMetadata)
    //   5. Build new ClusterState
};
```

### 2e. Registration in ClusterModule

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/ClusterModule.java`

```java
// Line 291 — NamedWriteable registration (binary serialization)
registerProjectCustom(entries, IngestMetadata.TYPE, IngestMetadata::new, IngestMetadata::readDiffFrom);

// Line 360 — NamedXContent registration (JSON parsing)
new NamedXContentRegistry.Entry(Metadata.ProjectCustom.class, new ParseField(IngestMetadata.TYPE), IngestMetadata::fromXContent)
```

### 2f. PutPipelineRequest

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/ingest/PutPipelineRequest.java`

```java
// Line 25
public class PutPipelineRequest extends AcknowledgedRequest<PutPipelineRequest> implements ToXContentObject {
    private final String id;                // line 27
    private final BytesReference source;    // line 28 — raw body
    private final XContentType xContentType; // line 29
    private final Integer version;          // line 30 — optimistic concurrency
```

---

## 3. Other Metadata.ProjectCustom Examples

### 3a. ViewMetadata (ESQL Views — most relevant precedent)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/metadata/ViewMetadata.java`

This is the **closest pattern** to what we need. It was added recently (same plugin) and stores a map of named objects.

```java
// Line 36
public final class ViewMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
    public static final String TYPE = "esql_view";
    // Line 38-41 — ENTRIES constant bundles both NamedWriteable registrations
    public static final List<NamedWriteableRegistry.Entry> ENTRIES = List.of(
        new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, TYPE, ViewMetadata::readFromStream),
        new NamedWriteableRegistry.Entry(NamedDiff.class, TYPE, in -> ViewMetadata.readDiffFrom(Metadata.ProjectCustom.class, TYPE, in))
    );
    public static final ViewMetadata EMPTY = new ViewMetadata(Collections.emptyMap());
    // Line 47 — storage: Map<String, View>
    private final Map<String, View> views;
```

Key simplifications vs IngestMetadata:
- **Extends `AbstractNamedDiffable`** instead of implementing diff manually — gets `readDiffFrom()` for free
- **ENTRIES constant** bundles NamedWriteable registrations — avoids scattered registration code
- **Line 71-73:** `readFromStream()` uses `in.readMap(View::new)` — simple one-liner
- **Line 104-106:** `writeTo()` uses `out.writeMap(this.views, StreamOutput::writeWriteable)`
- **Line 109-111:** `toXContentChunked()` uses `ChunkedToXContentHelper.xContentObjectFields()`
- `View` implements `Writeable` + `ToXContentObject` — the individual config item

**Registration in EsqlPlugin:**
```java
// EsqlPlugin.java line 413-414 — NamedWriteable
entries.addAll(ViewMetadata.ENTRIES);

// EsqlPlugin.java line 423-430 — NamedXContent
new NamedXContentRegistry.Entry(Metadata.ProjectCustom.class, new ParseField(ViewMetadata.TYPE), ViewMetadata::fromXContent)
```

### 3b. TransformMetadata (simple flags)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/transform/TransformMetadata.java`

```java
// Line 33
public class TransformMetadata implements Metadata.ProjectCustom {
    public static final String TYPE = "transform";
    private final boolean resetMode;    // line 51
    private final boolean upgradeMode;  // line 52
```

Simple example — just boolean flags, not a map of configs. Good for understanding minimal implementation but not structurally relevant.

### 3c. ModelRegistryClusterStateMetadata (inference — map of typed objects)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/registry/ModelRegistryClusterStateMetadata.java`

```java
// Line 49
public class ModelRegistryClusterStateMetadata implements Metadata.ProjectCustom {
    public static final String TYPE = "model_registry";
    // Line 156 — Map<String, MinimalServiceSettings>
    private final ImmutableOpenMap<String, MinimalServiceSettings> modelMap;
```

More complex example with:
- Uses `ImmutableOpenMap` (line 156) instead of `Map` for cluster state efficiency
- Has `DiffableUtils.MapDiff` in its diff class (line 344) for efficient incremental updates
- `withAddedModel()` / `withRemovedModel()` methods (lines 99, 128) — immutable update pattern
- `MinimalServiceSettings` implements `SimpleDiffable<MinimalServiceSettings>` for per-entry diffs

### 3d. WatcherMetadata (simple, uses AbstractNamedDiffable)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/watcher/WatcherMetadata.java`

```java
// Line 26
public class WatcherMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
    public static final String TYPE = "watcher";
    private final boolean manuallyStopped;  // line 30
```

Good example of using `AbstractNamedDiffable` as a shortcut.

---

## 4. REST Handler Registration

### Pattern 1: Core handlers — registered directly in ActionModule.initRestHandlers()

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/ActionModule.java`

```java
// Line 841
public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
    // Lines 859-1026 — direct registration of core handlers:
    registerHandler.accept(new RestPutPipelineAction());  // etc.
```

### Pattern 2: Plugin handlers — via ActionPlugin.getRestHandlers()

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/plugins/ActionPlugin.java`

```java
// Line 74
default Collection<RestHandler> getRestHandlers(
    Settings settings,
    NamedWriteableRegistry namedWriteableRegistry,
    RestController restController,
    ClusterSettings clusterSettings,
    IndexScopedSettings indexScopedSettings,
    SettingsFilter settingsFilter,
    IndexNameExpressionResolver indexNameExpressionResolver,
    Supplier<DiscoveryNodes> nodesInCluster,
    Predicate<NodeFeature> clusterSupportsFeature
) { return Collections.emptyList(); }
```

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/ActionModule.java`

```java
// Line 1028-1035 — plugin handlers registered in a loop:
for (ActionPlugin plugin : actionPlugins) {
    for (RestHandler handler : plugin.getRestHandlers(settings, namedWriteableRegistry, restController, ...)) {
        registerHandler.accept(handler);
    }
}
```

### ESQL Plugin handler registration

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/EsqlPlugin.java`

```java
// Line 354-380
public List<RestHandler> getRestHandlers(...) {
    List<RestHandler> releasedRestHandlers = List.of(
        new RestEsqlQueryAction(),
        new RestEsqlAsyncQueryAction(),
        // ...
    );
    if (ESQL_VIEWS_FEATURE_FLAG.isEnabled()) {
        List<RestHandler> restHandlers = new ArrayList<>(releasedRestHandlers);
        restHandlers.addAll(List.of(new RestPutViewAction(), new RestDeleteViewAction(), new RestGetViewAction()));
        return restHandlers;
    }
    return releasedRestHandlers;
}
```

Transport actions registered via `getActions()` (line 320-352):
```java
new ActionHandler(PutViewAction.INSTANCE, TransportPutViewAction.class),
new ActionHandler(DeleteViewAction.INSTANCE, TransportDeleteViewAction.class),
new ActionHandler(GetViewAction.INSTANCE, TransportGetViewAction.class)
```

---

## 5. MasterServiceTaskQueue Pattern

The ViewService shows the cleanest modern pattern.

### ViewService pattern (recommended)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/view/ViewService.java`

```java
// Line 48 — task queue declaration
private final MasterServiceTaskQueue<AckedClusterStateUpdateTask> taskQueue;

// Line 73-77 — task queue creation
this.taskQueue = clusterService.createTaskQueue(
    "update-esql-view-metadata",
    Priority.NORMAL,
    new SequentialAckingBatchedTaskExecutor<>()  // pre-built executor
);

// Line 98-137 — putView method
public void putView(ProjectId projectId, PutViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
    // 1. Validate (line 106-111)
    // 2. Check for no-op (line 113-117)
    // 3. Create AckedClusterStateUpdateTask (line 118-135)
    final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
        @Override
        public ClusterState execute(ClusterState currentState) {
            final ProjectMetadata project = currentState.metadata().getProject(projectId);
            final ViewMetadata viewMetadata = getMetadata(project);
            // ... modify views map ...
            var metadata = ProjectMetadata.builder(project).views(updatedViews);
            return ClusterState.builder(currentState).putProjectMetadata(metadata).build();
        }
    };
    taskQueue.submitTask("update-esql-view-metadata-[" + view.name() + "]", task, task.timeout());
}
```

**File:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/SequentialAckingBatchedTaskExecutor.java`

```java
// Line 21-27 — simple pre-built executor
public class SequentialAckingBatchedTaskExecutor<Task extends AckedClusterStateUpdateTask>
    extends SimpleBatchedAckListenerTaskExecutor<Task> {
    @Override
    public Tuple<ClusterState, ClusterStateAckListener> executeTask(Task task, ClusterState clusterState) throws Exception {
        return Tuple.tuple(task.execute(clusterState), task);
    }
}
```

This is much simpler than the IngestService pattern which uses a custom `ClusterStateTaskExecutor`. The `SequentialAckingBatchedTaskExecutor` delegates to each task's `execute()` method.

### TransportPutViewAction (the transport action half)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/view/TransportPutViewAction.java`

```java
// Line 24
public class TransportPutViewAction extends AcknowledgedTransportMasterNodeProjectAction<PutViewAction.Request> {
    // Line 50-57 — masterOperation just delegates to ViewService
    protected void masterOperation(Task task, PutViewAction.Request request, ProjectState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        viewService.putView(state.projectId(), request, listener);
    }
}
```

Uses `AcknowledgedTransportMasterNodeProjectAction` (file: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/support/master/AcknowledgedTransportMasterNodeProjectAction.java`) which:
- Extends `AcknowledgedTransportMasterNodeAction` (line 26)
- Injects `ProjectResolver` (line 29)
- Overrides `masterOperation` to pass `ProjectState` instead of raw `ClusterState` (line 60-65)
- Overrides `checkBlock` to pass `ProjectState` (line 73-78)

---

## 6. Current External Source Config Flow

### How config gets to external sources today

There is **no persistent config**. Configuration flows purely at query time:

1. **Parser** (`EXTERNAL "uri" WITH key=value, ...`) — produces query params as `Map<String, Expression>`
2. **ExternalSourceResolver.resolve()** (line 77-125 of `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`)
   - Calls `paramsToConfigMap()` (line 286-305) — converts `Map<String, Expression>` to `Map<String, Object>` (string values only)
3. **ExternalSourceResolver.resolveSingleSource()** (line 195-234) — iterates `DataSourceModule.sourceFactories()` looking for first `canHandle(path)`
   - Calls `factory.resolveMetadata(path, config)` — config is the `Map<String, Object>` from WITH params
4. **ExternalSourceResolver.wrapAsExternalSourceMetadata()** (line 307-353) — merges factory-resolved config with query-level params (query-level takes precedence)
5. **ExternalRelation** (line 53-68 of `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java`) — stores `SourceMetadata` which includes `config()`
6. **ExternalRelation.toPhysicalExec()** (line 115-127) — passes `metadata.config()` into `ExternalSourceExec`
7. **OperatorFactoryRegistry.factory()** (line 58-95 of `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/OperatorFactoryRegistry.java`) — receives config via `SourceOperatorContext.config()`

### Key SPI interfaces

**ExternalSourceFactory** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSourceFactory.java`):
```java
public interface ExternalSourceFactory {
    String type();
    boolean canHandle(String location);
    SourceMetadata resolveMetadata(String location, Map<String, Object> config);  // config from WITH clause
    default FilterPushdownSupport filterPushdownSupport() { return null; }
    default SourceOperatorFactoryProvider operatorFactory() { return null; }
    default SplitProvider splitProvider() { return SplitProvider.SINGLE; }
}
```

**SourceMetadata** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceMetadata.java`):
```java
public interface SourceMetadata {
    List<Attribute> schema();
    String sourceType();
    String location();
    default Optional<SourceStatistics> statistics() { return Optional.empty(); }
    default Optional<List<String>> partitionColumns() { return Optional.empty(); }
    default Map<String, Object> sourceMetadata() { return Map.of(); }
    default Map<String, Object> config() { return Map.of(); }  // line 116
}
```

Config keys are just strings (line 106-113 of SourceMetadata.java):
- "access_key", "secret_key", "endpoint", "region" — S3 credentials
- "target" — Flight target path
- No schema or validation

**DataSourcePlugin** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/DataSourcePlugin.java`):
- Plugins provide factories via `storageProviders()`, `formatReaders()`, `connectors()`, `tableCatalogs()`, `sourceFactories()`
- Config comes from `Settings` (node-level) not from any persisted datasource config
- `StorageProviderFactory.create(Settings s, Map<String, Object> config)` accepts both node settings AND per-query config

### The gap

Credentials must be passed in every query via `WITH access_key="...", secret_key="..."`. There is no way to:
- Persist a named datasource with credentials
- Reference a named datasource in a query
- Manage credentials separately from queries
- Share datasource configs across users

---

## 7. What Exactly Needs to Be Built

Based on the patterns above (especially the ViewMetadata/ViewService pattern), the following classes are needed:

### Layer 1: Cluster State Storage

#### `DataSourceDefinition` (the config item, analogous to `View` or `PipelineConfiguration`)
- Location: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/`
- Implements: `SimpleDiffable<DataSourceDefinition>`, `ToXContentObject`, `Writeable`
- Fields:
  - `String name` — datasource identifier (e.g., "my-s3-logs")
  - `String type` — source type (e.g., "s3", "flight", "iceberg")
  - `Map<String, Object> config` — connection config (endpoint, region, etc.)
  - `Map<String, Object> credentials` — sensitive fields (access_key, secret_key)
  - Optional: `long version` (optimistic concurrency)
  - Optional: `long createdTimestamp`, `long modifiedTimestamp`

#### `EsqlDataSourceMetadata` (the cluster state custom, analogous to `ViewMetadata`)
- Location: `server/src/main/java/org/elasticsearch/cluster/metadata/` OR `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/`
- Extends: `AbstractNamedDiffable<Metadata.ProjectCustom>`
- Implements: `Metadata.ProjectCustom`
- Fields: `Map<String, DataSourceDefinition> dataSources`
- Pattern: Follow ViewMetadata exactly — ENTRIES constant, readFromStream, fromXContent, toXContentChunked
- TYPE = `"esql_datasource"`

### Layer 2: Service

#### `DataSourceCrudService` (analogous to `ViewService`)
- Location: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/`
- Injected with: `ClusterService`
- Creates: `MasterServiceTaskQueue<AckedClusterStateUpdateTask>` in constructor
- Methods:
  - `putDataSource(ProjectId, PutDataSourceAction.Request, ActionListener<AcknowledgedResponse>)`
  - `deleteDataSource(ProjectId, DeleteDataSourceAction.Request, ActionListener<AcknowledgedResponse>)`
  - `getDataSource(ProjectId, String name) -> DataSourceDefinition`
  - `listDataSources(ProjectId) -> Map<String, DataSourceDefinition>`
- Uses `SequentialAckingBatchedTaskExecutor` (same as ViewService)
- Cluster state update pattern: `AckedClusterStateUpdateTask` with `execute(ClusterState)` that builds new `ProjectMetadata` with updated `EsqlDataSourceMetadata`

### Layer 3: Transport Actions

#### `PutDataSourceAction` + `TransportPutDataSourceAction`
- Action name: `"cluster:admin/esql/datasource/put"`
- Transport: extends `AcknowledgedTransportMasterNodeProjectAction`
- Delegates to `DataSourceCrudService.putDataSource()`
- Request: `PutDataSourceAction.Request extends AcknowledgedRequest` containing `DataSourceDefinition`

#### `DeleteDataSourceAction` + `TransportDeleteDataSourceAction`
- Action name: `"cluster:admin/esql/datasource/delete"`
- Same pattern as Put

#### `GetDataSourceAction` + `TransportGetDataSourceAction`
- Action name: `"cluster:admin/esql/datasource/get"`
- Response returns `DataSourceDefinition` with credentials masked
- Could use `LocalClusterStateRequest` pattern (like GetViewAction) since it only reads cluster state

### Layer 4: REST Handlers

#### `RestPutDataSourceAction`
- Route: `PUT /_query/datasource/{name}`
- Parses JSON body into `DataSourceDefinition`
- Creates `PutDataSourceAction.Request`

#### `RestDeleteDataSourceAction`
- Route: `DELETE /_query/datasource/{name}`

#### `RestGetDataSourceAction`
- Route: `GET /_query/datasource/{name}` and `GET /_query/datasource`
- Returns config with credentials masked (return `***` for sensitive fields)

### Layer 5: Registration in EsqlPlugin

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/EsqlPlugin.java`

Additions needed:

```java
// In getActions() — add ActionHandlers:
new ActionHandler(PutDataSourceAction.INSTANCE, TransportPutDataSourceAction.class),
new ActionHandler(DeleteDataSourceAction.INSTANCE, TransportDeleteDataSourceAction.class),
new ActionHandler(GetDataSourceAction.INSTANCE, TransportGetDataSourceAction.class)

// In getRestHandlers() — add REST handlers:
new RestPutDataSourceAction(), new RestDeleteDataSourceAction(), new RestGetDataSourceAction()

// In getNamedWriteables() — add NamedWriteable entries:
entries.addAll(EsqlDataSourceMetadata.ENTRIES);

// In getNamedXContent() — add XContent parser:
new NamedXContentRegistry.Entry(Metadata.ProjectCustom.class, new ParseField(EsqlDataSourceMetadata.TYPE), EsqlDataSourceMetadata::fromXContent)
```

### Layer 6: Integration with ExternalSourceResolver

The `ExternalSourceResolver` needs modification to:
1. Accept named datasource references (e.g., `FROM datasource::my-s3-logs`)
2. Look up `DataSourceDefinition` from cluster state
3. Merge persisted config with any query-time overrides (WITH clause)
4. Pass merged config to `ExternalSourceFactory.resolveMetadata()`

This integration is separate from the CRUD API itself but is the reason the CRUD API exists.

### Layer 7: Settings (optional but recommended)

```java
// Max datasource count setting (like ViewService.MAX_VIEWS_COUNT_SETTING)
Setting<Integer> MAX_DATASOURCES_COUNT = Setting.intSetting("esql.datasources.max_count", 100, ...);
```

### Summary: File Count

| Component | Files | Pattern Source |
|-----------|-------|---------------|
| DataSourceDefinition | 1 | View, PipelineConfiguration |
| EsqlDataSourceMetadata | 1 | ViewMetadata |
| DataSourceCrudService | 1 | ViewService |
| PutDataSourceAction + Transport | 2 | PutViewAction + TransportPutViewAction |
| DeleteDataSourceAction + Transport | 2 | DeleteViewAction + TransportDeleteViewAction |
| GetDataSourceAction + Transport | 2 | GetViewAction + TransportGetViewAction |
| RestPutDataSourceAction | 1 | RestPutViewAction |
| RestDeleteDataSourceAction | 1 | RestDeleteViewAction |
| RestGetDataSourceAction | 1 | RestGetViewAction |
| EsqlPlugin registration changes | 0 (modify existing) | — |
| **Total new files** | **12** | — |

The ViewMetadata/ViewService pattern is the ideal template because:
1. It is in the same plugin (ESQL)
2. It was written recently (modern patterns)
3. It uses `AbstractNamedDiffable` (less boilerplate)
4. It uses `AcknowledgedTransportMasterNodeProjectAction` (project-aware)
5. It uses `SequentialAckingBatchedTaskExecutor` (simplest task executor)
6. It stores a `Map<String, ConfigItem>` in `Metadata.ProjectCustom` — exactly our shape
