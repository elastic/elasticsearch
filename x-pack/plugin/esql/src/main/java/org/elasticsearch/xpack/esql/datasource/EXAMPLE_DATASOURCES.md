# Example Data Sources

Two built-in data sources that demonstrate the full SPI lifecycle without requiring external systems.
Both run entirely within the Elasticsearch cluster.

## Class Hierarchy

```
DataSource (interface) — capabilities() method
├── LakehouseDataSource — forDistributed(), composes partitioning.SplitPartitioner
│   └── [logs data source]
└── SqlDataSource — forCoordinatorOnly()
    └── JdbcDataSource (example JDBC implementation)
```

Distribution mode is declared via [`DataSource.capabilities()`](DataSource.java), returning a
[`DataSourceCapabilities`](DataSourceCapabilities.java):
- `DataSourceCapabilities.forDistributed()` — distributed execution with size-aware split grouping (FFD bin-packing)
- `DataSourceCapabilities.forCoordinatorOnly()` — coordinator-only execution with trivial grouping

Each base class brings its own optimization rules. The cluster catalog defines simple in-memory
filter/limit rules. `SqlDataSource` keeps its SQL translation rules (translateFilter → SqlFragment,
BuildSql, etc.). The rules are different enough that sharing them at the base class level doesn't help.

---

## 1. Cluster Catalog (`cluster`)

**Base class:** implements `DataSource` directly (overrides `capabilities()` → `forCoordinatorOnly()`)

**Purpose:** Expose cluster state as queryable tables — nodes, indices, shards — with full
ES|QL filter, sort, limit, and aggregation support.

### Tables

| Table | Source | Columns |
|-------|--------|---------|
| `nodes` | `ClusterState.nodes()` | name, node_id, host, ip, port, version, roles, heap_max, heap_used, disk_total, disk_used, load_1m, load_5m, load_15m, master, attributes |
| `indices` | `ClusterState.metadata()` | index, uuid, health, status, primary_count, replica_count, doc_count, store_size, creation_date, aliases, settings_version |
| `shards` | `ClusterState.routingTable()` | index, shard, node, state, primary, relocating_node, docs, store_size, segments, unassigned_reason |

### Example queries

```sql
-- Find relocating shards
FROM cluster:shards | WHERE state == "RELOCATING" | STATS count = COUNT(*) BY index | SORT count DESC

-- Node resource overview
FROM cluster:nodes | WHERE roles LIKE "*data*" | SORT heap_used DESC | LIMIT 10

-- Large indices
FROM cluster:indices | WHERE doc_count > 1000000 | SORT store_size DESC

-- Unassigned shard diagnostics
FROM cluster:shards | WHERE state == "UNASSIGNED" | STATS count = COUNT(*) BY unassigned_reason, index
```

### SPI mapping

| Phase | What happens |
|-------|-------------|
| **Resolve** | Parse table name (`nodes`/`indices`/`shards`), return schema from known column definitions |
| **Optimize** | Push filter → in-memory predicate; push limit (all evaluated on coordinator) |
| **Physical plan** | Default `DataSourceExec` |
| **Partition** | Single `CoordinatorPartition` (coordinator-only) |
| **Execute** | Read from `ClusterState`, filter/limit in memory, build Pages directly |

### What it demonstrates

- `capabilities()` → `forCoordinatorOnly()` (coordinator-only, no SQL)
- In-memory filter and limit pushdown (no translation to a query language)
- Multiple "tables" within a single data source
- Coordinator-only execution path

---

## 2. Cluster Logs (`logs`)

**Base class:** `LakehouseDataSource` (distributed)

**Purpose:** Read Elasticsearch JSON log files across the cluster. Each node reads its own
local log files and results are merged back to the coordinator.

### Schema

ES JSON log files (`gc.json`, `server.json`, `*_audit.json`, etc.) have a known structure:

| Column | Type | Source |
|--------|------|--------|
| @timestamp | datetime | `timestamp` field in JSON log line |
| level | keyword | `level` field (DEBUG, INFO, WARN, ERROR) |
| component | keyword | `component` field (logger name) |
| message | text | `message` field |
| node.name | keyword | `node.name` field |
| cluster.name | keyword | `cluster.name` field |
| stacktrace | text | `stacktrace` field (optional, multi-line) |
| type | keyword | `type` field (server, deprecation, audit, gc) |

### Example queries

```sql
-- Recent warnings across the cluster
FROM logs:elasticsearch | WHERE level == "WARN" AND @timestamp > NOW() - 1 hour | SORT @timestamp DESC

-- Error frequency by component
FROM logs:elasticsearch | WHERE level == "ERROR" | STATS count = COUNT(*) BY component | SORT count DESC

-- GC pressure across nodes
FROM logs:elasticsearch | WHERE type == "gc" | STATS count = COUNT(*) BY node.name | SORT count DESC

-- Deprecation warnings
FROM logs:elasticsearch | WHERE type == "deprecation" | STATS count = COUNT(*) BY message | SORT count DESC | LIMIT 20
```

### SPI mapping

| Phase | What happens |
|-------|-------------|
| **Resolve** | List log files in the ES log directory, infer schema from JSON structure |
| **Optimize** | Push time range filter (seek in files), push level filter, push limit |
| **Physical plan** | Default `DataSourceExec` wrapping the `LakehousePlan` |
| **Partition** | One partition per node — each node reads its own log files (`NodeAffinity.require(nodeId)`) |
| **Execute** | `StorageProvider` lists `.json` files → `FormatReader` parses JSON lines → Pages |

### Lakehouse SPI usage

| SPI Type | Implementation |
|----------|---------------|
| `StorageProvider` | `LocalFileSystemStorageProvider` — wraps `java.nio.file` to list/read files in the ES log directory |
| `StorageObject` | `LocalFileStorageObject` — wraps `FileChannel` for reading a single log file |
| `StorageEntry` | File metadata: path, size, lastModified |
| `StorageIterator` | Iterates over `*.json` files in the log directory |
| `FormatReader` | `JsonLogFormatReader` — parses ES JSON log lines into columnar data |
| `FormatReader.metadata()` | Returns known ES log schema (fixed, no inference needed) |
| `SourceMetadata` | Schema + estimated row count (from total file sizes / avg line length) |
| `FilterPushdownSupport` | Pushes `@timestamp` range filters for efficient seeking, `level` for skip |

### What it demonstrates

- `LakehouseDataSource` base class with StorageProvider + FormatReader composition
- Distributed execution: partitioned by node, each reads local files
- `StorageProvider` / `StorageObject` for file access
- `FormatReader` for format parsing (JSON log lines → columnar Pages)
- `FilterPushdownSupport` for predicate pushdown
- `LakehousePlan` with filter + limit pushdown

---

## Summary

| Aspect | Cluster Catalog | Cluster Logs |
|--------|----------------|--------------|
| **Syntax** | `FROM cluster:nodes` | `FROM logs:elasticsearch` |
| **Base class** | `DataSource` (coordinator-only) | `LakehouseDataSource` |
| **Execution** | Coordinator-only | Distributed to all nodes |
| **Data origin** | `ClusterState` (in-memory) | JSON log files (on-disk per node) |
| **Pushdown** | Filter, limit (in-memory) | Filter (time range, level), limit |
| **Node affinity** | N/A (coordinator-only) | `NodeAffinity.require(nodeId)` — each node reads only its own logs |
| **Partitioning** | None (single coordinator partition) | One partition per node |
| **SPI coverage** | Coordinator-only path, no SQL | StorageProvider + FormatReader composition |
| **New functionality** | Queryable cluster metadata via ES\|QL | Queryable cluster-wide log files via ES\|QL |
