# Cluster state (cluster_state.json) and node-role reference

Two quick references: (1) **jq** and **jless** for reading `cluster_state.json`, and (2) **how to tell which node type runs a piece of code** (master vs data, etc.) when reading the Elasticsearch codebase.

---

## 1. jq and jless cheat sheet for cluster_state.json

### Getting cluster_state.json

- **REST**: `GET /_cluster/state?metric=_all` (or specific metrics: `version,master_node,nodes,metadata,routing_table,routing_nodes,blocks,customs`).
- **File**: If you have a dump, it’s typically a single JSON object (same shape as the API response).

### Top-level shape (from `ClusterState`)

| Key | Type | Description |
|-----|------|-------------|
| `cluster_name` | string | Cluster name |
| `version` | number | Cluster state version |
| `state_uuid` | string | UUID of this cluster state |
| `master_node` | string | **Node ID** of the elected master (only if metric `master_node` or `_all`) |
| `blocks` | object | Cluster blocks (if requested) |
| `nodes` | object | Map **node_id → node descriptor** (if metric `nodes` or `_all`) |
| `nodes_versions` | array | Compatibility versions per node |
| `nodes_features` | object | Node feature flags |
| `metadata` | object | Indices, templates, etc. |
| `routing_table` | object | Index → shard routing (if requested) |
| `routing_nodes` | object | Unassigned shards + per-node shard list (if requested) |
| `customs` | object | Custom state components |

### Node descriptor shape (each entry under `nodes`)

Key = **node id**. Value is an object:

| Key | Type | Description |
|-----|------|-------------|
| `name` | string | Human-readable node name |
| `ephemeral_id` | string | Ephemeral node id (changes on restart) |
| `transport_address` | string | Transport address |
| `external_id` | string | External identifier |
| `attributes` | object | Key-value attributes |
| `roles` | array | **Array of role names**: `master`, `data`, `data_hot`, `ingest`, etc. |
| `version` | string | Node version |
| `min_index_version` | number | Min index version supported |
| `max_index_version` | number | Max index version supported |

### jq: essentials

```bash
# Pretty-print
jq . cluster_state.json

# Cluster name and version
jq '{cluster_name, version, state_uuid}' cluster_state.json

# Master node id
jq '.master_node' cluster_state.json

# All node ids
jq '.nodes | keys' cluster_state.json

# Node id → name
jq '.nodes | to_entries | map({key: .key, name: .value.name}) | from_entries' cluster_state.json

# Nodes that have the "master" role (master-eligible)
jq '.nodes | to_entries | map(select(.value.roles | index("master"))) | from_entries' cluster_state.json

# Nodes that have the "data" role
jq '.nodes | to_entries | map(select(.value.roles | index("data"))) | from_entries' cluster_state.json

# List node names with their roles
jq '.nodes | to_entries[] | {name: .value.name, roles: .value.roles}' cluster_state.json

# Find node by name (e.g. "node-1")
jq '.nodes | to_entries[] | select(.value.name == "node-1") | {id: .key, node: .value}' cluster_state.json

# Master node id and its name
jq '{master_node, master_name: .nodes[.master_node].name}' cluster_state.json

# Routing table: index names
jq '.routing_table.indices | keys' cluster_state.json

# Shards for an index (e.g. "my_index")
jq '.routing_table.indices.my_index.shards' cluster_state.json

# Unassigned shards (from routing_nodes)
jq '.routing_nodes.unassigned' cluster_state.json

# Metadata: index list
jq '.metadata.indices | keys' cluster_state.json
```

### jq: “who is master / data” from nodes

```bash
# Node ids that are master-eligible (have "master" in roles)
jq '[.nodes | to_entries[] | select(.value.roles | index("master")) | .key]' cluster_state.json

# Node ids that can hold data (have "data" or any data_* in roles)
jq '[.nodes | to_entries[] | select(.value.roles | any(contains("data"))) | .key]' cluster_state.json
```

### jless: essentials

[jless](https://jless.io/) is a pager for JSON; keys are navigable.

```bash
# Open file (arrow keys to expand/collapse, type to search)
jless cluster_state.json

# Open from stdin
jq -c . cluster_state.json | jless
```

- **Navigation**: `Enter` expand/collapse, `←`/`→` collapse parent / go to next sibling, `/` search, `n`/`N` next/previous match.
- **Filter by path**: Pre-filter with jq then pipe:  
  `jq '.nodes' cluster_state.json | jless`
- **Search**: `/master_node` then `Enter` to jump to that key; search for a node name or id to see where it appears.

### One-liners to answer common questions

```bash
# Who is the current master? (id and name)
jq '{master_id: .master_node, master_name: .nodes[.master_node].name}' cluster_state.json

# All nodes with roles
jq '.nodes | to_entries | map({name: .value.name, id: .key, roles: .value.roles})' cluster_state.json

# Indices in cluster
jq '.metadata.indices | keys' cluster_state.json
```

---

## 2. Determining which node runs code (master vs data, etc.)

When reading Elasticsearch server code, use these **precise, reliable** ways to see whether logic runs on **master** nodes, **data** nodes, or **any** node.

### 2.1 “Am I the local node?” and “What is my role?”

- **Local node** (the node this process is running on):
  - `ClusterService#localNode()` → `DiscoveryNode`  
    Example: `clusterService.localNode()`
  - Or from cluster state: `clusterState.nodes().getLocalNode()`
  - In coordination layer: `transportService.getLocalNode()` (same idea; may be used before cluster state has the local node).

- **Role checks on the local node** (use the `DiscoveryNode` from above):
  - **Master-eligible**: `localNode.isMasterNode()`  
    True if this node has the `master` role (can participate in elections).
  - **Currently elected master**: `clusterState.nodes().isLocalNodeElectedMaster()`  
    True iff the local node’s id equals `clusterState.nodes().getMasterNodeId()`.
  - **Can hold data**: `localNode.canContainData()`  
    True if this node has any data role (e.g. `data`, `data_hot`, `data_content`, or stateless `index`/`search`).
  - **Has the “data” role specifically**: `DiscoveryNode.hasDataRole(settings)` (from settings); on a `DiscoveryNode`: check roles for `DiscoveryNodeRole.DATA_ROLE`.
  - **Ingest**: `localNode.isIngestNode()`  
    True if this node has the `ingest` role.

**Relevant classes**:

- `org.elasticsearch.cluster.service.ClusterService` — `localNode()`, `state()`
- `org.elasticsearch.cluster.node.DiscoveryNode` — `isMasterNode()`, `canContainData()`, `isIngestNode()`, `getRoles()`
- `org.elasticsearch.cluster.node.DiscoveryNodes` — `getLocalNode()`, `getLocalNodeId()`, `getMasterNodeId()`, `getMasterNode()`, `isLocalNodeElectedMaster()`
- `org.elasticsearch.cluster.node.DiscoveryNodeRole` — `MASTER_ROLE`, `DATA_ROLE`, `INGEST_ROLE`, etc.; role names match JSON (e.g. `"master"`, `"data"`).

### 2.2 “Does this code run only on the elected master?”

- **MasterService**  
  Code that runs inside a **MasterService** task (e.g. submitted via `masterService.submitStateUpdateTask(...)` or a task queue) runs **only on the node that is currently the elected master**.  
  - The executor’s `runOnlyOnMaster()` (default `true`) means: if this node loses mastership before the task runs, the task is failed with `NotMasterException` and not executed.  
  - So: **if you see a `ClusterStateUpdateTask` / executor registered with MasterService, that code runs only on the master.**

- **Explicit checks in code**  
  - `clusterState.nodes().isLocalNodeElectedMaster()` — “am I the current master?”  
  - `assert clusterState.nodes().isLocalNodeElectedMaster()` — often used in code that is only supposed to run on the master.  
  - `state.nodes().getMasterNodeId()` — who is master (node id); compare with `state.nodes().getLocalNodeId()` to see “am I master?”.

So when reading code:

1. **Master-only logic**: Look for **MasterService** submissions or **`isLocalNodeElectedMaster()`** (or assertions using it). That code path runs only on the elected master.
2. **Data-node / shard logic**: Look for **shard-level** operations (e.g. `IndexShard`, `TransportReplicationAction` executing on a shard). Those run on nodes that **hold that shard** (data/index/search nodes). You can also check `localNode().canContainData()` in branches that are only relevant for data-holding nodes.
3. **Any node**: If there is no MasterService and no “only on master” or “only on data” check, the code may run on any node (e.g. coordination, or routing).

### 2.3 From Settings (startup / config) rather than cluster state

When the question is “what **kind** of node is this configured to be?” (e.g. in constructors or at bootstrap):

- `DiscoveryNode.isMasterNode(Settings settings)` — master-eligible?
- `DiscoveryNode.hasDataRole(Settings settings)` — has the `data` role?
- `DiscoveryNode.canContainData(Settings settings)` — any role that can contain data?
- `DiscoveryNode.isIngestNode(Settings settings)` — ingest role?

These use `node.roles` (or default roles) from config and don’t require cluster state.

### 2.4 Quick lookup table (code)

| Question | Code / pattern |
|----------|----------------|
| Am I the local node? | `clusterService.localNode()` or `clusterState.nodes().getLocalNode()` |
| Is this node master-eligible? | `localNode.isMasterNode()` |
| Am I the current elected master? | `clusterState.nodes().isLocalNodeElectedMaster()` |
| Does this node hold data? | `localNode.canContainData()` |
| Is this node ingest? | `localNode.isIngestNode()` |
| This block runs only on master | Code inside a **MasterService** task, or guarded by `isLocalNodeElectedMaster()` |
| This block runs on data nodes | Shard-level actions or checks like `localNode().canContainData()` |

Using this, you can reliably tell from the code whether a given path runs on master nodes, data nodes, or both.
