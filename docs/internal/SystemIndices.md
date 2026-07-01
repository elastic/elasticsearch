# System Indices and Data Streams

System indices and data streams are Elasticsearch's mechanism for storing internal feature state — security configuration, Kibana workflows, GeoIP databases, ML models, task results, and so on. They are hidden from users by design: users should not see, modify, or delete them through the ordinary index APIs.

This guide is written for two audiences:

- **Plugin developers** who need to define a new system index or data stream, update its mappings, or prepare it for a major-version migration.
- Developers who need to understand the **internal mechanics** of how system indices are managed.

---

## Table of Contents

1. [Concepts](#1-concepts)
2. [Types of System Resources](#2-types-of-system-resources)
3. [Defining a System Index](#3-defining-a-system-index)
4. [Defining a System Data Stream](#4-defining-a-system-data-stream)
5. [Associated Indices](#5-associated-indices)
6. [Updating Mappings for Managed System Indices](#6-updating-mappings-for-managed-system-indices)
7. [Updating Mappings for System Data Streams](#7-updating-mappings-for-system-data-streams)
8. [Major-Version Migration (Reindexing)](#8-major-version-migration-reindexing)
9. [Access Control](#9-access-control)
10. [Snapshots and Feature State](#10-snapshots-and-feature-state)
11. [Implementation Details](#11-implementation-details)

---

## 1. Concepts

### System indices vs. ordinary indices

A system index is an ordinary Elasticsearch index (or data stream) with two special properties:

- It is **hidden** (`index.hidden = true`) so it does not appear in wildcard expansions.
- It has `system = true` in its `IndexMetadata`, which causes Elasticsearch to apply extra access restrictions.

There are two ways to determine whether an index is a system index:

1. **Name-pattern matching** — check whether the index name matches any `SystemIndexDescriptor` pattern registered with `SystemIndices`. This is the primary mechanism used throughout the codebase for routing, access control, thread pool selection, and similar decisions. `SystemIndices` exposes several pattern-based methods (`isSystemIndex`, `isSystemDataStream`, `isSystemIndexBackingDataStream`, `isSystemName`, `isNetNewSystemIndex`) that are all backed by compiled Lucene automata and are called widely.

2. **Metadata inspection** — check `IndexMetadata.isSystem()`. This reflects the actual persisted state in the cluster state and is authoritative when the index already exists. However, it is not available when the index has not yet been created.

Both approaches are used in practice. Metadata inspection is more reliable for existing indices. Pattern matching is necessary for decision-making before an index exists, and is the go-to approach in the many code paths that work with names rather than full metadata objects.

All system index and data stream names must start with `.`.

### Features

System resources are grouped into **features** — one feature per plugin. A feature is identified by its name and has a description. It owns a set of `SystemIndexDescriptor`s, `SystemDataStreamDescriptor`s, and optionally `AssociatedIndexDescriptor`s.

Features are registered at node startup. Most come from plugins via the `SystemIndexPlugin` interface, but a small number of built-in features (task results, synonyms) are declared directly in server. Features are immutable after construction.

### Patterns and the primary index

A `SystemIndexDescriptor` does not describe a single index — it describes a *pattern* of index names (e.g. `.my-feature-*`). Multiple concrete indices can match: historical versions, reindex targets during migration, etc. Patterns also match aliases of system indices.

For managed descriptors, the **primary index** is the one canonical name that Elasticsearch creates and writes to (e.g. `.my-feature-1`). The primary index must satisfy the pattern.

System data streams are identified by their exact name (e.g. `.my-feature-logs`). The data stream name itself, as well as the names of the backing indices, are matched by `SystemIndices`.

### Multi-project

In the multi-project setup, each project has its own system indices and system data streams.

---

## 2. Types of System Resources

### Internal vs. External

**INTERNAL** system indices are implementation details of Elasticsearch or a plugin. They should not be directly accessible via the standard REST API. All access should go through dedicated plugin-owned endpoints.

Currently, access is still permitted if the user has `allowRestrictedIndices(true)` privilege, but a deprecation warning is issued. Full lockdown is a long-term goal, not current reality.

**EXTERNAL** system indices are accessible via the standard REST API, subject to:
- The user having `allowRestrictedIndices(true)` privilege, AND
- The REST request including the `X-elastic-product-origin` header with a value matching one of the descriptor's `allowedElasticProductOrigins`.

External system indices exist to support components such as Kibana that interact with Elasticsearch indices directly using the index API.

### Managed vs. Unmanaged

**MANAGED** means Elasticsearch controls the index's settings, mappings, and aliases. The descriptor defines them, and `SystemIndexMappingUpdateService` keeps them up to date automatically.

**UNMANAGED** means the owning component (e.g. Kibana) manages its own settings and mappings. Elasticsearch treats the index as a system index for access-restriction purposes only.

### The four types

| Enum constant         | Internal/External | Managed/Unmanaged |
|-----------------------|-------------------|-------------------|
| `INTERNAL_MANAGED`    | Internal          | Managed           |
| `INTERNAL_UNMANAGED`  | Internal          | Unmanaged         |
| `EXTERNAL_MANAGED`    | External          | Managed           |
| `EXTERNAL_UNMANAGED`  | External          | Unmanaged         |

### Net-new system indices

Net-new indices opt into the desired future behavior: all REST API requests that lack a valid `X-elastic-product-origin` header are **rejected outright** (not warned). New system indices should be net-new whenever there is no backwards-compatibility requirement. A fully internal net-new index (no allowed product origins) is completely locked down and can only be changed by restoring feature state from a snapshot.

**When adding a new system index, make it net-new unless there is a concrete reason not to.**

### System data streams

System data streams are always managed (Elasticsearch controls their templates). They are either INTERNAL or EXTERNAL.

---

## 3. Defining a System Index

### Step 1: Implement `SystemIndexPlugin`

Your plugin must implement `org.elasticsearch.plugins.SystemIndexPlugin`. It is an extension of `ActionPlugin`, since plugins providing system indices typically also expose REST endpoints.

```java
public class MyPlugin extends Plugin implements SystemIndexPlugin {

    @Override
    public String getFeatureName() {
        return "my_feature";   // used in snapshot feature-state APIs
    }

    @Override
    public String getFeatureDescription() {
        return "Manages state for My Feature";
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(MY_INDEX_DESCRIPTOR);
    }
}
```

Feature names must be unique across all plugins. The reserved name `"none"` is not allowed.

### Step 2: Build a `SystemIndexDescriptor`

Use the fluent builder:

```java
private static final int MY_INDEX_MAPPINGS_VERSION = 1;

private static final SystemIndexDescriptor MY_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
    .setIndexPattern(".my-feature-*")           // must start with '.', must not start with '.*'
    .setPrimaryIndex(".my-feature-1")            // the concrete index to create; must match the pattern
    .setDescription("Stores state for My Feature")
    .setMappings(getMyIndexMappings())           // JSON string; must embed a mappings_version field
    .setSettings(getMyIndexSettings())           // index.hidden=true is enforced automatically if omitted
    .setAliasName(".my-feature")                 // optional; allows a stable write target across reindexes
    .setOrigin("my_feature")                     // required for managed indices; used in client calls
    .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
    .setNetNew()                                 // strongly recommended for new indices
    .build();
```

**Required fields for managed indices:**
- `indexPattern` — must start with `.`, must not start with `.*`
- `primaryIndex` — must start with `.`, must satisfy the pattern
- `mappings` — JSON; must include the `_meta.managed_index_mappings_version` field (see below)
- `settings` — `index.hidden=true` is automatically added if absent; you may omit it or include it explicitly
- `origin` — identifies the subsystem for origin-based access control

**Mappings version field** — the mappings must embed an integer version in the `_meta` block:

```json
{
  "_meta": {
    "managed_index_mappings_version": 1
  },
  "dynamic": "strict",
  "properties": { ... }
}
```

This integer must be bumped every time mappings change (see [Updating Mappings](#6-updating-mappings-for-managed-system-indices)).

### Step 3: Register the descriptor

Return the descriptor from `getSystemIndexDescriptors(Settings)`. Elasticsearch collects all descriptors at startup. Patterns must not overlap with any other descriptor's pattern.

### Index format setting

The `index.format` setting can be used to distinguish index generations. If specified in the descriptor's settings, it must match the `indexFormat` value passed to the builder. On upgrade, if the live index's format does not match the descriptor's format, `SystemIndexMappingUpdateService` will report `NEEDS_UPGRADE` rather than `NEEDS_MAPPINGS_UPDATE`, and the index will not be automatically updated (reindexing is required instead).

### Thread pools

By default, system indices use `ThreadPool.Names.SYSTEM_READ` for reads/searches and `ThreadPool.Names.SYSTEM_WRITE` for writes (i.e. `ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS`). For operations that are especially latency-sensitive (e.g. authentication), you can use the critical thread pools instead:

```java
.setThreadPools(ExecutorNames.CRITICAL_SYSTEM_INDEX_THREAD_POOLS)
```

This ensures those operations are not starved by heavy user traffic even when the standard system thread pools are saturated.

### Replica settings

By default, each system index uses the replica settings from its descriptor (or the generic `index.number_of_replicas` default if the descriptor does not specify them). Operators can override replica configuration uniformly across **all** system indices using two dynamic cluster-scoped settings:

| Cluster setting | Index setting equivalent | Description |
|---|---|---|
| `cluster.system_indices.number_of_replicas` | `index.number_of_replicas` | Fixed replica count for all system indices |
| `cluster.system_indices.auto_expand_replicas` | `index.auto_expand_replicas` | Auto-expand replica behaviour for all system indices |

Both settings can be applied dynamically via the cluster settings API (`PUT /_cluster/settings`) or statically in `elasticsearch.yml`.

**Priority order** (highest to lowest):
1. `cluster.system_indices.*` set via `PUT /_cluster/settings` (cluster state)
2. `cluster.system_indices.*` in `elasticsearch.yml`
3. Descriptor-specified settings
4. Generic setting defaults

When a cluster-level setting is removed, each system index reverts individually: managed indices revert to their descriptor value; unmanaged indices fall back to the generic default.

The security plugin exposes `PUT /_security/settings` for per-index replica customisation of security indices. If `cluster.system_indices.*` settings are present in cluster state, they take precedence and override any value previously applied via that API.

### Templates

By default, user-defined index templates are NOT applied to system indices. The `.setAllowsTemplates()` option exists for legacy compatibility (currently used only by `.kibana_*` indices) and is intended to be removed in the future.

---

## 4. Defining a System Data Stream

Use `SystemDataStreamDescriptor`:

```java
private static final SystemDataStreamDescriptor MY_DATA_STREAM = new SystemDataStreamDescriptor(
    ".my-feature-logs",                          // name; must start with '.'
    "Logs for My Feature",
    SystemDataStreamDescriptor.Type.INTERNAL,
    buildComposableIndexTemplate(),              // defines mappings and settings
    Map.of("my-component", buildComponentTemplate()),
    List.of(),                                   // empty = no external access allowed
    "my_feature",                                // origin
    ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
);
```

Return it from `getSystemDataStreamDescriptors()` in your plugin.

**Key differences from system indices:**
- No `indexPattern` — the backing index pattern is derived automatically using standard patterns for data stream backing indices.
- Always managed.
- No mapping-version tracking (see [section 7](#7-updating-mappings-for-system-data-streams)).
- External data streams must supply at least one allowed product origin.

---

## 5. Associated Indices

Associated indices are NOT system indices. They live in user space and are intentionally visible to users. Typical use case: ML stores debug/audit output in associated indices that users may want to query.

```java
@Override
public Collection<AssociatedIndexDescriptor> getAssociatedIndexDescriptors() {
    return List.of(new AssociatedIndexDescriptor(".ml-anomalies-*", "ML anomaly results"));
}
```

Associated indices are included in feature-state snapshots and are restored when a feature state is restored. They are not protected from user modification.

---

## 6. Updating Mappings for Managed System Indices

`SystemIndexMappingUpdateService` runs on the master node and, for every project, pushes a `PutMappingRequest` to any managed system index whose live mapping version is lower than the descriptor's current version.

### Step-by-step procedure

**1. Update the mappings method.**

Change fields in the method that returns the mappings JSON. For example:

```java
private static XContentBuilder getMyIndexMappings() throws IOException {
    return jsonBuilder().startObject()
        .startObject("_meta")
            .field("managed_index_mappings_version", MY_INDEX_MAPPINGS_VERSION)
        .endObject()
        .field("dynamic", "strict")
        .startObject("properties")
            // ... your fields ...
        .endObject()
    .endObject();
}
```

**2. Bump the version constant.**

```java
private static final int MY_INDEX_MAPPINGS_VERSION = 2;  // was 1
```

**3. Add a prior descriptor for the previous version (BWC step).**

During a rolling upgrade, old data/master nodes in the cluster will still try to create the index using the old descriptor. Without a prior descriptor, `TransportCreateIndexAction` will reject the creation because the nodes do not understand the new version.

```java
// Capture the previous descriptor (old mappings + old version)
private static final SystemIndexDescriptor MY_INDEX_V1_DESCRIPTOR = SystemIndexDescriptor.builder()
    .setIndexPattern(".my-feature-*")
    .setPrimaryIndex(".my-feature-1")
    .setMappings(getMyIndexMappingsV1())   // the old mappings
    .setSettings(getMyIndexSettings())
    .setOrigin("my_feature")
    .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
    .setNetNew()
    .build();

// Attach it to the current descriptor
private static final SystemIndexDescriptor MY_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
    // ... (current mappings and version) ...
    .setPriorSystemIndexDescriptors(List.of(MY_INDEX_V1_DESCRIPTOR))
    .build();
```

**Rules for prior descriptors:**
- Prior descriptors must have a strictly lower `managed_index_mappings_version` than the current descriptor.
- No two descriptors (current + priors) may have the same version.
- Prior descriptors must have the same `indexPattern`, `primaryIndex`, and `aliasName`.
- Prior descriptors cannot themselves have prior descriptors (only one level of nesting).

**How the version is tracked:**

The version is embedded in the mappings JSON under `_meta.managed_index_mappings_version`. When `SystemIndexMappingUpdateService` checks an index, it reads this field from the live index's mapping metadata and compares it against the descriptor. If the live version is lower, it applies a `PutMappingRequest` using the descriptor's current mappings.

See `InferencePlugin.getSystemIndexDescriptors` for a complete real-world example.

### UpgradeStatus states

`SystemIndexMappingUpdateService` evaluates each managed descriptor's primary index and classifies it as one of:

| State                  | Meaning                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `UP_TO_DATE`           | Mappings are current; no action needed.                                 |
| `NEEDS_MAPPINGS_UPDATE`| Mappings are stale; a `PutMappingRequest` will be sent.                 |
| `NEEDS_UPGRADE`        | `index.format` does not match; reindexing (major-version migration) required. |
| `CLOSED`               | Index is closed; update deferred.                                       |
| `UNHEALTHY`            | Index health is RED; update deferred until health recovers.             |

Updates are skipped entirely on mixed-version clusters (some nodes on an older version), since the cluster might not understand the new mappings yet.

---

## 7. Updating Mappings for System Data Streams

Data stream mapping updates work differently because each backing index is immutable after creation.

**Procedure:**
1. Update the `ComponentTemplate`(s) in the descriptor definition by changing the mappings in Java code.
2. No version bump is required — there is no `managed_index_mappings_version` for data streams.
3. No prior descriptor is required — because each backing index has a fixed mapping, there is no BWC concern about mixed-cluster creation.

**What gets updated:**
- New backing indices created after the next rollover will use the updated mappings automatically.
- Existing backing indices are **not** updated retroactively.

This is intentional: data stream backing indices are append-only and their mappings are fixed at creation time.

---

## 8. Major-Version Migration (Reindexing)

When Elasticsearch upgrades across a major version, some indices may become write-unavailable because their data was written by an older Lucene version that the new major no longer supports. Those indices must be reindexed.

### Checking migration status

```
GET /_migration/system_features
```

Returns a list of features that have indices requiring migration.

### Triggering migration

```
POST /_migration/system_features
```

Starts a persistent task (`SystemIndexMigrator`) that reindexes all system indices and data streams that need it. The task is fault-tolerant: if the master fails over during migration, the task resumes from where it left off using `SystemIndexMigrationTaskState`.

The migrator:
1. Calls `prepareForIndicesMigration()` on each plugin before migrating its indices.
2. Creates a new index with the name `<original>-reindexed-for-<version>` (e.g. `.my-feature-1-reindexed-for-10`).
3. Sets a write block on the old index.
4. Reindexes all documents, optionally running a migration script.
5. Deletes the old index and makes the original name (or alias) point to the new index as an alias.
6. Calls `indicesMigrationComplete()` on each plugin after all its indices are done.

### Migration scripts

A descriptor can define a Painless script to transform documents during reindexing:

```java
.setMigrationScript("""
    // remove deprecated field
    ctx._source.remove('legacy_field');
""")
```

**When to provide a script:**
- Only in the last minor release of a major version (e.g. 8.19 for migration to v9).
- Once the minor branch has diverged from the next-major (main) branch.
- Do not carry the script into the next major branch — it is not needed there.

### Plugin hooks

Override these `SystemIndexPlugin` methods to take action around migration:

```java
@Override
public void prepareForIndicesMigration(ProjectMetadata project, Client client,
        ActionListener<Map<String, Object>> listener) {
    // Enter safe mode, reject writes, etc.
    // The Map returned here is persisted and passed to indicesMigrationComplete().
    listener.onResponse(Map.of("was_in_safe_mode", true));
}

@Override
public void indicesMigrationComplete(Map<String, Object> preUpgradeMetadata,
        Client client, ActionListener<Boolean> listener) {
    // Exit safe mode.
    listener.onResponse(true);
}
```

`prepareForIndicesMigration` may be called more than once in the event of a master failover. `indicesMigrationComplete` is called once all indices owned by the plugin have been migrated (or attempted), even if some failed.

---

## 9. Access Control

### REST header

External clients (Kibana, Fleet, and other Elastic products) signal their identity by including the `X-elastic-product-origin` HTTP header in REST requests. `RestController` reads this header and translates it into transport-level thread context headers for use by downstream code.

### Transport-level headers

The two transport-level headers that carry system index access information through the thread context are:

| Transport header | Meaning |
|-----------------|---------|
| `_system_index_access_allowed` | `RestController` always sets this explicitly: `"true"` when `X-elastic-product-origin` is present, `"false"` for ordinary REST requests that lack that header. Internal code paths (not going through `RestController`) typically leave the header absent; the access-level logic defaults to `true` when the header is absent, giving internal callers full access. |
| `_external_system_index_access_origin` | Set to the value of `X-elastic-product-origin` by `RestController` when that header is present; identifies which Elastic product is making the request. |

These are internal headers — they are not meant to be set directly by end users.

### Access levels

`SystemIndices.getSystemIndexAccessLevel(ThreadContext)` combines the two transport headers into an access level:

| Level | When | Effect |
|-------|------|--------|
| `ALL` | `_system_index_access_allowed=true`, no external origin | Full access to all system indices (used by internal Elasticsearch code paths). |
| `RESTRICTED` | `_system_index_access_allowed=true`, external origin present | Access allowed only to the external system indices whose descriptor lists that origin. |
| `NONE` | `_system_index_access_allowed` explicitly set to `"false"` (as `RestController` does for ordinary REST requests) | No system index access. |
| `BACKWARDS_COMPATIBLE_ONLY` | Set manually in special cases | Issues deprecation warnings rather than denying access; workaround for specific known APIs. |

For non-net-new system indices, `NONE` or a mismatched `RESTRICTED` result in a deprecation warning. For net-new system indices and system data streams, access is denied outright.

### Origin clients

Internal code that performs operations on system indices must use `OriginSettingClient` to set the origin on sub-requests:

```java
Client originClient = new OriginSettingClient(client, "my_feature");
```

The origin string must match what was set in the descriptor via `.setOrigin(...)`.

---

## 10. Snapshots and Feature State

System indices and their associated indices participate in **feature state snapshots**. When a snapshot is taken with `feature_states`, the indices belonging to each feature are included as a group.

When a feature state is restored:
- System indices matching the feature's descriptors are deleted and restored from the snapshot.
- Associated indices matching the feature's `AssociatedIndexDescriptor`s are also deleted and restored.

The `cleanUpFeature` method on `SystemIndexPlugin` (default implementation: delete all system and associated indices) is called by the Reset Features API, which is useful for tests and development.

---

## 11. Implementation Details

### How system indices are created

System indices are created through two different code paths.

**Explicit creation via the Create Index API (`TransportCreateIndexAction`)**

The handler resolves the requested index name against all registered descriptors using `SystemIndices.findMatchingDescriptor`. For net-new descriptors, access control is enforced immediately: the request is rejected unless the thread context carries `_system_index_access_allowed=true` with a matching product origin.

For unmanaged system indices the handler only ensures `index.hidden=true` before passing the request through to `MetadataCreateIndexService`.

For managed system indices with no request origin (i.e. requests not initiated internally by Elasticsearch itself), the handler applies the **mixed-cluster BWC logic**:
1. Looks up `ClusterState.getMinSystemIndexMappingVersions()` to find the lowest `MappingsVersion` reported by any node in the cluster for this index.
2. Calls `descriptor.getDescriptorCompatibleWith(minVersion)` to select the appropriate descriptor — possibly a prior descriptor — whose mappings version is compatible with the oldest node.
3. If no compatible descriptor exists (the cluster is too old), creation fails with an error message that names the minimum supported version.
4. Otherwise the mappings, settings, and alias are taken from the selected descriptor and passed to `MetadataCreateIndexService`.

Only the primary index name or the alias name may be specified in the request. Any other name that happens to match the pattern is rejected — the only exception is the migration reindex target, which is created by `SystemIndexMigrator` with the cause `"migrate-system-index"`.

**Implicit creation via auto-create (`AutoCreateAction`)**

When a document is written to an index that does not yet exist, `AutoCreateAction` triggers. It detects system indices by name using `SystemIndices.isSystemIndex(name)`. System data streams are detected similarly via `SystemIndices.validateDataStreamAccess`, which also checks access control.

The auto-create path applies the same mixed-cluster BWC logic as explicit creation for managed system indices: it selects the compatible descriptor via `getDescriptorCompatibleWith` before calling `MetadataCreateIndexService.applyCreateIndexRequest`. For unmanaged system indices it ensures `index.hidden=true`, just as explicit creation does.

Unlike explicit creation, auto-create does not enforce the primary-index-only restriction: writing a document to any name matching the descriptor's pattern triggers creation of that index.

**`MetadataCreateIndexService`**

Both paths ultimately call `MetadataCreateIndexService`, which performs the actual cluster state mutation. When the `CreateIndexClusterStateUpdateRequest` was built from a managed system index descriptor, it already carries the correct mappings and settings, so `MetadataCreateIndexService` treats it like any other index creation.

### `SystemIndices`

The central registry. Constructed during `Node` startup from all `SystemIndexPlugin` instances plus a small number of built-in server-side features (task results, synonyms). Not modified after construction.

Responsibilities:
- Validates that no two descriptors have overlapping patterns, that aliases are distinct, and that feature names are unique.
- Builds Lucene `Automaton` instances from all patterns and unions them into several combined automata (one for system indices, one for data stream backing indices, one for all system names, one for net-new indices, etc.).
- Exposes pattern-based query methods: `isSystemIndex`, `isSystemDataStream`, `isSystemIndexBackingDataStream`, `isSystemName`, `isNetNewSystemIndex`. These are called throughout the codebase for routing, access control, and thread pool selection.
- Exposes `getFeature(name)` and `getFeatures()` for migration and snapshot APIs.
- Provides `getSystemIndexAccessLevel(ThreadContext)` for access-control decisions.

### Pattern matching

Index patterns use a mangled regex syntax: `.` is always a literal, `*` expands to `.*`. Other Lucene `RegExp` operators (`+`, `~`, `[...]`, `(...)`) are also supported but rarely used. Patterns are compiled into Lucene `Automaton` instances at startup, determinized, and unioned together. This makes each `isSystemIndex`-style check O(string-length) with no per-descriptor overhead.

The union automaton also enables overlap detection at startup: `SystemIndices` verifies that no two descriptors' patterns match the same index name.

### `SystemIndexMappingUpdateService`

A `ClusterStateListener` running on the master node. On every cluster state update:
1. Skips if the cluster state is not yet recovered.
2. Skips if this node is not the elected master.
3. Skips on mixed-version clusters (rolling upgrades in progress).
4. For each project, for each managed descriptor whose primary index exists in that project, computes `UpgradeStatus`.
5. For any descriptor in `NEEDS_MAPPINGS_UPDATE`, sends a `PutMappingRequest` using an `OriginSettingClient`.

An `AtomicBoolean` (`isUpgradeInProgress`) prevents concurrent update attempts. The flag is reset (via `RefCountingRunnable`) only after all in-flight `PutMappingRequest`s have completed.

### `SystemIndexMetadataUpgradeService`

A separate `ClusterStateListener` responsible for keeping `system` flags and `hidden` flags correct in `IndexMetadata`.

When a new system index descriptor is added (e.g. after an upgrade), any existing index whose name matches the new pattern will automatically have `system=true` applied. Conversely, if a descriptor is removed, matched indices become non-system.

The service also ensures all system indices have `hidden=true`.

### `SystemIndexSettingsUpdateService`

A `ClusterStateListener` running on the master node. Propagates changes to `cluster.system_indices.number_of_replicas` and `cluster.system_indices.auto_expand_replicas` to all existing system indices.

- **Initial settings**: On first master election after cluster state recovery, applies any values from `elasticsearch.yml` that are not already present in the cluster state (cluster state takes precedence).
- **Dynamic changes**: On each cluster state change, detects additions, modifications, or removals of the two cluster-level settings and propagates them to all system indices in a single `UpdateSettingsRequest`.
- **Reset on removal**: When a cluster-level setting is removed, each index is reverted to its descriptor value (for managed indices) or the generic default (for unmanaged or descriptor-less indices), rather than applying a uniform value.

Indices are grouped by their effective settings before issuing updates so that one `UpdateSettingsRequest` is sent per group rather than per index.

### `SystemIndexMigrator`

A persistent task that performs major-version reindexing. State is serialized to `SystemIndexMigrationTaskState` so it survives master failover.

Key operations:
- `migrateSingleIndex()` — adds a write block, creates the target index, reindexes, then atomically deletes the old index and points the old name to the new index as an alias.
- `migrateDataStream()` — delegates to `ReindexDataStreamAction` and polls for completion.
- `reindex()` — issues a `ReindexRequest`, optionally injecting the descriptor's `migrationScript`.

The migrator processes one index at a time, in sequence, to avoid overwhelming the cluster.

### `Feature` class (inner class of `SystemIndices`)

Groups together all descriptors belonging to one plugin. Provides:
- `getIndexDescriptors()` / `getDataStreamDescriptors()` / `getAssociatedIndexDescriptors()`
- `cleanUpFeature(...)` — static utility for the default `SystemIndexPlugin.cleanUpFeature` implementation
- `prepareForIndicesMigration(...)` / `indicesMigrationComplete(...)` — delegates to the plugin

---

## Quick Reference

| Task | What to do |
|------|------------|
| Add a new system index | Implement `SystemIndexPlugin`, build a `SystemIndexDescriptor` (prefer `INTERNAL_MANAGED` + `.setNetNew()`), return from `getSystemIndexDescriptors()` |
| Update mappings | Change mappings, bump the version constant, add a prior descriptor for the old version |
| Add a new system data stream | Build a `SystemDataStreamDescriptor`, return from `getSystemDataStreamDescriptors()` |
| Update data stream mappings | Update the `ComponentTemplate` in the descriptor; no version bump or prior descriptor needed |
| Provide a migration script | Add `.setMigrationScript(...)` to the descriptor; only in the last minor of a major release |
| Hook into major-version migration | Override `prepareForIndicesMigration` / `indicesMigrationComplete` in your plugin |
| Check if index is system (by name) | Use `SystemIndices.isSystemIndex(name)` — automaton-based, O(string-length) |
| Check if index is system (from metadata) | Inspect `IndexMetadata.isSystem()` — authoritative for existing indices |
| Configure replicas for all system indices | Set `cluster.system_indices.number_of_replicas` or `cluster.system_indices.auto_expand_replicas` via `PUT /_cluster/settings` |
