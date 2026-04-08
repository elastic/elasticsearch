# Deep Dive: Schema Inference Cache (TP-10)

## 1. Current Schema Resolution Flow

### End-to-end trace: query text to resolved schema

The schema resolution flow is a sequential chain across five major components:

**Step 1: Parse** -- `EsqlParser` parses the query and creates `UnresolvedExternalRelation` nodes containing the raw `tablePath` expression (a `Literal` holding a `BytesRef` string).

**Step 2: PreAnalyze** -- `PreAnalyzer.preAnalyze()` walks the plan collecting `UnresolvedExternalRelation` nodes and extracting their string paths into the `icebergPaths` list.
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/PreAnalyzer.java`, lines 73-81.

**Step 3: Resolve (I/O)** -- `EsqlSession.preAnalyzeExternalSources()` calls `ExternalSourceResolver.resolve()` with the list of paths.
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java`, line 866.
- The resolver iterates all `ExternalSourceFactory` instances from `DataSourceModule.sourceFactories()` to find one whose `canHandle()` matches.
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`, lines 195-235 (`resolveSingleSource`).

**Step 3a: Single-file path** -- Calls `factory.resolveMetadata(path, config)`.
- For file-based sources, this goes through `FileSourceFactory.resolveMetadata()` (line 92), which:
  1. Creates a `StorageProvider` from the scheme (e.g., S3, GCS).
  2. Creates a `StorageObject` via `provider.newObject(storagePath)`.
  3. Looks up the `FormatReader` by file extension.
  4. Calls `reader.metadata(storageObject)` -- **this does actual network I/O**.
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java`, lines 92-110.

**Step 3b: Multi-file (glob) path** -- `resolveMultiFileSource()` first expands the glob via `GlobExpander` (which lists objects in the storage bucket -- more I/O), then resolves metadata from the first file only (FIRST_FILE_WINS strategy).
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`, lines 144-182.

**Step 3c: Concrete I/O for Parquet** -- `ParquetFormatReader.metadata()` opens the file with `ParquetFileReader.open()` using `SKIP_ROW_GROUPS` options (reads footer only, not row data). Still requires a network round-trip to read the Parquet footer metadata.
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`, lines 67-86.

**Step 4: Wrap** -- The resolver wraps the `SourceMetadata` into `ExternalSourceMetadata`, merging query-level config (WITH clause params) with metadata-level config.
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`, lines 307-353 (`wrapAsExternalSourceMetadata`).

**Step 5: Analyze** -- `Analyzer.ResolveExternalRelations` rule takes `ExternalSourceResolution` from `AnalyzerContext` and replaces `UnresolvedExternalRelation` with `ExternalRelation` (resolved logical plan node).
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Analyzer.java`, lines 456-473.

### Critical finding: every query re-reads metadata

Yes, **every query re-resolves metadata from scratch**. The `ExternalSourceResolver` is created fresh per query in `PlanExecutor.esql()` (line 93-96), and there is no caching layer anywhere in the chain. A user running `EXTERNAL "s3://bucket/data.parquet" | STATS count(*)` ten times will make ten network round-trips to read the Parquet footer.

- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/execution/PlanExecutor.java`, lines 93-96.

---

## 2. SourceMetadata Interface and Implementations

### Interface: `SourceMetadata`
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceMetadata.java`
- Fields (all via interface methods):
  - `schema()` -- `List<Attribute>` -- the resolved column schema
  - `sourceType()` -- `String` -- e.g., "parquet", "iceberg", "csv"
  - `location()` -- `String` -- the URI/path of the source
  - `statistics()` -- `Optional<SourceStatistics>` -- row counts, sizes (default: empty)
  - `partitionColumns()` -- `Optional<List<String>>` -- partition column names (default: empty)
  - `sourceMetadata()` -- `Map<String, Object>` -- opaque source-specific metadata (default: empty map)
  - `config()` -- `Map<String, Object>` -- configuration for operator creation (default: empty map)
- **Not serializable** via Writeable/NamedWriteable. It is a plain interface.

### Implementation: `SimpleSourceMetadata`
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SimpleSourceMetadata.java`
- Concrete, immutable class with all fields.
- **Has `equals()` and `hashCode()`** based on `schema`, `sourceType`, `location` (lines 117-137).
- Suitable for caching as a value type.

### Interface: `ExternalSourceMetadata`
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceMetadata.java`
- Extends `SourceMetadata` with deprecated backward-compat methods (`tablePath()`, `attributes()`).
- Used as the type in `ExternalSourceResolution.ResolvedSource`.

### Cacheability assessment
- `SimpleSourceMetadata` is immutable and thread-safe -- good for caching.
- `ExternalSourceMetadata` is often an anonymous inner class (see `ExternalSourceResolver.wrapAsExternalSourceMetadata()` line 327 and `enrichSchemaWithPartitionColumns()` line 258). These capture `metadata` and `config` references but are effectively immutable.
- The `schema()` list contains `Attribute` objects (specifically `ReferenceAttribute`), which are immutable value objects.
- **`sourceMetadata()` map can contain arbitrary objects** (e.g., Iceberg native `Schema` object). This is fine for caching since it is opaque pass-through; the cache just holds a reference.

---

## 3. Existing Caching in Datasources Code

**There is no schema metadata caching anywhere in the datasources code.**

The only "caching" in `DataSourceModule` is the `LazyPluginState` pattern (lines 266-324), which caches the *plugin factory instances* (not query results). This is a one-time lazy initialization of plugin classloading, not per-query result caching:
- `storageFactoriesCache` -- caches `Map<String, StorageProviderFactory>` (plugin-level)
- `formatFactoriesCache` -- caches `Map<String, FormatReaderFactory>` (plugin-level)
- `connectorFactoriesCache` -- caches `Map<String, ConnectorFactory>` (plugin-level)
- `catalogFactoriesCache` -- caches `Map<String, TableCatalogFactory>` (plugin-level)

File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/DataSourceModule.java`, lines 266-324.

`StorageProviderRegistry` also caches default `StorageProvider` instances per scheme (line 118, `providers` map), but this is the storage client itself, not metadata results.

**No memoization, no TTL cache, no result caching of any kind exists for schema metadata.**

---

## 4. Elasticsearch Cache Infrastructure

### `Cache<K, V>` class
- File: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/cache/Cache.java`
- 256-segment concurrent cache with LRU eviction.
- Supports:
  - `expireAfterAccess` -- evicts entries unused for N nanoseconds
  - `expireAfterWrite` -- evicts entries N nanoseconds after creation
  - `maximumWeight` -- weight-based eviction (default weigher: each entry = 1)
  - `RemovalListener` -- notification on eviction
  - `computeIfAbsent(key, loader)` -- atomic load-or-get (lines 389-468), ensures loader is called at most once per key
- Thread-safe: read-heavy optimization (read/write locks per segment).

### `CacheBuilder<K, V>` class
- File: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/cache/CacheBuilder.java`
- Fluent builder: `.setExpireAfterAccess(TimeValue)`, `.setExpireAfterWrite(TimeValue)`, `.setMaximumWeight(long)`, `.weigher(...)`, `.removalListener(...)`, `.build()`

### Concrete example: `ApiKeyService` (TTL + max weight)
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/authc/ApiKeyService.java`, lines 275-287.
- Pattern:
  ```java
  this.apiKeyAuthCache = CacheBuilder.<String, ListenableFuture<CachedApiKeyHashResult>>builder()
      .setExpireAfterAccess(ttl)
      .setMaximumWeight(maximumWeight)
      .removalListener(getAuthCacheRemovalListener(maximumWeight))
      .build();
  ```
- TTL and max size are controlled by `Setting<TimeValue>` and `Setting<Integer>` respectively.

### Concrete example: `ScriptCache` (configurable TTL + max size)
- File: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/script/ScriptCache.java`, lines 61-71.
- Pattern:
  ```java
  CacheBuilder<CacheKey, Object> cacheBuilder = CacheBuilder.builder();
  if (this.cacheSize >= 0) {
      cacheBuilder.setMaximumWeight(this.cacheSize);
  }
  if (this.cacheExpire.getNanos() != 0) {
      cacheBuilder.setExpireAfterAccess(this.cacheExpire);
  }
  this.cache = cacheBuilder.removalListener(new ScriptCacheRemovalListener()).build();
  ```

---

## 5. DataSourceModule: Service Initialization

- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/DataSourceModule.java`
- Created once in `EsqlPlugin.createComponents()`:
  - File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/EsqlPlugin.java`, lines 242-248.
  - Receives: `List<DataSourcePlugin>`, `DataSourceCapabilities`, `Settings`, `BlockFactory`, `ExecutorService`.
- Registered as a component (line 268): `dataSourceModule` is added to the components list.
- Holds singleton registries: `StorageProviderRegistry`, `FormatReaderRegistry`, `FilterPushdownRegistry`.
- Implements `Closeable` -- closed when the plugin shuts down.

**A schema cache singleton should live in `DataSourceModule`** because:
1. It is created once at plugin startup, has the right lifecycle (singleton per node).
2. It already holds the `Settings` object needed for cache configuration.
3. It is passed to `PlanExecutor`, which passes it to `ExternalSourceResolver`.
4. It implements `Closeable`, so the cache can be cleaned up properly.

---

## 6. Integration Point: Where the Cache Goes

### Primary integration: `ExternalSourceResolver.resolveSingleSource()`
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`, lines 195-235.
- This is the single choke-point where every metadata resolution goes through.
- Currently it iterates factories and calls `factory.resolveMetadata(path, config)`.

### Proposed cache-check location:
The cache check should go **at the top of `resolveSingleSource()`**, before the factory iteration loop. Pseudocode:

```java
private SourceMetadata resolveSingleSource(String path, Map<String, Object> config) {
    // Cache check
    SchemaCache.Key cacheKey = new SchemaCache.Key(path, config);
    SourceMetadata cached = schemaCache.get(cacheKey);
    if (cached != null) {
        LOGGER.debug("Schema cache hit for [{}]", path);
        return cached;
    }

    // ... existing scheme validation ...
    // ... existing factory iteration ...

    // Cache store (before return)
    schemaCache.put(cacheKey, metadata);
    return metadata;
}
```

### Secondary integration: `resolveMultiFileSource()` (glob paths)
- File: lines 144-182.
- For globs, the **schema** comes from `resolveSingleSource(firstFile)` on line 172, so the single-source cache already covers it.
- The **glob expansion** (file listing) is a separate I/O operation that could be cached independently, but that is a different concern (file-set caching, not schema caching).

---

## 7. Cache Key Considerations

### What makes two resolutions equivalent?

The `resolveSingleSource` method receives:
1. `String path` -- the resolved file path (e.g., `s3://bucket/data.parquet`). For globs, this is the first file's path.
2. `Map<String, Object> config` -- query parameters from the WITH clause (e.g., `access_key`, `secret_key`, `endpoint`, `region`).

### Proposed cache key: `(path, relevant_config_subset)`

**Must include:**
- `path` -- different files have different schemas
- Format-affecting config keys -- any config that might change schema resolution behavior

**Must NOT include:**
- Credential keys (`access_key`, `secret_key`, `session_token`) -- same file with different credentials should return same schema
- Runtime config (`batch_size`, `max_buffer_size`) -- these affect execution, not schema

**Recommended approach:**
- Use `(path)` as the cache key for the initial implementation. Config keys rarely affect schema resolution. The `config` map is primarily about storage credentials and connection settings -- a Parquet file at `s3://bucket/data.parquet` has the same schema regardless of which AWS credentials are used to access it.
- For `SchemaResolution` strategy: currently only `FIRST_FILE_WINS` is implemented. When `UNION_BY_NAME` or `STRICT` are implemented, the cache key for glob paths would need to include the strategy. But for single-file paths, the strategy is irrelevant.

### Edge case: connector-based sources
- For `ConnectorFactory` sources (Flight), `resolveMetadata()` calls the remote server. The schema could change if the remote server's table changes.
- TTL-based expiration handles this: after the TTL expires, the next query re-resolves.

### Proposed cache key record:
```java
record SchemaCacheKey(String path) {
    // path alone is sufficient; credentials don't affect schema
}
```

---

## 8. What Exactly Needs to Be Built

### New classes

#### 1. `SchemaCache` (new class in `org.elasticsearch.xpack.esql.datasources`)
```
x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/SchemaCache.java
```
- Wraps `Cache<String, SourceMetadata>` from `org.elasticsearch.common.cache.Cache`.
- Constructor takes `TimeValue ttl` and `int maxEntries`.
- Methods: `get(String path)`, `put(String path, SourceMetadata)`, `invalidate(String path)`, `invalidateAll()`, `stats()`.
- Thread-safe (the underlying `Cache` is already thread-safe).

#### 2. `SchemaCacheSettings` (new class or constants in existing class)
```
x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/SchemaCacheSettings.java
```
Or add constants directly to `EsqlPlugin`:
- `Setting<TimeValue> EXTERNAL_SOURCE_SCHEMA_CACHE_TTL` -- default 5 minutes, node-scoped, dynamic.
- `Setting<Integer> EXTERNAL_SOURCE_SCHEMA_CACHE_MAX_SIZE` -- default 1000, node-scoped.

### Modified classes

#### 3. `DataSourceModule` -- add schema cache singleton
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/DataSourceModule.java`
- Add field: `private final SchemaCache schemaCache;`
- Initialize in constructor from `Settings`.
- Add accessor: `public SchemaCache schemaCache()`
- Add to `close()`: invalidate/close the cache.

#### 4. `ExternalSourceResolver` -- integrate cache check
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`
- Add `SchemaCache` field (passed in constructor or obtained from `DataSourceModule`).
- In `resolveSingleSource()` (line 195): check cache before factory iteration, store result after successful resolution.
- The resolver already receives `DataSourceModule` in its constructor (line 64), so it can access `dataSourceModule.schemaCache()`.

#### 5. `EsqlPlugin` -- register new settings
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/EsqlPlugin.java`
- Add the two new settings to `getSettings()` (line 294).

### Test classes

#### 6. `SchemaCacheTests` (new)
```
x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/datasources/SchemaCacheTests.java
```
- Test TTL expiration, max size eviction, cache hit/miss, concurrent access.
- Test that identical paths return cached metadata.
- Test that different paths resolve independently.

#### 7. Modify `ExternalSourceResolverTests`
- File: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolverTests.java`
- Add tests verifying that repeated resolution of the same path hits the cache.
- Add tests for cache invalidation.

### Configuration

| Setting | Type | Default | Scope |
|---------|------|---------|-------|
| `esql.external_source.schema_cache.ttl` | TimeValue | `5m` | NodeScope, Dynamic |
| `esql.external_source.schema_cache.max_size` | Integer | `1000` | NodeScope |

Setting `ttl` to `0` or `-1` would disable caching entirely.

### Estimated complexity
- `SchemaCache`: ~80 lines (thin wrapper around `Cache<K,V>`).
- `SchemaCacheSettings`: ~20 lines (two `Setting<>` constants).
- `DataSourceModule` changes: ~15 lines.
- `ExternalSourceResolver` changes: ~20 lines (cache check + store in `resolveSingleSource`).
- `EsqlPlugin` changes: ~5 lines (register settings).
- Tests: ~200 lines.
- **Total: ~340 lines of production + test code.**

### Non-goals for initial implementation
- Glob expansion caching (file listing) -- separate concern, separate cache.
- Schema union-by-name caching -- only FIRST_FILE_WINS is implemented.
- Cross-node cache consistency -- each node caches independently (same as all other ES caches).
- Cache warming -- not needed; first query for each path populates the cache.
- Invalidation API -- not needed for TP; TTL handles staleness. A future CRUD API for data sources would be the natural place for explicit invalidation.
