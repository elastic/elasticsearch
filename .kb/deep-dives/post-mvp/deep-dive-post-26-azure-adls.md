# Deep Dive: Azure ADLS Gen2 Native Support (abfss:// URI Scheme)

## Executive Summary

The ESQL Azure data source plugin (`esql-datasource-azure`) currently supports `wasbs://` and `wasb://` URIs, routing all operations through the Azure Blob Storage SDK (`azure-storage-blob:12.27.1`). ADLS Gen2 accounts are accessible through Azure's blob compatibility layer (every ADLS Gen2 account exposes a `*.blob.core.windows.net` endpoint alongside the `*.dfs.core.windows.net` endpoint), so the current plugin already works for ADLS Gen2 data -- just via the blob API. Native `abfss://` support would require adding a new SDK dependency (`azure-storage-file-datalake`), which is **not** currently anywhere in the Elasticsearch dependency tree. However, the implementation would be structurally straightforward: the work is almost entirely additive (new URI scheme registration, new client type, reuse existing credential patterns).

**Estimated effort: 1.5-2 weeks for one engineer.**

---

## 1. Azure Data Source Plugin -- Current Implementation

### 1.1 Plugin Entry Point

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-azure/src/main/java/org/elasticsearch/xpack/esql/datasource/azure/AzureDataSourcePlugin.java`

The plugin declares two URI schemes and registers a single shared factory for both:

```java
// Line 34-36
@Override
public Set<String> supportedSchemes() {
    return Set.of("wasbs", "wasb");
}

// Line 39-62
@Override
public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
    StorageProviderFactory azureFactory = new StorageProviderFactory() { ... };
    return Map.of("wasbs", azureFactory, "wasb", azureFactory);
}
```

The factory creates an `AzureStorageProvider` with an `AzureConfiguration` parsed from WITH-clause fields: `connection_string`, `account`, `key`, `sas_token`, `endpoint`.

### 1.2 StorageProvider Implementation

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-azure/src/main/java/org/elasticsearch/xpack/esql/datasource/azure/AzureStorageProvider.java`

This is a 326-line class that exclusively uses the Azure Blob Storage SDK. Key observations:

- **Client type:** `BlobServiceClient` (line 46) -- the blob-only client, not the Data Lake client.
- **Imports:** `com.azure.storage.blob.BlobClient`, `BlobContainerClient`, `BlobServiceClient`, `BlobServiceClientBuilder` (lines 11-16).
- **No DFS imports at all.**

**URI format:** `wasbs://account.blob.core.windows.net/container/path/to/blob`
- Host = `account.blob.core.windows.net` (parsed at line 212-218, account extracted as substring before first `.`)
- Path = `/container/path/to/blob` (container is first segment, blob name is remainder)

**Scheme validation** (lines 205-209):
```java
private static void validateAzureScheme(StoragePath path) {
    String scheme = path.scheme().toLowerCase(Locale.ROOT);
    if (scheme.equals("wasbs") == false && scheme.equals("wasb") == false) {
        throw new IllegalArgumentException(
            "AzureStorageProvider only supports wasbs:// and wasb:// schemes, got: " + scheme);
    }
}
```

This is called at the top of every operation: `newObject()`, `listObjects()`, `exists()`.

**Endpoint construction** (lines 92-94, 97-101, 115-117): All default endpoints use `.blob.core.windows.net`:
```java
endpoint = "https://" + config.account() + ".blob.core.windows.net";
```

**Operations supported:**
- `newObject(StoragePath)` -- creates BlobClient for reads (line 125-131)
- `newObject(StoragePath, long)` -- with pre-known length (line 134-140)
- `newObject(StoragePath, long, Instant)` -- with length and modification time (line 143-149)
- `listObjects(StoragePath, boolean)` -- list blobs with prefix, supports recursive/hierarchical (line 152-162)
- `exists(StoragePath)` -- HEAD request via `blobClient.exists()` (line 165-171)
- `supportedSchemes()` -- returns `["wasbs", "wasb"]` (line 174-176)
- `close()` -- attempts to close underlying HTTP client (line 179-203)

### 1.3 StorageObject Implementation

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-azure/src/main/java/org/elasticsearch/xpack/esql/datasource/azure/AzureStorageObject.java`

Wraps a `BlobClient` and provides:
- `newStream()` -- full object read via `blobClient.openInputStream()` (line 73)
- `newStream(long position, long length)` -- range read via `BlobRange` (line 81-95)
- `length()` / `lastModified()` / `exists()` -- metadata via `blobClient.getProperties()` (lines 98-146)

All metadata is cached after first fetch. Range reads use `BlobRange` + `BlobRequestConditions`.

### 1.4 Configuration

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-azure/src/main/java/org/elasticsearch/xpack/esql/datasource/azure/AzureConfiguration.java`

Record with five fields: `connectionString`, `account`, `key`, `sasToken`, `endpoint`.

Authentication modes (in priority order, from `buildBlobServiceClient()` lines 80-122):
1. **Connection string** -- full Azure connection string (can override endpoint)
2. **Account + key** -- SharedKey authentication via `StorageSharedKeyCredential`
3. **Account + SAS token** -- SAS token authentication
4. **DefaultAzureCredential** -- fallback when no explicit credentials (managed identity, environment variables, Azure CLI, etc.)

---

## 2. StoragePath URI Parsing

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/StoragePath.java`

StoragePath is scheme-agnostic. It parses any URI with `scheme://host[:port]/path` format.

For `wasbs://myaccount.blob.core.windows.net/container/data/file.parquet`:
- scheme = `wasbs`
- host = `myaccount.blob.core.windows.net`
- port = `-1`
- path = `/container/data/file.parquet`

For a hypothetical `abfss://myaccount.dfs.core.windows.net/container/data/file.parquet`:
- scheme = `abfss`
- host = `myaccount.dfs.core.windows.net`
- port = `-1`
- path = `/container/data/file.parquet`

**StoragePath itself requires zero changes for `abfss://` support.** The URI structure is identical -- only the scheme and hostname suffix differ. The `extractAccountFromHost()` method in `AzureStorageProvider` (line 212-218) extracts the account name from the host by taking everything before the first `.`, which works identically for `myaccount.dfs.core.windows.net`.

---

## 3. Scheme-to-Provider Mapping

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/StorageProviderRegistry.java`

The registry maps lowercase scheme strings to `StorageProviderFactory` instances. Registration happens in `DataSourceModule` (line 86-125):

```java
for (String scheme : plugin.supportedSchemes()) {
    storageProviderRegistry.registerFactory(scheme, delegating);
}
```

To add `abfss://` support, `AzureDataSourcePlugin.supportedSchemes()` would need to include `"abfss"` and `"abfs"`, and `storageProviders()` would need to return factories for those keys.

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/DataSourceCapabilities.java`

Capabilities aggregate all schemes from all plugins. This is used for early validation in `ExternalSourceResolver.resolveSingleSource()` (line 200-203). No changes needed beyond what the plugin declares.

---

## 4. Azure SDK Dependencies

### 4.1 Current Dependencies

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-azure/build.gradle`

```groovy
// Lines 30-33
implementation 'com.azure:azure-storage-blob:12.27.1'
implementation 'com.azure:azure-storage-common:12.26.1'
implementation 'com.azure:azure-core:1.51.0'
implementation 'com.azure:azure-identity:1.13.2'
implementation 'com.azure:azure-core-http-netty:1.15.3'
```

Plus reactor-netty, reactor-core, and a full suite of Netty modules. The `thirdPartyAudit` section (lines 68-287) is extensive with ~150 `ignoreMissingClasses` entries.

### 4.2 Repository-Azure Comparison

**File:** `/Users/oleglvovitch/github/root/elasticsearch/modules/repository-azure/build.gradle`

Uses the same Azure SDK versions:
- `azure-storage-blob:12.27.1`
- `azure-storage-common:12.26.1`
- `azure-core:1.51.0`
- `azure-identity:1.13.2`
- `azure-core-http-netty:1.15.3`

Additionally includes `azure-storage-blob-batch:12.23.1` and `azure-storage-internal-avro:12.12.1` (not used by ESQL).

### 4.3 Missing SDK for ADLS Gen2

The Data Lake Storage SDK (`azure-storage-file-datalake`) is **not present anywhere** in the Elasticsearch dependency tree. No `.gradle` file, no Java import, no reference at all.

The required dependency would be:
```groovy
implementation 'com.azure:azure-storage-file-datalake:12.20.1'  // aligned with blob 12.27.1
```

The Data Lake SDK has a transitive dependency on `azure-storage-blob`, so they share the same underlying HTTP client and credential infrastructure. The version alignment matrix is:
- `azure-storage-blob:12.27.x` pairs with `azure-storage-file-datalake:12.20.x`
- Both depend on `azure-storage-common:12.26.x` and `azure-core:1.51.x`

---

## 5. Blob vs DFS Endpoint -- What's Different

### 5.1 Endpoint Hostnames

| Aspect | Blob API | DFS API |
|--------|----------|---------|
| Hostname | `*.blob.core.windows.net` | `*.dfs.core.windows.net` |
| URI scheme (Hadoop) | `wasb://` / `wasbs://` | `abfs://` / `abfss://` |
| Protocol | Azure Blob REST API | Azure Data Lake Storage Gen2 REST API |
| SDK | `azure-storage-blob` | `azure-storage-file-datalake` |

### 5.2 Compatibility Layer

Every ADLS Gen2 storage account has **both** endpoints active simultaneously. The blob compatibility layer means:
- **Read operations** (GET blob, list blobs, HEAD blob) work identically on both endpoints
- **The blob API can read any file stored via the DFS API**
- **Metadata operations** (list, exists, properties) work on both

The DFS endpoint provides additional capabilities:
- Hierarchical namespace operations (rename directory, POSIX ACLs)
- More efficient directory listing (O(1) per directory instead of prefix scan)
- True directory semantics (create/delete empty directories)
- POSIX-style permissions and ACLs

### 5.3 What Matters for ESQL

For ESQL's read-only use case (list files, read files, get metadata), the **blob compatibility layer covers 100% of functionality**. The DFS API provides:
- **Slightly more efficient directory listing** for large hierarchies (native directory operations vs. prefix enumeration)
- **ACL-aware access** (if the account uses POSIX ACLs instead of shared key/SAS)
- **Ecosystem compatibility** (Spark, Databricks, Hadoop all use `abfss://` for ADLS Gen2)

The most important reason for `abfss://` support is **ecosystem compatibility**: users who have data in ADLS Gen2 expect to use `abfss://` URIs, which is the standard convention in every major data tool. Telling them to rewrite URIs as `wasbs://` is a friction point.

---

## 6. Credential Patterns

### 6.1 Current Azure Credential Handling

The `AzureConfiguration` record and `buildBlobServiceClient()` method support four auth modes. All four work identically for both Blob and Data Lake APIs because they use the same `azure-identity` SDK:

1. **Connection string** -- Blob-API-specific format (`DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...`). Works for blob endpoint only. DFS endpoint doesn't use connection strings.
2. **Account + SharedKey** -- `StorageSharedKeyCredential` works for both Blob and DFS clients.
3. **SAS token** -- Works for both Blob and DFS, though SAS tokens are scoped per-endpoint.
4. **DefaultAzureCredential** -- Token-based OAuth2, works identically for both endpoints.

### 6.2 ADLS Gen2 Credential Implications

- **SharedKey** auth: Same key works for both blob and DFS endpoints on the same account. No changes needed.
- **SAS tokens**: SAS tokens are endpoint-specific. A SAS token generated for `*.blob.core.windows.net` won't work on `*.dfs.core.windows.net` and vice versa. The implementation would need to generate/accept DFS-specific SAS tokens.
- **DefaultAzureCredential / OAuth2**: Identical -- the OAuth2 token works across both endpoints.
- **Connection strings**: Not applicable for DFS. Would need to be replaced with `account` + `key` or `account` + SAS for DFS.

---

## 7. Implementation Approach -- Two Options

### Option A: Thin Translation Layer (Recommended for MVP)

**No new SDK dependency. Just translate `abfss://` URIs to `wasbs://` URIs internally.**

Since the blob compatibility layer handles all read operations, the simplest approach is:

1. Add `"abfss"` and `"abfs"` to `supportedSchemes()` in `AzureDataSourcePlugin`
2. In `AzureStorageProvider`, when receiving an `abfss://` URI:
   - Extract the account name from `account.dfs.core.windows.net`
   - Construct the blob endpoint: `https://account.blob.core.windows.net`
   - Use the existing `BlobServiceClient` with the blob endpoint
3. Update `validateAzureScheme()` to accept `abfss` and `abfs`
4. Update default endpoint construction to handle both hostname formats

**Changes required:**

| File | Change |
|------|--------|
| `AzureDataSourcePlugin.java` | Add `"abfss"`, `"abfs"` to `supportedSchemes()` and `storageProviders()` |
| `AzureStorageProvider.java` | Update `validateAzureScheme()` (line 205-209), `supportedSchemes()` (line 174-176), `buildBlobServiceClient()` (lines 92-94, 97-101, 115-117) to translate `.dfs.` to `.blob.` |
| `AzureStorageProvider.java` | Update `AzureStorageIterator` (line 309) to preserve original scheme in listed paths |
| Tests | Add tests for `abfss://` URI parsing, scheme validation, endpoint translation |

**Estimated effort: 2-3 days.** No new dependencies, no new SDK modules, minimal code changes.

**Limitations:**
- SAS tokens generated for DFS endpoint won't work (must use blob SAS tokens or SharedKey/OAuth2)
- No hierarchical namespace performance benefits (still does prefix-based listing)
- Connection strings still not supported for DFS endpoint

### Option B: Native DFS Client (Full ADLS Gen2 Support)

**Add `azure-storage-file-datalake` SDK and use `DataLakeServiceClient` for `abfss://` URIs.**

1. Add `azure-storage-file-datalake:12.20.1` dependency to `build.gradle`
2. Create `AzureDataLakeStorageProvider` that uses `DataLakeServiceClient`
3. Register it for `abfss` and `abfs` schemes
4. Implement `listObjects()` using `DataLakeFileSystemClient.listPaths()` (true directory listing)
5. Implement `newObject()` using `DataLakeFileClient.openInputStream()` / `read()`

**Changes required:**

| File | Change |
|------|--------|
| `build.gradle` | Add `azure-storage-file-datalake:12.20.1` + thirdPartyAudit entries |
| New: `AzureDataLakeStorageProvider.java` | ~300 lines, mirrors `AzureStorageProvider` with DFS client |
| New: `AzureDataLakeStorageObject.java` | ~150 lines, mirrors `AzureStorageObject` with DFS client |
| `AzureDataSourcePlugin.java` | Register both providers under their respective schemes |
| `AzureConfiguration.java` | Add DFS-specific endpoint logic |
| Tests | Full test suite for ADLS Gen2 paths |
| `gradle/verification-metadata.xml` | SHA256 checksums for new JAR |

**Estimated effort: 1.5-2 weeks.** New dependency, new classes, new audit entries, integration tests.

**Benefits over Option A:**
- DFS-specific SAS tokens work correctly
- Hierarchical namespace listing is more efficient
- True directory operations available
- No endpoint translation -- uses native DFS REST API

---

## 8. Specific Touchpoints for Adding `abfss://`

### 8.1 Files That Must Change (Either Option)

1. **`AzureDataSourcePlugin.java`** (line 34-36, 61): Add `abfss`/`abfs` to scheme sets
2. **`AzureStorageProvider.java`** (lines 174-176, 205-209): Update scheme lists and validation
3. **`AzureDataSourcePluginTests.java`**: Update scheme count assertions (line 28: `assertEquals(2, providers.size())` becomes 4)
4. **`AzureStorageProviderTests.java`**: Add `abfss://` path parsing tests

### 8.2 Files That Must Change (Option B Only)

5. **`build.gradle`** (line 30): Add `azure-storage-file-datalake` dependency
6. **New `AzureDataLakeStorageProvider.java`**: Full DFS StorageProvider implementation
7. **New `AzureDataLakeStorageObject.java`**: DFS StorageObject implementation
8. **`gradle/verification-metadata.xml`**: Checksum for new JAR

### 8.3 Integration Test Changes

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/AbstractExternalSourceSpecTestCase.java` (line 505-507)

Currently generates Azure test paths as:
```java
return "wasbs://" + ACCOUNT + ".blob.core.windows.net/" + CONTAINER + "/" + WAREHOUSE + "/" + relativePath;
```

Would need an additional `AZURE_DFS` backend option:
```java
return "abfss://" + ACCOUNT + ".dfs.core.windows.net/" + CONTAINER + "/" + WAREHOUSE + "/" + relativePath;
```

The `AzureFixtureUtils` (line 50-53) creates a `BlobServiceClient` for test fixtures. The Azure HTTP fixture (`AzureHttpFixture`) would need to handle DFS API endpoints for Option B, or the test can use the blob fixture with translated URIs for Option A.

---

## 9. Recommendation

**For MVP Tech Preview: Option A (thin translation layer).** 2-3 days of work, zero new dependencies, covers the primary use case (users can use `abfss://` URIs). Document the limitation that DFS-specific SAS tokens won't work.

**For Post-MVP: Option B (native DFS client).** Add the full `azure-storage-file-datalake` SDK, implement native DFS operations, support DFS SAS tokens, and get hierarchical namespace performance benefits.

The key insight is that **Option A gives users the URI they expect** (`abfss://`) with full read functionality, while the underlying implementation just translates to the blob endpoint. This is exactly what Spark does internally when `abfss://` URIs are used with the Hadoop ABFS driver -- the driver can use either the DFS or blob endpoint depending on configuration. For a read-only analytics query engine, the blob endpoint is sufficient.
