# Deep Dive: Iceberg Catalog Integration (Glue, Hive, Nessie, REST)

**Date**: 2026-03-03
**Branch**: `esql/connector-spi-v3` (main)
**Iceberg version**: 1.10.1 (`build-tools-internal/version.properties` line 27)

---

## 1. Current Metadata Reading: How IcebergCatalogAdapter Works Today

### The Static-Metadata-File Approach

The current implementation reads Iceberg metadata **directly from S3 object storage**, bypassing any catalog service entirely. It uses Iceberg's `StaticTableOperations` class, which loads a single metadata JSON file and constructs an immutable `Table` object from it.

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergCatalogAdapter.java`

**Resolution flow** (lines 41-59):
```java
public static IcebergTableMetadata resolveTable(String tablePath, S3Configuration s3Config) throws Exception {
    S3FileIO fileIO = S3FileIOFactory.create(s3Config);
    try {
        String metadataLocation = findLatestMetadataFile(tablePath, fileIO);
        StaticTableOperations ops = new StaticTableOperations(metadataLocation, fileIO);
        Table table = new BaseTable(ops, tablePath);
        Schema schema = table.schema();
        return new IcebergTableMetadata(tablePath, schema, s3Config, SOURCE_TYPE_ICEBERG, metadataLocation);
    } finally {
        IOUtils.closeWhileHandlingException(fileIO);
    }
}
```

**Metadata file discovery** (lines 75-118):
1. First tries `{tablePath}/metadata/version-hint.text` -- reads the version number, constructs `v{N}.metadata.json` path, verifies it exists via `S3FileIO.newInputFile().exists()`.
2. Fallback: brute-force scans from `v100.metadata.json` down to `v1.metadata.json`, returning the first that exists.

**Key imports** (lines 9-14):
```java
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
```

**Summary**: There is NO Iceberg `Catalog` interface usage whatsoever. The code reads metadata JSON files directly from S3 by path convention. This means:
- No catalog API calls (no REST, no Glue, no DynamoDB, no Hive Metastore)
- Table discovery requires knowing the exact S3 path to the table root
- Version tracking relies on `version-hint.text` or brute-force scanning
- No namespace/database concepts
- No transaction support for concurrent writers

---

## 2. Iceberg Dependency Version and Modules

**File**: `/Users/oleglvovitch/github/root/elasticsearch/build-tools-internal/version.properties` (line 27)
```
iceberg = 1.10.1
```

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/build.gradle`

### Modules included (lines 40-98):

| Module | Version | Line | Purpose |
|--------|---------|------|---------|
| `iceberg-core` | 1.10.1 | 40 | Core table operations, Schema, StaticTableOperations |
| `iceberg-aws` | 1.10.1 | 53 | S3FileIO, **also contains GlueCatalog and DynamoDbCatalog** |
| `iceberg-parquet` | 1.10.1 | 64 | Parquet file format support |
| `iceberg-arrow` | 1.10.1 | 81 | ArrowReader for vectorized reading |
| `parquet-hadoop-bundle` | 1.16.0 | 99 | Shaded Parquet classes |
| `hadoop-client-api` | 3.4.2 | 111 | Hadoop Configuration class |
| `hadoop-client-runtime` | 3.4.2 | 112 | Hadoop runtime |

### Modules NOT included:

| Module | Purpose | Notes |
|--------|---------|-------|
| `iceberg-hive-metastore` | HiveCatalog (`org.apache.iceberg.hive.HiveCatalog`) | Requires Hive Metastore Thrift client |
| `iceberg-nessie` | NessieCatalog | Requires Nessie client library |

### Exclusions on iceberg-aws (lines 53-63):
The `iceberg-aws` dependency explicitly excludes:
- `software.amazon.awssdk:bundle` (replaced by individual AWS SDK modules)
- `commons-codec`, `slf4j-api`, `checker-qual`, all Jackson modules

### AWS SDK dependencies (lines 123-165):
The following AWS SDK v2 modules are included:

| Module | Line | Notes |
|--------|------|-------|
| `annotations` | 123 | |
| `apache-client` | 124 | |
| `url-connection-client` | 125 | |
| `auth` | 126 | |
| `aws-core` | 127 | |
| `aws-xml-protocol` | 128 | |
| `aws-json-protocol` | 129 | |
| `http-client-spi` | 130 | |
| `identity-spi` | 131 | |
| `metrics-spi` | 132 | |
| `regions` | 133 | |
| `retries-spi` | 134 | |
| `kms` | 136 | Required by Iceberg's AwsProperties |
| `retries` | 137 | |
| `s3` | 138 | |
| `sdk-core` | 139 | |
| `sts` | 140 | |
| `utils` | 141 | |
| `profiles` | 142 | |

**NOT included**: `software.amazon.awssdk:glue`, `software.amazon.awssdk:dynamodb`

However, both `glue` and `dynamodb` appear in `gradle/verification-metadata.xml` (lines 5942, 5957), meaning their checksums are already verified. They are likely transitive dependencies of `iceberg-aws` that get excluded at runtime because the `bundle` exclusion removes the AWS SDK bundle but the individual modules still appear in the dependency graph for verification purposes.

---

## 3. Catalog Classes Available on the Classpath

### Currently Available (via iceberg-aws 1.10.1):

| Class | Module | Available? |
|-------|--------|-----------|
| `org.apache.iceberg.rest.RESTCatalog` | `iceberg-core` | **YES** -- RESTCatalog is in iceberg-core |
| `org.apache.iceberg.aws.glue.GlueCatalog` | `iceberg-aws` | **YES (class exists)** but requires `software.amazon.awssdk:glue` at runtime, which is NOT in the dependency list |
| `org.apache.iceberg.aws.dynamodb.DynamoDbCatalog` | `iceberg-aws` | **YES (class exists)** but requires `software.amazon.awssdk:dynamodb` at runtime |

### NOT Available:

| Class | Module Needed | Notes |
|-------|--------------|-------|
| `org.apache.iceberg.hive.HiveCatalog` | `iceberg-hive-metastore` | Requires Hive Metastore Thrift client, heavyweight |
| `org.apache.iceberg.nessie.NessieCatalog` | `iceberg-nessie` | Requires Nessie client library |

### Verification:

Searching the entire codebase for any usage of Iceberg's `Catalog` interface:
```
Grep for: org.apache.iceberg.catalog.Catalog  -> No matches found
Grep for: CatalogUtil|CatalogProperties        -> No matches found
Grep for: RESTCatalog|GlueCatalog|HiveCatalog|NessieCatalog -> No matches found
```

No Iceberg catalog implementations are used anywhere in the codebase today.

---

## 4. IcebergDataSourcePlugin: What It Registers

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergDataSourcePlugin.java`

```java
public class IcebergDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<String> supportedCatalogs() {
        return Set.of("iceberg");  // line 43
    }

    @Override
    public Map<String, TableCatalogFactory> tableCatalogs(Settings settings) {
        return Map.of("iceberg", s -> new IcebergTableCatalog());  // line 48
    }
}
```

**What it registers**:
- One catalog type: `"iceberg"`
- One `TableCatalogFactory` that creates `IcebergTableCatalog` instances
- Does NOT register: storage providers, format readers, connectors, operator factories, filter pushdown support, or named writeables
- Does NOT implement `supportedSchemes()`, `supportedFormats()`, `supportedExtensions()`, `storageProviders()`, `formatReaders()`, `sourceFactories()`, `operatorFactories()`, `filterPushdownSupport()`, or `namedWriteables()`

**Plugin metadata**:
- Plugin name: `esql-datasource-iceberg` (build.gradle line 21)
- Plugin class: `org.elasticsearch.xpack.esql.datasource.iceberg.IcebergDataSourcePlugin` (line 23)
- Extended plugins: `['x-pack-esql']` (line 24)
- Entitlements: `manage_threads`, `outbound_network` (`src/main/plugin-metadata/entitlement-policy.yaml`)

---

## 5. Iceberg Table API Usage: No Catalog Interface, Raw File Reading

The current code does NOT use Iceberg's `Catalog` interface at all. Instead, it uses low-level static table operations:

### Pattern used everywhere:

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergCatalogAdapter.java` (lines 46-52)
```java
StaticTableOperations ops = new StaticTableOperations(metadataLocation, fileIO);
Table table = new BaseTable(ops, tablePath);
Schema schema = table.schema();
```

Same pattern in `IcebergTableCatalog.planScan()` (lines 69-70):
```java
StaticTableOperations ops = new StaticTableOperations(metadata.metadataLocation(), fileIO);
Table table = new BaseTable(ops, tablePath);
```

Same pattern in `IcebergSourceOperatorFactory.createIcebergTableReader()` (lines 133-135):
```java
org.apache.iceberg.aws.s3.S3FileIO fileIO = S3FileIOFactory.create(s3Config);
org.apache.iceberg.StaticTableOperations ops = new org.apache.iceberg.StaticTableOperations(metadata.metadataLocation(), fileIO);
Table table = new org.apache.iceberg.BaseTable(ops, tablePath);
```

### What `StaticTableOperations` does:
- Takes a metadata file location and a FileIO instance
- Reads the metadata JSON file once
- Creates an immutable `TableMetadata` from it
- Does NOT support commits, updates, or transaction isolation
- Does NOT track the current metadata pointer (no catalog)
- Equivalent to Iceberg's `HadoopTables` approach but without Hadoop

### Key limitation:
If another process writes to the Iceberg table (e.g., Spark inserts data), the cached metadata in ES is stale. There is no mechanism to detect or refresh the metadata pointer. A real catalog (REST, Glue, etc.) solves this by providing the current metadata location on each `loadTable()` call.

---

## 6. AWS Dependencies

**File**: `/Users/oleglvovitch/github/root/elasticsearch/build-tools-internal/version.properties` (line 21)
```
awsv2sdk = 2.31.78
```

### Present in iceberg plugin:
- `software.amazon.awssdk:s3` (line 138)
- `software.amazon.awssdk:sts` (line 140)
- `software.amazon.awssdk:kms` (line 136)
- Plus 16 supporting modules (auth, regions, protocols, etc.)

### NOT present (needed for catalogs):
- `software.amazon.awssdk:glue` -- **needed for GlueCatalog**
- `software.amazon.awssdk:dynamodb` -- **needed for DynamoDbCatalog**

Both have checksums already in `gradle/verification-metadata.xml` (lines 5942, 5957), so adding them as explicit dependencies would not require regenerating verification metadata.

### S3FileIO creation:

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/S3FileIOFactory.java`

Creates `S3FileIO` with a pre-configured `S3Client` supplier (lines 60-115):
- Uses `UrlConnectionHttpClient` (not Apache HTTP client) to avoid entitlement violations (line 109)
- Explicitly creates empty `ProfileFile` to prevent AWS SDK from reading `~/.aws/credentials` (lines 69-79)
- Always uses `forcePathStyle(true)` for S3-compatible service compatibility (line 105)
- Falls back to hardcoded test credentials if none provided (lines 89-90)

---

## 7. What Exactly Needs to Be Built

### 7.1 REST Catalog

**Class available**: `org.apache.iceberg.rest.RESTCatalog` (in `iceberg-core`, already on classpath)

**Dependencies needed**: NONE -- RESTCatalog is in `iceberg-core` which is already included.

**What to build**:

1. **Wrapper class** (e.g., `IcebergRESTCatalogAdapter`):
   - Accepts configuration: `uri` (REST catalog endpoint), `warehouse`, `credential`, `token`, `scope`, `oauth2-server-uri`
   - Creates `RESTCatalog` instance, calls `initialize(name, properties)`
   - Implements `loadTable(TableIdentifier)` to get the `Table` object
   - Returns `IcebergTableMetadata` from the loaded table

2. **Configuration**:
   - `catalog_uri` -- REST catalog endpoint URL (e.g., `https://polaris.example.com/api/catalog`)
   - `warehouse` -- warehouse identifier
   - `credential` -- OAuth2 client_id:client_secret for catalog authentication
   - `token` -- Bearer token alternative to credential
   - `catalog_name` -- namespace for the catalog

3. **Key advantage**: RESTCatalog is the **universal standard** -- AWS Glue now exposes a REST catalog API, Apache Polaris and Gravitino implement it, and Snowflake/Databricks are moving toward it. Building REST catalog support first gives the broadest coverage.

4. **Networking**: RESTCatalog makes HTTP requests to the catalog server. The existing entitlement `outbound_network` covers this. May need to configure the HTTP client to use `UrlConnectionHttpClient` (same approach as S3FileIOFactory) to avoid entitlement issues with the Apache HTTP client.

5. **Estimated complexity**: LOW -- the `RESTCatalog` class handles all the protocol details. The wrapper is ~100-200 lines of code.

### 7.2 AWS Glue Catalog

**Class available**: `org.apache.iceberg.aws.glue.GlueCatalog` (in `iceberg-aws`, already on classpath as a .class file)

**Dependencies needed**:
- `software.amazon.awssdk:glue:${versions.awsv2sdk}` -- the Glue client SDK
  - Checksum already in `gradle/verification-metadata.xml` line 5957
- Possibly `software.amazon.awssdk:dynamodb:${versions.awsv2sdk}` -- if using Glue with DynamoDB lock manager
  - Checksum already in `gradle/verification-metadata.xml` line 5942

**What to build**:

1. **build.gradle changes**: Add to `x-pack/plugin/esql-datasource-iceberg/build.gradle`:
   ```groovy
   implementation "software.amazon.awssdk:glue:${versions.awsv2sdk}"
   ```
   Plus corresponding entries in the `tasks.withType(AbstractDependenciesTask)` block.

2. **Wrapper class** (e.g., `IcebergGlueCatalogAdapter`):
   - Accepts configuration: `glue.id` (Glue catalog ID, defaults to AWS account ID), `region`, credentials
   - Creates `GlueCatalog` instance with AWS credentials from ES keystore
   - Implements `loadTable(TableIdentifier)`
   - Must configure `GlueCatalog` with the same `S3FileIO` for consistency

3. **Configuration**:
   - `glue_catalog_id` -- AWS Glue catalog ID (optional, defaults to caller's account)
   - `region` -- AWS region
   - `access_key` / `secret_key` or IAM role -- credentials
   - `database` -- Glue database name (maps to Iceberg namespace)
   - `table` -- Glue table name

4. **Credential management**: Should reuse the ES keystore pattern from `Clusters.java` (lines 47-48) where S3 credentials are stored in keystore, not plaintext settings.

5. **Estimated complexity**: MEDIUM -- needs AWS SDK Glue client wiring, credential management integration with ES keystore, and Glue-specific error handling.

### 7.3 Hive Metastore Catalog

**Class NOT available**: `org.apache.iceberg.hive.HiveCatalog` is in `iceberg-hive-metastore`, not currently in the dependency tree.

**Dependencies needed**:
- `org.apache.iceberg:iceberg-hive-metastore:${versions.iceberg}`
- Transitive: Hive Metastore Thrift client (`org.apache.hive:hive-metastore`), which pulls in a LARGE dependency tree including Thrift, Hadoop, and many legacy libraries

**What to build**:

1. **build.gradle changes**: Add `iceberg-hive-metastore` with extensive exclusions to avoid jar hell (Hive pulls in its own versions of Guava, Hadoop, protobuf, etc.)

2. **Wrapper class**: Similar pattern to REST/Glue adapters.

3. **Configuration**:
   - `hive.metastore.uris` -- Thrift URI(s) for the Hive Metastore (e.g., `thrift://hms-host:9083`)
   - `database` and `table` names

4. **Key challenges**:
   - **Dependency hell**: Hive Metastore has a massive, conflicting dependency tree. The Elasticsearch jar hell checker will flag many conflicts. Extensive exclusions and possibly shading would be needed.
   - **Thrift networking**: Hive uses Apache Thrift for communication, which creates threads and uses networking patterns that may conflict with ES entitlements.
   - **Hadoop dependency**: HiveCatalog requires Hadoop `Configuration`, which is already present but may need additional Hadoop classes.

5. **Estimated complexity**: HIGH -- the dependency management alone is a major effort. Consider whether this is worth the investment vs. recommending users expose Hive Metastore via a REST catalog interface (e.g., Apache Polaris).

### 7.4 Nessie Catalog

**Class NOT available**: `org.apache.iceberg.nessie.NessieCatalog` is in `iceberg-nessie`, not currently in the dependency tree.

**Dependencies needed**:
- `org.apache.iceberg:iceberg-nessie:${versions.iceberg}`
- Transitive: Nessie client library (`org.projectnessie.nessie:nessie-client`)

**What to build**:

1. **build.gradle changes**: Add `iceberg-nessie` with exclusions.

2. **Wrapper class**: Similar pattern to other adapters.

3. **Configuration**:
   - `nessie.uri` -- Nessie server URL
   - `nessie.ref` -- branch/tag name (default: `main`)
   - `nessie.auth.type` -- authentication type (NONE, BEARER, OAUTH2)
   - `nessie.auth.token` -- bearer token if applicable

4. **Key advantage**: Nessie provides git-like branching for data lakes, useful for time-travel and audit.

5. **Key consideration**: Nessie now supports the Iceberg REST Catalog API. So instead of using the native `NessieCatalog` class, Nessie can be accessed via `RESTCatalog` with the Nessie REST endpoint. This means **Nessie support may come for free with REST catalog support**.

6. **Estimated complexity**: MEDIUM if using native `NessieCatalog`; FREE if accessed via REST catalog protocol.

---

## 8. Architectural Integration Points

### 8.1 Where catalogs plug in

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/TableCatalog.java`

The `TableCatalog` interface (line 32) is what `IcebergTableCatalog` implements today:
```java
public interface TableCatalog extends ExternalSourceFactory, Closeable {
    String catalogType();
    boolean canHandle(String path);
    SourceMetadata metadata(String tablePath, Map<String, Object> config) throws IOException;
    List<DataFile> planScan(String tablePath, Map<String, Object> config, List<Object> predicates) throws IOException;
}
```

The `metadata()` method (line 38) receives a config map with credentials. Today, `IcebergTableCatalog.extractS3Config()` (lines 103-113) extracts `access_key`, `secret_key`, `endpoint`, `region` from this map.

For catalog integration, this config map would also carry catalog-specific configuration: `catalog_type` (rest/glue/hive/nessie), `catalog_uri`, `glue_catalog_id`, etc.

### 8.2 IcebergTableCatalog.canHandle() limitation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java` (lines 41-44)

```java
public boolean canHandle(String path) {
    return path != null && (path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://"));
}
```

This only handles S3 paths. For catalog-based resolution, the "path" would be a logical table identifier (e.g., `my_database.my_table` or `iceberg://catalog/db/table`), not an S3 path. The `canHandle()` logic needs to be extended.

### 8.3 IcebergTableCatalog.planScan() -- already implemented

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java` (lines 59-93)

The `planScan()` method already:
1. Resolves the table via `IcebergCatalogAdapter.resolveTable()`
2. Creates `S3FileIO` and `StaticTableOperations`
3. Runs `table.newScan().planFiles()` to enumerate `FileScanTask` objects
4. Wraps each as a `DataFile` with path, format, size, record count

For catalog integration, only step 1-2 would change: instead of `StaticTableOperations`, use `catalog.loadTable(tableId)` to get the `Table` object. Steps 3-4 remain identical.

### 8.4 IcebergSourceOperatorFactory -- NOT wired

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java` (lines 97-106)

```java
public SourceOperator get(DriverContext driverContext) {
    // TODO: Implement async source operator creation
    throw new UnsupportedOperationException(
        "Direct Iceberg source operator creation is not yet supported."
    );
}
```

The operator factory is NOT functional. The `IcebergDataSourcePlugin` does NOT register it (it only registers `TableCatalog`, not `operatorFactory`). The `createIcebergTableReader()` method (lines 127-173) has a complete implementation using `ArrowReader` but it cannot be invoked because `get()` throws.

### 8.5 IcebergPushdownFilters -- implemented but not wired to TableCatalog

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java`

The `convert()` method (lines 55-138) translates ESQL expressions to Iceberg `Expression` objects. It supports:
- Binary comparisons (=, !=, <, <=, >, >=) -- lines 57-69
- IN lists -- lines 72-83
- IS NULL / IS NOT NULL -- lines 86-93
- Range expressions -- lines 96-111
- Boolean AND/OR -- lines 115-125
- NOT -- lines 129-134

This is complete and tested but not connected to the `TableCatalog.planScan()` method, which ignores the `predicates` parameter (line 76-77: "For now, we don't apply predicates at the scan planning level").

### 8.6 SplitProvider returns SINGLE

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSourceFactory.java` (line 37)

```java
default SplitProvider splitProvider() {
    return SplitProvider.SINGLE;
}
```

`IcebergTableCatalog` inherits this default, meaning Iceberg tables execute on a SINGLE node today. For catalog integration, `planScan()` results could be converted into splits for distributed execution.

---

## 9. Recommended Implementation Order

### Priority 1: REST Catalog (lowest effort, broadest coverage)
- Zero new dependencies
- `RESTCatalog` is in `iceberg-core` (already on classpath)
- Covers: Apache Polaris, Apache Gravitino, Tabular, AWS Glue (via REST API), Nessie (via REST API), Snowflake Polaris
- Estimated effort: 1-2 weeks

### Priority 2: Glue Catalog (medium effort, high demand for AWS users)
- Add `software.amazon.awssdk:glue` (checksum already verified)
- `GlueCatalog` is in `iceberg-aws` (already on classpath)
- Direct integration with AWS analytics ecosystem
- Estimated effort: 2-3 weeks (including credential management)

### Priority 3: Nessie Catalog (may be free via REST)
- If REST catalog is implemented, Nessie can use the REST protocol
- Native `NessieCatalog` would need `iceberg-nessie` dependency
- Estimated effort: 0 (via REST) or 2 weeks (native)

### Priority 4: Hive Metastore Catalog (high effort, declining relevance)
- Massive dependency tree, jar hell risk
- Consider recommending REST catalog bridge instead
- Estimated effort: 3-4 weeks (dependency management is the hard part)

---

## 10. Summary of Key Files

| File | Purpose | Lines |
|------|---------|-------|
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/build.gradle` | Dependencies, versions, exclusions | 365 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergCatalogAdapter.java` | Static metadata file reading | 143 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergDataSourcePlugin.java` | Plugin registration | 50 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java` | TableCatalog impl with planScan | 178 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableMetadata.java` | Schema mapping (Iceberg -> ESQL types) | 180 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/S3FileIOFactory.java` | S3FileIO creation with credential management | 134 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/S3Configuration.java` | S3 credential config | 126 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java` | ESQL -> Iceberg filter conversion | 143 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java` | Operator factory (NOT wired, throws) | 261 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/TableCatalog.java` | SPI interface for table catalogs | 77 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/DataSourcePlugin.java` | Plugin SPI with tableCatalogs() | 128 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ExternalSourceFactory.java` | Base factory interface with splitProvider() | 39 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/OperatorFactoryRegistry.java` | Dispatch for source operators | 108 |
| `/Users/oleglvovitch/github/root/elasticsearch/build-tools-internal/version.properties` | iceberg=1.10.1, awsv2sdk=2.31.78 | 50 |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/plugin-metadata/entitlement-policy.yaml` | manage_threads, outbound_network | 2 |
