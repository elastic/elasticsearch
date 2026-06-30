# External Data Source Abstraction

This package provides a pluggable abstraction for accessing external data sources in ESQL queries.

## Architecture Overview

The ESQL data source architecture uses a modular plugin-based design with three main layers:

1. **Storage Layer** - Protocol-agnostic access to storage objects (files, blobs, etc.)
2. **Format Layer** - Data format parsing into ESQL Page batches
3. **Catalog Layer** - Table metadata management for data lake formats (Iceberg, Delta Lake)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ESQL Query Engine                                      │
│                                                                                  │
│  1. Resolve source → get SourceMetadata                                          │
│  2. Plan execution (use schema, statistics)                                      │
│  3. Execute (use FormatReader/TableCatalog for data)                             │
└─────────────────────────────────────────────────────────────────────────────────┘
                                     │
                           ┌─────────┴─────────┐
                           │   SourceMetadata  │  (unified metadata output)
                           │   - schema()      │
                           │   - sourceType()  │
                           │   - location()    │
                           │   - statistics()  │
                           └─────────┬─────────┘
                                     │
           ┌─────────────────────────┼─────────────────────────┐
           │                         │                         │
           ▼                         ▼                         ▼
┌───────────────────┐    ┌────────────────────┐    ┌────────────────────────┐
│   FormatReader    │    │   TableCatalog     │    │   SchemaRegistry       │
│   .metadata()     │    │   .metadata()      │    │   (future: Glue,       │
│                   │    │                    │    │    Hive Metastore)     │
│   Parquet, CSV    │    │   Iceberg, Delta   │    │                        │
└─────────┬─────────┘    └─────────┬──────────┘    └────────────────────────┘
          │                        │
          │                        │ (reuses FormatReader for data)
          │                        ▼
          │                ┌───────────────────┐
          │                │   FormatReader    │  (for actual data reading)
          │                │   .read()         │
          │                └───────────────────┘
          │
          ▼
┌───────────────────┐
│  StorageProvider  │  (byte access layer)
│  S3, HTTP, etc    │
└───────────────────┘
```

## Plugin Architecture

The data source system uses Elasticsearch's plugin mechanism for extensibility. Each data source
capability is provided by a plugin implementing the `DataSourcePlugin` interface.

### DataSourcePlugin Interface

```java
public interface DataSourcePlugin {
    // Storage providers for accessing data (S3, GCS, Azure, HTTP)
    Map<String, StorageProviderFactory> storageProviders(Settings settings);

    // Format readers for parsing data files (Parquet, CSV, ORC)
    Map<String, FormatReaderFactory> formatReaders(Settings settings);

    // Table catalog connectors (Iceberg, Delta Lake)
    Map<String, TableCatalogFactory> tableCatalogs(Settings settings);

    // Custom operator factories for complex datasources
    Map<String, SourceOperatorFactoryProvider> operatorFactories(Settings settings);

    // Filter pushdown support for predicate pushdown optimization
    Map<String, FilterPushdownSupport> filterPushdownSupport(Settings settings);
}
```

### Available Plugins

| Plugin Module | Description | Provides |
|---------------|-------------|----------|
| **Built-in** (esql core) | Basic storage and format support | HTTP/HTTPS, Local filesystem, CSV format |
| **esql-datasource-parquet** | Parquet file format support | Parquet format reader |
| **esql-datasource-s3** | AWS S3 storage support | S3 storage provider (s3://, s3a://, s3n://) |
| **esql-datasource-gcs** | Google Cloud Storage support | GCS storage provider (gs://) |
| **esql-datasource-azure** | Azure Blob Storage support | Azure storage provider (wasbs://, wasb://) |
| **esql-datasource-iceberg** | Apache Iceberg table support | Iceberg table catalog, Arrow vectorized reading |

### Plugin Discovery

Plugins are discovered at startup via Java's ServiceLoader mechanism:

```
src/main/resources/META-INF/services/org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin
```

The `DataSourceModule` collects all plugins and populates the registries:

```java
// In EsqlPlugin.createComponents()
List<DataSourcePlugin> plugins = pluginsService.filterPlugins(DataSourcePlugin.class);
DataSourceModule module = new DataSourceModule(plugins, settings, blockFactory, executor);
```

## Core Interfaces

### StorageProvider

Abstracts access to storage systems (HTTP, S3, local filesystem, etc.):

```java
public interface StorageProvider extends Closeable {
    StorageObject newObject(StoragePath path);
    StorageIterator listObjects(StoragePath directory) throws IOException;
    boolean exists(StoragePath path) throws IOException;
    List<String> supportedSchemes();
}
```

**Built-in Implementations:**
- `HttpStorageProvider` - HTTP/HTTPS access with Range request support
- `LocalStorageProvider` - Local filesystem access (for testing/development)

**Plugin Implementations:**
- `S3StorageProvider` - AWS S3 access (in esql-datasource-s3)
- `GcsStorageProvider` - Google Cloud Storage access (in esql-datasource-gcs)
- `AzureStorageProvider` - Azure Blob Storage access (in esql-datasource-azure)

### StorageObject

Represents a readable object with metadata:

```java
public interface StorageObject {
    InputStream newStream() throws IOException;
    InputStream newStream(long position, long length) throws IOException;
    long length() throws IOException;
    Instant lastModified() throws IOException;
    boolean exists() throws IOException;
    StoragePath path();
}
```

### FormatReader

Parses data formats into ESQL Pages:

```java
public interface FormatReader extends Closeable {
    SourceMetadata metadata(StorageObject object) throws IOException;
    CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException;
    String formatName();
    List<String> fileExtensions();
}
```

**Built-in Implementations:**
- `CsvFormatReader` - CSV/TSV files

**Plugin Implementations:**
- `ParquetFormatReader` - Apache Parquet columnar format (in esql-datasource-parquet)

### TableCatalog

Connects to table catalog systems for data lake formats:

```java
public interface TableCatalog extends Closeable {
    SourceMetadata metadata(String tablePath, Map<String, Object> config) throws IOException;
    List<DataFile> planScan(String tablePath, Map<String, Object> config, List<Object> predicates) throws IOException;
    String catalogType();
    boolean canHandle(String path);
}
```

**Plugin Implementations:**
- `IcebergTableCatalog` - Apache Iceberg tables (in esql-datasource-iceberg)

### SourceMetadata

Unified metadata output from any schema discovery mechanism:

```java
public interface SourceMetadata {
    List<Attribute> schema();
    String sourceType();
    String location();
    Optional<SourceStatistics> statistics();
    Optional<List<String>> partitionColumns();
}
```

## Usage Examples

### Example 1: Reading a CSV file over HTTP

```java
// Storage provider and format reader are automatically selected based on URI
StoragePath path = StoragePath.of("https://example.com/data/sales.csv");

// Get provider from registry (populated by plugins)
StorageProvider provider = storageProviderRegistry.getProvider(path);
FormatReader reader = formatReaderRegistry.getByExtension(path.objectName());

// Create operator factory
ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(
    provider,
    reader,
    path,
    attributes,
    1000  // batch size
);
```

### Example 2: Reading a Parquet file from S3

```java
// Requires esql-datasource-s3 and esql-datasource-parquet plugins
StoragePath path = StoragePath.of("s3://my-bucket/data/sales.parquet");

StorageProvider provider = storageProviderRegistry.getProvider(path);  // S3StorageProvider
FormatReader reader = formatReaderRegistry.getByExtension(".parquet"); // ParquetFormatReader

ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(
    provider,
    reader,
    path,
    attributes,
    5000
);
```

### Example 3: Reading an Iceberg table

```java
// Requires esql-datasource-iceberg plugin
TableCatalog catalog = dataSourceModule.createTableCatalog("iceberg", settings);

// Get table metadata (schema, statistics, partitions)
SourceMetadata metadata = catalog.metadata("s3://bucket/warehouse/db/table", config);

// Plan scan with predicate pushdown
List<DataFile> files = catalog.planScan(tablePath, config, predicates);

// Read data files using ParquetFormatReader
for (DataFile file : files) {
    StorageObject obj = storageProvider.newObject(file.path());
    Iterator<Page> pages = parquetReader.read(obj, columns, batchSize);
    // process pages...
}
```

## Design Principles

1. **Plugin Isolation** - Heavy dependencies (Parquet, Iceberg, AWS SDK) are isolated in separate plugin modules to avoid jar hell
2. **Pure Java SPI** - Core interfaces use only Java stdlib types, ESQL compute types, or other SPI types
3. **Unified Metadata** - All schema sources return `SourceMetadata` for consistency
4. **Standard InputStream** - Uses `java.io.InputStream` for compatibility with existing Elasticsearch code
5. **Range-based reads** - `newStream(position, length)` pattern for efficient columnar format access
6. **Pluggable** - New storage protocols and formats can be added independently via plugins

## Adding New Storage Providers

To add a new storage provider (e.g., GCS, Azure Blob):

1. Create a new plugin module: `x-pack/plugin/esql-datasource-<name>/`
2. Implement `StorageProvider` and `StorageObject` interfaces
3. Create a `DataSourcePlugin` implementation
4. Register via `META-INF/services/org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin`

Example:

```java
public class GcsDataSourcePlugin extends Plugin implements DataSourcePlugin {
    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        return Map.of("gs", s -> new GcsStorageProvider(s));
    }
}
```

## Adding New Format Readers

To add a new format reader (e.g., ORC, Avro):

1. Create a new plugin module or add to existing one
2. Implement `FormatReader` interface
3. Register via `DataSourcePlugin.formatReaders()`

Example:

```java
public class OrcFormatReader implements FormatReader {
    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        InputStream stream = object.newStream();
        // Use ORC library to parse data
        return new OrcBatchIterator(stream, projectedColumns, batchSize);
    }

    @Override
    public String formatName() { return "orc"; }

    @Override
    public List<String> fileExtensions() { return List.of(".orc"); }
}
```

## Module Structure

```
x-pack/plugin/
├── esql/                              # Core ESQL plugin
│   └── src/main/java/.../datasources/
│       ├── spi/                       # SPI interfaces (DataSourcePlugin, FormatReader, etc.)
│       ├── builtin/                   # Built-in plugin (HTTP, Local, CSV)
│       ├── DataSourceModule.java      # Plugin discovery and registry population
│       └── ...
│
├── esql-datasource-parquet/           # Parquet format plugin
│   ├── build.gradle                   # Parquet, Hadoop dependencies
│   └── src/main/java/.../parquet/
│       ├── ParquetDataSourcePlugin.java
│       └── ParquetFormatReader.java
│
├── esql-datasource-s3/                # S3 storage plugin
│   ├── build.gradle                   # AWS SDK dependencies
│   └── src/main/java/.../s3/
│       ├── S3DataSourcePlugin.java
│       └── S3StorageProvider.java
│
├── esql-datasource-gcs/               # GCS storage plugin
│   └── src/main/java/.../gcs/
│       ├── GcsDataSourcePlugin.java
│       └── GcsStorageProvider.java
│
├── esql-datasource-azure/             # Azure Blob Storage plugin
│   └── src/main/java/.../azure/
│       ├── AzureDataSourcePlugin.java
│       └── AzureStorageProvider.java
│
└── esql-datasource-iceberg/           # Iceberg table catalog plugin
    ├── build.gradle                   # Iceberg, Arrow, AWS SDK dependencies
    └── src/main/java/.../iceberg/
        ├── IcebergDataSourcePlugin.java
        ├── IcebergTableCatalog.java
        └── ...
```

## Cancellation and Partial Results for EXTERNAL Queries

A long-running EXTERNAL query can exit in four different ways. Each surfaces results differently so a
client can tell "stop and give me what you have" apart from "abort, throw it all away":

| How the query ends                                    | Response body  | `is_partial` flag |
|-------------------------------------------------------|----------------|-------------------|
| Task cancel / client disconnect                       | none — fails with `TaskCancelledException` | n/a (no body) |
| `DELETE /_query/async/{id}`                           | none — cancels the task, then deletes the saved entry | n/a (no body) |
| `POST /_query/async/{id}/stop`                        | rows already accepted into the response pipeline | `true`        |
| Lenient truncation (e.g. `error_mode: skip_row`)      | rows the source emitted before the truncation | `true`     |

This mirrors the existing `_query/async` + `allow_partial_results` semantics — EXTERNAL is not idiosyncratic.
The defaults (1 s `wait_for_completion_timeout`, the `RestCancellableNodeClient` disconnect hookup) apply
to EXTERNAL queries with no additional setup.

### Why cancel is hard-fail, not partial

Cancel is the ES-wide convention for "abort this work; I don't want its output". A
`TaskCancelledException` surfaces as an HTTP 400 with no result body, which keeps EXTERNAL aligned with
the rest of the ESQL action surface (`EsqlActionTaskIT.assertCancelled`). The same code path runs when
a sync client disconnects mid-query — `RestCancellableNodeClient` cancels the task on disconnect, so the
client cannot accidentally end up holding a half-built result it never asked for. EXTERNAL also enforces
this explicitly at resolution time: `ExternalSourceResolver.throwIfCancelled` reads "cancellation is never
masked as a partial-stats result". Users who want to keep their buffered rows must use STOP, not cancel.

### Why STOP returns buffered rows

`EsqlAsyncStopAction` reaches the coordinator's `ExchangeService#finishSessionEarly`, whose Javadoc
records the explicit contract: "unlike cancel, this does not discard the results". The early-finish
closes the upstream exchange source; the data driver running the EXTERNAL operators sees its exchange
sink closed via `Driver#initializeEarlyTerminationChecker`, throws `DriverEarlyTerminationException`,
and tears down its operators. Pages that already crossed the operator boundary into the response sink
stay in the response; pages still queued in the per-driver `AsyncExternalSourceBuffer` are dropped via
`buffer.finish(true)`. The producer thread reading the external source observes `noMoreInputs` on its
next iteration of the drain loop (it parks on `waitForSpace` when the buffer is full, and is woken via
`notifyNotFull`) and exits without producing more pages.

### A note on the opt-in "always return partial on first cancel"

ClickHouse offers a `partial_result_on_first_cancel` setting (default off) that flips cancel into a
soft early-return. ESQL has explicitly deferred that — STOP is the answer when a user wants their
buffered rows back. Adopting a ClickHouse-style cancel-as-partial mode would be additive and is out of
scope for the current contract.

## Testing

The abstraction includes comprehensive tests:

- `LocalStorageProviderTests` - Tests for local filesystem access
- `HttpStorageProviderTests` - Tests for HTTP/HTTPS access
- `CsvFormatReaderTests` - Tests for CSV parsing
- `DataSourceModuleTests` - Integration tests for plugin discovery
- `ExternalSourceOperatorFactoryTests` - Integration tests

Run tests:

```bash
# Core ESQL tests
./gradlew :x-pack:plugin:esql:test --tests "*LocalStorageProvider*"
./gradlew :x-pack:plugin:esql:test --tests "*DataSourceModule*"

# Plugin-specific tests
./gradlew :x-pack:plugin:esql-datasource-parquet:test
./gradlew :x-pack:plugin:esql-datasource-s3:test
./gradlew :x-pack:plugin:esql-datasource-iceberg:test

# Integration tests
./gradlew :x-pack:plugin:esql-datasource-iceberg:qa:javaRestTest
./gradlew :x-pack:plugin:esql-datasource-parquet:qa:javaRestTest
```

## Future Enhancements

1. **Additional Storage Providers** - HDFS
2. **Additional Format Readers** - ORC, Avro, JSON Lines
3. **Additional Table Catalogs** - Delta Lake, Apache Hudi
4. **Performance Optimizations** - File splitting, parallel reads, caching
5. **Filter Pushdown** - Extended predicate pushdown for all formats
