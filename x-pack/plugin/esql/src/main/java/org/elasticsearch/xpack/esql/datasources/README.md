# External Data Source Abstraction

This package provides a pluggable abstraction for accessing external data sources in ESQL queries.

## Architecture Overview

The abstraction consists of two main layers:

1. **Storage Layer** - Protocol-agnostic access to storage objects (files, blobs, etc.)
2. **Format Layer** - Data format parsing into ESQL Page batches

```
┌─────────────────────────────────────────┐
│         ESQL Query Pipeline             │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│   ExternalSourceOperatorFactory         │
│   (Combines Storage + Format)           │
└─────────┬──────────────────┬────────────┘
          │                  │
┌─────────▼─────────┐ ┌─────▼──────────┐
│ StorageProvider   │ │ FormatReader   │
│ (HTTP, S3, Local) │ │ (CSV, Parquet) │
└───────────────────┘ └────────────────┘
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

**Implementations:**
- `HttpStorageProvider` - HTTP/HTTPS access with Range request support
- `S3StorageProvider` - AWS S3 access
- `LocalStorageProvider` - Local filesystem access (for testing/development)

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
    List<Attribute> getSchema(StorageObject object) throws IOException;
    CloseableIterator<Page> read(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize
    ) throws IOException;
    String formatName();
    List<String> fileExtensions();
}
```

**Implementations:**
- `CsvFormatReader` - CSV/TSV files
- `ParquetFormatReader` - Apache Parquet columnar format

## Usage Examples

### Example 1: Reading a CSV file over HTTP

```java
// Create storage provider
HttpConfiguration config = new HttpConfiguration(
    Duration.ofSeconds(30),  // connect timeout
    Duration.ofMinutes(5),   // request timeout
    true,                    // follow redirects
    Map.of()                 // custom headers
);
HttpStorageProvider storageProvider = new HttpStorageProvider(config, executor);

// Create format reader
CsvFormatReader formatReader = new CsvFormatReader();

// Create storage path
StoragePath path = StoragePath.of("https://example.com/data/sales.csv");

// Define schema
List<Attribute> attributes = List.of(
    new FieldAttribute(Source.EMPTY, "product", DataType.KEYWORD),
    new FieldAttribute(Source.EMPTY, "amount", DataType.DOUBLE),
    new FieldAttribute(Source.EMPTY, "region", DataType.KEYWORD)
);

// Create operator factory
ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(
    storageProvider,
    formatReader,
    path,
    attributes,
    1000  // batch size
);

// Use in LocalExecutionPlanner
SourceOperator operator = factory.get(driverContext);
```

### Example 2: Reading a Parquet file from local filesystem

```java
// Create storage provider
LocalStorageProvider storageProvider = new LocalStorageProvider();

// Create format reader
ParquetFormatReader formatReader = new ParquetFormatReader();

// Create storage path
StoragePath path = StoragePath.of("file:///data/warehouse/sales.parquet");

// Define schema (inferred from Parquet metadata)
StorageObject object = storageProvider.newObject(path);
List<Attribute> attributes = formatReader.getSchema(object);

// Create operator factory
ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(
    storageProvider,
    formatReader,
    path,
    attributes,
    5000  // batch size
);
```

### Example 3: Using Registries for Pluggable Discovery

```java
// Setup registries (typically done at initialization)
StorageProviderRegistry storageRegistry = new StorageProviderRegistry();
storageRegistry.register("http", path -> new HttpStorageProvider(httpConfig, executor));
storageRegistry.register("https", path -> new HttpStorageProvider(httpConfig, executor));
storageRegistry.register("s3", path -> new S3StorageProvider(s3Config));
storageRegistry.register("file", path -> new LocalStorageProvider());

FormatReaderRegistry formatRegistry = new FormatReaderRegistry();
formatRegistry.register(new CsvFormatReader());
formatRegistry.register(new ParquetFormatReader());

// Use in query planning
String uri = "https://example.com/data/sales.csv";
StoragePath path = StoragePath.of(uri);

// Automatic provider selection based on scheme
StorageProvider provider = storageRegistry.getProvider(path);

// Automatic format selection based on file extension
FormatReader reader = formatRegistry.getByExtension(path.objectName());

// Create operator
ExternalSourceOperatorFactory factory = new ExternalSourceOperatorFactory(
    provider,
    reader,
    path,
    attributes,
    1000
);
```

## Integration with LocalExecutionPlanner

The `LocalExecutionPlanner` includes a method `planExternalSourceGeneric()` that demonstrates
how to integrate the generic abstraction:

```java
private PhysicalOperation planExternalSourceGeneric(
    ExternalSourceExec externalSource,
    StorageProvider storageProvider,
    FormatReader formatReader,
    LocalExecutionPlannerContext context
) {
    // Parse the storage path
    StoragePath path = StoragePath.of(externalSource.sourcePath());
    
    // Determine page size based on estimated row size
    int pageSize = calculatePageSize(externalSource.estimatedRowSize());
    
    // Create the operator factory
    SourceOperator.SourceOperatorFactory factory = new ExternalSourceOperatorFactory(
        storageProvider,
        formatReader,
        path,
        externalSource.output(),
        pageSize
    );
    
    return PhysicalOperation.fromSource(factory, layout.build());
}
```

## Design Principles

1. **Pure Java APIs** - No dependencies on Iceberg, Hadoop, or other external frameworks in core interfaces
2. **Standard InputStream** - Uses `java.io.InputStream` for compatibility with existing Elasticsearch code
3. **Range-based reads** - `newStream(position, length)` pattern for efficient columnar format access
4. **Simple cases first** - CSV over HTTP works without pulling in datalake dependencies
5. **Pluggable** - New storage protocols and formats can be added independently

## Adding New Storage Providers

To add a new storage provider (e.g., GCS, Azure Blob):

1. Implement `StorageProvider` interface
2. Implement `StorageObject` interface
3. Register with `StorageProviderRegistry`

Example:

```java
public class GcsStorageProvider implements StorageProvider {
    private final Storage gcsClient;
    
    @Override
    public StorageObject newObject(StoragePath path) {
        String bucket = path.host();
        String blobName = path.path();
        return new GcsStorageObject(gcsClient, bucket, blobName, path);
    }
    
    @Override
    public List<String> supportedSchemes() {
        return List.of("gs");
    }
    
    // ... other methods
}

// Register
storageRegistry.register("gs", path -> new GcsStorageProvider(gcsConfig));
```

## Adding New Format Readers

To add a new format reader (e.g., ORC, Avro):

1. Implement `FormatReader` interface
2. Register with `FormatReaderRegistry`

Example:

```java
public class OrcFormatReader implements FormatReader {
    @Override
    public CloseableIterator<Page> read(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize
    ) throws IOException {
        InputStream stream = object.newStream();
        // Use ORC library to parse data
        // Convert to ESQL Pages
        return new OrcBatchIterator(stream, projectedColumns, batchSize);
    }
    
    @Override
    public String formatName() {
        return "orc";
    }
    
    @Override
    public List<String> fileExtensions() {
        return List.of(".orc");
    }
}

// Register
formatRegistry.register(new OrcFormatReader());
```

## Testing

The abstraction includes comprehensive tests:

- `LocalStorageProviderTests` - Tests for local filesystem access
- `HttpStorageProviderTests` - Tests for HTTP/HTTPS access
- `ExternalSourceOperatorFactoryTests` - Integration tests

Run tests:

```bash
./gradlew :x-pack:plugin:esql:test --tests "*LocalStorageProvider*"
./gradlew :x-pack:plugin:esql:test --tests "*ExternalSourceOperatorFactory*"
```

## Future Enhancements

1. **Datalake Support** - Higher-level adapters for Iceberg, Delta Lake (already implemented)
2. **Additional Protocols** - GCS, Azure Blob, HDFS
3. **Additional Formats** - ORC, Avro, JSON Lines
4. **Performance Optimizations** - File splitting, parallel reads, caching
5. **Filter Pushdown** - Push predicates to storage/format layer
