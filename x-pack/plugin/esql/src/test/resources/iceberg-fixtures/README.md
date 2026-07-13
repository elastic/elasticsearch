# Iceberg Test Fixtures

This directory contains pre-built Iceberg metadata and Parquet files used for testing.

## Purpose

These fixtures serve files directly through the S3HttpFixture, eliminating the need for manual test data setup via `addBlobToFixture()` calls. Files placed here are automatically loaded into the fixture's blob storage when tests run.

## Directory Structure

Files in this directory are mapped to S3 paths preserving their structure:

```
iceberg-fixtures/
├── README.md                           # This file
├── db/                                 # Database directory
│   └── table/                          # Table directory
│       ├── metadata/                   # Iceberg metadata files
│       │   ├── v1.metadata.json        # Table metadata version 1
│       │   └── version-hint.text       # Current version pointer
│       └── data/                       # Parquet data files
│           └── part-00000.parquet      # Data file
└── standalone/                         # Standalone Parquet files (no Iceberg metadata)
    └── simple.parquet                  # Simple Parquet file for direct reading
```

## S3 Path Mapping

Files are automatically mapped to S3 paths:

- `iceberg-fixtures/db/table/metadata/v1.metadata.json` → `s3://iceberg-test/warehouse/db/table/metadata/v1.metadata.json`
- `iceberg-fixtures/standalone/simple.parquet` → `s3://iceberg-test/warehouse/standalone/simple.parquet`

## Usage in Tests

### Automatic Loading

All files in this directory are automatically loaded when tests extending `AbstractS3HttpFixtureTest` start:

```java
public class MyIcebergTest extends AbstractS3HttpFixtureTest {
    
    public void testReadIcebergTable() throws Exception {
        // Files from iceberg-fixtures/ are already loaded!
        Catalog catalog = createCatalog();
        TableIdentifier tableId = TableIdentifier.of("db", "table");
        Table table = catalog.loadTable(tableId);
        
        // Use the table...
    }
}
```

### Manual Addition (Still Supported)

You can still add files programmatically if needed:

```java
public void testWithDynamicData() {
    // Add a file at runtime
    addBlobToFixture("dynamic/test.parquet", parquetBytes);
    
    // Use it...
}
```

## Fixture Categories

### 1. Parquet Format Compatibility

Test different Parquet versions and encodings:

- `parquet-v1/` - Parquet format version 1 files
- `parquet-v2/` - Parquet format version 2 files
- `dictionary-encoded/` - Dictionary-encoded columns
- `plain-encoded/` - Plain-encoded columns

### 2. Edge Cases

Test boundary conditions and special cases:

- `edge-cases/all-nulls.parquet` - File with all null values
- `edge-cases/empty-columns.parquet` - File with empty columns
- `edge-cases/large-strings.parquet` - File with large string values

### 3. Iceberg Tables

Complete Iceberg table structures with metadata:

- `db/table/` - Full Iceberg table with metadata and data files

### 4. Regression Tests

Specific files that reproduce known bugs or issues.

## Generating Fixtures

### Using Test Data Generators

The `org.elasticsearch.xpack.esql.iceberg.testdata.generation` package provides utilities for generating test fixtures.

**Note**: These utilities use Parquet's Hadoop-based APIs (`parquet-hadoop`) for writing files. While they import
Hadoop classes, they use `LocalInputFile`/`LocalOutputFile` which bypass Hadoop's FileSystem and work directly with
`java.nio.file.Path`. The `Configuration` class is created with `Configuration(false)` to avoid loading Hadoop
resources and triggering security manager issues.

```java
// Generate a simple Parquet file
ParquetWriterUtil.writeParquet(
    schema,
    rows,
    outputFile,
    ParquetWriterConfig.defaults()
);

// Generate Iceberg metadata
IcebergMetadataGenerator.generateMetadata(
    tableName,
    parquetFile,
    outputDir,
    IcebergMetadataConfig.defaults()
);
```

### Using External Tools

You can also generate fixtures using external tools like Apache Spark or Iceberg CLI:

```python
# Using PySpark
df = spark.createDataFrame([
    (1, "Alice", 30),
    (2, "Bob", 25)
], ["id", "name", "age"])

df.write.format("parquet").save("simple.parquet")
```

### Regenerating All Fixtures

To regenerate all fixtures, run the generator tests:

```bash
./gradlew :x-pack:plugin:esql:test --tests "*IcebergMetadataGeneratorTests"
```

## Size Guidelines

- Keep individual files under 1MB when possible
- Total fixture size should stay under 10MB
- Use compression for text-based metadata files
- Prefer minimal schemas (3-5 columns) unless testing specific scenarios

## Best Practices

1. **Minimal Data**: Include only the minimum data needed to test the scenario
2. **Clear Naming**: Use descriptive names that indicate what the fixture tests
3. **Documentation**: Add comments in test code explaining why each fixture exists
4. **Regeneration**: Document how to regenerate fixtures if schema changes
5. **Version Control**: Commit fixtures as binary files (they're small and stable)

## Troubleshooting

### Fixtures Not Loading

If fixtures aren't loading, check:

1. Files are in the correct directory: `src/test/resources/iceberg-fixtures/`
2. Test class extends `AbstractS3HttpFixtureTest`
3. Check logs for "Loaded fixtures from iceberg-fixtures directory"

### Path Mapping Issues

If S3 paths don't match expectations:

1. Verify file paths use forward slashes (/)
2. Check that paths are relative to `iceberg-fixtures/` root
3. Use `printRequestSummary()` to see actual S3 requests

### File Not Found in Tests

If tests can't find expected files:

1. Verify the S3 path matches the fixture path
2. Check bucket name is `iceberg-test` and warehouse is `warehouse`
3. Use `s3Fixture.getHandler().blobs()` to inspect loaded files

## Related Documentation

- [S3 Request Logging](../../../../../../../docs/s3-request-logging.md) - Debugging S3 operations
- [Iceberg Testing Strategy](../../../../../../../.cursor/plans/iceberg_testing_strategy_decision.md) - Overall testing approach
- [Test Data Generation](../testdata/generation/) - Programmatic fixture generation
