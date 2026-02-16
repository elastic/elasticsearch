# ESQL Iceberg Data Source Plugin

This plugin provides Apache Iceberg table catalog support for ESQL external data sources.

## Overview

The Iceberg plugin enables ESQL to query Apache Iceberg tables stored in S3. Iceberg is an open table format for large analytic datasets that provides ACID transactions, schema evolution, and efficient metadata management.

## Features

- **Iceberg Table Catalog** - Read Iceberg table metadata and schema
- **Schema Discovery** - Automatically resolve schema from Iceberg metadata
- **Partition Pruning** - Skip data files based on partition predicates
- **Predicate Pushdown** - Push filter expressions to Iceberg for efficient scanning
- **Arrow Vectorized Reading** - High-performance columnar data reading via Apache Arrow
- **S3 Integration** - Native S3 file I/O for cloud-native deployments

## Usage

Once installed, the plugin enables querying Iceberg tables via their metadata location:

```sql
FROM "s3://my-bucket/warehouse/db/sales_table"
| WHERE sale_date >= "2024-01-01" AND region = "EMEA"
| STATS total = SUM(amount) BY product
```

The plugin automatically detects Iceberg tables by looking for the `metadata/` directory structure.

### Iceberg Table Structure

```
s3://bucket/warehouse/db/table/
├── data/
│   ├── part-00000.parquet
│   ├── part-00001.parquet
│   └── ...
└── metadata/
    ├── v1.metadata.json
    ├── v2.metadata.json
    ├── snap-*.avro
    └── version-hint.text
```

## Dependencies

This plugin bundles significant dependencies for Iceberg, Arrow, and AWS support:

### Iceberg Core

| Dependency | Version | Purpose |
|------------|---------|---------|
| iceberg-core | 1.x | Iceberg table operations |
| iceberg-aws | 1.x | S3FileIO implementation |
| iceberg-parquet | 1.x | Parquet file support |
| iceberg-arrow | 1.x | Arrow vectorized reading |

### Apache Arrow

| Dependency | Version | Purpose |
|------------|---------|---------|
| arrow-vector | 18.x | Arrow vector types |
| arrow-memory-core | 18.x | Arrow memory management |
| arrow-memory-unsafe | 18.x | Off-heap memory allocation |

### Apache Parquet & Hadoop

| Dependency | Version | Purpose |
|------------|---------|---------|
| parquet-hadoop-bundle | 1.16.0 | Parquet file reading |
| hadoop-client-api | 3.4.1 | Hadoop Configuration |
| hadoop-client-runtime | 3.4.1 | Hadoop runtime |

### AWS SDK

| Dependency | Version | Purpose |
|------------|---------|---------|
| software.amazon.awssdk:s3 | 2.x | S3 client |
| software.amazon.awssdk:sts | 2.x | STS for role assumption |
| software.amazon.awssdk:kms | 2.x | KMS for encryption |

## Architecture

```
┌─────────────────────────────────────────┐
│        IcebergDataSourcePlugin           │
│  implements DataSourcePlugin             │
└─────────────────┬───────────────────────┘
                  │
                  │ provides
                  ▼
┌─────────────────────────────────────────┐
│         IcebergTableCatalog              │
│  implements TableCatalog                 │
│                                          │
│  - metadata(tablePath, config)           │
│  - planScan(tablePath, config, preds)    │
│  - catalogType() → "iceberg"             │
│  - canHandle(path)                       │
└─────────────────┬───────────────────────┘
                  │
                  │ uses
                  ▼
┌─────────────────────────────────────────┐
│        IcebergCatalogAdapter             │
│                                          │
│  Adapts Iceberg's StaticTableOperations  │
│  to work with S3 metadata locations      │
└─────────────────┬───────────────────────┘
                  │
                  │ uses
                  ▼
┌─────────────────────────────────────────┐
│          S3FileIOFactory                 │
│                                          │
│  Creates S3FileIO instances for          │
│  Iceberg table operations                │
└─────────────────────────────────────────┘
```

## Supported Iceberg Features

| Feature | Status |
|---------|--------|
| Schema discovery | Supported |
| Column projection | Supported |
| Partition pruning | Supported |
| Predicate pushdown | Supported |
| Time travel | Not yet supported |
| Schema evolution | Read-only |
| Hidden partitioning | Supported |
| Row-level deletes | Not yet supported |

## Supported Data Types

| Iceberg Type | ESQL Type |
|--------------|-----------|
| boolean | BOOLEAN |
| int | INTEGER |
| long | LONG |
| float | DOUBLE |
| double | DOUBLE |
| decimal | DOUBLE |
| date | DATE |
| time | TIME |
| timestamp | DATETIME |
| timestamptz | DATETIME |
| string | KEYWORD |
| uuid | KEYWORD |
| fixed | KEYWORD |
| binary | KEYWORD (base64) |
| list | Not yet supported |
| map | Not yet supported |
| struct | Not yet supported |

## Predicate Pushdown

The plugin supports pushing filter predicates to Iceberg for partition pruning and data skipping:

```sql
-- Partition pruning: only scans partitions matching the predicate
FROM "s3://bucket/table"
| WHERE sale_date >= "2024-01-01"

-- Data skipping: uses column statistics to skip row groups
FROM "s3://bucket/table"
| WHERE amount > 1000
```

Supported predicates:
- Equality: `=`, `!=`
- Comparison: `<`, `<=`, `>`, `>=`
- NULL checks: `IS NULL`, `IS NOT NULL`
- IN lists: `field IN (value1, value2, ...)`
- Boolean AND/OR combinations

## Configuration

### S3 Configuration

S3 access is configured via environment variables or Elasticsearch settings:

```bash
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
```

### Iceberg-specific Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `esql.iceberg.s3.endpoint` | (AWS default) | Custom S3 endpoint (for MinIO, etc.) |
| `esql.iceberg.s3.path_style_access` | false | Use path-style S3 access |

## Building

```bash
./gradlew :x-pack:plugin:esql-datasource-iceberg:build
```

## Testing

```bash
# Unit tests
./gradlew :x-pack:plugin:esql-datasource-iceberg:test

# Integration tests (requires S3 fixture)
./gradlew :x-pack:plugin:esql-datasource-iceberg:qa:javaRestTest
```

## Test Fixtures

The `qa/` directory contains test fixtures for integration testing:

```
qa/src/javaRestTest/resources/iceberg-fixtures/
├── employees/           # Sample Iceberg table
│   ├── data/
│   │   └── data.parquet
│   └── metadata/
│       ├── v1.metadata.json
│       └── ...
└── standalone/
    └── employees.parquet  # Standalone Parquet file
```

## Security Considerations

- Use IAM roles for S3 access when running on AWS
- Enable S3 bucket encryption for data at rest
- Use VPC endpoints for private S3 access
- Consider using AWS Lake Formation for fine-grained access control

## Installation

The plugin is bundled with Elasticsearch and enabled by default when the ESQL feature is available.

## License

Elastic License 2.0
