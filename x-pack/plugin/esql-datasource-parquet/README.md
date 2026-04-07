# ESQL Parquet Data Source Plugin

This plugin provides Apache Parquet format support for ESQL external data sources.

## Overview

The Parquet plugin enables ESQL to read Parquet files from any storage provider (HTTP, S3, local filesystem). Parquet is a columnar storage format optimized for analytics workloads, providing efficient compression and encoding schemes.

## Features

- **Schema Discovery** - Automatically reads schema from Parquet file metadata
- **Column Projection** - Only reads requested columns for efficient I/O
- **Batch Reading** - Configurable batch sizes for memory-efficient processing
- **Direct Page Conversion** - Converts Parquet data directly to ESQL Page format

## Usage

Once installed, the plugin automatically registers the Parquet format reader. ESQL will use it for any file with a `.parquet` extension:

```sql
FROM "https://example.com/data/sales.parquet"
| WHERE region = "EMEA"
| STATS total = SUM(amount) BY product
```

```sql
FROM "s3://my-bucket/warehouse/events.parquet"
| KEEP timestamp, user_id, event_type
| SORT timestamp DESC
| LIMIT 1000
```

## Dependencies

This plugin bundles the following major dependencies:

| Dependency | Version | Purpose |
|------------|---------|---------|
| parquet-hadoop-bundle | 1.16.0 | Parquet file reading and writing |
| hadoop-client-api | 3.4.1 | Hadoop Configuration class (required by Parquet) |
| hadoop-client-runtime | 3.4.1 | Hadoop runtime support |

### Why Hadoop Dependencies?

The Hadoop dependencies are required because:
1. `ParquetFileReader` has method overloads that reference Hadoop `Configuration` in their signatures
2. `ParquetReadOptions.Builder()` constructor creates `HadoopParquetConfiguration` internally
3. `parquet-hadoop-bundle` includes shaded Parquet classes but not Hadoop Configuration

## Architecture

```
┌─────────────────────────────────────────┐
│         ParquetDataSourcePlugin          │
│  implements DataSourcePlugin             │
└─────────────────┬───────────────────────┘
                  │
                  │ provides
                  ▼
┌─────────────────────────────────────────┐
│         ParquetFormatReader              │
│  implements FormatReader                 │
│                                          │
│  - metadata(StorageObject)               │
│  - read(StorageObject, columns, batch)   │
│  - formatName() → "parquet"              │
│  - fileExtensions() → [".parquet"]       │
└─────────────────┬───────────────────────┘
                  │
                  │ uses
                  ▼
┌─────────────────────────────────────────┐
│      ParquetStorageObjectAdapter         │
│                                          │
│  Adapts StorageObject to Parquet's       │
│  InputFile interface for random access   │
└─────────────────────────────────────────┘
```

## Supported Data Types

| Parquet Type | ESQL Type |
|--------------|-----------|
| BOOLEAN | BOOLEAN |
| INT32 | INTEGER |
| INT64 | LONG |
| FLOAT | DOUBLE |
| DOUBLE | DOUBLE |
| BINARY (UTF8) | KEYWORD |
| BINARY | KEYWORD (base64) |
| INT96 (timestamp) | DATETIME |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | DATETIME |
| DECIMAL | DOUBLE |
| LIST | Not yet supported |
| MAP | Not yet supported |
| STRUCT | Not yet supported |

## Building

```bash
./gradlew :x-pack:plugin:esql-datasource-parquet:build
```

## Testing

```bash
# Unit tests
./gradlew :x-pack:plugin:esql-datasource-parquet:test

# Integration tests
./gradlew :x-pack:plugin:esql-datasource-parquet:qa:javaRestTest
```

## Installation

The plugin is bundled with Elasticsearch and enabled by default when the ESQL feature is available.

## License

Elastic License 2.0
