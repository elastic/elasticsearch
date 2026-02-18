# ESQL S3 Data Source Plugin

This plugin provides AWS S3 storage support for ESQL external data sources.

## Overview

The S3 plugin enables ESQL to read data files directly from Amazon S3 buckets. It supports multiple S3 URI schemes and integrates with AWS authentication mechanisms.

## Features

- **S3 Storage Access** - Read files directly from S3 buckets
- **Multiple URI Schemes** - Supports `s3://`, `s3a://`, and `s3n://` schemes
- **Range Requests** - Efficient partial file reads for columnar formats
- **AWS Authentication** - Supports IAM roles, access keys, and instance profiles

## Usage

Once installed, the plugin automatically registers the S3 storage provider. Use S3 URIs in ESQL queries:

```sql
FROM "s3://my-bucket/data/sales.parquet"
| WHERE region = "EMEA"
| STATS total = SUM(amount) BY product
```

```sql
FROM "s3a://analytics-bucket/events/2024/01/events.csv"
| KEEP timestamp, user_id, event_type
| SORT timestamp DESC
```

### URI Schemes

| Scheme | Description |
|--------|-------------|
| `s3://` | Standard S3 URI scheme |
| `s3a://` | Hadoop S3A connector scheme (compatible) |
| `s3n://` | Legacy Hadoop S3 native scheme (compatible) |

## Configuration

S3 access is configured via Elasticsearch settings or environment variables:

### Environment Variables

```bash
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
```

### IAM Roles

When running on EC2 or EKS, the plugin automatically uses IAM roles attached to the instance or pod.

## Dependencies

This plugin bundles the AWS SDK v2:

| Dependency | Version | Purpose |
|------------|---------|---------|
| software.amazon.awssdk:s3 | 2.x | S3 client |
| software.amazon.awssdk:auth | 2.x | AWS authentication |
| software.amazon.awssdk:sts | 2.x | STS for role assumption |
| software.amazon.awssdk:apache-client | 2.x | HTTP client |
| org.apache.httpcomponents:httpclient | 4.x | HTTP transport |

## Architecture

```
┌─────────────────────────────────────────┐
│          S3DataSourcePlugin              │
│  implements DataSourcePlugin             │
└─────────────────┬───────────────────────┘
                  │
                  │ provides
                  ▼
┌─────────────────────────────────────────┐
│          S3StorageProvider               │
│  implements StorageProvider              │
│                                          │
│  - newObject(StoragePath)                │
│  - listObjects(StoragePath)              │
│  - exists(StoragePath)                   │
│  - supportedSchemes() → [s3, s3a, s3n]   │
└─────────────────┬───────────────────────┘
                  │
                  │ creates
                  ▼
┌─────────────────────────────────────────┐
│           S3StorageObject                │
│  implements StorageObject                │
│                                          │
│  - newStream()                           │
│  - newStream(position, length)           │
│  - length()                              │
│  - lastModified()                        │
│  - exists()                              │
└─────────────────────────────────────────┘
```

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `newObject()` | Create a reference to an S3 object |
| `newStream()` | Read entire object as InputStream |
| `newStream(pos, len)` | Read byte range (for columnar formats) |
| `length()` | Get object size via HEAD request |
| `lastModified()` | Get object modification time |
| `exists()` | Check if object exists |
| `listObjects()` | List objects with prefix |

## Building

```bash
./gradlew :x-pack:plugin:esql-datasource-s3:build
```

## Testing

```bash
# Unit tests
./gradlew :x-pack:plugin:esql-datasource-s3:test
```

## Security Considerations

- Store AWS credentials securely using IAM roles or Elasticsearch keystore
- Use VPC endpoints for private S3 access
- Enable S3 bucket policies to restrict access
- Consider using S3 Access Points for fine-grained access control

## Installation

The plugin is bundled with Elasticsearch and enabled by default when the ESQL feature is available.

## License

Elastic License 2.0
