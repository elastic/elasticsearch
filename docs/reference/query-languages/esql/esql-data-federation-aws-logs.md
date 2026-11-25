---
navigation_title: "AWS logs in S3"
description: "Register a scalable ES|QL Data Federation dataset over VPC Flow Logs, CloudTrail, or Application Load Balancer logs in Amazon S3."
applies_to:
  stack: preview 9.6+
  serverless: preview
products:
  - id: elasticsearch
---

# Register and query AWS logs with {{esql}} Data Federation

Use this page to register a dataset over [Amazon VPC Flow Logs](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs.html), [AWS CloudTrail](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-events.html) (after converting native `Records` files), or [Application Load Balancer (ALB) access logs](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html) in Amazon S3, and to [query](esql-data-federation-querying.md) a time window without listing the whole bucket.

## Before you begin

- An [S3 data source](esql-data-federation-s3.md).
- Each example includes [`lag`](esql-data-federation-partition-spec.md) because AWS writes objects under a delivery date, not the event time inside the file.

## Choose a layout [layouts]

Pick the layout that matches the objects in your bucket. Prefer Hive-compatible Parquet with an hour folder when you have the choice: listing can stop at the hour, and Parquet reads only the columns the query names. For why Parquet is cheaper to scan, refer to [Amazon S3 sizing](esql-data-federation-sizing-s3.md#format-differences).

| Log | Format | Typical object key |
|---|---|---|
| [VPC Flow Logs](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3-path.html) | Hive Parquet | `AWSLogs/aws-account-id=*/aws-service=vpcflowlogs/aws-region=*/year=*/month=*/day=*/hour=*/*.parquet` |
| [VPC Flow Logs](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3-path.html) | Gzip text | `AWSLogs/<account>/vpcflowlogs/<region>/YYYY/MM/DD/*.log.gz` |
| [CloudTrail](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-examples.html) | Gzip JSON (`Records` array; convert to NDJSON or Parquet) | `AWSLogs/<account>/CloudTrail/<region>/YYYY/MM/DD/*.json.gz` |
| [ALB access logs](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html) | Gzip text | `AWSLogs/<account>/elasticloadbalancing/<region>/YYYY/MM/DD/*_app.*.log.gz` |

A wide multi-account VPC Flow Logs bucket is often more than 100 accounts and tens of regions (about 150 prefixes), hundreds of thousands of files per day, and tens of days of retention. Filter account and region on every query. A time filter alone still has to walk every prefix.

## VPC Flow Logs as Hive Parquet [vpc-hive]

Prefer this layout at scale. Folders use `key=*`, including `hour=*`. The `start` and `end` fields are Unix epoch seconds; map `start` to `@timestamp` and bind `@timestamp` in `partition_spec`. Hyphenated Hive keys need backticks. Do not bind `aws-region=region` unless the Parquet file itself has a `region` column. For the same registration in the `partition_spec` reference, refer to [Hive-compatible VPC Flow Logs](esql-data-federation-partition-spec.md#configure-hive-compatible-vpc-flow-logs).

```console
PUT /_query/dataset/vpc_flow
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs/AWSLogs/aws-account-id=*/aws-service=vpcflowlogs/aws-region=*/year=*/month=*/day=*/hour=*/*.parquet",
  "settings": {
    "partition_spec": "year(@timestamp), month(@timestamp), day(@timestamp), hour(@timestamp), year(end), month(end), day(end), hour(end), lag(@timestamp, 20m), lag(end, 10m)"
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date", "path": "start", "format": "epoch_second" },
      "end": { "type": "date", "format": "epoch_second" }
    }
  }
}
```

```esql
FROM vpc_flow
| WHERE `aws-account-id` == 123456789012
    AND `aws-region` == "eu-west-1"
    AND @timestamp >= "2026-11-18T00:00:00Z"
    AND @timestamp < "2026-11-19T00:00:00Z"
| STATS COUNT(*)
```

Hour folders make a one-hour query list far fewer files than a one-day query. On a bucket with about 510,000 files per day, one hour is on the order of 20,000 files, or about 40,000 when lag keeps the next hour.

## VPC Flow Logs as gzip text [vpc-text]

Default [S3 VPC Flow Logs](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3-path.html) are space-delimited text with no header line. Set `"header_row": false`. With no header, columns are `col0`, `col1`, and so on; for how those names are assigned, refer to [](esql-data-federation-schema.md). In the [default format](https://docs.aws.amazon.com/vpc/latest/userguide/flow-log-records.html#default-format), `start` is Unix epoch seconds at `col10`. Map it to `@timestamp` with `"format": "epoch_second"`. A custom format changes that index.

```console
PUT /_query/dataset/vpc_flow_text
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs/AWSLogs/*/vpcflowlogs/*/*/*/*/*.log.gz",
  "settings": {
    "format": "csv",
    "delimiter": " ",
    "header_row": false,
    "partition_detection": "template",
    "partition_path": "{account}/vpcflowlogs/{region}/{year}/{month}/{day}",
    "partition_spec": "year(@timestamp), month(@timestamp), day(@timestamp), lag(@timestamp, 20m)"
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date", "path": "col10", "format": "epoch_second" }
    }
  }
}
```

Filter account, region, and a closed time range. Put those filters before `STATS` or `LIMIT` so folder pruning runs:

```esql
FROM vpc_flow_text
| WHERE account == 123456789012
    AND region == "eu-west-1"
    AND @timestamp >= "2026-11-18T00:00:00Z"
    AND @timestamp < "2026-11-19T00:00:00Z"
| STATS COUNT(*)
```

All-digit folder names are inferred as numbers, so an account ID with a leading zero is stored without it. Filter on the inferred integer. See [Limitations](#known-limits).

## CloudTrail [cloudtrail]

Convert native CloudTrail files before you register this dataset. Default [CloudTrail trail files](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-examples.html) are gzip JSON documents of the form `{"Records": [ {...}, ... ]}`. That is not [NDJSON](esql-data-federation-file-formats.md). {{esql}} Data Federation does not unwrap `Records`. After you have one JSON event per line (no `Records` wrapper), gzipped, use the example below. Point `resource` at the converted objects, not at the original trail files. Bind [`eventTime`](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-record-contents.html) and include lag: the folder date is still the [delivery date](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-examples.html).

```console
PUT /_query/dataset/cloudtrail_ndjson
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs/AWSLogs/*/CloudTrail/*/*/*/*/*.json.gz",
  "settings": {
    "format": "ndjson",
    "partition_detection": "template",
    "partition_path": "{account}/CloudTrail/{region}/{year}/{month}/{day}",
    "partition_spec": "year(@timestamp), month(@timestamp), day(@timestamp), lag(@timestamp, 20m)"
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date", "path": "eventTime" }
    }
  }
}
```

```esql
FROM cloudtrail_ndjson
| WHERE account == 123456789012
    AND region == "us-east-1"
    AND @timestamp >= "2026-11-18T00:00:00Z"
    AND @timestamp < "2026-11-19T00:00:00Z"
| STATS COUNT(*)
```

If you already store CloudTrail as Hive-compatible Parquet, register it like [VPC Hive Parquet](#vpc-hive): keep the `key=*` folders, bind `eventTime` (or the mapped `@timestamp`) with `lag`, and filter `aws-account-id` and `aws-region`. Do not invent Hive folder names that are not on disk.

:::{dropdown} Native CloudTrail files, Athena, and Firehose
The NDJSON reader splits on line terminators and treats each line as one event. A native CloudTrail file is one JSON value whose events sit in the `Records` array, and pretty-printed files embed newlines inside that value. Do not register native `*.json.gz` trail objects as `ndjson` and expect one row per event.

AWS does not publish a conversion of those trail files to NDJSON. [Amazon Athena](https://docs.aws.amazon.com/athena/latest/ug/cloudtrail-logs.html) reads the native objects with a CloudTrail-specific input format that this reader does not have. [Amazon Data Firehose](https://docs.aws.amazon.com/firehose/latest/dev/record-format-conversion.html) converts newline-delimited JSON to Parquet or ORC, and it rejects a JSON array as input.

A trail can also [send events to CloudWatch Logs](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/send-cloudtrail-events-to-cloudwatch-logs.html), where each event is already one log message rather than a `Records` array. Exporting that stream to NDJSON or Parquet is still a conversion step.

:::

## ALB access logs [alb]

[ALB access logs](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html) are space-separated text with quoted fields, gzipped, under `elasticloadbalancing`. There is no header. Default CSV `mode` is `quoted`, which is what the quoted request and user-agent fields need.

The timestamp is the second field (`col1`), an ISO-8601 value. Folder dates are delivery dates, so include lag.

ALB object names contain `app.`. Network Load Balancer and Classic Load Balancer logs can share the same prefix and put a different value in `col1`. Restrict `resource` to ALB files:

```console
PUT /_query/dataset/alb_access
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs/AWSLogs/*/elasticloadbalancing/*/*/*/*/*_app.*.log.gz",
  "settings": {
    "format": "csv",
    "delimiter": " ",
    "header_row": false,
    "partition_detection": "template",
    "partition_path": "{account}/elasticloadbalancing/{region}/{year}/{month}/{day}",
    "partition_spec": "year(@timestamp), month(@timestamp), day(@timestamp), lag(@timestamp, 20m)"
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date", "path": "col1" }
    }
  }
}
```

```esql
FROM alb_access
| WHERE account == 123456789012
    AND region == "us-east-1"
    AND @timestamp >= "2026-11-18T00:00:00Z"
    AND @timestamp < "2026-11-19T00:00:00Z"
| STATS COUNT(*)
```

## Account for delivery lag [delivery-lag]

Every example on this page includes [`lag`](esql-data-federation-partition-spec.md#account-for-a-delivery-date). Size `duration` from the delay you observe. `lag` only widens listing; the extra folder is then listed in full.

On one production VPC Flow Logs prefix with a daily gzip layout, a one-day query with `lag(@timestamp, 20m)` listed 49 GB of gzip files instead of 24 GB, because the next day's folder was kept even though only its first files could hold matching rows. That roughly 2× listed volume is specific to a daily folder grain. An hourly layout with a 20-minute lag adds one hour folder to 24. After listing, Parquet can still skip nonmatching row groups in that extra folder; gzip text cannot.

{{es}} does not derive a file-name filter from `@timestamp`. A [`LIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-like) on [`_file.name`](esql-data-federation-querying.md#use-metadata-columns) can drop files from the extra folder before they count toward [`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files). Do not filter to a single calendar day in the file name if objects can contain earlier starts: a file named with `20261119T0000Z` can hold flows from November 18, which is why `lag` kept that folder.

That `LIKE` does not reduce the objects visited during listing ([`esql.external.max_listed_objects`](esql-data-federation-cluster-settings.md#esql-external-max-listed-objects)). Listing and kept-file caps are independent: a selective `LIKE` can stay under the kept-file cap while listing still hits `max_listed_objects`.

## Size queries against listing limits [scale]

[`esql.external.max_discovered_files`](esql-data-federation-cluster-settings.md#esql-external-max-discovered-files) caps files kept after `_file.*` filters. Its default is 15% of node heap divided by about 1180 bytes per file, minimum 25,000 and maximum 10,000,000. On a 4 GB heap that default is about 546,000 kept files. [`esql.external.max_listed_objects`](esql-data-federation-cluster-settings.md#esql-external-max-listed-objects) caps objects visited while listing. A `_file.*` LIKE does not shrink the listed-objects count.

On measured Hive VPC Flow Logs (up to four partition keys per file), a one-day listing that keeps 360,000 to 517,000 files after account, region, time, and lag pruning fits on any heap of 4 GB or more. At that default 4 GB cap, the 4-key Hive listing used about 18% of heap, which is under the 60% request breaker. Gzip text and 1-key Hive are charged closer to the 1180-byte planning budget per file, so the file-count cap trips before the breaker.

Do not raise `max_discovered_files` to cover an unfiltered bucket. Filter account and region, prefer Hive Parquet with hour folders, and keep the query's time range closed. A daily-folder layout at about 500,000 files per day can keep close to a million files for a one-day query once lag adds the next folder, which exceeds the default 4 GB cap. A one-day Hive listing of about 500,000 files after pruning fits on a node with at least 4 GB of heap.

For heap and cores once the files are listed, refer to [Amazon S3 sizing](esql-data-federation-sizing-s3.md). For request and transfer charges, including the extra objects `lag` keeps, refer to [Amazon S3 costs](esql-data-federation-costs-s3.md).

## Limitations [known-limits]

- Lag keeps the next day or hour folder in full. On a daily gzip layout, a one-day window can list roughly twice the in-window bytes. On an hourly layout, 20-minute lag adds one folder. {{es}} does not auto-narrow that folder from `@timestamp`. A `_file.name` LIKE can drop files whose names fall on the next calendar day while still containing in-window rows. See [Account for delivery lag](#delivery-lag).
- Account IDs lose leading zeros. All-digit folder names are inferred as integers, so `AWSLogs/012345678901/...` becomes `account` / `aws-account-id` = `12345678901`. Declaring that partition column as `keyword` is rejected because partition columns are path-derived. Filter on the inferred integer so pruning still runs. After that filter, format a 12-digit string if you need the padded ID for display or a join. Declaring a type or format for a partition column is not supported.
- `_file.*` LIKE does not prune `max_listed_objects`. It does drop non-matching files before `max_discovered_files`.

For the `partition_spec` syntax these examples use, refer to [Skip folders with file column filters](esql-data-federation-partition-spec.md).

## Related pages [related]

* [Skip folders with file column filters](esql-data-federation-partition-spec.md): `partition_spec`, `lag`, and `lead`
* [Resource patterns](esql-data-federation-patterns.md): glob syntax and template placeholders
* [File formats](esql-data-federation-file-formats.md): Parquet, CSV, and NDJSON
* [Amazon S3 sizing](esql-data-federation-sizing-s3.md): heap, cores, and file layout
* [Amazon S3 costs](esql-data-federation-costs-s3.md): LIST, GET, and transfer charges
* [Cluster settings](esql-data-federation-cluster-settings.md): `max_discovered_files` and `max_listed_objects`
* [VPC Flow Logs](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs.html): records, default fields, and [S3 prefixes](https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3-path.html)
* [CloudTrail log files](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-examples.html): `Records` array and [event fields](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-record-contents.html)
* [ALB access logs](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html): file names and field list
