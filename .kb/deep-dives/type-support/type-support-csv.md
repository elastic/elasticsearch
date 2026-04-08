# CSV/TSV Type Support in ES|QL External Data Sources

CSV files in ES|QL currently use a **header-based schema** — types must be declared in the first line as `name:type` pairs (e.g., `age:integer,name:keyword`). There is no content-based type inference. The schema line is parsed by `parseSchema()` (`CsvFormatReader.java:412-433`), which splits on `:` and passes the uppercased type name to `parseDataType()` (`CsvFormatReader.java:435-447`). The CSV reader supports 8 type aliases mapping to 7 distinct ESQL DataTypes.

**Plain CSV files crash.** If the header line contains plain field names without `:type` annotations (which is how 99.9% of CSV files in the wild look), the reader throws `ParsingException: Invalid CSV schema format`. This is the single biggest usability gap — see [CSV-0](#csv-0-plain-csv-headers-crash-instead-of-auto-inferring-types).

### How we got here

RFC 4180 defines the header line as field names only — no type annotations. The `name:type` syntax is **ES|QL's own convention** with no prior art in any standard or tool. We surveyed existing approaches:

- **W3C CSVW** and **Frictionless Data Table Schema** use sidecar JSON metadata files — effectively no real-world adoption outside UK government and a few scientific niches.
- **CSV Schema** (UK National Archives) uses a separate `.csvs` validation file — digital preservation only.
- **DuckDB, Spark, Pandas** — the industry standard — use **auto-inference** (sniff N rows, guess types) with optional **query-time schema override**. No types in the file itself.

Nobody puts type information in CSV headers. The dominant pattern is auto-inference, which is what users expect.

### Why the remaining bugs matter

For the `name:type` path (which should remain supported as an explicit override), two type coercion bugs reject common real-world values: Python/Pandas output `True`/`False`, Excel outputs `TRUE`/`FALSE`, PostgreSQL exports zone-less timestamps, Excel exports date-only values. Rejecting these means rejecting most CSV files even when the user has gone through the effort of adding type annotations.

The complete per-type breakdown is in [Appendix](#appendix-per-type-breakdown). The summary and issue tables below focus on what's broken.

## Summary

| Status | Count | Types |
|---|---|---|
| 🔴 No auto-inference | 1 | Plain CSV headers (no `:type`) crash instead of inferring types |
| 🟢 Fully works | 6 | INTEGER, LONG, DOUBLE, KEYWORD, TEXT, NULL |
| 🔴 Crash | 2 | BOOLEAN (case variants), DATETIME (date-only / zone-less) |
| ⬛ Missing alias | 20 | FLOAT, SHORT, BYTE, HALF_FLOAT, SCALED_FLOAT, IP, UNSIGNED_LONG, VERSION, DATE_NANOS, GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE, DATE_PERIOD, TIME_DURATION, OBJECT, COUNTER_*, AGGREGATE_METRIC_DOUBLE |

### MVP

| Status | Type | Prevalence | Description | Issue |
|---|---|---|---|---|
| 🔴 | Plain CSV headers | **Blocks all normal CSV files.** 99.9% of CSVs have no type annotations. | Plain CSV headers crash instead of auto-inferring types | [CSV-0](#csv-0-plain-csv-headers-crash-instead-of-auto-inferring-types) |
| 🔴 | BOOLEAN case variants | Very high | Case-sensitive boolean parsing rejects common variants | [CSV-1](#csv-1-case-sensitive-boolean-parsing-rejects-common-variants) |
| 🔴 | DATETIME date-only / zone-less | Very high | Date-only and zone-less timestamps crash the query | [CSV-2](#csv-2-date-only-and-zone-less-timestamps-crash-the-query) |
| ⬛ | FLOAT, SHORT, BYTE, HALF_FLOAT, SCALED_FLOAT | High — standard type names in every language | Missing trivial numeric aliases | [CSV-3](#csv-3-missing-trivial-numeric-aliases) |

### Post-MVP

| Status | Type | Prevalence | Description | Issue |
|---|---|---|---|---|
| ⬛ | IP, UNSIGNED_LONG, VERSION, DATE_NANOS | High (IP), medium (others) | Missing common typed column support | [CSV-4](#csv-4-missing-common-typed-column-support) |
| ⬛ | GEO_POINT, GEO_SHAPE, spatial, temporal, TSDB | Medium (geo), very low (rest) | Missing spatial and niche types | [CSV-5](#csv-5-missing-spatial-and-niche-types) |

---

## Issue Details

### CSV-0: Plain CSV headers crash instead of auto-inferring types

**Estimate:** 1.5w
**Priority:** MVP — 🔴 blocks all normal CSV files
**Affected types:** All — the reader cannot process any CSV file without `name:type` headers

#### Prevalence

**Blocks virtually every real-world CSV file.** RFC 4180 defines the header line as field names only — no type annotations. No standard, tool, or convention puts type information in CSV headers. The `name:type` syntax is ES|QL's own invention. Every CSV file produced by Excel, PostgreSQL, Pandas, databases, APIs, and log pipelines has plain headers like `name,age,created_at,active`. All of these crash immediately.

#### The industry standard: auto-inference

Every major tool that reads CSV files uses automatic type inference by sampling data rows:

| Tool | Sample size | Candidate types (in priority order) | Fallback |
|---|---|---|---|
| **DuckDB** | 2048 rows | NULL, BOOLEAN, BIGINT, DOUBLE, TIME, DATE, TIMESTAMP, VARCHAR | VARCHAR |
| **Pandas** | Entire file | int64, float64, bool, datetime64, object (string) | object |
| **Spark** (`inferSchema=true`) | Entire file | IntegerType, LongType, DoubleType, BooleanType, TimestampType, StringType | StringType |
| **NDJSON reader** (our own) | 100 rows | INTEGER, LONG, DOUBLE, BOOLEAN, DATETIME, KEYWORD | KEYWORD |

DuckDB achieves 90-99% accuracy on the [Pollock CSV benchmark](https://motherduck.com/blog/taming-wild-csvs-with-duckdb-data-engineering/) with auto-detection alone. All tools fall back to string/VARCHAR/KEYWORD when a column's values don't match any typed candidate.

Our own NDJSON reader already implements this pattern — `NdJsonSchemaInferrer` samples the first 100 rows, examines JSON tokens, tries `Instant.parse()` for datetime detection, and resolves type conflicts via a priority system (`DATETIME > KEYWORD > DOUBLE > LONG > INTEGER`). The CSV reader has no equivalent.

#### User impact

When a user runs `EXTERNAL "s3://bucket/data.csv"` on any normal CSV file (plain `name,age,active` header without `:type` annotations), the query fails immediately with `ParsingException: Invalid CSV schema format: [name]. Expected 'name:type'`. No data is returned. There is no workaround other than editing the CSV file to add type annotations to every column — which defeats the purpose of querying external data as-is.

This is more impactful than any other CSV bug. CSV-1 and CSV-2 affect specific types; CSV-0 prevents using CSV files entirely.

**Testing gap:** No test exercises plain headers. Every test in `CsvFormatReaderTests` uses `name:type` headers.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

public class ReproCsv0Test extends ESTestCase {

    // Plain header "name,age,active" — should infer types, not crash
    public void testPlainHeaderInfersTypes() throws Exception {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
        String csv = "name,age,active\nAlice,30,true\nBob,25,false\n";
        var reader = new CsvFormatReader(blockFactory);
        // Should not throw — should infer name=KEYWORD, age=INTEGER, active=BOOLEAN
        SourceMetadata meta = reader.metadata(createStorageObject(csv));
        assertEquals("CSV-0: should detect 3 columns", 3, meta.schema().size());
        assertEquals("CSV-0: 'name' should be KEYWORD",
            DataType.KEYWORD, meta.schema().get(0).dataType());
    }

    private StorageObject createStorageObject(String csvContent) {
        byte[] bytes = csvContent.getBytes(StandardCharsets.UTF_8);
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(bytes); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(bytes, (int) pos, (int) Math.min(len, bytes.length - pos));
            }
            @Override public long length() { return bytes.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.csv"); }
        };
    }
}
```

**Fails with:** `ParsingException: Invalid CSV schema format: [name]. Expected 'name:type'` at `CsvFormatReader.java:420`.

#### Why it happens

`parseSchema()` at `CsvFormatReader.java:412-433` unconditionally splits each header column on `:` and requires exactly two parts:

```java
// CsvFormatReader.java:418-421 — no fallback for plain headers
String[] parts = trimmedColumn.split(":");
if (parts.length != 2) {
    throw new ParsingException("Invalid CSV schema format: [{}]. Expected 'name:type'", column);
}
```

There is no detection of whether the header contains type annotations. No fallback to auto-inference. No way to specify types at query time.

#### Fix

Two parts, which can be delivered incrementally:

**Part 1 — Auto-inference for plain headers (core fix):**

When `parseSchema()` detects that headers don't contain `:` separators, fall back to a `CsvSchemaInferrer` that samples the first N rows (100-2048, configurable) and infers types. The inference loop for each column:

1. Try `Booleans.parseBoolean(value.toLowerCase())` → BOOLEAN
2. Try `Integer.parseInt(value)` → INTEGER
3. Try `Long.parseLong(value)` → LONG
4. Try `Double.parseDouble(value)` → DOUBLE
5. Try `Instant.parse(value)` → DATETIME
6. Try `LocalDateTime.parse(value)` / `LocalDate.parse(value)` → DATETIME
7. Fallback → KEYWORD

Use the same priority-based conflict resolution as NDJSON: when a column has mixed types across sample rows, widen to the higher-priority type. DuckDB's candidate list (`NULL, BOOLEAN, BIGINT, DOUBLE, TIME, DATE, TIMESTAMP, VARCHAR`) is a good reference.

This mirrors the existing `NdJsonSchemaInferrer` pattern — same approach, adapted for CSV's row-based text format instead of JSON tokens. ~150-200 lines for the inferrer class, ~20 lines to wire it into `CsvFormatReader`.

**Part 2 — Query-time schema override (optional, matches DuckDB/Spark):**

Allow users to specify column types in the EXTERNAL source config, e.g.:
```
EXTERNAL "s3://bucket/data.csv" WITH (columns = "price:double, ip:ip, active:boolean")
```

This is the standard escape hatch when auto-inference gets it wrong — every major tool supports it (DuckDB's `columns={}`, Spark's `.schema()`, Pandas' `dtype={}`). This can be delivered as a separate follow-up.

---

### CSV-1: Case-sensitive boolean parsing rejects common variants

**Estimate:** 0.5d
**Priority:** MVP — 🔴 query crashes, no data returned
**Affected types:** BOOLEAN

#### Prevalence

Extremely common. Python's `csv.writer` produces `True`/`False`. Pandas `to_csv()` produces `True`/`False`. Excel exports `TRUE`/`FALSE`. Every major data tool produces capitalized booleans. Only the ES|QL test framework itself consistently uses lowercase. Any user querying a CSV file from a non-Elasticsearch source will hit this crash on the first boolean column.

#### Why it's wrong

The reader only accepts exact lowercase `true`/`false`, but virtually every tool that produces CSV files uses capitalized booleans: Python's `csv.writer` outputs `True`/`False`, Pandas `to_csv()` outputs `True`/`False`, Excel outputs `TRUE`/`FALSE`, R outputs `TRUE`/`FALSE`. These four tools cover the vast majority of CSV files in the wild. The reader rejects all of them.

#### User impact

When a boolean column contains any case variant other than exact lowercase `true`/`false` (e.g., `True`, `TRUE`, `FALSE`), the query fails immediately with `EsqlIllegalArgumentException: Failed to parse CSV value [True] as [BOOLEAN]`. The entire query aborts — not just the row with the bad value. The user cannot work around this without pre-processing the file to lowercase all boolean values. Since virtually every non-Elasticsearch tool produces capitalized booleans, this affects most real-world CSV files with boolean columns.

**Testing gap:** `CsvFormatReaderTests` only tests lowercase `true`/`false`. No test for `True`, `TRUE`, `False`, `FALSE` — the case variants that real-world CSV tools produce.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

public class ReproCsv1Test extends ESTestCase {

    // "True" (capitalized) crashes with EsqlIllegalArgumentException
    public void testBooleanCapitalizedTrue() throws Exception {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
        String csv = "active:boolean\nTrue\n";
        var reader = new CsvFormatReader(blockFactory);
        // Should not throw — "True" should be accepted as boolean true
        try (var iter = reader.read(createStorageObject(csv), List.of("active"), 1024)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            BooleanBlock block = (BooleanBlock) page.getBlock(0);
            assertTrue("CSV-1: 'True' should be parsed as boolean true", block.getBoolean(0));
        }
    }

    private StorageObject createStorageObject(String csvContent) {
        byte[] bytes = csvContent.getBytes(StandardCharsets.UTF_8);
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(bytes); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(bytes, (int) pos, (int) Math.min(len, bytes.length - pos));
            }
            @Override public long length() { return bytes.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.csv"); }
        };
    }
}
```

**Fails with:** `EsqlIllegalArgumentException: Failed to parse CSV value [True] as [BOOLEAN]` at `CsvFormatReader.java:806`.

#### Why it happens

`tryParseBoolean()` at `CsvFormatReader.java:771-778` calls `Booleans.parseBoolean(value)`. The `Booleans.parseBoolean()` method (`Booleans.java:56-64`) uses exact string equality: `"true".equals(value)` and `"false".equals(value)`. Both are **case-sensitive**. Anything that isn't exactly `"true"` or `"false"` throws `IllegalArgumentException`.

This is different from Elasticsearch's index-time boolean coercion, which accepts `true`, `True`, `TRUE`, `1`, `yes`, etc.

```java
// CsvFormatReader.java:771-778 — case-sensitive boolean parsing
private Object tryParseBoolean(String value) {
    try {
        return Booleans.parseBoolean(value);  // only "true" or "false"
    } catch (IllegalArgumentException e) {
        lastFieldError = "Failed to parse CSV value [" + value + "] as [BOOLEAN]";
        return null;
    }
}
```

#### Fix

Make the existing check case-insensitive: `value.toLowerCase(Locale.ROOT)` before calling `Booleans.parseBoolean()`. This accepts `True`, `TRUE`, `False`, `FALSE` — the variants that Python, Pandas, Excel, and R actually produce. ~1 line change.

---

### CSV-2: Date-only and zone-less timestamps crash the query

**Estimate:** 0.5d
**Priority:** MVP — 🔴 query crashes, no data returned
**Affected types:** DATETIME

#### Prevalence

Very common. Most CSV exports from databases, spreadsheets, and data tools produce either date-only (`2021-01-01`) or zone-less timestamps (`2021-01-01T10:30:00`). Full ISO-8601 with zone designator (`2021-01-01T10:30:00Z`) is the minority format. Excel exports dates without timezone. PostgreSQL `COPY TO CSV` produces zone-less timestamps by default. Any user querying CSV files from these common sources will hit this crash.

#### Why it's wrong

The CSV reader is stricter than Elasticsearch's own `date` field type, which accepts `2021-01-01` and `2021-01-01T10:30:00` during indexing. Most CSV files in the wild use date-only (`YYYY-MM-DD`) or zone-less timestamps — Excel exports dates without timezone, PostgreSQL `COPY TO CSV` produces zone-less timestamps by default. The full `2021-01-01T00:00:00Z` format that the reader requires is the minority.

ES|QL already has a `time_zone` parameter on the query request (defaults to UTC, configurable via REST body or `SET time_zone`). It flows through `EsqlQueryRequest` → `EsqlSession` → `Configuration.zoneId()` and is available to date functions like `DateParse`. But the `FormatReader` SPI never receives it — the custom format path at `CsvFormatReader.java:790` hardcodes `ZoneOffset.UTC` regardless of what the user set on the query. Zone-less timestamps should use the query's timezone, not a hardcoded default.

#### User impact

When a datetime column contains a date-only value like `2021-01-01` or a zone-less timestamp like `2021-01-01T10:30:00`, the query fails with `EsqlIllegalArgumentException: Failed to parse CSV datetime value [2021-01-01]`. The entire query aborts. The user cannot work around this without pre-processing the file to append `T00:00:00Z` or `Z` suffixes, or using the `datetime_format` config option — which most users won't discover. Only full ISO-8601 with a zone designator (`Z` or `+offset`) is accepted.

**Testing gap:** `CsvFormatReaderTests` only tests epoch millis and full ISO-8601 with `Z`. No test for date-only or zone-less timestamp formats, despite these being the most common formats in real-world CSV files.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

public class ReproCsv2Test extends ESTestCase {

    // Date-only "2021-01-01" crashes — Instant.parse() requires time + zone
    public void testDateOnlyCrashes() throws Exception {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
        String csv = "ts:datetime\n2021-01-01\n";
        var reader = new CsvFormatReader(blockFactory);
        // Should not throw — should parse as 2021-01-01T00:00:00Z
        try (var iter = reader.read(createStorageObject(csv), List.of("ts"), 1024)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            long expected = Instant.parse("2021-01-01T00:00:00Z").toEpochMilli();
            assertEquals("CSV-2: '2021-01-01' should be parsed as midnight UTC",
                expected, block.getLong(0));
        }
    }

    // Zone-less "2021-01-01T10:30:00" crashes — Instant.parse() requires zone
    public void testZonelessCrashes() throws Exception {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
        String csv = "ts:datetime\n2021-01-01T10:30:00\n";
        var reader = new CsvFormatReader(blockFactory);
        // Should not throw — should assume UTC
        try (var iter = reader.read(createStorageObject(csv), List.of("ts"), 1024)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            long expected = Instant.parse("2021-01-01T10:30:00Z").toEpochMilli();
            assertEquals("CSV-2: '2021-01-01T10:30:00' should be parsed as UTC",
                expected, block.getLong(0));
        }
    }

    private StorageObject createStorageObject(String csvContent) {
        byte[] bytes = csvContent.getBytes(StandardCharsets.UTF_8);
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(bytes); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(bytes, (int) pos, (int) Math.min(len, bytes.length - pos));
            }
            @Override public long length() { return bytes.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.csv"); }
        };
    }
}
```

**Fails with:** `EsqlIllegalArgumentException: Failed to parse CSV datetime value [2021-01-01]` and `Failed to parse CSV datetime value [2021-01-01T10:30:00]` at `CsvFormatReader.java:806`.

#### Why it happens

`tryParseDatetime()` at `CsvFormatReader.java:780-802` has a three-step parsing chain:

1. **Epoch millis** — if `looksNumeric(value)` (all digits, optional leading `-`), tries `Long.parseLong()`. Date-only and zone-less strings are not numeric, so this path is skipped.
2. **Custom format** — if `datetimeFormatter != null` (set via `datetime_format` config option), tries `LocalDateTime.parse()`. No custom formatter by default, so this path is skipped.
3. **ISO-8601** — `Instant.parse(value)` at line 797. This method requires both a time component and a zone designator (`Z` or `+offset`). `2021-01-01` fails because there's no `T` or time. `2021-01-01T10:30:00` fails because there's no `Z` or offset.

```java
// CsvFormatReader.java:796-801 — Instant.parse() rejects common formats
try {
    return Instant.parse(value).toEpochMilli();     // needs Z or +offset
} catch (DateTimeParseException e) {
    lastFieldError = "Failed to parse CSV datetime value [" + value + "]";
    return null;
}
```

#### Fix

Two parts:

**Part 1 — Accept date-only and zone-less timestamps (crash fix):**

Add two fallback parsing attempts in `tryParseDatetime()` before the final error:

1. Try `LocalDateTime.parse(value).toInstant(queryZoneId).toEpochMilli()` for zone-less timestamps like `2021-01-01T10:30:00`.
2. Try `LocalDate.parse(value).atStartOfDay(queryZoneId).toInstant().toEpochMilli()` for date-only values like `2021-01-01`.

Both use the query's `time_zone` parameter (which defaults to UTC). ~10 lines in `CsvFormatReader`.

**Part 2 — Thread query timezone to FormatReader:**

The `FormatReader.read()` SPI currently has no `ZoneId` parameter. The query timezone lives in `Configuration.zoneId()` but never reaches the reader. Options:
- Add `ZoneId` to `FormatReader.read()` signature (touches all FormatReader implementations)
- Pass `ZoneId` at `CsvFormatReader` construction time via `ExternalSourceOperatorFactory`

Either way, this also fixes the existing bug where the custom format path (`datetimeFormatter` at line 790) hardcodes `ZoneOffset.UTC` instead of using the query timezone. Can be done in the same PR as Part 1.

---

### CSV-3: Missing trivial numeric aliases

**Estimate:** 0.5d
**Priority:** MVP — consistency issue, trivial fix
**Affected types:** FLOAT, SHORT, BYTE, HALF_FLOAT, SCALED_FLOAT

#### Why it matters

The `name:type` header is our own convention — RFC 4180 has no concept of typed headers. Since we invented this syntax, users who discover `integer` or `double` will immediately try `float`, `short`, or `byte` — these are standard type names in every programming language and data tool. Failing with `illegal data type [FLOAT]` when `DOUBLE` works is confusing and inconsistent.

#### What's missing

| Header alias | Maps to | Parsing | Effort |
|---|---|---|---|
| `FLOAT`, `F` | DOUBLE | Existing `Double.parseDouble()` — ESQL widens FLOAT to DOUBLE internally | 1 line |
| `SHORT` | INTEGER | Existing `Integer.parseInt()` — ESQL widens SHORT to INTEGER internally | 1 line |
| `BYTE` | INTEGER | Existing `Integer.parseInt()` — ESQL widens BYTE to INTEGER internally | 1 line |
| `HALF_FLOAT` | DOUBLE | Existing `Double.parseDouble()` — ESQL widens to DOUBLE | 1 line |
| `SCALED_FLOAT` | DOUBLE | Existing `Double.parseDouble()` — ESQL widens to DOUBLE | 1 line |

#### Fix

Add `case` branches in `parseDataType()`. No new parsing logic — all map to existing DOUBLE or INTEGER parsers. ~5 lines total.

---

### CSV-4: Missing common typed column support

**Estimate:** 1w
**Priority:** Post-MVP — needs new parsers, but high-value types
**Affected types:** IP, UNSIGNED_LONG, VERSION, DATE_NANOS

#### Why it matters

These are common types that users will expect to work in CSV files, especially for log analytics (the primary use case for external data sources). A user exporting firewall logs to CSV will have IP columns; a user with high-precision event data will have nanosecond timestamps.

#### What's missing

| Header alias | Maps to | Parsing needed | Prevalence |
|---|---|---|---|
| `IP` | IP | `InetAddresses.forString(value)` → encode as `BytesRef`. Needs validation for IPv4/IPv6. | **High** — log data, network data, security data. Every firewall/access log CSV has IP columns. |
| `UNSIGNED_LONG`, `UL` | UNSIGNED_LONG | `Long.parseUnsignedLong(value)` or `BigInteger` → ESQL's unsigned long encoding. Need to handle the full 0–2^64-1 range. | **Medium** — network captures, protobuf exports, snowflake IDs in upper range. |
| `VERSION`, `V` | VERSION | ES Version type parser — semver strings like `1.2.3`. | **Medium** — package manifests, dependency exports, release tracking CSVs. |
| `DATE_NANOS`, `DN` | DATE_NANOS | Nanosecond-precision ISO-8601 parser → epoch nanos. Extends the datetime parsing chain. | **Medium** — high-frequency trading, scientific measurements, OpenTelemetry trace exports. |

#### Fix

Each type needs: (1) a `case` in `parseDataType()`, (2) a `tryParseXxx()` method in the parsing chain, (3) the appropriate block builder in the value writer. IP and VERSION need the ESQL-specific binary encodings. UNSIGNED_LONG needs careful range handling. DATE_NANOS extends the existing datetime chain to produce `LongBlock` with epoch nanos instead of epoch millis. ~30-50 lines per type.

---

### CSV-5: Missing spatial and niche types

**Estimate:** 2w
**Priority:** Post-MVP — needs parsing conventions and design decisions
**Affected types:** GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE, DATE_PERIOD, TIME_DURATION, OBJECT, COUNTER_*, AGGREGATE_METRIC_DOUBLE

#### Why it matters

These types have no obvious text representation in CSV, so they need design decisions about parsing conventions before implementation.

#### What's missing

| Header alias | Maps to | Design question | Prevalence |
|---|---|---|---|
| `GEO_POINT` | GEO_POINT | What text format? WKT (`POINT(lon lat)`), comma-separated (`lat,lon`), or GeoJSON? Comma-separated conflicts with CSV delimiters. | **Medium** — location data, IoT sensor CSVs, fleet tracking. |
| `GEO_SHAPE` | GEO_SHAPE | WKT is the natural choice (`POLYGON(...)`) but values can be very long. | **Low** — boundary data, coverage areas. |
| `CARTESIAN_POINT`, `CARTESIAN_SHAPE` | CARTESIAN_* | Same conventions as geo but in Cartesian coordinates. | **Very low** — CAD exports, game world data. |
| `DATE_PERIOD` | DATE_PERIOD | ISO-8601 duration (`P1Y2M3D`)? ESQL-specific syntax? | **Very low** — scheduling data. |
| `TIME_DURATION` | TIME_DURATION | ISO-8601 duration (`PT1H30M`)? | **Very low** — SLA tracking, elapsed time. |
| `OBJECT` | — | CSV is flat — no nesting. Would need JSON-in-cell convention. | **N/A** — doesn't fit CSV's flat model. |
| `COUNTER_INTEGER`, `COUNTER_LONG`, `COUNTER_DOUBLE` | COUNTER_* | TSDB-specific monotonic counters. Semantics don't map to CSV import. | **N/A** — TSDB internal, not a CSV use case. |
| `AGGREGATE_METRIC_DOUBLE` | — | Multi-value aggregate. Doesn't map to a single CSV cell. | **N/A** — TSDB internal. |

#### Fix

**GEO_POINT** is the highest-value item. WKT parsing via `WellKnownText.fromWKT()` is the cleanest approach — avoids comma ambiguity and is a recognized standard. GEO_SHAPE uses the same parser. The rest are low priority or don't fit CSV's model.

---

## Appendix: Per-Type Breakdown

### INTEGER (INT / I)

`parseDataType()` at `CsvFormatReader.java:437`: `case "INTEGER", "INT", "I" -> DataType.INTEGER`

Parsing: `Integer.parseInt(value)` at `CsvFormatReader.java:744-751`.

| Input | Status | What Happens |
|---|---|---|
| `42`, `-7`, `0` | 🟢 | Parsed correctly. |
| `007` | 🟢 | Leading zeros accepted by `Integer.parseInt()`. |
| `3.14` | 🔴 | `NumberFormatException` — no decimal support. |
| `> MAX_INT` | 🔴 | `NumberFormatException` — overflow. |
| `""`, `null` | 🟢 | Null check at line 723 returns null. |

### LONG (L)

`parseDataType()` at `CsvFormatReader.java:438`: `case "LONG", "L" -> DataType.LONG`

Parsing: `Long.parseLong(value)` at `CsvFormatReader.java:753-760`.

| Input | Status | What Happens |
|---|---|---|
| `9999999999`, `-1` | 🟢 | Parsed correctly. |
| `3.14` | 🔴 | `NumberFormatException`. |
| `> MAX_LONG` | 🔴 | `NumberFormatException` — overflow. |
| `""`, `null` | 🟢 | Null. |

### DOUBLE (D)

`parseDataType()` at `CsvFormatReader.java:439`: `case "DOUBLE", "D" -> DataType.DOUBLE`

Parsing: `Double.parseDouble(value)` at `CsvFormatReader.java:762-769`.

| Input | Status | What Happens |
|---|---|---|
| `95.5`, `1e10` | 🟢 | Parsed correctly. |
| `NaN`, `Infinity` | 🟡 | Accepted but may surprise downstream. |
| `""`, `null` | 🟢 | Null. |

### KEYWORD (K / STRING / S) and TEXT (TXT)

`parseDataType()` at `CsvFormatReader.java:440-441`.

Parsing: `new BytesRef(value)` — no parsing, raw string wrapped.

| Input | Status | What Happens |
|---|---|---|
| Any non-null, non-empty string | 🟢 | Stored as-is. |
| `null` (literal) | 🟢 | Treated as null — by design, matches Pandas/Spark convention. Configurable via `null_value` option. |
| `""` (empty) | 🟢 | Treated as null — by design, matches Pandas/Spark/PostgreSQL convention. |
| `" hello "` (whitespace) | 🟢 | Trimmed — by design, most CSV consumers strip whitespace. |

### BOOLEAN (BOOL)

`parseDataType()` at `CsvFormatReader.java:442`: `case "BOOLEAN", "BOOL" -> DataType.BOOLEAN`

Parsing: `Booleans.parseBoolean(value)` at `CsvFormatReader.java:771-778`.

| Input | Status | What Happens |
|---|---|---|
| `true`, `false` | 🟢 | Parsed correctly. |
| `True`, `TRUE`, `False`, `FALSE` | 🔴 | Crash — [CSV-1](#csv-1-case-sensitive-boolean-parsing-rejects-common-variants). |
| `""`, `null` | 🟢 | Null. |

### DATETIME (DATE / DT)

`parseDataType()` at `CsvFormatReader.java:443`: `case "DATETIME", "DATE", "DT" -> DataType.DATETIME`

Parsing: Three-step chain at `CsvFormatReader.java:780-802` — epoch millis → custom format → `Instant.parse()`.

| Input | Status | What Happens |
|---|---|---|
| `1609459200000` (epoch) | 🟢 | Parsed as epoch millis. |
| `2021-01-01T00:00:00Z` | 🟢 | `Instant.parse()` succeeds. |
| `2021-01-01T00:00:00+02:00` | 🟢 | `Instant.parse()` converts to UTC. |
| `2021-01-01` (date only) | 🔴 | Crash — [CSV-2](#csv-2-date-only-and-zone-less-timestamps-crash-the-query). |
| `2021-01-01T10:30:00` (no zone) | 🔴 | Crash — same issue. |
| `""`, `null` | 🟢 | Null. |

### NULL (N)

`parseDataType()` at `CsvFormatReader.java:444`: `case "NULL", "N" -> DataType.NULL`

Every cell becomes null regardless of content.

### Custom `name:type` Header Convention

RFC 4180 defines the header line as field names only — no type annotations. The `name:type` syntax (e.g., `age:integer,name:keyword`) is **ES|QL's own convention**, implemented in `parseSchema()` at `CsvFormatReader.java:412-433`. Since we invented this convention, we should implement it consistently — users who discover one type name will expect the others to work too. Currently 8 aliases map to 7 ESQL types; 20 ESQL types have no alias and throw `EsqlIllegalArgumentException: illegal data type [<TYPENAME>]`.

The missing types fall into three tiers — see [CSV-3](#csv-3-missing-trivial-numeric-aliases), [CSV-4](#csv-4-missing-common-typed-column-support), and [CSV-5](#csv-5-missing-spatial-and-niche-types) below.
