# Parquet Type Support in ES|QL External Data Sources

Parquet defines 8 physical types (`BOOLEAN`, `INT32`, `INT64`, `INT96`, `FLOAT`, `DOUBLE`, `BINARY`, `FIXED_LEN_BYTE_ARRAY`) with logical type annotations layered on top to express higher-level semantics (dates, decimals, strings, UUIDs, etc.). ES|QL's Parquet reader uses **parquet-mr 1.16.0** (`parquet-hadoop-bundle`) and maps Parquet types to ESQL `DataType` values in `ParquetFormatReader.convertParquetTypeToEsql()` (`ParquetFormatReader.java:205-227`). The read path dispatches on the assigned ESQL type in `readColumnBlock()` (`ParquetFormatReader.java:365-378`).

The complete per-type breakdown is in [Appendix B](#appendix-b-per-type-breakdown). The summary and issue tables below focus on what's broken.

## Summary

| Status | Count | Types |
|---|---|---|
| 🟢 Fully works | 13 | BOOLEAN, INT32, INT32+DATE, INT32+INT(8/16/32,s), INT32+UINT_16, INT64, INT64+TIMESTAMP(MILLIS), INT64+INT(64,s), FLOAT, DOUBLE, BINARY, BINARY+STRING, BINARY+ENUM |
| 🔴 Wrong data | 7 | INT32+DECIMAL, INT64+DECIMAL, INT64+TIMESTAMP(MICROS), INT64+TIMESTAMP(NANOS), BINARY+DECIMAL, FIXED_LEN+DECIMAL, FIXED_LEN+FLOAT16 |
| 🟡 Partial | 10 | INT32+UINT_8, INT32+UINT_32, INT32+TIME_MILLIS, INT64+UINT_64, INT64+TIME_MICROS, INT64+TIME_NANOS, BINARY+JSON, BINARY+BSON, FIXED_LEN+UUID, FIXED_LEN+INTERVAL |
| ⬛ Unsupported (null) | 5 | INT96, GROUP, LIST, MAP, STRUCT |

### MVP

| Status | Type | Prevalence | Description | Issue |
|---|---|---|---|---|
| ⬛ | INT96 | Legacy but sticky — default for Spark <3.0 (pre-2020). Historical archives. | INT96 timestamps silently return null | [PARQUET-3](#parquet-3-int96-timestamps-silently-return-null) |
| ⬛ | LIST of primitives | 3rd most common Parquet type. Spark arrays, tags, embeddings. | LIST of primitives silently returns null | [PARQUET-10](#parquet-10-list-of-primitives-silently-returns-null) |
| 🔴 | INT32/INT64/BINARY/FIXED_LEN + DECIMAL(p,s) | Ubiquitous. Every financial/e-commerce dataset. | Decimal values silently wrong by factor of 10^scale | [PARQUET-1](#parquet-1-decimal-values-silently-wrong-by-factor-of-10scale) |
| 🔴 | INT64 + TIMESTAMP(MICROS/NANOS) | Extremely common. Spark/PyArrow/DuckDB default to MICROS. | Micros/nanos timestamps interpreted as millis | [PARQUET-2](#parquet-2-microsnanos-timestamps-interpreted-as-millis) |
| 🟡 | INT64 + TIMESTAMP(*, !UTC) | Medium. Legacy Hive, JDBC exports, PyArrow with tz=None. | Local timestamps silently treated as UTC | [PARQUET-12](#parquet-12-local-timestamps-silently-treated-as-utc) |
| 🔴 | FIXED_LEN + FLOAT16 | Low as scalar, but prerequisite for `LIST<FLOAT16>` embeddings. | FLOAT16 as raw bytes | [PARQUET-8](#parquet-8-float16-as-raw-bytes) |
| 🟡 | FIXED_LEN + UUID | Common. Standard PKs in web apps, event tracking, Iceberg. | UUID as raw bytes | [PARQUET-5](#parquet-5-uuid-as-raw-bytes) |

### Post-MVP

| Status | Type | Prevalence | Description | Issue |
|---|---|---|---|---|
| ⬛ | GROUP, MAP, STRUCT, nested LIST | MAP/STRUCT in DB exports and ETL. Nested LIST rare. | MAP, STRUCT, and nested LIST silently return null | [PARQUET-11](#parquet-11-map-struct-and-nested-list-silently-return-null) |
| 🔴 | INT32/INT64 + UINT_8/32/64 | Low-medium. Protobuf, network captures, game telemetry. | Unsigned integer overflow | [PARQUET-4](#parquet-4-unsigned-integer-overflow) |
| 🟡 | INT32/INT64 + TIME(MILLIS/MICROS/NANOS) | Rare. JDBC exports from PostgreSQL/MySQL only. | Time types as raw integers | [PARQUET-6](#parquet-6-time-types-as-raw-integers) |
| 🟡 | BINARY + JSON/BSON | Low-medium for JSON; very rare for BSON. | JSON/BSON as opaque keyword | [PARQUET-7](#parquet-7-jsonbson-as-opaque-keyword) |
| 🟡 | FIXED_LEN + INTERVAL | Very rare. Deprecated in Parquet spec. | Interval as raw bytes | [PARQUET-9](#parquet-9-interval-as-raw-bytes) |

---

## Issue Details

### PARQUET-1: Decimal values silently wrong by factor of 10^scale

**Estimate:** 0.5w
**Priority:** MVP — 🔴 silently wrong data, no error
**Affected types:** INT32+DECIMAL, INT64+DECIMAL, BINARY+DECIMAL, FIXED_LEN+DECIMAL

#### Prevalence

Decimal is the most common Parquet logical type after STRING. It is used in virtually every financial, e-commerce, and analytics dataset. Spark, PyArrow, and DuckDB all write DECIMAL columns. Any user querying Parquet files from a data warehouse, BI pipeline, or financial system will encounter this.

#### User impact

When a Parquet file contains DECIMAL columns (any physical encoding — INT32, INT64, BINARY, or FIXED_LEN_BYTE_ARRAY), there are two distinct failure modes depending on the physical encoding:

**INT32 and INT64 decimals — values wrong by a constant factor:**
The column appears in the schema as INTEGER (for INT32) or LONG (for INT64). Every value is wrong by exactly 10^scale, where scale is the number of decimal places. For Decimal(7,2) stored in INT32, a value of `123.45` is stored internally as the unscaled integer `12345`. The reader returns `12345` to the user. For Decimal(18,4) stored in INT64, `123456.7890` appears as `1234567890`.

This is a silent data corruption bug — the query succeeds, returns results, shows no errors or warnings. A user building a financial report will see amounts that are 100x (or 10,000x) too large. `STATS SUM(price)` produces a total that is off by the same factor. There is no indication that anything is wrong.

**BINARY and FIXED_LEN_BYTE_ARRAY decimals — garbled output:**
The column appears in the schema as KEYWORD. The Parquet spec encodes large decimals (precision >18) as big-endian two's-complement byte arrays. The reader wraps these raw bytes in a `BytesRef` and displays them as a keyword string. The user sees garbled characters — raw binary data interpreted as UTF-8 text. The numeric value is completely unrecoverable from the output.

**Testing gap:** No test for any DECIMAL variant in either schema inference or data reading. `ParquetFormatReaderTests` exercises BOOLEAN, INT32, INT64, FLOAT, DOUBLE, and BINARY+STRING — but never DECIMAL. The test data only uses bare physical types without logical annotations.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

public class ReproParquet1Test extends ESTestCase {

    // INT32+DECIMAL: value 123.45 stored as unscaled int 12345 — reader returns INTEGER 12345
    public void testDecimalInt32WrongByScaleFactor() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 7)).named("price")
            .named("test_schema");
        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("price", 12345)));
        var meta = new ParquetFormatReader(blockFactory).metadata(createStorageObject(data));
        assertEquals(DataType.DOUBLE, meta.schema().get(0).dataType());
    }

    // INT64+DECIMAL: value 123456.7890 stored as unscaled long 1234567890
    public void testDecimalInt64WrongByScaleFactor() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.decimalType(4, 18)).named("amount")
            .named("test_schema");
        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("amount", 1234567890L)));
        var meta = new ParquetFormatReader(blockFactory).metadata(createStorageObject(data));
        assertEquals(DataType.DOUBLE, meta.schema().get(0).dataType());
    }

    // FIXED_LEN+DECIMAL: big-endian two's-complement bytes shown as garbled KEYWORD
    public void testDecimalFixedLenGarbledOutput() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(12)
            .as(LogicalTypeAnnotation.decimalType(2, 28)).named("big_price")
            .named("test_schema");
        byte[] unscaled = new byte[12];
        unscaled[10] = 0x30; unscaled[11] = 0x39; // 12345 = 0x3039
        byte[] data = createParquetFile(schema, f -> {
            Group g = f.newGroup();
            g.add("big_price", Binary.fromConstantByteArray(unscaled));
            return List.of(g);
        });
        var meta = new ParquetFormatReader(blockFactory).metadata(createStorageObject(data));
        assertEquals(DataType.DOUBLE, meta.schema().get(0).dataType());
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface GroupCreator { List<Group> create(SimpleGroupFactory f); }

    static byte[] createParquetFile(MessageType schema, GroupCreator gc) throws IOException {
        var out = new ByteArrayOutputStream();
        OutputFile of = new OutputFile() {
            @Override public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    long pos = 0;
                    @Override public long getPos() { return pos; }
                    @Override public void write(int b) throws IOException { out.write(b); pos++; }
                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len); pos += len;
                    }
                    @Override public void close() throws IOException { out.close(); }
                };
            }
            @Override public PositionOutputStream createOrOverwrite(long h) throws IOException { return create(h); }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 0; }
            @Override public String getPath() { return "memory://test.parquet"; }
        };
        try (var w = ExampleParquetWriter.builder(of).withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
            for (Group g : gc.create(new SimpleGroupFactory(schema))) w.write(g);
        }
        return out.toByteArray();
    }

    static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.parquet"); }
        };
    }
}
```

**Fails with:**
- INT32: `expected DOUBLE but was INTEGER`
- INT64: `expected DOUBLE but was LONG`
- FIXED_LEN: `expected DOUBLE but was KEYWORD`

#### Why it happens

`convertParquetTypeToEsql()` (`ParquetFormatReader.java:205-227`) never checks for `DecimalLogicalTypeAnnotation`. The switch on `PrimitiveTypeName` maps:
- INT32 → checks only `DateLogicalTypeAnnotation` (line 214), else `DataType.INTEGER`
- INT64 → checks only `TimestampLogicalTypeAnnotation` (line 215), else `DataType.LONG`
- BINARY/FIXED_LEN → checks only `StringLogicalTypeAnnotation` (line 219), else `DataType.KEYWORD`

The `DecimalLogicalTypeAnnotation` class provides `getScale()` and `getPrecision()`, but neither method is ever called. The read path uses `cr.getInteger()` / `cr.getLong()` / `cr.getBinary().getBytes()` which return the raw unscaled representation.

```java
// ParquetFormatReader.java:214-215 — Decimal annotation never checked
case INT32 -> logical instanceof DateLogicalTypeAnnotation ? DataType.DATETIME : DataType.INTEGER;
case INT64 -> logical instanceof TimestampLogicalTypeAnnotation ? DataType.DATETIME : DataType.LONG;
```

#### Fix

Add a `DecimalLogicalTypeAnnotation` check early in `convertParquetTypeToEsql()`, before the physical-type switch. For INT32/INT64: map to `DataType.DOUBLE`, and in the read path divide the raw value by `Math.pow(10, scale)`. For BINARY/FIXED_LEN: decode the byte array into a `BigInteger`, construct `new BigDecimal(bigint, scale)`, call `.doubleValue()`. Precision loss beyond 15 significant digits is inherent to double and should be documented.

#### How Parquet encodes DECIMAL

Parquet has one logical type — DECIMAL(precision, scale) — but stores it using **four different physical types** depending on precision. The value stored is always `actual_value × 10^scale` (the "unscaled" representation), and it is the reader's responsibility to divide by `10^scale` to recover the real number. Our reader does not do this division for any of the four encodings.

| Precision | Physical Type | Storage | Example: value `123.45`, Decimal(7,2) |
|---|---|---|---|
| ≤9 digits | INT32 | Unscaled value as signed 32-bit int | Stored as `12345` |
| 10–18 digits | INT64 | Unscaled value as signed 64-bit long | Stored as `12345L` |
| 19–38 digits | FIXED_LEN_BYTE_ARRAY | Big-endian two's-complement bytes, fixed width | Stored as e.g. 12 bytes |
| Any | BINARY | Same encoding as FIXED_LEN but variable length | Used by some older writers |

This is why DECIMAL appears in four separate type sections above — once under each physical type.

---

### PARQUET-2: MICROS/NANOS timestamps interpreted as MILLIS

**Estimate:** 0.5w
**Priority:** MVP — 🔴 silently wrong data, no error
**Affected types:** INT64+TIMESTAMP(MICROS), INT64+TIMESTAMP(NANOS)

#### Prevalence

Very high. **Spark defaults to MICROS timestamps.** PyArrow defaults to MICROS. DuckDB writes MICROS. The Parquet specification recommends MICROS as the default timestamp unit. The majority of Parquet files produced by modern tools use TIMESTAMP(MICROS, isAdjustedToUTC=true). MILLIS is primarily used by legacy Hive-ecosystem writers. NANOS is less common but appears in Go-based writers and some database exports.

Any user querying Parquet files produced by Spark, PyArrow, Pandas, Polars, DuckDB, or DataFusion will hit this bug on every timestamp column.

#### User impact

When a Parquet file contains INT64 TIMESTAMP columns with MICROS or NANOS resolution (the default in Spark, PyArrow, and DuckDB), every timestamp in the file is wrong, and the magnitude of the error makes the data completely unusable:

**MICROS:** A timestamp of `2024-01-01T00:00:00Z` is stored as `1704067200000000` microseconds since epoch. The reader stores this raw value as epoch milliseconds (ESQL DATETIME). The user sees a date in approximately **year 55987**. Every timestamp is shifted ~54,000 years into the future. A log analysis query like `FROM "s3://logs.parquet" | WHERE @timestamp > NOW() - 1 day` returns zero results because all timestamps are far in the future. A time-series dashboard shows no data.

**NANOS:** A timestamp of `2024-01-01T00:00:00Z` is stored as `1704067200000000000` nanoseconds. The reader stores this as epoch milliseconds, producing a date so far in the future that it overflows normal display. Every timestamp is off by a factor of 1,000,000.

In both cases the query succeeds with no errors or warnings. The DATETIME column is present and populated — just with wildly wrong values.

**Testing gap:** No test for TIMESTAMP with MICROS or NANOS unit. `ParquetFormatReaderTests` does not exercise any timestamp logical annotations — all INT64 columns are bare LONG. The `employees.parquet` fixture may use MILLIS (which works correctly by accident), masking the MICROS/NANOS bug.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

public class ReproParquet2Test extends ESTestCase {

    // TIMESTAMP(MICROS): 2024-01-01T00:00:00Z stored as 1704067200000000 micros — reader returns raw value as millis
    public void testTimestampMicrosWrongByThousand() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test_schema");
        long expectedMillis = 1704067200000L; // 2024-01-01T00:00:00Z
        long storedMicros = expectedMillis * 1000;
        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("ts", storedMicros)));
        try (var iter = new ParquetFormatReader(blockFactory).read(createStorageObject(data), List.of("ts"), 10)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            long actual = ((LongBlock) page.getBlock(0)).getLong(0);
            assertEquals("PARQUET-2: MICROS timestamp should be divided by 1000 to get MILLIS",
                expectedMillis, actual);
        }
    }

    // TIMESTAMP(NANOS): 2024-01-01T00:00:00Z stored as 1704067200000000000 nanos — reader returns raw value as millis
    public void testTimestampNanosWrongByMillion() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ts")
            .named("test_schema");
        long expectedMillis = 1704067200000L;
        long storedNanos = expectedMillis * 1_000_000;
        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("ts", storedNanos)));
        try (var iter = new ParquetFormatReader(blockFactory).read(createStorageObject(data), List.of("ts"), 10)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            long actual = ((LongBlock) page.getBlock(0)).getLong(0);
            assertEquals("PARQUET-2: NANOS timestamp should be converted, not stored raw",
                expectedMillis, actual);
        }
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface GroupCreator { List<Group> create(SimpleGroupFactory f); }

    static byte[] createParquetFile(MessageType schema, GroupCreator gc) throws IOException {
        var out = new ByteArrayOutputStream();
        OutputFile of = new OutputFile() {
            @Override public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    long pos = 0;
                    @Override public long getPos() { return pos; }
                    @Override public void write(int b) throws IOException { out.write(b); pos++; }
                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len); pos += len;
                    }
                    @Override public void close() throws IOException { out.close(); }
                };
            }
            @Override public PositionOutputStream createOrOverwrite(long h) throws IOException { return create(h); }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 0; }
            @Override public String getPath() { return "memory://test.parquet"; }
        };
        try (var w = ExampleParquetWriter.builder(of).withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
            for (Group g : gc.create(new SimpleGroupFactory(schema))) w.write(g);
        }
        return out.toByteArray();
    }

    static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.parquet"); }
        };
    }
}
```

**Fails with:**
- MICROS: `expected 1704067200000 but was 1704067200000000` — raw microseconds stored as millis, date shows ~year 55987.
- NANOS: `expected 1704067200000 but was 1704067200000000000`.

#### Why it happens

`convertParquetTypeToEsql()` correctly recognizes `TimestampLogicalTypeAnnotation` at line 215 and maps to `DataType.DATETIME`. The problem is in the read path. At line 477, the code reads:

```java
// ParquetFormatReader.java:477
datetimeValues[i] = cr.getLong();
```

This stores the raw long value directly as ESQL epoch millis, regardless of the timestamp unit. The `TimestampLogicalTypeAnnotation` class provides `getUnit()` which returns `MILLIS`, `MICROS`, or `NANOS`, but `getUnit()` is never called anywhere in the read path. For MILLIS the raw value happens to already be in the right unit, so it works. For MICROS and NANOS, the value needs to be divided by 1,000 or 1,000,000 respectively.

#### Fix

In the DATETIME read path (around line 477), retrieve the `TimestampLogicalTypeAnnotation` from the column's `PrimitiveType`, call `getUnit()`, and apply the appropriate conversion:
- `MILLIS` → store directly (current behavior, correct)
- `MICROS` → divide by 1,000
- `NANOS` → map to `DataType.DATE_NANOS` to preserve full nanosecond precision (we try to never lose precision)

The logical annotation is already available — it was checked at line 215 during type mapping. It just needs to be threaded through to the read method.

---

### PARQUET-12: Local timestamps silently treated as UTC

**Estimate:** 0.5w
**Priority:** MVP — 🟡 silent timezone shift, no error
**Affected types:** INT64+TIMESTAMP(*, isAdjustedToUTC=false)

#### Prevalence

Medium. Parquet's `TimestampLogicalTypeAnnotation` has an `isAdjustedToUTC` flag. When `true`, the stored value is a UTC epoch (the common case — Spark, PyArrow, DuckDB default to this). When `false`, the stored value is in local time — the writer's timezone is implied but not recorded. This appears in:
- Legacy Hive-ecosystem writers that default to `isAdjustedToUTC=false`
- Some JDBC exports where the source database stores local timestamps
- PyArrow when explicitly constructed with `tz=None`

#### User impact

When a Parquet file contains TIMESTAMP columns with `isAdjustedToUTC=false`, the stored long value represents local time (e.g., `2024-01-15T10:30:00` in the writer's timezone). The reader treats it as UTC epoch millis without any adjustment. If the writer was in `America/New_York` (UTC-5), every timestamp is silently shifted 5 hours forward. A meeting at `10:30 AM EST` appears as `3:30 PM UTC` — wrong by the writer's UTC offset.

The query succeeds with no errors or warnings. The timestamps look plausible (unlike PARQUET-2 where values are off by 1000x), making this harder to detect.

**Testing gap:** No test for `isAdjustedToUTC=false` timestamps. All test fixtures use UTC-adjusted timestamps.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class ReproParquet12Test extends ESTestCase {

    public void testLocalTimestampTreatedAsUtc() throws Exception {
        var blockFactory = ReproParquet2Test.blockFactory();
        // isAdjustedToUTC=false means local time, not UTC
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts")
            .named("test_schema");

        // This value represents "2024-01-15T10:30:00" in LOCAL time (not UTC)
        long localMillis = 1705313400000L; // 2024-01-15T10:30:00 as if UTC
        byte[] data = ReproParquet2Test.createParquetFile(schema, f -> List.of(f.newGroup().append("ts", localMillis)));

        try (var iter = new ParquetFormatReader(blockFactory).read(
                ReproParquet2Test.createStorageObject(data), List.of("ts"), 10)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            long actual = ((LongBlock) page.getBlock(0)).getLong(0);

            // The reader should either:
            // 1. Apply the query timezone to convert local -> UTC, or
            // 2. At minimum, emit a warning that the timestamp has no timezone
            // Currently it silently treats the local value as UTC — no conversion, no warning.
            // This test documents the current (wrong) behavior.
            assertEquals("Local timestamp silently treated as UTC — no timezone adjustment applied",
                localMillis, actual);
            // The above assertion PASSES — proving the bug: the value is stored as-is
            // with no indication that it's local time, not UTC.
        }
    }
}
```

**Behavior:** The test passes — proving the bug. The local timestamp is stored as-is with no timezone adjustment and no warning. Users see timestamps that are silently shifted by their UTC offset.

#### Why it happens

`convertParquetTypeToEsql()` at line 215 checks `logical instanceof TimestampLogicalTypeAnnotation` but never inspects the `isAdjustedToUTC()` flag:

```java
// ParquetFormatReader.java:215
case INT64 -> logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
    ? DataType.DATETIME : DataType.LONG;
```

Both `isAdjustedToUTC=true` and `isAdjustedToUTC=false` timestamps are mapped to DATETIME and read identically at line 477 (`values[i] = cr.getLong()`). There is no branch for local timestamps.

#### Fix

Two options:

**Option A — Apply query timezone (preferred):**
When `isAdjustedToUTC=false`, treat the stored value as local time in the query's `time_zone` parameter (from `Configuration.zoneId()`, defaults to UTC). Convert to UTC epoch millis: `localMillis - zoneOffset.getTotalSeconds() * 1000`. This requires threading the query timezone to the FormatReader — same infrastructure needed for CSV issue #323.

**Option B — Emit warning and assume UTC:**
When `isAdjustedToUTC=false`, log a warning ("Parquet TIMESTAMP column '{name}' has no timezone; treating as UTC") and store the value as-is. This is less correct but simpler and doesn't require SPI changes.

Option A is preferred because it aligns with how Elasticsearch itself handles timezone-naive `date` fields (it uses the index's configured timezone or UTC). The query timezone threading is already needed for CSV (#323), so the infrastructure cost is shared.

---

### PARQUET-8: FLOAT16 as raw bytes

**Estimate:** 0.5w
**Priority:** MVP — 🔴 garbled output
**Affected types:** FIXED_LEN_BYTE_ARRAY+FLOAT16

#### Prevalence

Low as a standalone scalar today — FLOAT16 was added to the Parquet spec in v2.10 (2023). However, the fix is trivial (~15 lines) and FLOAT16 is a prerequisite for vector embedding support: once LIST of primitives is supported (see [PARQUET-10](#parquet-10-list-of-primitives-silently-returns-null)), `LIST<FLOAT16>` is the compact format for storing embeddings (half the size of `LIST<FLOAT>`). Supporting the scalar type now means embeddings work end-to-end as soon as LIST lands.

Note: vector embeddings in Parquet are stored as `LIST<FLOAT>` or `LIST<FLOAT16>`, not as scalar FLOAT16. The scalar type alone doesn't enable embeddings — LIST support is required. Whether those vectors should eventually map to Elasticsearch's `dense_vector` type (enabling KNN search) is a separate design question for Post-MVP.

#### User impact

When a Parquet file contains FLOAT16 columns (IEEE 754 half-precision), an `embedding` column shows 2-character garbled strings instead of numeric values. The 2-byte IEEE 754 half-precision float is stored as raw bytes in a KEYWORD field. The numeric value is completely unrecoverable from the displayed output. No error.

**Testing gap:** No test for FLOAT16 logical type annotation. `ParquetFormatReaderTests` tests FLOAT (32-bit, widened to DOUBLE) but never FLOAT16 (16-bit, FIXED_LEN_BYTE_ARRAY).

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.List;

public class ReproParquet8Test extends ESTestCase {

    // FLOAT16: 2-byte IEEE 754 half-precision float shown as garbled KEYWORD
    public void testFloat16ShowsGarbledBytes() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(2)
            .as(LogicalTypeAnnotation.float16Type())
            .named("weight")
            .named("test_schema");
        short halfFloat = Float.floatToFloat16(3.14f);
        ByteBuffer f16Buf = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
        f16Buf.putShort(halfFloat);
        byte[] data = createParquetFile(schema, f -> {
            Group g = f.newGroup();
            g.add("weight", Binary.fromConstantByteArray(f16Buf.array()));
            return List.of(g);
        });
        var meta = new ParquetFormatReader(blockFactory).metadata(createStorageObject(data));
        assertEquals("PARQUET-8: FLOAT16 should map to DOUBLE, not KEYWORD",
            DataType.DOUBLE, meta.schema().get(0).dataType());
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface GroupCreator { List<Group> create(SimpleGroupFactory f); }

    static byte[] createParquetFile(MessageType schema, GroupCreator gc) throws IOException {
        var out = new ByteArrayOutputStream();
        OutputFile of = new OutputFile() {
            @Override public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    long pos = 0;
                    @Override public long getPos() { return pos; }
                    @Override public void write(int b) throws IOException { out.write(b); pos++; }
                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len); pos += len;
                    }
                    @Override public void close() throws IOException { out.close(); }
                };
            }
            @Override public PositionOutputStream createOrOverwrite(long h) throws IOException { return create(h); }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 0; }
            @Override public String getPath() { return "memory://test.parquet"; }
        };
        try (var w = ExampleParquetWriter.builder(of).withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
            for (Group g : gc.create(new SimpleGroupFactory(schema))) w.write(g);
        }
        return out.toByteArray();
    }

    static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.parquet"); }
        };
    }
}
```

**Fails with:** `AssertionError: expected DOUBLE but was KEYWORD` — `Float16LogicalTypeAnnotation` never checked.

#### Why it happens

`convertParquetTypeToEsql()` does not check for `Float16LogicalTypeAnnotation`. FIXED_LEN_BYTE_ARRAY falls to `DataType.KEYWORD` (line 223). The 2 bytes are wrapped in `BytesRef` without decoding.

#### Fix

Check for `Float16LogicalTypeAnnotation`, map to `DataType.DOUBLE`. In the read path, decode using Java 21's `Float.float16ToFloat()` and widen to double. ~15 lines.

---

### PARQUET-3: INT96 timestamps silently return NULL

**Estimate:** 0.5w
**Priority:** MVP — ⬛ all values NULL, no error
**Affected types:** INT96

#### Prevalence

Medium but high-impact for affected users. INT96 was the default timestamp format across the Hadoop ecosystem until Spark 3.0 switched to INT64+MICROS in June 2020. Files written before that cutoff by Spark, Hive, or Impala contain INT96 timestamps and will until someone re-exports them. New files from modern tools do not use INT96.

#### User impact

When a Parquet file contains INT96 columns (the legacy timestamp encoding used by Spark <3.0 and Impala), the column appears in the schema with type `unsupported`. Every row returns NULL for this column. Running `| KEEP timestamp_col` works — the column is present — but every value is null. `| WHERE timestamp_col IS NOT NULL` returns zero rows. `| STATS COUNT(timestamp_col)` returns 0.

This is particularly confusing because the file clearly has timestamp data (visible in other tools like `parquet-tools` or DuckDB), and the column appears in the schema — it just looks empty. There is no error message explaining why.

**Testing gap:** No test for INT96 physical type. `ParquetFormatReaderTests` uses only INT32, INT64, FLOAT, DOUBLE, BOOLEAN, and BINARY. No test data contains INT96 columns.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.List;

public class ReproParquet3Test extends ESTestCase {

    // INT96: 12-byte legacy timestamp (Julian day + nanos-of-day) returns UNSUPPORTED/NULL
    public void testInt96TimestampReturnsNull() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT96).named("ts")
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("id")
            .named("test_schema");
        // Encode 2024-01-01T00:00:00Z as INT96: Julian day 2460311, 0 nanos within day
        ByteBuffer buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(0L);
        buf.putInt(2460311);
        byte[] data = createParquetFile(schema, f -> {
            Group g = f.newGroup();
            g.add("ts", Binary.fromConstantByteArray(buf.array()));
            g.add("id", 1L);
            return List.of(g);
        });
        var meta = new ParquetFormatReader(blockFactory).metadata(createStorageObject(data));
        assertNotEquals(
            "PARQUET-3: INT96 should be recognized as a timestamp, not UNSUPPORTED",
            DataType.UNSUPPORTED, meta.schema().get(0).dataType());
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface GroupCreator { List<Group> create(SimpleGroupFactory f); }

    static byte[] createParquetFile(MessageType schema, GroupCreator gc) throws IOException {
        var out = new ByteArrayOutputStream();
        OutputFile of = new OutputFile() {
            @Override public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    long pos = 0;
                    @Override public long getPos() { return pos; }
                    @Override public void write(int b) throws IOException { out.write(b); pos++; }
                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len); pos += len;
                    }
                    @Override public void close() throws IOException { out.close(); }
                };
            }
            @Override public PositionOutputStream createOrOverwrite(long h) throws IOException { return create(h); }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 0; }
            @Override public String getPath() { return "memory://test.parquet"; }
        };
        try (var w = ExampleParquetWriter.builder(of).withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
            for (Group g : gc.create(new SimpleGroupFactory(schema))) w.write(g);
        }
        return out.toByteArray();
    }

    static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.parquet"); }
        };
    }
}
```

**Fails with:** `AssertionError: PARQUET-3: INT96 should be recognized as a timestamp, not UNSUPPORTED. Actual: UNSUPPORTED`.

#### Why it happens

INT96 falls through the `default` branch in `convertParquetTypeToEsql()` at line 225, returning `DataType.UNSUPPORTED`. See the [INT96 section](#int96) for the full history of this format.

INT96 is a 12-byte format: 8 bytes for nanoseconds within the day + 4 bytes for the Julian day number. Unlike INT64 timestamps, INT96 has no `LogicalTypeAnnotation` — the physical type alone implies "timestamp." The code at line 215 only recognizes timestamps via `TimestampLogicalTypeAnnotation`, so bare INT96 is not recognized.

```java
// ParquetFormatReader.java:225 — INT96 hits the default case
default -> DataType.UNSUPPORTED;
```

At read time, UNSUPPORTED columns produce `blockFactory.newConstantNullBlock(rowsToRead)` (line 348), so every value is null.

#### Fix

Add `case INT96` to the switch in `convertParquetTypeToEsql()`, map to `DataType.DATE_NANOS` (INT96 carries nanosecond precision and we should preserve it). In the read path, decode the 12 bytes (little-endian):

```java
long nanosOfDay = ByteBuffer.wrap(bytes, 0, 8).order(LITTLE_ENDIAN).getLong();
int julianDay = ByteBuffer.wrap(bytes, 8, 4).order(LITTLE_ENDIAN).getInt();
long epochNanos = ((long)(julianDay - 2440588)) * 86400000000000L + nanosOfDay;
```

The constant `2440588` is the Julian day number for the Unix epoch (1970-01-01). This is a well-known conversion implemented by every major Parquet reader (Spark, PyArrow, DuckDB, parquet-mr). ~10 lines of code.

---

### PARQUET-10: LIST of primitives silently returns NULL

**Estimate:** 1w
**Priority:** MVP — ⬛ all values NULL, no error
**Affected types:** LIST of primitive types (LIST<STRING>, LIST<INT>, LIST<FLOAT>, etc.)

#### Prevalence

**LIST is the 3rd most common Parquet type** after primitive scalars and STRING. Every Spark DataFrame with an array column produces LIST. Iceberg tables use LIST extensively. Tags, categories, multi-valued labels, and vector embeddings (`LIST<FLOAT>`, `LIST<FLOAT16>`) are all LIST columns. Without LIST support, a huge class of Parquet files has columns that silently appear as NULL — users see the column name in the schema, expect data, and get nothing.

#### User impact

When a Parquet file contains LIST columns (e.g., `LIST<STRING>` for tags, `LIST<DOUBLE>` for embeddings), array columns appear in the schema as `unsupported`. Every row returns NULL. The column is visible in `| KEEP *` but every value is null. No error message. A `tags` column containing `["red", "blue"]` shows as null. An `embedding` column containing 768 floats shows as null.

**Testing gap:** No test for LIST type. `ParquetFormatReaderTests` only creates schemas with primitive columns. No test data contains LIST columns, despite LIST being the 3rd most common Parquet type.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

public class ReproParquet10Test extends ESTestCase {

    // LIST<STRING>: 3-level Parquet list encoding returns UNSUPPORTED/NULL
    public void testListOfPrimitivesReturnsNull() throws Exception {
        var blockFactory = blockFactory();
        MessageType schema = new MessageType(
            "test_schema",
            Types.buildGroup(Type.Repetition.REQUIRED)
                .as(LogicalTypeAnnotation.listType())
                .addField(
                    Types.buildGroup(Type.Repetition.REPEATED)
                        .addField(
                            Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("element")
                        )
                        .named("list")
                )
                .named("tags"),
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id")
        );
        byte[] data = createParquetFile(schema, f -> {
            Group g = f.newGroup();
            Group tags = g.addGroup("tags");
            tags.addGroup("list").add("element", "red");
            tags.addGroup("list").add("element", "blue");
            g.add("id", 1L);
            return List.of(g);
        });
        var meta = new ParquetFormatReader(blockFactory).metadata(createStorageObject(data));
        assertNotEquals(
            "PARQUET-10: LIST of strings should not be UNSUPPORTED",
            DataType.UNSUPPORTED, meta.schema().get(0).dataType());
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface GroupCreator { List<Group> create(SimpleGroupFactory f); }

    static byte[] createParquetFile(MessageType schema, GroupCreator gc) throws IOException {
        var out = new ByteArrayOutputStream();
        OutputFile of = new OutputFile() {
            @Override public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    long pos = 0;
                    @Override public long getPos() { return pos; }
                    @Override public void write(int b) throws IOException { out.write(b); pos++; }
                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len); pos += len;
                    }
                    @Override public void close() throws IOException { out.close(); }
                };
            }
            @Override public PositionOutputStream createOrOverwrite(long h) throws IOException { return create(h); }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 0; }
            @Override public String getPath() { return "memory://test.parquet"; }
        };
        try (var w = ExampleParquetWriter.builder(of).withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
            for (Group g : gc.create(new SimpleGroupFactory(schema))) w.write(g);
        }
        return out.toByteArray();
    }

    static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.parquet"); }
        };
    }
}
```

**Fails with:** `AssertionError: PARQUET-10: LIST of strings should not be UNSUPPORTED. Actual: UNSUPPORTED`.

#### Why it happens

`convertParquetTypeToEsql()` checks `parquetType.isPrimitive() == false` at line 206 and returns `DataType.UNSUPPORTED` immediately for all non-primitive types. LIST, MAP, STRUCT, and repeated groups are all non-primitive in Parquet's type system. The parquet-mr library fully preserves the nested schema — group children, list element types, map key/value types — but our code does not traverse the hierarchy.

```java
// ParquetFormatReader.java:206-207
if (parquetType.isPrimitive() == false) {
    return DataType.UNSUPPORTED;
}
```

At read time, UNSUPPORTED columns produce `blockFactory.newConstantNullBlock(rowsToRead)` at line 348.

#### Fix

Traverse the 3-level Parquet list encoding (`optional group <name> (LIST) { repeated group list { optional <element-type> element; } }`), extract the element type, and emit multi-value blocks via `beginPositionEntry()`/`endPositionEntry()`. The ESQL multi-value infrastructure already exists. Scoped to flat lists of primitive types only (no nested lists, no list-of-struct). Medium complexity.

---

### PARQUET-11: MAP, STRUCT, and nested LIST silently return NULL

**Estimate:** 1w each for STRUCT, MAP
**Priority:** Post-MVP — ⬛ all values NULL, no error
**Affected types:** GROUP, MAP, nested STRUCT, nested LIST, LIST of STRUCT

#### Prevalence

MAP and STRUCT are less common than LIST but appear in database exports, complex ETL pipelines, and Hive-style schemas. Nested LIST (list-of-list) is rare.

#### User impact

When a Parquet file contains GROUP, MAP, STRUCT, or nested LIST columns, columns with complex types appear in the schema as `unsupported`. Every row returns NULL. No error message.

For MAP: key-value pairs are inaccessible — a `metadata` column containing `{"env": "prod"}` shows as null.
For STRUCT: nested fields are inaccessible — an `address` column with `city` and `zip` sub-fields shows as null. Unlike NDJSON (which flattens nested objects to dot-notation), Parquet structs are not flattened.

**Testing gap:** No test for MAP, STRUCT, GROUP, or nested LIST. Same root cause as [PARQUET-10](#parquet-10-list-of-primitives-silently-returns-null) — `convertParquetTypeToEsql()` returns UNSUPPORTED for all non-primitive types.

#### Why it happens

Same as [PARQUET-10](#parquet-10-list-of-primitives-silently-returns-null) — `parquetType.isPrimitive() == false` at line 206 returns `DataType.UNSUPPORTED` for all non-primitive types.

#### Fix

**STRUCT:** Flatten to dot-notation column names (e.g., `address.city`, `address.zip`). Each leaf becomes its own ESQL column. Requires changes to schema discovery and column projection. Medium complexity.

**MAP:** Design decision needed — could flatten to `col.key`/`col.value` columns or represent as JSON string.

**Nested LIST / LIST of STRUCT:** Requires design work for how to represent nested multi-values or complex elements in ESQL's block model.

---

### PARQUET-5: UUID as raw bytes

**Estimate:** 0.5w
**Priority:** MVP — 🟡 data present but unusable for filtering or display
**Affected types:** FIXED_LEN_BYTE_ARRAY+UUID

#### Prevalence

Common. UUIDs are standard primary keys in web applications, event tracking (Segment, Snowplow), and microservice architectures. Parquet files from PostgreSQL exports, Iceberg tables, and ETL pipelines frequently contain UUID columns.

#### User impact

When a Parquet file contains UUID columns (FIXED_LEN_BYTE_ARRAY with UUID logical type), a column with UUID values (e.g., `request_id`) appears as KEYWORD but displays 16 bytes of raw binary data instead of the standard `550e8400-e29b-41d4-a716-446655440000` string format. The output is not human-readable.

Filtering does not work: `WHERE request_id == "550e8400-e29b-41d4-a716-446655440000"` returns zero matches because the stored value is 16 raw bytes, not the 36-character formatted string. Joining with another source on UUID also fails for the same reason. There is no ESQL function to convert raw bytes to a UUID string, so there is no workaround.

**Testing gap:** No test for UUID logical type annotation. `ParquetFormatReaderTests` tests BINARY+STRING (KEYWORD) but never FIXED_LEN_BYTE_ARRAY with UUID annotation. No test data contains UUID columns.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class ReproParquet5Test extends ESTestCase {

    // UUID: 16-byte FIXED_LEN_BYTE_ARRAY with UUID annotation shows raw bytes instead of formatted string
    public void testUuidShowsRawBytes() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
            .as(LogicalTypeAnnotation.uuidType())
            .named("request_id")
            .named("test_schema");
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        ByteBuffer uuidBuf = ByteBuffer.allocate(16);
        uuidBuf.putLong(uuid.getMostSignificantBits());
        uuidBuf.putLong(uuid.getLeastSignificantBits());
        byte[] data = createParquetFile(schema, f -> {
            Group g = f.newGroup();
            g.add("request_id", Binary.fromConstantByteArray(uuidBuf.array()));
            return List.of(g);
        });
        try (var iter = new ParquetFormatReader(blockFactory).read(createStorageObject(data), List.of("request_id"), 10)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            BytesRef actual = block.getBytesRef(0, new BytesRef());
            assertEquals(
                "PARQUET-5: UUID should be formatted as standard string",
                "550e8400-e29b-41d4-a716-446655440000", actual.utf8ToString());
        }
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface GroupCreator { List<Group> create(SimpleGroupFactory f); }

    static byte[] createParquetFile(MessageType schema, GroupCreator gc) throws IOException {
        var out = new ByteArrayOutputStream();
        OutputFile of = new OutputFile() {
            @Override public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    long pos = 0;
                    @Override public long getPos() { return pos; }
                    @Override public void write(int b) throws IOException { out.write(b); pos++; }
                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len); pos += len;
                    }
                    @Override public void close() throws IOException { out.close(); }
                };
            }
            @Override public PositionOutputStream createOrOverwrite(long h) throws IOException { return create(h); }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 0; }
            @Override public String getPath() { return "memory://test.parquet"; }
        };
        try (var w = ExampleParquetWriter.builder(of).withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
            for (Group g : gc.create(new SimpleGroupFactory(schema))) w.write(g);
        }
        return out.toByteArray();
    }

    static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.parquet"); }
        };
    }
}
```

**Fails with:** `AssertionError` — raw 16 bytes instead of formatted UUID string.

#### Why it happens

`convertParquetTypeToEsql()` does not check for `UUIDLogicalTypeAnnotation`. The FIXED_LEN_BYTE_ARRAY case (line 217) only checks `StringLogicalTypeAnnotation` (line 219), then falls to `DataType.KEYWORD` (line 223). The 16-byte raw UUID value is wrapped in `BytesRef` without any formatting.

```java
// ParquetFormatReader.java:217-224 — UUID annotation not recognized
case BINARY, FIXED_LEN_BYTE_ARRAY -> {
    if (logical instanceof StringLogicalTypeAnnotation) { yield DataType.KEYWORD; }
    yield DataType.KEYWORD;  // UUID falls here — raw bytes
}
```

#### Fix

Check for `UUIDLogicalTypeAnnotation`, keep `DataType.KEYWORD`, but format the 16 bytes as `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` in the read path. ~10 lines using manual hex formatting or `new UUID(msb, lsb).toString()`.

---

### PARQUET-4: Unsigned integer overflow

**Estimate:** 0.5w
**Priority:** Post-MVP — 🟡 silently wrong for large values, no error
**Affected types:** INT32+UINT_32, INT64+UINT_64 (also INT32+UINT_8 for values 128-255)

#### Prevalence

Low-medium. UINT_32 appears in C/C++-origin datasets (protobuf serialization, network captures, game telemetry). UINT_64 is used for unsigned counters and hash IDs. Most analytics workflows use signed integers.

#### User impact

When a Parquet file contains unsigned integer columns (UINT_8, UINT_32, UINT_64) with values exceeding the signed range, the following occurs:

**UINT_32:** A column of unsigned 32-bit integers shows correct values from 0 to 2,147,483,647. Values from 2,147,483,648 to 4,294,967,295 appear as negative numbers. For example, an `object_id` of `3000000000` shows as `-1294967296`. A query like `WHERE object_id > 0` silently drops roughly half the valid unsigned range. `ORDER BY object_id` puts large IDs (which appear negative) before small ones.

**UINT_64:** A column of unsigned 64-bit integers shows correct values up to 9,223,372,036,854,775,807. Larger values wrap negative. This affects very large counters, hash values, and snowflake IDs near the upper end of the unsigned 64-bit range.

**UINT_8:** Values 128-255 appear negative. In practice, UINT_8 columns with values >127 are extremely rare, so this is unlikely to be encountered.

No errors or warnings are shown in any case.

**Testing gap:** No test for unsigned integer annotations. `ParquetFormatReaderTests` uses bare INT32 (INTEGER) and INT64 (LONG) without `IntLogicalTypeAnnotation`. No test data contains unsigned columns.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

public class ReproParquet4Test extends ESTestCase {

    // UINT_32: value 3,000,000,000 overflows signed int to -1,294,967,296
    public void testUint32OverflowToNegative() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false)) // unsigned
            .named("object_id")
            .named("test_schema");
        int unsignedBits = (int) 3_000_000_000L; // negative in signed int
        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("object_id", unsignedBits)));
        var meta = new ParquetFormatReader(blockFactory).metadata(createStorageObject(data));
        assertEquals(
            "PARQUET-4: UINT_32 should map to LONG to hold unsigned 32-bit range",
            DataType.LONG, meta.schema().get(0).dataType());
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface GroupCreator { List<Group> create(SimpleGroupFactory f); }

    static byte[] createParquetFile(MessageType schema, GroupCreator gc) throws IOException {
        var out = new ByteArrayOutputStream();
        OutputFile of = new OutputFile() {
            @Override public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    long pos = 0;
                    @Override public long getPos() { return pos; }
                    @Override public void write(int b) throws IOException { out.write(b); pos++; }
                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len); pos += len;
                    }
                    @Override public void close() throws IOException { out.close(); }
                };
            }
            @Override public PositionOutputStream createOrOverwrite(long h) throws IOException { return create(h); }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 0; }
            @Override public String getPath() { return "memory://test.parquet"; }
        };
        try (var w = ExampleParquetWriter.builder(of).withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
            for (Group g : gc.create(new SimpleGroupFactory(schema))) w.write(g);
        }
        return out.toByteArray();
    }

    static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.parquet"); }
        };
    }
}
```

**Fails with:** `AssertionError: expected LONG but was INTEGER` — `IntLogicalTypeAnnotation.isSigned()` never checked.

#### Why it happens

`convertParquetTypeToEsql()` does not check for `IntLogicalTypeAnnotation`. The INT32 case (line 214) only checks `DateLogicalTypeAnnotation`. The `IntLogicalTypeAnnotation` class provides `isSigned()` and `getBitWidth()`, but neither is called. Java's `cr.getInteger()` and `cr.getLong()` always return signed values.

```java
// ParquetFormatReader.java:214 — only Date checked; IntLogicalTypeAnnotation ignored
case INT32 -> logical instanceof DateLogicalTypeAnnotation ? DataType.DATETIME : DataType.INTEGER;
```

#### Fix

For UINT_32: detect `IntLogicalTypeAnnotation` with `isSigned() == false && getBitWidth() == 32`, map to `DataType.LONG`, read via `Integer.toUnsignedLong(cr.getInteger())`. For UINT_64: map to `DataType.UNSIGNED_LONG`, read via `cr.getLong()` (raw bits are correct; only the semantic interpretation changes). UINT_8 and UINT_16 fit in signed int32, so no urgent fix needed.

---

### PARQUET-6: TIME types as raw integers

**Estimate:** 0.5w
**Priority:** Post-MVP — 🟡 numerically correct but not human-readable
**Affected types:** INT32+TIME(MILLIS), INT64+TIME(MICROS), INT64+TIME(NANOS)

#### TIMESTAMP vs TIME

Parquet has two temporal annotation families that look similar but serve very different purposes:

- **TIMESTAMP** — a point in time (e.g., `2024-01-15T10:30:00Z`). This is what log entries, event data, and time-series analytics are built on. Ubiquitous in Parquet files. Supported with MILLIS/MICROS/NANOS units.
- **TIME** — a time-of-day with no date component (e.g., `10:30:00`). Represents concepts like "store opening hours" or "scheduled meeting time." It is a relational database modeling concept, uncommon in analytics Parquet files. Only appears when someone exports a database table (PostgreSQL, MySQL) to Parquet via JDBC.

We prioritize TIMESTAMP because our target audience is log analytics and data lake querying, where virtually every temporal column is a TIMESTAMP. TIME is Post-MVP because it's rare and ESQL has no TIME data type to map to — unlike TIMESTAMP where the fix is a unit conversion, TIME requires either a new ESQL type or a cosmetic format-as-string workaround.

#### Prevalence

Low. TIME columns are uncommon in Parquet — they are a relational database concept (see above). They appear in JDBC-exported files from databases with explicit TIME columns (PostgreSQL, MySQL).

#### User impact

When a Parquet file contains TIME columns (TIME_MILLIS, TIME_MICROS, or TIME_NANOS), a `meeting_start` column shows `43200000` (milliseconds since midnight) instead of `12:00:00`. The number is mathematically correct — 43,200,000 ms equals 12 hours — but users have no way to know the unit or interpret the value without external knowledge. For MICROS, the value is `43200000000`; for NANOS, `43200000000000`.

**Testing gap:** No test for TIME logical type annotations. `ParquetFormatReaderTests` uses bare INT32 (INTEGER) without `TimeLogicalTypeAnnotation`. No test data contains TIME columns.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

public class ReproParquet6Test extends ESTestCase {

    // TIME(MILLIS): 43200000 (12:00:00 as millis-since-midnight) shown as raw integer
    public void testTimeMillisAsRawInteger() throws Exception {
        var blockFactory = blockFactory();
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("start_time")
            .named("test_schema");
        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("start_time", 43200000)));
        var meta = new ParquetFormatReader(blockFactory).metadata(createStorageObject(data));
        assertNotEquals(
            "PARQUET-6: TIME_MILLIS should not map to raw INTEGER",
            DataType.INTEGER, meta.schema().get(0).dataType());
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface GroupCreator { List<Group> create(SimpleGroupFactory f); }

    static byte[] createParquetFile(MessageType schema, GroupCreator gc) throws IOException {
        var out = new ByteArrayOutputStream();
        OutputFile of = new OutputFile() {
            @Override public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    long pos = 0;
                    @Override public long getPos() { return pos; }
                    @Override public void write(int b) throws IOException { out.write(b); pos++; }
                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len); pos += len;
                    }
                    @Override public void close() throws IOException { out.close(); }
                };
            }
            @Override public PositionOutputStream createOrOverwrite(long h) throws IOException { return create(h); }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 0; }
            @Override public String getPath() { return "memory://test.parquet"; }
        };
        try (var w = ExampleParquetWriter.builder(of).withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
            for (Group g : gc.create(new SimpleGroupFactory(schema))) w.write(g);
        }
        return out.toByteArray();
    }

    static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.parquet"); }
        };
    }
}
```

**Fails with:** `AssertionError: PARQUET-6: TIME_MILLIS should not map to raw INTEGER. Actual: INTEGER`.

#### Why it happens

ESQL has no TIME data type. `convertParquetTypeToEsql()` does not check for `TimeLogicalTypeAnnotation`. TIME(MILLIS) on INT32 falls to `DataType.INTEGER` (line 214). TIME(MICROS/NANOS) on INT64 falls to `DataType.LONG` (line 215). There is no target type to map to.

This is fundamentally different from the TIMESTAMP fix (PARQUET-2), which is a unit conversion. TIMESTAMP has a target type (DATETIME); TIME does not. A real fix requires adding a TIME data type to ESQL, which is a larger effort not justified by the low prevalence of TIME columns in Parquet.

#### Fix

Could map to `DataType.KEYWORD` and format as `HH:mm:ss.SSS` string, losing arithmetic capability but gaining readability. Or keep as INTEGER/LONG (current behavior) and document the unit. A proper fix requires an ESQL TIME type, which is out of scope.

---

### PARQUET-7: JSON/BSON as opaque keyword

**Estimate:** 0.5w
**Priority:** Post-MVP — 🟡 data present but not structured
**Affected types:** BINARY+JSON, BINARY+BSON

#### Prevalence

Low-medium for JSON (semi-structured data pipelines, Spark streaming). Very low for BSON (MongoDB exports via custom writers only).

#### User impact

When a Parquet file contains JSON or BSON logical type columns, the behavior differs by type:

**JSON:** Column appears as KEYWORD containing the full JSON text, e.g., `{"name": "Alice", "age": 30}`. The text is readable, but there is no way to access nested fields — no `json_col.name`, no `JSON_VALUE()` function. Users can work around this with `DISSECT` or `GROK`, which is fragile. For simple JSON this is adequate.

**BSON:** Column appears as KEYWORD containing raw binary BSON bytes — mojibake characters. The data is technically present but entirely unusable without external deserialization.

**Testing gap:** No test for JSON or BSON logical type annotations. `ParquetFormatReaderTests` tests BINARY+STRING but not BINARY+JSON or BINARY+BSON.

#### Why it happens

`convertParquetTypeToEsql()` does not check for `JsonLogicalTypeAnnotation` or `BsonLogicalTypeAnnotation`. Both fall to the default KEYWORD path (line 223). ESQL has no JSON or BSON data type.

#### Fix

JSON: acceptable as-is for now — the text is at least readable. Long-term, add JSON field access functions. BSON: consider mapping to `DataType.UNSUPPORTED` (null) rather than displaying garbled bytes, or decode BSON to JSON string representation.

---

### PARQUET-9: INTERVAL as raw bytes

**Estimate:** 0.5w
**Priority:** Post-MVP — 🟡 data present but not decoded
**Affected types:** FIXED_LEN_BYTE_ARRAY+INTERVAL

#### Prevalence

Very low. The Parquet INTERVAL type is deprecated in the spec (replaced by more specific logical types). Very few modern writers produce it.

#### User impact

When a Parquet file contains INTERVAL columns (FIXED_LEN_BYTE_ARRAY(12) with INTERVAL annotation), a `duration` column shows 12 bytes of binary data as a keyword string. The three interval components (4 bytes months, 4 bytes days, 4 bytes milliseconds) are not decoded or displayed. No error.

**Testing gap:** No test for INTERVAL logical type annotation. Very low priority given the type is deprecated in the Parquet spec.

#### Why it happens

The Parquet reader does not check for interval annotations on FIXED_LEN_BYTE_ARRAY. The 12-byte value falls to `DataType.KEYWORD` (line 223). ESQL has `DATE_PERIOD` and `TIME_DURATION` types internally, but neither is mapped from Parquet INTERVAL.

#### Fix

Low priority. Could decode to a human-readable string like `P2Y3M15DT10H30M` and store as KEYWORD, or map to `DataType.UNSUPPORTED` to avoid garbled output.

---

## Appendix B: Per-Type Breakdown

Complete mapping of every Parquet type to ESQL, organized by physical type. Rows ordered by status: ⬛ unsupported → 🔴 wrong data → 🟡 partial → 🟢 works.

### BOOLEAN

| Logical Annotation | ESQL DataType | Status |
|---|---|---|
| (none) | BOOLEAN | 🟢 |

### INT32

`convertParquetTypeToEsql` checks only for `DateLogicalTypeAnnotation` (line 214). All other logical annotations fall through to `DataType.INTEGER`.

| Logical Annotation | ESQL DataType | Status | Issue |
|---|---|---|---|
| DECIMAL(p, s) | INTEGER | 🔴 | [PARQUET-1](#parquet-1-decimal-values-silently-wrong-by-factor-of-10scale) |
| INT(8, unsigned) | INTEGER | 🟡 | [PARQUET-4](#parquet-4-unsigned-integer-overflow) |
| INT(32, unsigned) | INTEGER | 🟡 | [PARQUET-4](#parquet-4-unsigned-integer-overflow) |
| TIME(MILLIS) | INTEGER | 🟡 | [PARQUET-6](#parquet-6-time-types-as-raw-integers) |
| (none) | INTEGER | 🟢 | |
| DATE | DATETIME | 🟢 | |
| INT(8, signed) | INTEGER | 🟢 | |
| INT(16, signed) | INTEGER | 🟢 | |
| INT(32, signed) | INTEGER | 🟢 | |
| INT(16, unsigned) | INTEGER | 🟢 | |

### INT64

`convertParquetTypeToEsql` checks only for `TimestampLogicalTypeAnnotation` (line 215). All other logical annotations fall through to `DataType.LONG`.

| Logical Annotation | ESQL DataType | Status | Issue |
|---|---|---|---|
| TIMESTAMP(MICROS) | DATETIME | 🔴 | [PARQUET-2](#parquet-2-microsnanos-timestamps-interpreted-as-millis) |
| TIMESTAMP(NANOS) | DATETIME | 🔴 | [PARQUET-2](#parquet-2-microsnanos-timestamps-interpreted-as-millis) |
| DECIMAL(p, s) | LONG | 🔴 | [PARQUET-1](#parquet-1-decimal-values-silently-wrong-by-factor-of-10scale) |
| INT(64, unsigned) | LONG | 🟡 | [PARQUET-4](#parquet-4-unsigned-integer-overflow) |
| TIME(MICROS) | LONG | 🟡 | [PARQUET-6](#parquet-6-time-types-as-raw-integers) |
| TIME(NANOS) | LONG | 🟡 | [PARQUET-6](#parquet-6-time-types-as-raw-integers) |
| (none) | LONG | 🟢 | |
| INT(64, signed) | LONG | 🟢 | |
| TIMESTAMP(MILLIS) | DATETIME | 🟢 | |

### INT96

Legacy timestamp encoding. See [PARQUET-3](#parquet-3-int96-timestamps-silently-return-null) for full history (Impala ~2012, replaced by INT64+TIMESTAMP in Parquet v2.6/2018, Spark 3.0 switched default June 2020).

| Logical Annotation | ESQL DataType | Status | Issue |
|---|---|---|---|
| (none / legacy timestamp) | UNSUPPORTED | ⬛ | [PARQUET-3](#parquet-3-int96-timestamps-silently-return-null) |

### FLOAT

| Logical Annotation | ESQL DataType | Status | Issue |
|---|---|---|---|
| (none) | DOUBLE | 🟢 | |

Widened from float32 to float64 losslessly.

### DOUBLE

| Logical Annotation | ESQL DataType | Status | Issue |
|---|---|---|---|
| (none) | DOUBLE | 🟢 | |

### BINARY

`convertParquetTypeToEsql` checks for `StringLogicalTypeAnnotation` (line 219). Everything else falls to `DataType.KEYWORD` (line 223).

| Logical Annotation | ESQL DataType | Status | Issue |
|---|---|---|---|
| DECIMAL(p, s) | KEYWORD | 🔴 | [PARQUET-1](#parquet-1-decimal-values-silently-wrong-by-factor-of-10scale) |
| JSON | KEYWORD | 🟡 | [PARQUET-7](#parquet-7-jsonbson-as-opaque-keyword) |
| BSON | KEYWORD | 🟡 | [PARQUET-7](#parquet-7-jsonbson-as-opaque-keyword) |
| (none) | KEYWORD | 🟢 | |
| STRING (UTF8) | KEYWORD | 🟢 | |
| ENUM | KEYWORD | 🟢 | |

### FIXED_LEN_BYTE_ARRAY

Shares the `case BINARY, FIXED_LEN_BYTE_ARRAY` branch (line 217). Same logic as BINARY.

| Logical Annotation | ESQL DataType | Status | Issue |
|---|---|---|---|
| DECIMAL(p, s) | KEYWORD | 🔴 | [PARQUET-1](#parquet-1-decimal-values-silently-wrong-by-factor-of-10scale) |
| FLOAT16 | KEYWORD | 🔴 | [PARQUET-8](#parquet-8-float16-as-raw-bytes) |
| UUID | KEYWORD | 🟡 | [PARQUET-5](#parquet-5-uuid-as-raw-bytes) |
| INTERVAL | KEYWORD | 🟡 | [PARQUET-9](#parquet-9-interval-as-raw-bytes) |
| (none) | KEYWORD | 🟢 | |

### Non-Primitive Types (GROUP / LIST / MAP / STRUCT)

| Parquet Type | ESQL DataType | Status | Issue |
|---|---|---|---|
| LIST (of primitives) | UNSUPPORTED | ⬛ | [PARQUET-10](#parquet-10-list-of-primitives-silently-returns-null) |
| GROUP | UNSUPPORTED | ⬛ | [PARQUET-11](#parquet-11-map-struct-and-nested-list-silently-return-null) |
| LIST (nested / of struct) | UNSUPPORTED | ⬛ | [PARQUET-11](#parquet-11-map-struct-and-nested-list-silently-return-null) |
| MAP | UNSUPPORTED | ⬛ | [PARQUET-11](#parquet-11-map-struct-and-nested-list-silently-return-null) |
| Nested STRUCT | UNSUPPORTED | ⬛ | [PARQUET-11](#parquet-11-map-struct-and-nested-list-silently-return-null) |
