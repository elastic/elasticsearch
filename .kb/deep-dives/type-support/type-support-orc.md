# ORC Type Support in ES|QL External Data Sources

ORC uses `TypeDescription.Category` enum with 19 values to describe column types. ES|QL's ORC reader (Apache ORC 2.2.2, Hive Storage API 2.8.1) maps these to ESQL data types in `OrcFormatReader.convertOrcTypeToEsql()` (`OrcFormatReader.java:167-179`). The read path converts ORC's columnar `ColumnVector` into ESQL `Block` in `createBlock()` (`OrcFormatReader.java:270-301`).

The complete per-type breakdown is in [Appendix](#appendix-per-type-breakdown). The summary and issue tables below focus on what's broken.

## Summary

| Status | Count | Types |
|---|---|---|
| 🟢 Fully works | 10 | BOOLEAN, BYTE, SHORT, INT, LONG, DATE, FLOAT, DOUBLE, STRING, VARCHAR |
| 🔴 Crash | 1 | DECIMAL (all precisions) |
| 🟡 Partial | 3 | CHAR (trailing spaces — see note), BINARY (raw bytes as KEYWORD), TIMESTAMP/TIMESTAMP_INSTANT (nanosecond truncation) |
| ⬛ Unsupported (null) | 4 | LIST, MAP, STRUCT, UNION |

**Note on CHAR:** The existing document described trailing-space padding as a bug. Testing showed the ORC library strips trailing spaces during the write-read cycle, so the issue does not manifest. CHAR is counted as 🟢 in practice but kept as 🟡 because the reader does not explicitly strip spaces — files produced by other writers (e.g., Hive) may still contain padded values.

### MVP

| Status | Type | Prevalence | Description | Issue |
|---|---|---|---|---|
| ⬛ | LIST of primitives | 2nd most common complex type. Spark arrays, tags, embeddings. | LIST of primitives silently returns NULL | [ORC-4](#orc-4-list-of-primitives-silently-returns-null) |
| 🔴 | DECIMAL (all precisions) | Ubiquitous. Every financial/e-commerce dataset. Hive default for money. | Decimal columns crash query | [ORC-1](#orc-1-decimal-columns-crash-query) |

### Post-MVP

| Status | Type | Prevalence | Description | Issue |
|---|---|---|---|---|
| ⬛ | MAP, STRUCT, UNION | Medium (MAP/STRUCT), very rare (UNION). DB exports, ETL pipelines. | MAP, STRUCT, UNION silently return NULL | [ORC-5](#orc-5-map-struct-union-silently-return-null) |
| 🟡 | TIMESTAMP/TIMESTAMP_INSTANT with sub-ms precision | Common type, but sub-ms precision is rare in practice. | Timestamp nanosecond precision silently truncated | [ORC-2](#orc-2-timestamp-nanosecond-precision-silently-truncated) |
| 🟡 | BINARY | Rare. Serialized protobuf, Avro, custom formats. | Binary data shown as garbled keyword | [ORC-3](#orc-3-binary-data-shown-as-garbled-keyword) |

---

## Issue Details

### ORC-1: Decimal columns crash query

**Estimate:** 0.5w
**Priority:** MVP — 🔴 query crashes, no data returned
**Affected types:** DECIMAL (all precisions — both `DecimalColumnVector` and `Decimal64ColumnVector`)

#### Prevalence

Ubiquitous. DECIMAL is one of the most common ORC types in analytics workloads. Hive defaults to DECIMAL for monetary data. Spark writes DECIMAL columns for `BigDecimal` and fixed-point types. Every financial, e-commerce, billing, and accounting dataset in ORC format uses DECIMAL. Any user querying ORC files from a Hive warehouse, Spark pipeline, or Presto/Trino output will hit this crash on every DECIMAL column.

#### User impact

When an ORC file contains any DECIMAL column (regardless of precision or scale), the query fails immediately with `QlIllegalArgumentException: Unsupported column type: DecimalColumnVector`. The entire query fails — not just the decimal column. No results are returned. The user cannot work around this by avoiding the column (`| DROP price`) because the crash occurs during page conversion, which processes all columns in the batch.

**Testing gap:** No test for any DECIMAL variant. `OrcFormatReaderTests` tests LONG, INT, DOUBLE, STRING, BOOLEAN, TIMESTAMP_INSTANT, and DATE — but never DECIMAL. No test data contains DECIMAL columns despite DECIMAL being one of the most common ORC types.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;

public class ReproOrc1Test extends ESTestCase {

    // DECIMAL(10,2): value 123.45 crashes with QlIllegalArgumentException
    public void testDecimalCrashes() throws Exception {
        var blockFactory = blockFactory();
        TypeDescription schema = TypeDescription.createStruct()
            .addField("price", TypeDescription.createDecimal().withScale(2).withPrecision(10));
        byte[] data = createOrcFile(schema, batch -> {
            batch.size = 1;
            DecimalColumnVector decCol = (DecimalColumnVector) batch.cols[0];
            decCol.set(0, new HiveDecimalWritable("123.45"));
        });
        var reader = new OrcFormatReader(blockFactory);
        // Should not throw — should read the decimal value as a double
        try (var iter = reader.read(createStorageObject(data), List.of("price"), 1024)) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals("ORC-1: DECIMAL(10,2) should be read as 123.45",
                123.45, block.getDouble(0), 0.001);
        }
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface BatchPopulator { void populate(VectorizedRowBatch batch); }

    byte[] createOrcFile(TypeDescription schema, BatchPopulator pop) throws IOException {
        java.nio.file.Path tempFile = createTempFile();
        Files.delete(tempFile);
        Path orcPath = new Path(tempFile.toUri());
        Configuration conf = new Configuration(false);
        conf.set("orc.key.provider", "memory");
        NoPermissionLocalFileSystem localFs = new NoPermissionLocalFileSystem();
        localFs.setConf(conf);
        try (Writer w = OrcFile.createWriter(orcPath, OrcFile.writerOptions(conf)
                .setSchema(schema).fileSystem(localFs)
                .compress(org.apache.orc.CompressionKind.NONE))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            pop.populate(batch);
            w.addRowBatch(batch);
        }
        return Files.readAllBytes(tempFile);
    }

    static class NoPermissionLocalFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override @SuppressForbidden(reason = "Bypass Hadoop Shell.<clinit>")
        protected OutputStream createOutputStreamWithMode(Path p, boolean append,
                org.apache.hadoop.fs.permission.FsPermission perm) throws IOException {
            return new FileOutputStream(pathToFile(p), append);
        }
        @Override public void setPermission(Path p, org.apache.hadoop.fs.permission.FsPermission perm) {}
        @Override @SuppressForbidden(reason = "Hadoop API requires java.io.File")
        protected boolean mkOneDirWithMode(Path p, java.io.File p2f,
                org.apache.hadoop.fs.permission.FsPermission perm) throws IOException {
            return p2f.mkdir() || p2f.isDirectory();
        }
    }

    StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.orc"); }
        };
    }
}
```

**Fails with:** `QlIllegalArgumentException: Unsupported column type: DecimalColumnVector` at `OrcFormatReader.java:323`.

#### Why it happens

`convertOrcTypeToEsql()` correctly maps DECIMAL to `DataType.DOUBLE` (line 176). The problem is in the read path. `createBlock()` dispatches DOUBLE to `createDoubleBlock()` (line 296). Inside `createDoubleBlock()`:

1. `vector instanceof DoubleColumnVector` (line 304) — fails, because `DecimalColumnVector` is not a `DoubleColumnVector`
2. `vector instanceof LongColumnVector` (line 313) — fails, because `DecimalColumnVector` extends `ColumnVector` directly

Neither branch matches, so execution falls through to `throw new QlIllegalArgumentException(...)` (line 323).

```java
// OrcFormatReader.java:303-323 — DecimalColumnVector not handled
private Block createDoubleBlock(ColumnVector vector, int rowCount) {
    if (vector instanceof DoubleColumnVector doubleVector) { ... }      // fails
    else if (vector instanceof LongColumnVector longVector) { ... }     // fails
    throw new QlIllegalArgumentException("Unsupported column type: " + vector.getClass().getSimpleName());
}
```

The ORC reader does not import `DecimalColumnVector` at all (imports at lines 12-17 include only `BytesColumnVector`, `ColumnVector`, `DoubleColumnVector`, `LongColumnVector`, `TimestampColumnVector`, `VectorizedRowBatch`).

#### Fix

Import `DecimalColumnVector` (and `HiveDecimalWritable`). Add an `instanceof DecimalColumnVector` branch in `createDoubleBlock()` that calls `decVector.vector[i].doubleValue()` on each `HiveDecimalWritable` element. This handles all DECIMAL precisions correctly — `HiveDecimalWritable.doubleValue()` returns the properly scaled value. ~15 lines. Precision loss beyond 15 significant digits is inherent to double.

---

### ORC-2: Timestamp nanosecond precision silently truncated

**Estimate:** 0.5w
**Priority:** Post-MVP — 🟡 sub-millisecond data lost silently
**Affected types:** TIMESTAMP, TIMESTAMP_INSTANT

#### Prevalence

TIMESTAMP is extremely common in ORC files — log data, event streams, audit trails. However, the vast majority of timestamps have only millisecond precision, which works correctly. Only timestamps with sub-millisecond precision (microseconds or nanoseconds) are affected. These appear in high-frequency trading data, scientific measurements, and some database exports. The millis-precision case (the common one) is lossless.

#### User impact

When an ORC file contains timestamps with sub-millisecond precision (microseconds or nanoseconds), the sub-millisecond digits are silently dropped. A timestamp like `2024-01-15T10:30:00.123456789Z` appears as `2024-01-15T10:30:00.123Z` — the last 6 digits (`456789` nanoseconds) are lost. There is no error or warning. `STATS COUNT(*)` and `WHERE` filters based on the millisecond component still work correctly. Only sub-millisecond comparisons or aggregations are affected. For millis-precision timestamps (the common case), behavior is correct.

**Testing gap:** `OrcFormatReaderTests.testReadTimestampColumn` sets `nanos[0] = 0` and `nanos[1] = 0` — deliberately avoiding sub-millisecond precision. No test exercises non-zero sub-millisecond nanos. The `nanos[]` array functionality is completely untested.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;

public class ReproOrc2Test extends ESTestCase {

    // TIMESTAMP_INSTANT with sub-ms nanoseconds — should map to DATE_NANOS
    public void testTimestampNanosecondTruncation() throws Exception {
        var blockFactory = blockFactory();
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event_time", TypeDescription.createTimestampInstant());
        long epochMillis = Instant.parse("2024-01-15T10:30:00.123Z").toEpochMilli();
        byte[] data = createOrcFile(schema, batch -> {
            batch.size = 1;
            TimestampColumnVector tsCol = (TimestampColumnVector) batch.cols[0];
            tsCol.time[0] = epochMillis;
            tsCol.nanos[0] = 123456789; // full nanoseconds within the second
        });
        var reader = new OrcFormatReader(blockFactory);
        var meta = reader.metadata(createStorageObject(data));
        assertEquals("ORC-2: TIMESTAMP with sub-ms precision should map to DATE_NANOS",
            DataType.DATE_NANOS, meta.schema().get(0).dataType());
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface BatchPopulator { void populate(VectorizedRowBatch batch); }

    byte[] createOrcFile(TypeDescription schema, BatchPopulator pop) throws IOException {
        java.nio.file.Path tempFile = createTempFile();
        Files.delete(tempFile);
        Path orcPath = new Path(tempFile.toUri());
        Configuration conf = new Configuration(false);
        conf.set("orc.key.provider", "memory");
        NoPermissionLocalFileSystem localFs = new NoPermissionLocalFileSystem();
        localFs.setConf(conf);
        try (Writer w = OrcFile.createWriter(orcPath, OrcFile.writerOptions(conf)
                .setSchema(schema).fileSystem(localFs)
                .compress(org.apache.orc.CompressionKind.NONE))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            pop.populate(batch);
            w.addRowBatch(batch);
        }
        return Files.readAllBytes(tempFile);
    }

    static class NoPermissionLocalFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override @SuppressForbidden(reason = "Bypass Hadoop Shell.<clinit>")
        protected OutputStream createOutputStreamWithMode(Path p, boolean append,
                org.apache.hadoop.fs.permission.FsPermission perm) throws IOException {
            return new FileOutputStream(pathToFile(p), append);
        }
        @Override public void setPermission(Path p, org.apache.hadoop.fs.permission.FsPermission perm) {}
        @Override @SuppressForbidden(reason = "Hadoop API requires java.io.File")
        protected boolean mkOneDirWithMode(Path p, java.io.File p2f,
                org.apache.hadoop.fs.permission.FsPermission perm) throws IOException {
            return p2f.mkdir() || p2f.isDirectory();
        }
    }

    StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.orc"); }
        };
    }
}
```

**Fails with:** `expected DATE_NANOS but was DATETIME` — nanosecond precision not preserved.

#### Why it happens

ORC's `TimestampColumnVector` has two arrays: `long[] time` (epoch milliseconds) and `int[] nanos` (nanoseconds within the second, 0-999999999). The code at line 362 calls `tsVector.getTime(i)` which returns only the millisecond component:

```java
// OrcFormatReader.java:360-362 — nanos[] array ignored
long[] millis = new long[rowCount];
for (int i = 0; i < rowCount; i++) {
    millis[i] = tsVector.getTime(i);  // only milliseconds, nanos dropped
}
```

The sub-millisecond portion from `nanos[]` is never read. For millis-precision timestamps, `nanos[]` is zero, so this is lossless. For sub-millisecond data, up to 999,999 nanoseconds are silently dropped per timestamp.

Additionally, `convertOrcTypeToEsql()` maps both TIMESTAMP and TIMESTAMP_INSTANT to `DataType.DATETIME` (millis precision) rather than `DataType.DATE_NANOS` (nanos precision). Without changing the output type, the nanos data has nowhere to go.

#### Fix

Map TIMESTAMP/TIMESTAMP_INSTANT to `DataType.DATE_NANOS` to preserve full nanosecond precision. In the read path, compute epoch nanos:
```java
long epochNanos = (tsVector.getTime(i) / 1000) * 1_000_000_000L + tsVector.getNanos(i);
```

Alternatively, keep DATETIME (millis) as the default and only use DATE_NANOS when the file actually contains sub-millisecond data — but this requires a two-pass approach or a config option.

---

### ORC-3: Binary data shown as garbled keyword

**Estimate:** 0.5w
**Priority:** Post-MVP — 🟡 garbled output, semantically wrong
**Affected types:** BINARY

#### Prevalence

Low. BINARY columns in ORC contain raw bytes — serialized protobuf, Avro records, custom binary formats, or opaque payloads. Most analytics workloads don't query binary columns directly. They appear in database exports where a column was defined as `VARBINARY` or `BLOB`.

#### User impact

When an ORC file contains a BINARY column (e.g., serialized protobuf, Avro records, opaque payloads), the raw bytes are wrapped in `BytesRef` and exposed as KEYWORD — producing garbled characters. Null bytes, non-UTF-8 sequences, and arbitrary binary data are displayed as mojibake. String functions like `LENGTH` operate on raw bytes, not meaningful characters. No error is shown.

**Testing gap:** No test for BINARY columns. `OrcFormatReaderTests` tests STRING and VARCHAR (both map to KEYWORD correctly) but never BINARY. No test data contains BINARY columns.

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;

public class ReproOrc3Test extends ESTestCase {

    // BINARY: raw bytes wrapped in BytesRef and exposed as KEYWORD
    public void testBinaryAsGarbledKeyword() throws Exception {
        var blockFactory = blockFactory();
        TypeDescription schema = TypeDescription.createStruct()
            .addField("payload", TypeDescription.createBinary());
        byte[] rawBytes = new byte[] { 0x00, 0x01, (byte) 0xFF, (byte) 0xFE, 0x42 };
        byte[] data = createOrcFile(schema, batch -> {
            batch.size = 1;
            BytesColumnVector payloadCol = (BytesColumnVector) batch.cols[0];
            payloadCol.setVal(0, rawBytes);
        });
        var reader = new OrcFormatReader(blockFactory);
        var meta = reader.metadata(createStorageObject(data));
        assertNotEquals("ORC-3: BINARY should not map to KEYWORD",
            DataType.KEYWORD, meta.schema().get(0).dataType());
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface BatchPopulator { void populate(VectorizedRowBatch batch); }

    byte[] createOrcFile(TypeDescription schema, BatchPopulator pop) throws IOException {
        java.nio.file.Path tempFile = createTempFile();
        Files.delete(tempFile);
        Path orcPath = new Path(tempFile.toUri());
        Configuration conf = new Configuration(false);
        conf.set("orc.key.provider", "memory");
        NoPermissionLocalFileSystem localFs = new NoPermissionLocalFileSystem();
        localFs.setConf(conf);
        try (Writer w = OrcFile.createWriter(orcPath, OrcFile.writerOptions(conf)
                .setSchema(schema).fileSystem(localFs)
                .compress(org.apache.orc.CompressionKind.NONE))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            pop.populate(batch);
            w.addRowBatch(batch);
        }
        return Files.readAllBytes(tempFile);
    }

    static class NoPermissionLocalFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override @SuppressForbidden(reason = "Bypass Hadoop Shell.<clinit>")
        protected OutputStream createOutputStreamWithMode(Path p, boolean append,
                org.apache.hadoop.fs.permission.FsPermission perm) throws IOException {
            return new FileOutputStream(pathToFile(p), append);
        }
        @Override public void setPermission(Path p, org.apache.hadoop.fs.permission.FsPermission perm) {}
        @Override @SuppressForbidden(reason = "Hadoop API requires java.io.File")
        protected boolean mkOneDirWithMode(Path p, java.io.File p2f,
                org.apache.hadoop.fs.permission.FsPermission perm) throws IOException {
            return p2f.mkdir() || p2f.isDirectory();
        }
    }

    StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.orc"); }
        };
    }
}
```

**Fails with:** `ORC-3: BINARY should not map to KEYWORD. Actual: KEYWORD`.

#### Why it happens

`convertOrcTypeToEsql()` maps BINARY to `DataType.KEYWORD` (line 174). The `createBytesRefBlock()` method treats the raw bytes identically to STRING/VARCHAR — wrapping them in `BytesRef` without any interpretation. BINARY data is semantically different from text: it may contain null bytes and non-UTF-8 sequences that produce garbled output.

```java
// OrcFormatReader.java:174 — BINARY treated same as STRING
case BINARY -> DataType.KEYWORD;
```

#### Fix

Map BINARY to `DataType.UNSUPPORTED` (returns null) rather than KEYWORD to avoid garbled output. Alternatively, hex-encode the bytes and store as KEYWORD (`"00 01 ff fe 42"`). A proper fix requires an ESQL binary type, which doesn't exist.

---

### ORC-4: LIST of primitives silently returns NULL

**Estimate:** 1w
**Priority:** MVP — ⬛ all values NULL, no error
**Affected types:** LIST of primitive types (LIST<STRING>, LIST<INT>, LIST<DOUBLE>, etc.)

#### Prevalence

**LIST is the 2nd most common complex ORC type** after STRUCT. Hive tables with `ARRAY<STRING>`, Spark DataFrames with array columns, and tag/label columns all produce LIST. Without LIST support, array columns silently appear as NULL — users see the column name in the schema but get no data.

#### User impact

When an ORC file contains a LIST column (e.g., `ARRAY<STRING>` for tags, `ARRAY<DOUBLE>` for embeddings), the column appears in the schema as `unsupported` and every row returns NULL. The column is visible in `| KEEP *` but every value is null. No error message. A `tags` column containing `["red", "blue"]` shows as null.

**Testing gap:** `OrcFormatReaderTests.testReadUnsupportedTypeReturnsNullBlock` tests that LIST returns UNSUPPORTED and null blocks — this confirms the current behavior but is not a gap. The gap is that there is no test for correct LIST reading (since it's not implemented).

#### How to reproduce

```java
package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;

public class ReproOrc4Test extends ESTestCase {

    // LIST<STRING>: tags column silently returns NULL
    public void testListReturnsNull() throws Exception {
        var blockFactory = blockFactory();
        TypeDescription schema = TypeDescription.createStruct()
            .addField("tags", TypeDescription.createList(TypeDescription.createString()))
            .addField("id", TypeDescription.createLong());
        byte[] data = createOrcFile(schema, batch -> {
            batch.size = 1;
            ListColumnVector tagsCol = (ListColumnVector) batch.cols[0];
            BytesColumnVector tagsChild = (BytesColumnVector) tagsCol.child;
            tagsChild.ensureSize(2, false);
            tagsCol.offsets[0] = 0;
            tagsCol.lengths[0] = 2;
            tagsChild.setVal(0, "red".getBytes(StandardCharsets.UTF_8));
            tagsChild.setVal(1, "blue".getBytes(StandardCharsets.UTF_8));
            ((LongColumnVector) batch.cols[1]).vector[0] = 1L;
        });
        var reader = new OrcFormatReader(blockFactory);
        var meta = reader.metadata(createStorageObject(data));
        assertNotEquals("ORC-4: LIST of strings should not be UNSUPPORTED",
            DataType.UNSUPPORTED, meta.schema().get(0).dataType());
    }

    // --- Helpers ---

    static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker("none")).build();
    }

    @FunctionalInterface interface BatchPopulator { void populate(VectorizedRowBatch batch); }

    byte[] createOrcFile(TypeDescription schema, BatchPopulator pop) throws IOException {
        java.nio.file.Path tempFile = createTempFile();
        Files.delete(tempFile);
        Path orcPath = new Path(tempFile.toUri());
        Configuration conf = new Configuration(false);
        conf.set("orc.key.provider", "memory");
        NoPermissionLocalFileSystem localFs = new NoPermissionLocalFileSystem();
        localFs.setConf(conf);
        try (Writer w = OrcFile.createWriter(orcPath, OrcFile.writerOptions(conf)
                .setSchema(schema).fileSystem(localFs)
                .compress(org.apache.orc.CompressionKind.NONE))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            pop.populate(batch);
            w.addRowBatch(batch);
        }
        return Files.readAllBytes(tempFile);
    }

    static class NoPermissionLocalFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override @SuppressForbidden(reason = "Bypass Hadoop Shell.<clinit>")
        protected OutputStream createOutputStreamWithMode(Path p, boolean append,
                org.apache.hadoop.fs.permission.FsPermission perm) throws IOException {
            return new FileOutputStream(pathToFile(p), append);
        }
        @Override public void setPermission(Path p, org.apache.hadoop.fs.permission.FsPermission perm) {}
        @Override @SuppressForbidden(reason = "Hadoop API requires java.io.File")
        protected boolean mkOneDirWithMode(Path p, java.io.File p2f,
                org.apache.hadoop.fs.permission.FsPermission perm) throws IOException {
            return p2f.mkdir() || p2f.isDirectory();
        }
    }

    StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override public InputStream newStream() { return new ByteArrayInputStream(data); }
            @Override public InputStream newStream(long pos, long len) {
                return new ByteArrayInputStream(data, (int) pos, (int) Math.min(len, data.length - pos));
            }
            @Override public long length() { return data.length; }
            @Override public Instant lastModified() { return Instant.now(); }
            @Override public boolean exists() { return true; }
            @Override public StoragePath path() { return StoragePath.of("memory://test.orc"); }
        };
    }
}
```

**Fails with:** `ORC-4: LIST of strings should not be UNSUPPORTED. Actual: UNSUPPORTED`.

#### Why it happens

`convertOrcTypeToEsql()` maps all non-primitive complex types to `DataType.UNSUPPORTED` via the `default` branch (line 177). At read time, the `default` case in `createBlock()` (line 299) produces `blockFactory.newConstantNullBlock(rowCount)`.

```java
// OrcFormatReader.java:177 — LIST, MAP, STRUCT, UNION all hit default
default -> DataType.UNSUPPORTED;
```

ORC's `ListColumnVector` provides structured access to array data — `offsets[]` and `lengths[]` arrays index into a child `ColumnVector`. The infrastructure for reading array data exists in the ORC library, but our code never accesses it.

#### Fix

Import `ListColumnVector`. In `convertOrcTypeToEsql()`, when category is LIST, extract the child type (`orcType.getChildren().get(0)`), convert it recursively, and return that type (the ESQL block will be multi-valued). In `createBlock()`, add a LIST case that reads `offsets[]`/`lengths[]` from the `ListColumnVector` and emits multi-value blocks via `beginPositionEntry()`/`endPositionEntry()`.

---

### ORC-5: MAP, STRUCT, UNION silently return NULL

**Estimate:** 1w each for STRUCT, MAP
**Priority:** Post-MVP — ⬛ all values NULL, no error
**Affected types:** MAP, STRUCT, UNION

#### Prevalence

MAP appears in ETL pipelines and key-value metadata columns. STRUCT appears in nested schema designs and database exports. UNION is deprecated since Hive 3.0 and extremely rare.

#### User impact

When an ORC file contains MAP, STRUCT, or UNION columns, they appear in the schema as `unsupported` and every row returns NULL. No error message. For MAP: a `metadata` column containing `{"env": "prod"}` shows as null. For STRUCT: an `address` column with `city` and `zip` sub-fields shows as null. For UNION: the discriminated union value is inaccessible.

**Testing gap:** No test for MAP, STRUCT, or UNION columns.

#### Why it happens

Same as [ORC-4](#orc-4-list-of-primitives-silently-returns-null) — all non-primitive types fall through the `default` branch in `convertOrcTypeToEsql()` (line 177), mapping to `DataType.UNSUPPORTED`.

#### Fix

**STRUCT:** Flatten to dot-notation column names. Each leaf becomes its own ESQL column.

**MAP:** Design decision needed — flatten to key/value columns or represent as JSON string.

---

## Appendix: Per-Type Breakdown

Complete mapping of every ORC type to ESQL, organized by vector type. Rows ordered by status: ⬛ unsupported → 🔴 wrong data → 🟡 partial → 🟢 works.

### LongColumnVector Types

| ORC Category | ESQL DataType | Status | Issue |
|---|---|---|---|
| BOOLEAN | BOOLEAN | 🟢 | |
| BYTE | INTEGER | 🟢 | |
| SHORT | INTEGER | 🟢 | |
| INT | INTEGER | 🟢 | |
| LONG | LONG | 🟢 | |
| DATE | DATETIME | 🟢 | |

### DoubleColumnVector Types

| ORC Category | ESQL DataType | Status | Issue |
|---|---|---|---|
| FLOAT | DOUBLE | 🟢 | |
| DOUBLE | DOUBLE | 🟢 | |

### BytesColumnVector Types

| ORC Category | ESQL DataType | Status | Issue |
|---|---|---|---|
| BINARY | KEYWORD | 🟡 | [ORC-3](#orc-3-binary-data-shown-as-garbled-keyword) |
| STRING | KEYWORD | 🟢 | |
| VARCHAR | KEYWORD | 🟢 | |
| CHAR | KEYWORD | 🟢 | Trailing spaces stripped by ORC library during write-read cycle |

### TimestampColumnVector Types

| ORC Category | ESQL DataType | Status | Issue |
|---|---|---|---|
| TIMESTAMP | DATETIME | 🟡 | [ORC-2](#orc-2-timestamp-nanosecond-precision-silently-truncated) |
| TIMESTAMP_INSTANT | DATETIME | 🟡 | [ORC-2](#orc-2-timestamp-nanosecond-precision-silently-truncated) |

### DecimalColumnVector Types

| ORC Category | ESQL DataType | Status | Issue |
|---|---|---|---|
| DECIMAL (any precision) | DOUBLE | 🔴 | [ORC-1](#orc-1-decimal-columns-crash-query) |

### Complex Types

| ORC Category | ESQL DataType | Status | Issue |
|---|---|---|---|
| LIST | UNSUPPORTED | ⬛ | [ORC-4](#orc-4-list-of-primitives-silently-returns-null) |
| MAP | UNSUPPORTED | ⬛ | [ORC-5](#orc-5-map-struct-union-silently-return-null) |
| STRUCT | UNSUPPORTED | ⬛ | [ORC-5](#orc-5-map-struct-union-silently-return-null) |
| UNION | UNSUPPORTED | ⬛ | [ORC-5](#orc-5-map-struct-union-silently-return-null) |
