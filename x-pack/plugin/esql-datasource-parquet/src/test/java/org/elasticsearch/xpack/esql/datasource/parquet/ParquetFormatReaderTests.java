/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.UninitializedArrays;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class ParquetFormatReaderTests extends ESTestCase {

    @BeforeClass
    public static void assertUninitializedArraysFastPath() {
        // The parquet read path relies on UninitializedArrays' Unsafe-backed allocation;
        // fail loudly rather than silently exercising the zero-initialized fallback.
        UninitializedArrays.ensureUnsafeEnabled();
    }

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * The schema-vs-planner mismatch fallback in {@code ParquetFormatReader} now emits a response
     * Warning header alongside the existing {@code logger.warn}. Drop accumulated warnings so the
     * parent {@code ensureNoWarnings} post-check passes; tests that assert on them call
     * {@code drainWarnings()} from inside the test method.
     */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            // Swap in a fresh empty context (we deliberately do not restore() - the parent
            // ESTestCase provides a fresh threadContext for the next test, so the stashed one
            // can be discarded).
            threadContext.stashContext();
        }
    }

    public void testFormatName() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        assertEquals("parquet", reader.formatName());
    }

    public void testFileExtensions() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<String> extensions = reader.fileExtensions();
        assertEquals(2, extensions.size());
        assertTrue(extensions.contains(".parquet"));
        assertTrue(extensions.contains(".parq"));
    }

    public void testReadSchemaFromSimpleParquet() throws Exception {
        // Create a simple parquet file with known schema
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("active")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("name", "Alice");
            group1.add("age", 30);
            group1.add("active", true);
            return List.of(group1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        List<Attribute> attributes = metadata.schema();

        assertEquals(4, attributes.size());

        assertEquals("id", attributes.get(0).name());
        assertEquals(DataType.LONG, attributes.get(0).dataType());

        assertEquals("name", attributes.get(1).name());
        assertEquals(DataType.KEYWORD, attributes.get(1).dataType());

        assertEquals("age", attributes.get(2).name());
        assertEquals(DataType.INTEGER, attributes.get(2).dataType());

        assertEquals("active", attributes.get(3).name());
        assertEquals(DataType.BOOLEAN, attributes.get(3).dataType());
    }

    /**
     * esql-planning#1056: a top-level list column must be published under its attribute name (so
     * {@code findColumn} hits) but with an <em>unknown</em> null count, which makes {@code COUNT} /
     * {@code IS NOT NULL} decline the footer fast path and scan. Before the fix the list stats were
     * keyed by the leaf path {@code ints.list.element}, never matching the attribute {@code ints}, so
     * the column was published under no name at all. The flat control keeps its concrete null count.
     */
    public void testListColumnPublishedWithUnknownNullCount() throws Exception {
        Type intList = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("ints");
        Type id = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id");
        MessageType schema = new MessageType("test_schema", id, intList);

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> rows = new ArrayList<>();
            for (int r = 0; r < 5; r++) {
                Group g = factory.newGroup();
                g.add("id", (long) r);
                Group list = g.addGroup("ints");
                // Row 2 is a genuinely-null (empty) list; the rest are non-null 2-element lists.
                if (r != 2) {
                    list.addGroup("list").append("element", r * 10);
                    list.addGroup("list").append("element", r * 10 + 1);
                }
                rows.add(g);
            }
            return rows;
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(createStorageObject(parquetData));
        assertTrue(metadata.statistics().isPresent());
        assertTrue(metadata.statistics().get().columnStatistics().isPresent());
        Map<String, SourceStatistics.ColumnStatistics> cols = metadata.statistics().get().columnStatistics().get();

        // The list column is registered under its attribute name (not the leaf "ints.list.element")
        // so findColumn hits, but with an unknown null count so COUNT/IS NOT NULL fall back to scan.
        assertTrue("list column must be registered under its attribute name", cols.containsKey("ints"));
        assertEquals("list column null count must be unknown", OptionalLong.empty(), cols.get("ints").nullCount());

        // The flat control keeps a concrete null count — the footer fast path is preserved for it.
        assertTrue(cols.containsKey("id"));
        assertEquals(OptionalLong.of(0L), cols.get("id").nullCount());
    }

    /**
     * esql-planning#1055: a list nested in a STRUCT is surfaced by the flattener at its logical dotted
     * name {@code s.blist} (not the struct root {@code s}, and not the raw leaf {@code s.blist.list.element}).
     * The stats must therefore publish under {@code s.blist} with an <em>unknown</em> null count — like a
     * top-level list (#1056), a stats-based {@code rowCount - nullCount} count is wrong for a leaf with
     * several values per row, so COUNT falls back to a scan. No phantom marker is published under the
     * struct root {@code s}, and the flat struct leaf {@code s.a} keeps its concrete null count.
     */
    public void testStructNestedListPublishedWithUnknownNullCount() throws Exception {
        Type blist = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("blist");
        Type structS = Types.optionalGroup().required(PrimitiveType.PrimitiveTypeName.INT64).named("a").addField(blist).named("s");
        MessageType schema = new MessageType("test_schema", structS);

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> rows = new ArrayList<>();
            for (int r = 0; r < 3; r++) {
                Group g = factory.newGroup();
                Group s = g.addGroup("s");
                s.add("a", (long) r);
                Group list = s.addGroup("blist");
                list.addGroup("list").append("element", r * 10);
                list.addGroup("list").append("element", r * 10 + 1);
                rows.add(g);
            }
            return rows;
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(createStorageObject(parquetData));
        assertTrue(metadata.statistics().isPresent());
        Map<String, SourceStatistics.ColumnStatistics> cols = metadata.statistics().get().columnStatistics().get();

        // No phantom marker under the struct root: the leaf keys at its logical name, not path[0].
        assertFalse("must not publish a marker under the struct root", cols.containsKey("s"));
        // The nested list is published under its flattener name with an unknown null count (#1055).
        assertTrue("nested list must be registered under its logical name", cols.containsKey("s.blist"));
        assertEquals("nested list null count must be unknown", OptionalLong.empty(), cols.get("s.blist").nullCount());
        // The flat struct leaf still publishes normally with a concrete null count.
        assertTrue(cols.containsKey("s.a"));
        assertEquals(OptionalLong.of(0L), cols.get("s.a").nullCount());
    }

    /**
     * Parity: {@link ParquetFormatReader#metadataAsync} must resolve the same schema as the
     * synchronous {@link ParquetFormatReader#metadata}. The async path prefetches the footer tail via
     * {@code readBytesAsync} (completed here on a separate probe pool), seeds the footer-byte cache and
     * then runs the CPU-only parse; the resulting {@link SourceMetadata} must be indistinguishable
     * from the fully-synchronous path.
     */
    public void testMetadataAsyncMatchesSync() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("id", 7L);
            g.add("name", "Alice");
            g.add("age", 30);
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Synchronous baseline against its own (freshly cleared) object.
        SourceMetadata syncMeta = reader.metadata(createStorageObject(parquetData));
        List<Attribute> syncSchema = syncMeta.schema();

        // Async path over an object whose reads complete on a separate pool.
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        ExecutorService probePool = Executors.newFixedThreadPool(2);
        AtomicInteger asyncReadCount = new AtomicInteger();
        try {
            StorageObject asyncObject = createAsyncStorageObject(parquetData, probePool, asyncReadCount, null);
            PlainActionFuture<SourceMetadata> future = new PlainActionFuture<>();
            reader.metadataAsync(asyncObject, probePool, future);
            SourceMetadata asyncMeta = future.actionGet(30, TimeUnit.SECONDS);

            assertEquals("footer tail should be prefetched exactly once", 1, asyncReadCount.get());
            List<Attribute> asyncSchema = asyncMeta.schema();
            assertEquals(syncSchema.size(), asyncSchema.size());
            for (int i = 0; i < syncSchema.size(); i++) {
                assertEquals(syncSchema.get(i).name(), asyncSchema.get(i).name());
                assertEquals(syncSchema.get(i).dataType(), asyncSchema.get(i).dataType());
            }
            // The async path parses via TailBackedInputFile, a different open path than the sync
            // ParquetStorageObjectAdapter — statistics are part of the metadata contract, so assert
            // row count, byte size and per-column stats match, not just the schema.
            assertStatisticsEqual(syncMeta, asyncMeta);
        } finally {
            probePool.shutdownNow();
        }
    }

    /** Asserts that two {@link SourceMetadata} carry identical row-count, byte-size and per-column statistics. */
    private static void assertStatisticsEqual(SourceMetadata expected, SourceMetadata actual) {
        assertEquals("statistics presence must match", expected.statistics().isPresent(), actual.statistics().isPresent());
        if (expected.statistics().isPresent() == false) {
            return;
        }
        SourceStatistics expectedStats = expected.statistics().get();
        SourceStatistics actualStats = actual.statistics().get();
        assertEquals("row count", expectedStats.rowCount(), actualStats.rowCount());
        assertEquals("size in bytes", expectedStats.sizeInBytes(), actualStats.sizeInBytes());
        assertEquals(
            "column-statistics presence",
            expectedStats.columnStatistics().isPresent(),
            actualStats.columnStatistics().isPresent()
        );
        if (expectedStats.columnStatistics().isPresent() == false) {
            return;
        }
        Map<String, SourceStatistics.ColumnStatistics> expectedCols = expectedStats.columnStatistics().get();
        Map<String, SourceStatistics.ColumnStatistics> actualCols = actualStats.columnStatistics().get();
        assertEquals("column-statistics keys", expectedCols.keySet(), actualCols.keySet());
        for (Map.Entry<String, SourceStatistics.ColumnStatistics> entry : expectedCols.entrySet()) {
            SourceStatistics.ColumnStatistics expectedCol = entry.getValue();
            SourceStatistics.ColumnStatistics actualCol = actualCols.get(entry.getKey());
            String col = entry.getKey();
            assertEquals("null count [" + col + "]", expectedCol.nullCount(), actualCol.nullCount());
            assertEquals("min value [" + col + "]", expectedCol.minValue(), actualCol.minValue());
            assertEquals("max value [" + col + "]", expectedCol.maxValue(), actualCol.maxValue());
            assertEquals("column size [" + col + "]", expectedCol.sizeInBytes(), actualCol.sizeInBytes());
        }
    }

    /**
     * Large footer: a file whose footer exceeds the {@link ParquetFormatReader#FOOTER_TAIL_PREFETCH_BYTES}
     * tail window must trigger a second, exact-range {@code readBytesAsync} covering the whole footer,
     * and still parse to the correct schema. A wide (many-column) schema inflates the footer well past
     * the 64 KiB tail while staying under the footer-byte cache's per-entry cap.
     */
    public void testMetadataAsyncLargeFooterIssuesSecondRead() throws Exception {
        int columns = 2000;
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < columns; i++) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named("col_" + i);
        }
        MessageType schema = builder.named("wide_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("col_0", 1L);
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<Attribute> syncSchema = reader.metadata(createStorageObject(parquetData)).schema();

        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        ExecutorService probePool = Executors.newFixedThreadPool(2);
        AtomicInteger asyncReadCount = new AtomicInteger();
        List<long[]> reads = new CopyOnWriteArrayList<>();
        try {
            StorageObject asyncObject = createAsyncStorageObject(parquetData, probePool, asyncReadCount, reads);
            PlainActionFuture<SourceMetadata> future = new PlainActionFuture<>();
            reader.metadataAsync(asyncObject, probePool, future);
            SourceMetadata asyncMeta = future.actionGet(30, TimeUnit.SECONDS);

            assertEquals("footer larger than the tail window should trigger a second read", 2, asyncReadCount.get());
            assertEquals("first read is the bounded tail prefetch", ParquetFormatReader.FOOTER_TAIL_PREFETCH_BYTES, reads.get(0)[1]);
            assertThat(
                "second read must cover the full footer, larger than the tail window",
                reads.get(1)[1],
                greaterThan((long) ParquetFormatReader.FOOTER_TAIL_PREFETCH_BYTES)
            );
            assertEquals(syncSchema.size(), asyncMeta.schema().size());
            assertEquals(columns, asyncMeta.schema().size());
        } finally {
            probePool.shutdownNow();
        }
    }

    /**
     * No-buffer-leak (success): every {@link DirectReadBuffer} handed to {@code metadataAsync} —
     * both the tail prefetch and the second full-footer read of a wide footer — must be closed by the
     * reader once its bytes have been copied out. A file wide enough to force the two-read path
     * exercises both allocations.
     */
    public void testMetadataAsyncReleasesBuffersOnSuccess() throws Exception {
        int columns = 2000;
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < columns; i++) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named("col_" + i);
        }
        byte[] parquetData = createParquetFile(builder.named("wide_schema"), factory -> {
            Group g = factory.newGroup();
            g.add("col_0", 1L);
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        ExecutorService probePool = Executors.newFixedThreadPool(2);
        AtomicInteger openBuffers = new AtomicInteger();
        AtomicInteger allocated = new AtomicInteger();
        try {
            StorageObject asyncObject = createBufferTrackingAsyncStorageObject(parquetData, probePool, openBuffers, allocated, -1);
            PlainActionFuture<SourceMetadata> future = new PlainActionFuture<>();
            reader.metadataAsync(asyncObject, probePool, future);
            future.actionGet(30, TimeUnit.SECONDS);

            assertThat("both reads must allocate a buffer", allocated.get(), equalTo(2));
            assertEquals("every prefetched buffer must be released after its bytes are copied", 0, openBuffers.get());
        } finally {
            probePool.shutdownNow();
        }
    }

    /**
     * No-buffer-leak (failure): when the second (full-footer) read fails, the reader must already have
     * released the first (tail) buffer and must not leak any buffer, while surfacing the failure.
     */
    public void testMetadataAsyncReleasesBuffersOnReadFailure() throws Exception {
        int columns = 2000;
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < columns; i++) {
            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named("col_" + i);
        }
        byte[] parquetData = createParquetFile(builder.named("wide_schema"), factory -> {
            Group g = factory.newGroup();
            g.add("col_0", 1L);
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        ExecutorService probePool = Executors.newFixedThreadPool(2);
        AtomicInteger openBuffers = new AtomicInteger();
        AtomicInteger allocated = new AtomicInteger();
        try {
            // Fail the second read (index 1): the full-footer fetch that follows the tail prefetch.
            StorageObject asyncObject = createBufferTrackingAsyncStorageObject(parquetData, probePool, openBuffers, allocated, 1);
            PlainActionFuture<SourceMetadata> future = new PlainActionFuture<>();
            reader.metadataAsync(asyncObject, probePool, future);
            expectThrows(Exception.class, () -> future.actionGet(30, TimeUnit.SECONDS));

            assertEquals("the tail buffer must be released even though the follow-up read failed", 0, openBuffers.get());
        } finally {
            probePool.shutdownNow();
        }
    }

    /**
     * Short-read fallback: {@code readBytesAsync}'s SPI contract permits returning fewer bytes than
     * requested. The async parse treats the prefetched bytes as a suffix ending at the file length, so a
     * short read would misalign every footer offset. This mock returns a short buffer whose trailing 8
     * bytes forge a valid-looking Parquet trailer (small footer length + {@code PAR1}) — enough to send
     * the unguarded path straight into {@code parseTailOnExecutor} with a misaligned window (which then
     * mis-parses or throws). {@code metadataAsync} must instead detect the short read and fall back to the
     * synchronous parse, yielding metadata identical to the fully-synchronous path.
     */
    public void testMetadataAsyncShortReadFallsBackToSync() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("short_read_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("id", 42L);
            g.add("name", "Bob");
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata syncMeta = reader.metadata(createStorageObject(parquetData));

        // A deliberately short buffer with a forged trailer: footerLength=20 (LE int32) followed by PAR1.
        // footerRegion (20 + 8) <= buffer length (40), so the unguarded path would parse straight from
        // these misaligned bytes instead of the real file suffix.
        byte[] shortBuffer = new byte[40];
        int forgedFooterLength = 20;
        int base = shortBuffer.length - 8;
        shortBuffer[base] = (byte) (forgedFooterLength & 0xFF);
        shortBuffer[base + 1] = (byte) ((forgedFooterLength >> 8) & 0xFF);
        shortBuffer[base + 2] = (byte) ((forgedFooterLength >> 16) & 0xFF);
        shortBuffer[base + 3] = (byte) ((forgedFooterLength >> 24) & 0xFF);
        shortBuffer[base + 4] = 'P';
        shortBuffer[base + 5] = 'A';
        shortBuffer[base + 6] = 'R';
        shortBuffer[base + 7] = '1';

        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        ExecutorService probePool = Executors.newFixedThreadPool(2);
        AtomicInteger asyncReadCount = new AtomicInteger();
        try {
            StorageObject asyncObject = new StorageObject() {
                @Override
                public InputStream newStream() {
                    return new ByteArrayInputStream(parquetData);
                }

                @Override
                public InputStream newStream(long position, long length) {
                    int pos = (int) position;
                    int len = (int) Math.min(length, parquetData.length - position);
                    return new ByteArrayInputStream(parquetData, pos, len);
                }

                @Override
                public void readBytesAsync(
                    long position,
                    long length,
                    DirectBufferFactory factory,
                    Executor ignored,
                    ActionListener<DirectReadBuffer> listener
                ) {
                    asyncReadCount.incrementAndGet();
                    probePool.execute(() -> listener.onResponse(new DirectReadBuffer(ByteBuffer.wrap(shortBuffer), () -> {})));
                }

                @Override
                public long length() {
                    return parquetData.length;
                }

                @Override
                public Instant lastModified() {
                    return Instant.ofEpochMilli(0);
                }

                @Override
                public boolean exists() {
                    return true;
                }

                @Override
                public StoragePath path() {
                    return StoragePath.of("memory://short-read-test.parquet");
                }
            };

            PlainActionFuture<SourceMetadata> future = new PlainActionFuture<>();
            reader.metadataAsync(asyncObject, probePool, future);
            SourceMetadata asyncMeta = future.actionGet(30, TimeUnit.SECONDS);

            assertEquals("the tail prefetch must be attempted once before falling back", 1, asyncReadCount.get());
            assertEquals(syncMeta.schema().size(), asyncMeta.schema().size());
            for (int i = 0; i < syncMeta.schema().size(); i++) {
                assertEquals(syncMeta.schema().get(i).name(), asyncMeta.schema().get(i).name());
                assertEquals(syncMeta.schema().get(i).dataType(), asyncMeta.schema().get(i).dataType());
            }
            assertStatisticsEqual(syncMeta, asyncMeta);
        } finally {
            probePool.shutdownNow();
        }
    }

    public void testReadDataFromSimpleParquet() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("name", "Alice");
            group1.add("score", 95.5);

            Group group2 = factory.newGroup();
            group2.add("id", 2L);
            group2.add("name", "Bob");
            group2.add("score", 87.3);

            Group group3 = factory.newGroup();
            group3.add("id", 3L);
            group3.add("name", "Charlie");
            group3.add("score", 92.1);

            return List.of(group1, group2, group3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            // Check first row
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);

            // Check second row
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);

            // Check third row
            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(2));
            assertEquals(new BytesRef("Charlie"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(2, new BytesRef()));
            assertEquals(92.1, ((DoubleBlock) page.getBlock(2)).getDouble(2), 0.001);

            assertFalse(iterator.hasNext());
        }
    }

    public void testReadCompressedSnappy() throws Exception {
        assertCompressedReadRoundTrip(CompressionCodecName.SNAPPY);
    }

    public void testReadCompressedGzip() throws Exception {
        assertCompressedReadRoundTrip(CompressionCodecName.GZIP);
    }

    public void testReadCompressedZstd() throws Exception {
        assertCompressedReadRoundTrip(CompressionCodecName.ZSTD);
    }

    public void testReadCompressedLz4Raw() throws Exception {
        assertCompressedReadRoundTrip(CompressionCodecName.LZ4_RAW);
    }

    private void assertCompressedReadRoundTrip(CompressionCodecName codec) throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("name", "Alice");
            group1.add("score", 95.5);

            Group group2 = factory.newGroup();
            group2.add("id", 2L);
            group2.add("name", "Bob");
            group2.add("score", 87.3);

            return List.of(group1, group2);
        }, codec);

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);

            assertFalse(iterator.hasNext());
        }
    }

    public void testReadWithColumnProjection() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("name", "Alice");
            group1.add("score", 95.5);

            Group group2 = factory.newGroup();
            group2.add("id", 2L);
            group2.add("name", "Bob");
            group2.add("score", 87.3);

            return List.of(group1, group2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Project only name and score columns
        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("name", "score"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount()); // Only 2 projected columns

            // Check values - note: order matches projection order
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(1)).getDouble(0), 0.001);

            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(1)).getDouble(1), 0.001);
        }
    }

    /**
     * Empty projection (e.g. EXTERNAL parquet | STATS COUNT(*) — or a query that only references
     * {@code _file.*} virtual columns) must not allocate any column reader. The reader emits
     * row-count-only pages directly from the row group metadata, and the request circuit breaker
     * stays at zero because no buffer is ever charged. Regression test for the ~44KB-per-query
     * leak in https://github.com/elastic/elasticsearch/issues/149393.
     */
    public void testReadWithEmptyProjectionEmitsCountOnlyPagesAndDoesNotAllocate() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new java.util.ArrayList<>();
            for (int i = 0; i < 5; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("name", "row" + i);
                g.add("score", (double) i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        var trackingBreaker = new LimitedBreaker("test", ByteSizeValue.ofMb(64));
        var localFactory = new BlockFactory(trackingBreaker, this.blockFactory.bigArrays());
        ParquetFormatReader reader = new ParquetFormatReader(localFactory);

        long beforeRead = trackingBreaker.getUsed();
        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of(), 10)) {
            long afterOpen = trackingBreaker.getUsed();
            int totalRows = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                assertEquals("count-only path must emit zero blocks", 0, page.getBlockCount());
                totalRows += page.getPositionCount();
            }
            assertEquals(5, totalRows);
            // After open, only the parquet I/O window buffer / footer parsing may have been charged
            // — the count-only path must not grow the breaker further per row group / page. (Iterating
            // pages allocates no column readers, no decode buffers, no value blocks.)
            assertEquals("count-only iteration must not allocate per-page; breaker must not grow", afterOpen, trackingBreaker.getUsed());
        }
        // After close everything allocated during open is released and the breaker returns to its initial level.
        assertEquals("count-only path must release all allocations on close", beforeRead, trackingBreaker.getUsed());
    }

    public void testCircuitBreaker() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            var groups = new ArrayList<Group>();
            for (int i = 0; i < 1000; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                group.add("score", i * 1.5);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        {
            var limitedFactory = new BlockFactory(new LimitedBreaker("test", ByteSizeValue.ofBytes(100)), this.blockFactory.bigArrays());
            var reader = new ParquetFormatReader(limitedFactory);

            // Read the schema without creating any ESQL block. This is enough to trip the breaker.
            assertThrows(CircuitBreakingException.class, () -> reader.metadata(storageObject));

            // Sanity check
            assertEquals(0, limitedFactory.breaker().getUsed());
        }

        {
            // The window buffer (DEFAULT_WINDOW_SIZE) is now tracked by the circuit breaker, so the limit
            // must be large enough to accommodate the window plus leave headroom to trip on page allocation.
            var limitedFactory = new BlockFactory(
                new LimitedBreaker("test", ByteSizeValue.ofBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 1000)),
                this.blockFactory.bigArrays()
            );
            var reader = new ParquetFormatReader(limitedFactory);

            // Read the schema is now ok
            var metadata = reader.metadata(storageObject);
            assertEquals(0, limitedFactory.breaker().getUsed());

            // Reading a page trips the breaker
            assertThrows(CircuitBreakingException.class, () -> {
                try (var iter = reader.read(storageObject, List.of("id", "score"), 1000)) {
                    iter.next();
                }
            });
            reader.close();
            assertEquals(0, limitedFactory.breaker().getUsed());
        }
    }

    /**
     * The optimized iterator is page-at-a-time and does not bulk-allocate row groups. The only
     * tracked allocation that can trip the breaker mid-iteration is the per-row-group prefetch,
     * whose bytes are accounted via the Arrow allocator on the REQUEST breaker. If the breaker
     * trips during a prefetch, the future fails and {@code takePendingPrefetch} falls back to
     * sync I/O for that row group (see {@code OptimizedParquetColumnIterator}).
     *
     * <p>This test verifies two related properties:
     * <ul>
     *   <li>A breaker too tight to accommodate the file footer trips on file-open and releases
     *       all reserved bytes.</li>
     *   <li>A breaker tight enough that the per-row-group prefetch cannot fit, but large enough
     *       for the footer and the sliding window, still produces correct results via the sync
     *       fallback and releases all bytes on close.</li>
     * </ul>
     */
    public void testCircuitBreakerTripsOnLargerRowGroup() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("test_schema");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(8 * 1024L)
                .withPageSize(512)
                .build()
        ) {
            for (int i = 0; i < 200; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                g.add("payload", "x".repeat(10 + i));
                writer.write(g);
            }
        }
        byte[] parquetData = outputStream.toByteArray();
        assertThat(parquetData.length, greaterThan(2 * 1024));

        StorageObject storageObject = createStorageObject(parquetData);

        // 1. Breaker too small for the footer → trip on open, no leak.
        {
            var tinyBreaker = new LimitedBreaker("test", ByteSizeValue.ofBytes(256));
            var tinyFactory = new BlockFactory(tinyBreaker, this.blockFactory.bigArrays());
            try (var reader = new ParquetFormatReader(tinyFactory)) {
                expectThrows(CircuitBreakingException.class, () -> reader.read(storageObject, List.of("id", "payload"), 1_000_000));
            }
            assertEquals(0, tinyBreaker.getUsed());
        }

        // 2. Breaker fits the footer and the sliding window but leaves only modest headroom.
        // Per-row-group prefetches that exceed the headroom trip the Arrow allocator, fail their
        // future, and trigger the sync-I/O fallback in {@code takePendingPrefetch}. The iteration
        // still produces all rows and releases every byte on close. Exact prefetch-vs-fallback
        // mix depends on row-group size and codec, which is fine — the regression we care about
        // here is "no leaks and no errors under a tight allocator budget".
        {
            var smallBreaker = new LimitedBreaker(
                "test",
                ByteSizeValue.ofBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 64 * 1024)
            );
            var smallFactory = new BlockFactory(smallBreaker, this.blockFactory.bigArrays());
            var pageCount = new AtomicInteger();
            int totalRows = 0;
            try (
                var reader = new ParquetFormatReader(smallFactory);
                var iter = reader.read(storageObject, List.of("id", "payload"), 1_000_000)
            ) {
                while (iter.hasNext()) {
                    var page = iter.next();
                    totalRows += page.getPositionCount();
                    page.close();
                    pageCount.incrementAndGet();
                }
            }
            assertThat(pageCount.get(), greaterThan(0));
            assertEquals(200, totalRows);
            assertEquals(0, smallBreaker.getUsed());
        }
    }

    public void testProjectedColumnMissingFromFileReturnsNullBlock() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("name", "Alice");
            group1.add("score", 95.5);

            Group group2 = factory.newGroup();
            group2.add("id", 2L);
            group2.add("name", "Bob");
            group2.add("score", 87.3);

            return List.of(group1, group2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("id", "nonexistent", "score"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertTrue(page.getBlock(1).isNull(0));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);

            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertTrue(page.getBlock(1).isNull(1));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);
        }
    }

    public void testReadWithBatching() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 1; i <= 25; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                group.add("value", i * 10);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        int batchSize = 10;
        int totalRows = 0;

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, batchSize)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
            }
        }

        assertEquals(25, totalRows);
    }

    public void testReadBooleanColumn() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("active")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("active", true);

            Group group2 = factory.newGroup();
            group2.add("id", 2L);
            group2.add("active", false);

            return List.of(group1, group2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            assertTrue(((BooleanBlock) page.getBlock(1)).getBoolean(0));
            assertFalse(((BooleanBlock) page.getBlock(1)).getBoolean(1));
        }
    }

    public void testReadIntegerColumn() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("count").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("count", 100);

            Group group2 = factory.newGroup();
            group2.add("count", 200);

            Group group3 = factory.newGroup();
            group3.add("count", 300);

            return List.of(group1, group2, group3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());

            assertEquals(100, ((IntBlock) page.getBlock(0)).getInt(0));
            assertEquals(200, ((IntBlock) page.getBlock(0)).getInt(1));
            assertEquals(300, ((IntBlock) page.getBlock(0)).getInt(2));
        }
    }

    public void testReadFloatColumn() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.FLOAT).named("temperature").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("temperature", 98.6f);

            Group group2 = factory.newGroup();
            group2.add("temperature", 37.0f);

            return List.of(group1, group2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            // Float is converted to double
            assertEquals(98.6, ((DoubleBlock) page.getBlock(0)).getDouble(0), 0.1);
            assertEquals(37.0, ((DoubleBlock) page.getBlock(0)).getDouble(1), 0.1);
        }
    }

    public void testReadWithRowLimit() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                group.add("value", i * 10);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Read with a row limit of 10 — Parquet reader respects the budget natively
        try (
            CloseableIterator<Page> iterator = reader.read(storageObject, FormatReadContext.builder().batchSize(50).rowLimit(10).build())
        ) {
            int totalRows = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
            }
            assertEquals(10, totalRows);
        }
    }

    public void testReadWithRowLimitNoLimit() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 1; i <= 25; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // NO_LIMIT should read all rows
        try (CloseableIterator<Page> iterator = reader.read(storageObject, FormatReadContext.of(null, 10))) {
            int totalRows = 0;
            while (iterator.hasNext()) {
                totalRows += iterator.next().getPositionCount();
            }
            assertEquals(25, totalRows);
        }
    }

    public void testReadWithColumnProjectionAndLimit() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 1; i <= 50; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                group.add("name", "name_" + i);
                group.add("score", i * 1.5);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Project only "name" column with limit of 5
        try (CloseableIterator<Page> iterator = reader.read(storageObject, FormatReadContext.of(List.of("name"), 10).withRowLimit(5))) {
            int totalRows = 0;
            int totalBlocks = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                totalBlocks = page.getBlockCount();
            }
            assertEquals(1, totalBlocks); // Only 1 projected column
            assertEquals(5, totalRows);
        }
    }

    public void testReadOptionalColumnsWithNulls() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("active")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("id", 1L);
            g1.add("name", "Alice");
            g1.add("age", 30);
            g1.add("score", 95.5);
            g1.add("active", true);

            Group g2 = factory.newGroup();
            g2.add("id", 2L);
            // name, age, score, active are all null

            Group g3 = factory.newGroup();
            g3.add("id", 3L);
            g3.add("name", "Charlie");
            // age is null
            g3.add("score", 88.0);
            g3.add("active", false);

            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            assertEquals(5, page.getBlockCount());

            LongBlock idBlock = (LongBlock) page.getBlock(0);
            assertEquals(1L, idBlock.getLong(0));
            assertEquals(2L, idBlock.getLong(1));
            assertEquals(3L, idBlock.getLong(2));

            // name: "Alice", null, "Charlie"
            BytesRefBlock nameBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
            assertTrue(nameBlock.isNull(1));
            assertEquals(new BytesRef("Charlie"), nameBlock.getBytesRef(2, new BytesRef()));

            // age: 30, null, null
            IntBlock ageBlock = (IntBlock) page.getBlock(2);
            assertFalse(ageBlock.isNull(0));
            assertEquals(30, ageBlock.getInt(ageBlock.getFirstValueIndex(0)));
            assertTrue(ageBlock.isNull(1));
            assertTrue(ageBlock.isNull(2));

            // score: 95.5, null, 88.0
            DoubleBlock scoreBlock = (DoubleBlock) page.getBlock(3);
            assertFalse(scoreBlock.isNull(0));
            assertEquals(95.5, scoreBlock.getDouble(scoreBlock.getFirstValueIndex(0)), 0.001);
            assertTrue(scoreBlock.isNull(1));
            assertFalse(scoreBlock.isNull(2));
            assertEquals(88.0, scoreBlock.getDouble(scoreBlock.getFirstValueIndex(2)), 0.001);

            // active: true, null, false
            BooleanBlock activeBlock = (BooleanBlock) page.getBlock(4);
            assertFalse(activeBlock.isNull(0));
            assertTrue(activeBlock.getBoolean(activeBlock.getFirstValueIndex(0)));
            assertTrue(activeBlock.isNull(1));
            assertFalse(activeBlock.isNull(2));
            assertFalse(activeBlock.getBoolean(activeBlock.getFirstValueIndex(2)));

            assertFalse(iterator.hasNext());
        }
    }

    /**
     * Regression: attribute nullability must reflect Parquet repetition.
     * <ul>
     *   <li>{@code REQUIRED} top-level fields → {@link Nullability#FALSE} (schema-level non-null guarantee).</li>
     *   <li>{@code OPTIONAL} fields and {@code optionalList()} → {@link Nullability#TRUE} (the cell itself can be absent).</li>
     *   <li>Top-level {@code REPEATED} primitives (the legacy un-annotated list form) → {@link Nullability#TRUE}.</li>
     *   <li>{@code requiredList()} → {@link Nullability#FALSE} (the list group must be present, even though its
     *       elements can be null — element-level nullability is not modelled at the attribute level).</li>
     * </ul>
     * A wrong default would let downstream planner rules (e.g. {@code COALESCE} simplification, {@code IS NULL}
     * rewriting, {@code FoldNull}) drop legitimate null rows for {@code OPTIONAL} columns.
     */
    public void testSchemaAttributeNullabilityReflectsRepetition() throws Exception {
        Type requiredList = Types.requiredList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("required_tags");
        Type optionalList = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("optional_tags");

        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("req_id")
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named("opt_id")
            .repeated(PrimitiveType.PrimitiveTypeName.INT32)
            .named("rep_value")
            .addField(requiredList)
            .addField(optionalList)
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> List.of());
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<Attribute> attributes = reader.metadata(storageObject).schema();
        assertEquals(5, attributes.size());

        assertNullability(attributes, "req_id", Nullability.FALSE);
        assertNullability(attributes, "opt_id", Nullability.TRUE);
        assertNullability(attributes, "rep_value", Nullability.TRUE);
        assertNullability(attributes, "required_tags", Nullability.FALSE);
        assertNullability(attributes, "optional_tags", Nullability.TRUE);
    }

    private static void assertNullability(List<Attribute> attributes, String name, Nullability expected) {
        Attribute attr = attributes.stream()
            .filter(a -> a.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new AssertionError("attribute [" + name + "] not found"));
        assertEquals("attribute [" + name + "]", expected, attr.nullable());
    }

    public void testReadOptionalLongWithNulls() throws Exception {
        MessageType schema = Types.buildMessage().optional(PrimitiveType.PrimitiveTypeName.INT64).named("value").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("value", 100L);
            Group g2 = factory.newGroup();
            // value is null
            Group g3 = factory.newGroup();
            g3.add("value", 300L);
            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            LongBlock block = (LongBlock) page.getBlock(0);
            assertFalse(block.isNull(0));
            assertEquals(100L, block.getLong(block.getFirstValueIndex(0)));
            assertTrue(block.isNull(1));
            assertFalse(block.isNull(2));
            assertEquals(300L, block.getLong(block.getFirstValueIndex(2)));
        }
    }

    // --- DECIMAL tests ---

    public void testReadDecimalInt32Column() throws Exception {
        // DECIMAL(9, 2) backed by INT32: value 12345 represents 123.45
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("price")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("price", 12345); // 123.45
            Group g2 = factory.newGroup();
            g2.add("price", -9900); // -99.00
            Group g3 = factory.newGroup();
            g3.add("price", 0);
            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DOUBLE, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(123.45, block.getDouble(0), 0.001);
            assertEquals(-99.00, block.getDouble(1), 0.001);
            assertEquals(0.0, block.getDouble(2), 0.001);
        }
    }

    public void testReadDecimalInt64Column() throws Exception {
        // DECIMAL(18, 4) backed by INT64: value 123456789 represents 12345.6789
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.decimalType(4, 18))
            .named("amount")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("amount", 123456789L); // 12345.6789
            Group g2 = factory.newGroup();
            g2.add("amount", -50000L); // -5.0000
            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DOUBLE, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(12345.6789, block.getDouble(0), 0.0001);
            assertEquals(-5.0, block.getDouble(1), 0.0001);
        }
    }

    public void testReadDecimalFixedLenColumn() throws Exception {
        // DECIMAL(10, 2) backed by FIXED_LEN_BYTE_ARRAY(8)
        int fixedLen = 8;
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(fixedLen)
            .as(LogicalTypeAnnotation.decimalType(2, 10))
            .named("total")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("total", Binary.fromConstantByteArray(toFixedLenDecimal(1234567, fixedLen))); // 12345.67
            Group g2 = factory.newGroup();
            g2.add("total", Binary.fromConstantByteArray(toFixedLenDecimal(-100, fixedLen))); // -1.00
            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(12345.67, block.getDouble(0), 0.01);
            assertEquals(-1.00, block.getDouble(1), 0.01);
        }
    }

    public void testReadDecimalBinaryColumn() throws Exception {
        // DECIMAL(10, 2) backed by BINARY
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.decimalType(2, 10))
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("value", Binary.fromConstantByteArray(BigInteger.valueOf(9999).toByteArray())); // 99.99
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(99.99, block.getDouble(0), 0.01);
        }
    }

    // --- TIMESTAMP MICROS/NANOS tests ---

    public void testReadTimestampMicrosColumn() throws Exception {
        // TIMESTAMP(MICROS, adjustedToUTC=true) maps to DATE_NANOS and preserves sub-millisecond precision.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test_schema");

        long epochMillis = 946728000000L; // 2000-01-01T12:00:00Z
        // .123456 fractional seconds: the .456 microseconds must survive (were truncated before the fix).
        long epochMicros = epochMillis * 1000 + 456;
        long expectedNanos = epochMicros * 1000;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("ts", epochMicros);
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATE_NANOS, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals("timestamp[us] must decode to epoch-nanos with full precision", expectedNanos, block.getLong(0));
        }
    }

    public void testReadTimestampNanosColumn() throws Exception {
        // TIMESTAMP(NANOS, adjustedToUTC=true) maps to DATE_NANOS and passes the epoch-nanos value through unchanged.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ts")
            .named("test_schema");

        long epochMillis = 946728000000L; // 2000-01-01T12:00:00Z
        // .123456789 fractional seconds: all nine digits must survive (were truncated before the fix).
        long epochNanos = epochMillis * 1_000_000 + 456_789;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("ts", epochNanos);
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATE_NANOS, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals("timestamp[ns] must decode to epoch-nanos with full precision", epochNanos, block.getLong(0));
        }
    }

    public void testReadTimestampMillisColumn() throws Exception {
        // TIMESTAMP(MILLIS, adjustedToUTC=true) — existing behavior, verifying it still works
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts")
            .named("test_schema");

        long epochMillis = 946728000000L;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("ts", epochMillis);
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals("timestamp[ms] must remain DATETIME", DataType.DATETIME, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals(epochMillis, block.getLong(0));
        }
    }

    /**
     * A single Parquet file mixing timestamp[ms]/[us]/[ns] columns with distinct sub-millisecond fractions:
     * the millis column stays DATETIME (epoch-millis) while the micros/nanos columns resolve to DATE_NANOS and
     * round-trip their full fractional precision (the core of elastic/esql-planning#1027).
     */
    public void testReadMixedTimestampUnitsPreservesPrecision() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts_ms")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts_us")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ts_ns")
            .named("test_schema");

        long baseMillis = 1_767_225_600_123L; // 2026-01-01T00:00:00.123Z
        long micros = baseMillis * 1_000 + 456;         // ...00.123456
        long nanos = baseMillis * 1_000_000 + 456_789;  // ...00.123456789

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("ts_ms", baseMillis);
            g.add("ts_us", micros);
            g.add("ts_ns", nanos);
            return List.of(g);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATETIME, metadata.schema().get(0).dataType());
        assertEquals(DataType.DATE_NANOS, metadata.schema().get(1).dataType());
        assertEquals(DataType.DATE_NANOS, metadata.schema().get(2).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(baseMillis, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(micros * 1_000, ((LongBlock) page.getBlock(1)).getLong(0));
            assertEquals(nanos, ((LongBlock) page.getBlock(2)).getLong(0));
        }
    }

    /**
     * A timestamp[us] value beyond the representable date_nanos range (~year 2262) has no nanosecond
     * representation, so it is returned as null rather than silently wrapping around. A defined in-range
     * value in the same column is unaffected.
     */
    public void testReadTimestampMicrosOutOfRangeReturnsNull() throws Exception {
        MessageType schema = Types.buildMessage()
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test_schema");

        long inRangeMicros = 946728000000L * 1_000; // 2000-01-01T12:00:00Z
        // Year ~2600 in micros: in range for micros/millis, but micros*1000 overflows the date_nanos (long-nanos) range.
        long outOfRangeMicros = 20_000_000_000_000_000L;
        assertTrue("precondition: value must overflow nanos scaling", ParquetColumnDecoding.microsOverflowsNanos(outOfRangeMicros));

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("ts", inRangeMicros);
            Group g2 = factory.newGroup();
            g2.add("ts", outOfRangeMicros);
            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATE_NANOS, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertFalse("in-range value must be present", block.isNull(0));
            assertEquals(inRangeMicros * 1_000, block.getLong(0));
            assertTrue("out-of-range value must be null", block.isNull(1));
        }
    }

    // --- TIME logical type tests (PARQUET-6) ---
    // Storage rule: TIME_MILLIS (INT32) → DataType.LONG, raw ms value in LongBlock (no conversion);
    // TIME_MICROS (INT64) → DataType.LONG, converted to nanoseconds (×1_000);
    // TIME_NANOS (INT64) → DataType.LONG, raw ns value in LongBlock (no conversion).

    public void testTimeMillisLogicalType() throws Exception {
        // 12:00:00 encoded as TIME_MILLIS (INT32): 43_200_000 ms since midnight.
        // TIME_MILLIS maps to LONG; raw ms value is widened to long and stored as-is.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("start_time")
            .named("test_schema");

        int rawMillis = 43_200_000; // 12:00:00 in ms since midnight

        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("start_time", rawMillis)));
        StorageObject so = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(so);
        assertEquals("TIME_MILLIS should map to LONG", DataType.LONG, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(so, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals("TIME_MILLIS: raw ms value widened to long", (long) rawMillis, block.getLong(0));
            page.releaseBlocks();
        }
    }

    public void testTimeMicrosLogicalType() throws Exception {
        // 12:00:00 encoded as TIME_MICROS (INT64): 43_200_000_000 µs since midnight.
        // Expected in LongBlock: 43_200_000_000 * 1_000 = 43_200_000_000_000 ns.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("start_time")
            .named("test_schema");

        long rawMicros = 43_200_000_000L; // 12:00:00 in µs since midnight
        long expectedNanos = rawMicros * 1_000L;

        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("start_time", rawMicros)));
        StorageObject so = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(so);
        assertEquals("TIME_MICROS should map to LONG", DataType.LONG, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(so, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals("TIME_MICROS must be converted to nanoseconds (×1_000)", expectedNanos, block.getLong(0));
            page.releaseBlocks();
        }
    }

    public void testTimeNanosLogicalType() throws Exception {
        // 12:00:00 encoded as TIME_NANOS (INT64): 43_200_000_000_000 ns since midnight.
        // Expected in LongBlock: same raw value — already nanoseconds, no conversion.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("start_time")
            .named("test_schema");

        long rawNanos = 43_200_000_000_000L; // 12:00:00 in ns since midnight

        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("start_time", rawNanos)));
        StorageObject so = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(so);
        assertEquals("TIME_NANOS should map to LONG", DataType.LONG, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(so, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals("TIME_NANOS value is already nanoseconds, stored as-is", rawNanos, block.getLong(0));
            page.releaseBlocks();
        }
    }

    public void testTimeMillisNullableLogicalType() throws Exception {
        // Nullable TIME_MILLIS column: one null row, one value row.
        // TIME_MILLIS maps to LONG; the block is a LongBlock with the raw ms value.
        MessageType schema = Types.buildMessage()
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("start_time")
            .named("test_schema");

        int rawMillis = 3_600_000; // 01:00:00 in ms

        byte[] data = createParquetFile(schema, f -> {
            Group g1 = f.newGroup(); // null row
            Group g2 = f.newGroup().append("start_time", rawMillis);
            return List.of(g1, g2);
        });
        StorageObject so = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(so, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertTrue("first row should be null", block.isNull(0));
            assertEquals("second row: raw ms value widened to long", (long) rawMillis, block.getLong(1));
            page.releaseBlocks();
        }
    }

    public void testTimeMicrosNullableLogicalType() throws Exception {
        MessageType schema = Types.buildMessage()
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("start_time")
            .named("test_schema");

        long rawMicros = 3_600_000_000L; // 01:00:00 in µs
        long expectedNanos = rawMicros * 1_000L;

        byte[] data = createParquetFile(schema, f -> {
            Group g1 = f.newGroup(); // null row
            Group g2 = f.newGroup().append("start_time", rawMicros);
            return List.of(g1, g2);
        });
        StorageObject so = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(so, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertTrue("first row should be null", block.isNull(0));
            assertEquals("TIME_MICROS must be converted to nanoseconds (×1_000)", expectedNanos, block.getLong(1));
            page.releaseBlocks();
        }
    }

    public void testTimeNanosNullableLogicalType() throws Exception {
        MessageType schema = Types.buildMessage()
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("start_time")
            .named("test_schema");

        long rawNanos = 3_600_000_000_000L; // 01:00:00 in ns

        byte[] data = createParquetFile(schema, f -> {
            Group g1 = f.newGroup(); // null row
            Group g2 = f.newGroup().append("start_time", rawNanos);
            return List.of(g1, g2);
        });
        StorageObject so = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(so, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertTrue("first row should be null", block.isNull(0));
            assertEquals("TIME_NANOS value is already nanoseconds, stored as-is", rawNanos, block.getLong(1));
            page.releaseBlocks();
        }
    }

    // --- JSON/BSON logical type tests ---

    public void testJsonLogicalType() throws Exception {
        // BINARY + JSON annotation: UTF-8 encoded JSON string, maps to KEYWORD.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.jsonType())
            .named("payload")
            .named("test_schema");

        byte[] jsonBytes = "{\"x\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("payload", Binary.fromConstantByteArray(jsonBytes))));
        StorageObject so = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(so);
        assertEquals("JSON annotation should map to KEYWORD", DataType.KEYWORD, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(so, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertEquals(new BytesRef(jsonBytes), block.getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testBsonLogicalType() throws Exception {
        // BINARY + BSON annotation: opaque binary, not human-readable — maps to UNSUPPORTED.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.bsonType())
            .named("doc")
            .named("test_schema");

        StorageObject so = createStorageObject(
            createParquetFile(schema, f -> List.of(f.newGroup().append("doc", Binary.fromConstantByteArray(new byte[] { 0x05, 0x00 }))))
        );
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(so);
        assertEquals("BSON annotation should map to UNSUPPORTED", DataType.UNSUPPORTED, metadata.schema().get(0).dataType());
    }

    public void testIntervalLogicalType() throws Exception {
        // FIXED_LEN_BYTE_ARRAY(12) + INTERVAL annotation: months+days+ms has no single ESQL equivalent;
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(12)
            .as(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
            .named("duration")
            .named("test_schema");

        StorageObject so = createStorageObject(
            createParquetFile(schema, f -> List.of(f.newGroup().append("duration", Binary.fromConstantByteArray(new byte[12]))))
        );
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(so);
        assertEquals("INTERVAL annotation should map to UNSUPPORTED", DataType.UNSUPPORTED, metadata.schema().get(0).dataType());
    }

    // --- INT96 timestamp tests ---

    public void testReadInt96TimestampColumn() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT96).named("ts").named("test_schema");

        // 2000-01-01T12:00:00Z → Julian day 2451545, nanos = 12h = 43_200_000_000_000
        int julianDay = 2_451_545;
        long nanosOfDay = 43_200_000_000_000L;
        long expectedMillis = 946728000000L;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("ts", new NanoTime(julianDay, nanosOfDay));
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATETIME, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals(expectedMillis, block.getLong(0));
        }
    }

    // --- FLOAT16 tests ---

    public void testReadFloat16Column() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(2)
            .as(LogicalTypeAnnotation.float16Type())
            .named("val")
            .named("test_schema");

        float value1 = 3.14f;
        float value2 = -1.0f;
        float value3 = 0.0f;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("val", Binary.fromConstantByteArray(toFloat16Bytes(value1)));
            Group g2 = factory.newGroup();
            g2.add("val", Binary.fromConstantByteArray(toFloat16Bytes(value2)));
            Group g3 = factory.newGroup();
            g3.add("val", Binary.fromConstantByteArray(toFloat16Bytes(value3)));
            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DOUBLE, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(Float.float16ToFloat(Float.floatToFloat16(value1)), block.getDouble(0), 0.01);
            assertEquals(Float.float16ToFloat(Float.floatToFloat16(value2)), block.getDouble(1), 0.001);
            assertEquals(0.0, block.getDouble(2), 0.001);
        }
    }

    // --- UUID tests ---

    public void testReadUuidColumn() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(16)
            .as(LogicalTypeAnnotation.uuidType())
            .named("id")
            .named("test_schema");

        UUID uuid1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000000");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("id", Binary.fromConstantByteArray(toUuidBytes(uuid1)));
            Group g2 = factory.newGroup();
            g2.add("id", Binary.fromConstantByteArray(toUuidBytes(uuid2)));
            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.KEYWORD, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertEquals(uuid1.toString(), block.getBytesRef(0, new BytesRef()).utf8ToString());
            assertEquals(uuid2.toString(), block.getBytesRef(1, new BytesRef()).utf8ToString());
        }
    }

    // --- Raw binary / malformed UTF-8 tests ---

    public void testRawBinaryColumnsMapToKeywordAndAreSanitized() throws Exception {
        // Un-annotated BINARY/FIXED_LEN_BYTE_ARRAY is how legacy writers (Impala, older Spark) store strings,
        // so it maps to KEYWORD rather than UNSUPPORTED to avoid dropping legitimate string columns. Because
        // the bytes may be arbitrary, the reader sanitizes them to well-formed UTF-8 so KEYWORD ops stay total.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("raw_binary")
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(4)
            .named("raw_fixed")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("raw_binary", Binary.fromConstantByteArray(new byte[] { (byte) 0xFF, (byte) 0xFE, 0x00 }));
            g.add("raw_fixed", Binary.fromConstantByteArray(new byte[] { (byte) 0xF8, (byte) 0xFF, 0x01, 0x02 }));
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject storageObject = createStorageObject(parquetData);
        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(2, metadata.schema().size());
        assertEquals(DataType.KEYWORD, metadata.schema().get(0).dataType());
        assertEquals(DataType.KEYWORD, metadata.schema().get(1).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            BytesRefBlock rawBinary = (BytesRefBlock) page.getBlock(0);
            BytesRefBlock rawFixed = (BytesRefBlock) page.getBlock(1);
            // 0xFF and 0xFE are invalid lead bytes -> one U+FFFD each; 0x00/0x01/0x02 are valid ASCII.
            assertEquals(new BytesRef("\uFFFD\uFFFD\u0000"), rawBinary.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("\uFFFD\uFFFD\u0001\u0002"), rawFixed.getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testStringColumnWithInvalidUtf8IsSanitized() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("s")
            .named("test_schema");

        // 0xFF is never a valid UTF-8 lead byte (it is exactly what crashes the TopN Utf8 encoder).
        byte[] invalid = { (byte) 0xFF };
        byte[] valid = "ok".getBytes(StandardCharsets.UTF_8);
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("s", Binary.fromConstantByteArray(invalid));
            Group g2 = factory.newGroup();
            g2.add("s", Binary.fromConstantByteArray(valid));
            return List.of(g1, g2);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject storageObject = createStorageObject(parquetData);
        assertEquals(DataType.KEYWORD, reader.metadata(storageObject).schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertEquals(new BytesRef("\uFFFD"), block.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("ok"), block.getBytesRef(1, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testStringListWithInvalidUtf8IsSanitized() throws Exception {
        Type listType = Types.optionalList()
            .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("tags");
        MessageType schema = new MessageType("test_schema", listType);

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            Group list = g.addGroup("tags");
            list.addGroup("list").append("element", Binary.fromConstantByteArray(new byte[] { (byte) 0xC3, (byte) 0x28 }));
            list.addGroup("list").append("element", Binary.fromConstantByteArray("valid".getBytes(StandardCharsets.UTF_8)));
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject storageObject = createStorageObject(parquetData);
        assertEquals(DataType.KEYWORD, reader.metadata(storageObject).schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertEquals(2, block.getValueCount(0));
            int start = block.getFirstValueIndex(0);
            // 0xC3 0x28: 0xC3 is a 2-byte lead but 0x28 is not a continuation -> one U+FFFD then '('.
            assertEquals(new BytesRef("\uFFFD("), block.getBytesRef(start, new BytesRef()));
            assertEquals(new BytesRef("valid"), block.getBytesRef(start + 1, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testUuidListIsFormattedNotSanitized() throws Exception {
        // A UUID-annotated list element is a raw 16-byte payload (usually not valid UTF-8). It must be
        // hex-formatted like the scalar UUID path, never fed through the UTF-8 sanitizer.
        Type listType = Types.optionalList()
            .optionalElement(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(16)
            .as(LogicalTypeAnnotation.uuidType())
            .named("ids");
        MessageType schema = new MessageType("test_schema", listType);

        UUID uuid1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000000");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            Group list = g.addGroup("ids");
            list.addGroup("list").append("element", Binary.fromConstantByteArray(toUuidBytes(uuid1)));
            list.addGroup("list").append("element", Binary.fromConstantByteArray(toUuidBytes(uuid2)));
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject storageObject = createStorageObject(parquetData);
        assertEquals(DataType.KEYWORD, reader.metadata(storageObject).schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertEquals(2, block.getValueCount(0));
            int start = block.getFirstValueIndex(0);
            assertEquals(uuid1.toString(), block.getBytesRef(start, new BytesRef()).utf8ToString());
            assertEquals(uuid2.toString(), block.getBytesRef(start + 1, new BytesRef()).utf8ToString());
            page.releaseBlocks();
        }
    }

    // --- LIST tests ---

    public void testReadListOfIntegersColumn() throws Exception {
        Type listType = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("numbers");
        MessageType schema = new MessageType("test_schema", listType);

        byte[] parquetData = createParquetFile(schema, factory -> {
            // Row 0: [1, 2, 3]
            Group g1 = factory.newGroup();
            Group list1 = g1.addGroup("numbers");
            list1.addGroup("list").append("element", 1);
            list1.addGroup("list").append("element", 2);
            list1.addGroup("list").append("element", 3);

            // Row 1: [10]
            Group g2 = factory.newGroup();
            Group list2 = g2.addGroup("numbers");
            list2.addGroup("list").append("element", 10);

            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.INTEGER, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());

            IntBlock block = (IntBlock) page.getBlock(0);
            // Row 0: [1, 2, 3]
            assertEquals(3, block.getValueCount(0));
            int start0 = block.getFirstValueIndex(0);
            assertEquals(1, block.getInt(start0));
            assertEquals(2, block.getInt(start0 + 1));
            assertEquals(3, block.getInt(start0 + 2));

            // Row 1: [10]
            assertEquals(1, block.getValueCount(1));
            assertEquals(10, block.getInt(block.getFirstValueIndex(1)));
        }
    }

    public void testReadListOfStringsColumn() throws Exception {
        Type listType = Types.optionalList()
            .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("tags");
        MessageType schema = new MessageType("test_schema", listType);

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            Group list1 = g1.addGroup("tags");
            list1.addGroup("list").append("element", "red");
            list1.addGroup("list").append("element", "blue");

            Group g2 = factory.newGroup();
            Group list2 = g2.addGroup("tags");
            list2.addGroup("list").append("element", "green");

            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.KEYWORD, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());

            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            // Row 0: ["red", "blue"]
            assertEquals(2, block.getValueCount(0));
            int start0 = block.getFirstValueIndex(0);
            assertEquals(new BytesRef("red"), block.getBytesRef(start0, new BytesRef()));
            assertEquals(new BytesRef("blue"), block.getBytesRef(start0 + 1, new BytesRef()));

            // Row 1: ["green"]
            assertEquals(1, block.getValueCount(1));
            assertEquals(new BytesRef("green"), block.getBytesRef(block.getFirstValueIndex(1), new BytesRef()));
        }
    }

    public void testReadListWithNullList() throws Exception {
        Type listType = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT64).named("values");
        MessageType schema = new MessageType("test_schema", listType);

        byte[] parquetData = createParquetFile(schema, factory -> {
            // Row 0: [100, 200]
            Group g1 = factory.newGroup();
            Group list1 = g1.addGroup("values");
            list1.addGroup("list").append("element", 100L);
            list1.addGroup("list").append("element", 200L);

            // Row 1: null (no addGroup call)
            Group g2 = factory.newGroup();

            // Row 2: [300]
            Group g3 = factory.newGroup();
            Group list3 = g3.addGroup("values");
            list3.addGroup("list").append("element", 300L);

            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());

            LongBlock block = (LongBlock) page.getBlock(0);
            // Row 0: [100, 200]
            assertFalse(block.isNull(0));
            assertEquals(2, block.getValueCount(0));
            int start0 = block.getFirstValueIndex(0);
            assertEquals(100L, block.getLong(start0));
            assertEquals(200L, block.getLong(start0 + 1));

            // Row 1: null
            assertTrue(block.isNull(1));

            // Row 2: [300]
            assertFalse(block.isNull(2));
            assertEquals(1, block.getValueCount(2));
            assertEquals(300L, block.getLong(block.getFirstValueIndex(2)));
        }
    }

    public void testReadListOfUnsignedLongColumn() throws Exception {
        assertReadListOfUnsignedLongColumn(new ParquetFormatReader(blockFactory, false)); // baseline reader
    }

    public void testReadListOfUnsignedLongColumnOptimizedReader() throws Exception {
        assertReadListOfUnsignedLongColumn(new ParquetFormatReader(blockFactory, true)); // optimized reader
    }

    /**
     * A LIST of unsigned_long (Parquet INT64 with {@code intType(64, false)}) must sign-flip-encode each element
     * ({@code value ^ 2^63}), just like the scalar path, so the always-decoding output edge produces the true unsigned
     * value. Before the encode was added, list columns fell into the unsupported branch and read back as all-null.
     * Both reader paths route list columns through the same shared decoder, so both are exercised here.
     */
    private void assertReadListOfUnsignedLongColumn(ParquetFormatReader reader) throws Exception {
        Type listType = Types.optionalList()
            .optionalElement(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.intType(64, false)) // unsigned, bit-width 64
            .named("values");
        MessageType schema = new MessageType("test_schema", listType);

        long maxUnsigned = 0xFFFFFFFFFFFFFFFFL; // 2^64-1
        // 2^63 + 100: as an unsigned value this is > Long.MAX_VALUE, so it encodes (^ 2^63) to a non-negative long --
        // the opposite side of the sign boundary from 0 and 100, which encode to negative longs.
        long aboveSignedMaxUnsigned = 0x8000000000000064L;
        byte[] parquetData = createParquetFile(schema, factory -> {
            // Row 0: [0, 2^63+100, 2^64-1] -- spans both sides of the encoding's sign boundary
            Group g1 = factory.newGroup();
            Group list1 = g1.addGroup("values");
            list1.addGroup("list").append("element", 0L);
            list1.addGroup("list").append("element", aboveSignedMaxUnsigned);
            list1.addGroup("list").append("element", maxUnsigned);

            // Row 1: null list (no addGroup call)
            Group g2 = factory.newGroup();

            // Row 2: [100, null element, 200]
            Group g3 = factory.newGroup();
            Group list3 = g3.addGroup("values");
            list3.addGroup("list").append("element", 100L);
            list3.addGroup("list"); // element absent -> null within the list
            list3.addGroup("list").append("element", 200L);

            // Row 3: [] (empty, non-null list)
            Group g4 = factory.newGroup();
            g4.addGroup("values");

            return List.of(g1, g2, g3, g4);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.UNSIGNED_LONG, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(4, page.getPositionCount());

            LongBlock block = (LongBlock) page.getBlock(0);
            // Row 0: [0, 2^63+100, 2^64-1] sign-flip-encoded
            assertFalse(block.isNull(0));
            assertEquals(3, block.getValueCount(0));
            int start0 = block.getFirstValueIndex(0);
            assertEquals(0L ^ Long.MIN_VALUE, block.getLong(start0));
            assertEquals(aboveSignedMaxUnsigned ^ Long.MIN_VALUE, block.getLong(start0 + 1));
            assertEquals(maxUnsigned ^ Long.MIN_VALUE, block.getLong(start0 + 2));

            // Row 1: null list
            assertTrue(block.isNull(1));

            // Row 2: [100, 200] encoded; the null element is dropped (multivalue blocks do not hold null slots within a list)
            assertFalse(block.isNull(2));
            assertEquals(2, block.getValueCount(2));
            int start2 = block.getFirstValueIndex(2);
            assertEquals(100L ^ Long.MIN_VALUE, block.getLong(start2));
            assertEquals(200L ^ Long.MIN_VALUE, block.getLong(start2 + 1));

            // Row 3: [] empty list -> read back as null (the shared list decoder maps an empty list to a null position;
            // ESQL multivalue blocks have no distinct empty-list representation). This is pre-existing list behavior,
            // independent of the unsigned encoding, asserted here to document it for the unsigned_long path.
            assertTrue(block.isNull(3));
        }
    }

    public void testReadListOfDateNanosColumn() throws Exception {
        assertReadListOfDateNanosColumn(new ParquetFormatReader(blockFactory, false)); // baseline reader
    }

    public void testReadListOfDateNanosColumnOptimizedReader() throws Exception {
        assertReadListOfDateNanosColumn(new ParquetFormatReader(blockFactory, true)); // optimized reader
    }

    /**
     * A LIST of timestamp[us] resolves to a DATE_NANOS multivalue column: each element is scaled to epoch-nanos with
     * full precision, null lists stay null, and null elements within a list are dropped (multivalue blocks have no
     * per-element null slot). Both reader paths route list columns through the same shared decoder.
     */
    private void assertReadListOfDateNanosColumn(ParquetFormatReader reader) throws Exception {
        Type listType = Types.optionalList()
            .optionalElement(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("values");
        MessageType schema = new MessageType("test_schema", listType);

        long micros1 = 946728000000L * 1_000 + 111; // sub-millisecond fraction .000111 ms
        long micros2 = 946728000000L * 1_000 + 222;
        long micros3 = 946728000000L * 1_000 + 333;
        byte[] parquetData = createParquetFile(schema, factory -> {
            // Row 0: [micros1, micros2]
            Group g1 = factory.newGroup();
            Group list1 = g1.addGroup("values");
            list1.addGroup("list").append("element", micros1);
            list1.addGroup("list").append("element", micros2);

            // Row 1: null list
            Group g2 = factory.newGroup();

            // Row 2: [micros3, null element]
            Group g3 = factory.newGroup();
            Group list3 = g3.addGroup("values");
            list3.addGroup("list").append("element", micros3);
            list3.addGroup("list"); // element absent -> null within the list

            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATE_NANOS, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());

            LongBlock block = (LongBlock) page.getBlock(0);
            // Row 0: [micros1, micros2] scaled to nanos with full precision
            assertFalse(block.isNull(0));
            assertEquals(2, block.getValueCount(0));
            int start0 = block.getFirstValueIndex(0);
            assertEquals(micros1 * 1_000, block.getLong(start0));
            assertEquals(micros2 * 1_000, block.getLong(start0 + 1));

            // Row 1: null list
            assertTrue(block.isNull(1));

            // Row 2: [micros3]; the null element is dropped
            assertFalse(block.isNull(2));
            assertEquals(1, block.getValueCount(2));
            assertEquals(micros3 * 1_000, block.getLong(block.getFirstValueIndex(2)));
        }
    }

    // --- LIST-under-STRUCT tests (elastic/esql-planning#1055) ---
    //
    // A list leaf reached through a struct (struct<list<...>>, the shape of e.g. SQuAD's `answers`)
    // used to bind to no column descriptor and silently read as all-null: the flattener names the
    // leaf at its parent dotted path (answers.text) while the descriptor's raw path carries the
    // synthetic LIST wrapper (answers.text.list.element). These tests assert the leaves now
    // round-trip their multivalues on both the optimized and baseline read paths.

    /** Builds a {@code struct<list<string> text, list<int> answer_start>} schema with a top-level id. */
    private static MessageType squadAnswersSchema() {
        return new MessageType(
            "test_schema",
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
            Types.optionalGroup()
                .addField(
                    Types.optionalList()
                        .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("text")
                )
                .addField(Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("answer_start"))
                .named("answers")
        );
    }

    public void testReadStructOfListRoundTripsBothPaths() throws Exception {
        MessageType schema = squadAnswersSchema();
        byte[] parquetData = createParquetFile(schema, factory -> {
            // Row 0: single-element lists (the canonical SQuAD row shape)
            Group g0 = factory.newGroup();
            g0.add("id", 1L);
            Group ans0 = g0.addGroup("answers");
            ans0.addGroup("text").addGroup("list").append("element", "Saint Bernadette Soubirous");
            ans0.addGroup("answer_start").addGroup("list").append("element", 515);

            // Row 1: multi-element lists
            Group g1 = factory.newGroup();
            g1.add("id", 2L);
            Group ans1 = g1.addGroup("answers");
            Group text1 = ans1.addGroup("text");
            text1.addGroup("list").append("element", "alpha");
            text1.addGroup("list").append("element", "beta");
            Group starts1 = ans1.addGroup("answer_start");
            starts1.addGroup("list").append("element", 10);
            starts1.addGroup("list").append("element", 20);

            // Row 2: struct present but lists absent -> null leaves
            Group g2 = factory.newGroup();
            g2.add("id", 3L);
            g2.addGroup("answers");

            return List.of(g0, g1, g2);
        });

        // Default reader wraps memory storage in a ParquetStorageObjectAdapter and drives the
        // optimized iterator; withBaselinePath() forces the row-at-a-time baseline iterator.
        for (ParquetFormatReader reader : List.of(
            new ParquetFormatReader(blockFactory),
            new ParquetFormatReader(blockFactory).withBaselinePath()
        )) {
            StorageObject storageObject = createStorageObject(parquetData);
            try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("id", "answers.text", "answers.answer_start"), 10)) {
                assertTrue(iterator.hasNext());
                Page page = iterator.next();
                assertEquals(3, page.getPositionCount());

                BytesRefBlock text = (BytesRefBlock) page.getBlock(1);
                IntBlock starts = (IntBlock) page.getBlock(2);

                // Row 0
                assertEquals(1, text.getValueCount(0));
                assertEquals(new BytesRef("Saint Bernadette Soubirous"), text.getBytesRef(text.getFirstValueIndex(0), new BytesRef()));
                assertEquals(1, starts.getValueCount(0));
                assertEquals(515, starts.getInt(starts.getFirstValueIndex(0)));

                // Row 1 (multivalue)
                assertEquals(2, text.getValueCount(1));
                int t1 = text.getFirstValueIndex(1);
                assertEquals(new BytesRef("alpha"), text.getBytesRef(t1, new BytesRef()));
                assertEquals(new BytesRef("beta"), text.getBytesRef(t1 + 1, new BytesRef()));
                assertEquals(2, starts.getValueCount(1));
                int s1 = starts.getFirstValueIndex(1);
                assertEquals(10, starts.getInt(s1));
                assertEquals(20, starts.getInt(s1 + 1));

                // Row 2 (absent lists -> null, not spurious data)
                assertTrue(text.isNull(2));
                assertTrue(starts.isNull(2));

                page.releaseBlocks();
            }
        }
    }

    /** COUNT-style guard: the non-null count of the struct-nested leaf must equal the populated rows. */
    public void testStructOfListCountMatchesNonNull() throws Exception {
        MessageType schema = squadAnswersSchema();
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g0 = factory.newGroup();
            g0.add("id", 1L);
            g0.addGroup("answers").addGroup("text").addGroup("list").append("element", "x");

            // Row 1: entirely absent struct -> null leaf
            Group g1 = factory.newGroup();
            g1.add("id", 2L);

            return List.of(g0, g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("answers.text"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            BytesRefBlock text = (BytesRefBlock) page.getBlock(0);
            int nonNull = 0;
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (text.isNull(i) == false) {
                    nonNull++;
                }
            }
            assertEquals("struct-nested list leaf must not read as all-null", 1, nonNull);
            page.releaseBlocks();
        }
    }

    public void testLogicalLeafNameStopsAtEnclosingList() {
        MessageType schema = new MessageType(
            "test_schema",
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
            // top-level list<int> (control)
            Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("vals"),
            // struct<primitive> (control)
            Types.optionalGroup().optional(PrimitiveType.PrimitiveTypeName.INT32).named("age").named("person"),
            // struct<list<string>> (the fixed case)
            Types.optionalGroup()
                .addField(
                    Types.optionalList()
                        .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("text")
                )
                .named("answers")
        );
        Set<String> logicalNames = new HashSet<>();
        for (var desc : schema.getColumns()) {
            logicalNames.add(ParquetFormatReader.logicalLeafName(schema, desc.getPath()));
        }
        assertTrue(logicalNames.contains("id"));
        assertTrue(logicalNames.contains("vals"));
        assertTrue(logicalNames.contains("person.age"));
        assertTrue("struct-nested list must resolve to its parent dotted name", logicalNames.contains("answers.text"));
        // The synthetic LIST wrapper must never leak into a logical leaf name.
        for (String name : logicalNames) {
            assertFalse(name, name.contains(".list.") || name.endsWith(".element"));
        }
    }

    public void testResolveColumnInfoBindsStructNestedList() {
        MessageType schema = squadAnswersSchema();
        ColumnInfo info = ParquetFormatReader.resolveColumnInfo(schema, "answers.text");
        assertNotNull("struct-nested list leaf must resolve to a ColumnInfo", info);
        assertEquals(DataType.KEYWORD, info.esqlType());
        assertTrue("list leaf must carry a repetition level", info.maxRepLevel() > 0);
    }

    public void testBuildColumnInfosBindsStructNestedList() {
        MessageType schema = squadAnswersSchema();
        List<Attribute> attributes = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "answers.text", DataType.KEYWORD, Nullability.TRUE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "answers.answer_start", DataType.INTEGER, Nullability.TRUE, null, false)
        );
        ColumnInfo[] infos = ParquetFormatReader.buildColumnInfos(schema, attributes);
        assertNotNull("answers.text must bind to a descriptor", infos[0]);
        assertTrue(infos[0].maxRepLevel() > 0);
        assertNotNull("answers.answer_start must bind to a descriptor", infos[1]);
        assertTrue(infos[1].maxRepLevel() > 0);
    }

    /**
     * Directly verifies the raw Parquet leaf statistics of a struct-nested list column to justify
     * the two different aggregate-pushdown decisions:
     * <ul>
     *   <li><b>COUNT is a correctness issue.</b> The leaf's {@code numNulls} counts per-row null
     *       lists, and {@code rowCount} counts rows — neither is the number of values. So the
     *       stats formula {@code rowCount - numNulls} does not equal the true multivalue count, and
     *       COUNT must fall back to the read path.</li>
     *   <li><b>MIN/MAX are a missed optimization, not a correctness issue.</b> The leaf's min/max
     *       are computed over every element in the column, which is exactly ES|QL
     *       {@code MIN}/{@code MAX} over a multivalue field, so they can safely be pushed down.</li>
     * </ul>
     */
    public void testListLeafStatisticsSemantics() throws Exception {
        MessageType schema = squadAnswersSchema();
        byte[] parquetData = createParquetFile(schema, factory -> {
            // answer_start elements across rows: [515], [10,20], (null list) -> 3 values total
            Group g0 = factory.newGroup();
            g0.add("id", 1L);
            g0.addGroup("answers").addGroup("answer_start").addGroup("list").append("element", 515);

            Group g1 = factory.newGroup();
            g1.add("id", 2L);
            Group starts1 = g1.addGroup("answers").addGroup("answer_start");
            starts1.addGroup("list").append("element", 10);
            starts1.addGroup("list").append("element", 20);

            Group g2 = factory.newGroup();
            g2.add("id", 3L);
            g2.addGroup("answers");

            return List.of(g0, g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        // Open with a config-free ParquetReadOptions (as the reader does) to avoid pulling in the
        // Hadoop XML Configuration stack, which is not on the unit-test classpath.
        org.apache.parquet.ParquetReadOptions options = org.apache.parquet.ParquetReadOptions.builder(new PlainParquetConfiguration())
            .build();
        try (
            org.apache.parquet.hadoop.ParquetFileReader reader = org.apache.parquet.hadoop.ParquetFileReader.open(
                new ParquetStorageObjectAdapter(storageObject, blockFactory.arrowAllocator()),
                options
            )
        ) {
            org.apache.parquet.hadoop.metadata.BlockMetaData rowGroup = reader.getRowGroups().get(0);
            org.apache.parquet.hadoop.metadata.ColumnChunkMetaData leaf = null;
            for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData col : rowGroup.getColumns()) {
                if (col.getPath().toDotString().startsWith("answers.answer_start")) {
                    leaf = col;
                }
            }
            assertNotNull("answer_start leaf chunk must exist", leaf);
            org.apache.parquet.column.statistics.Statistics<?> stats = leaf.getStatistics();

            // MIN/MAX span all elements (10..515) -> equal to ES|QL MIN/MAX over the multivalue.
            assertEquals(10L, ((Number) stats.genericGetMin()).longValue());
            assertEquals(515L, ((Number) stats.genericGetMax()).longValue());

            // COUNT correctness: 3 values were written, but rowCount - numNulls does not equal 3,
            // so a stats-based count would be wrong and must fall back to reading.
            long trueValueCount = 3;
            long statsBasedCount = rowGroup.getRowCount() - stats.getNumNulls();
            assertNotEquals("rowCount - numNulls must not accidentally equal the multivalue count", trueValueCount, statsBasedCount);
        }
    }

    // --- UUID formatting unit test ---

    public void testFormatUuid() {
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        byte[] bytes = toUuidBytes(uuid);
        String formatted = ParquetColumnDecoding.formatUuid(bytes);
        assertEquals("550e8400-e29b-41d4-a716-446655440000", formatted);
    }

    public void testMetadataReturnsCorrectSourceType() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group = factory.newGroup();
            group.add("id", 1L);
            return List.of(group);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals("parquet", metadata.sourceType());
    }

    public void testStatisticsSurviveEmbedding() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("score")
            .named("stats_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("age", 20 + (i % 60));
                g.add("score", (long) (i * 10));
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertTrue("statistics() should be present", metadata.statistics().isPresent());

        var stats = metadata.statistics().get();
        assertTrue("Row count should be present", stats.rowCount().isPresent());
        assertEquals(100L, stats.rowCount().getAsLong());

        var enriched = org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.embedStatistics(
            metadata.sourceMetadata(),
            stats
        );

        assertEquals(100L, enriched.get("_stats.row_count"));
        assertEquals(0L, enriched.get("_stats.columns.age.null_count"));
        assertEquals(20, enriched.get("_stats.columns.age.min"));
        assertEquals(79, enriched.get("_stats.columns.age.max"));
        assertEquals(0L, enriched.get("_stats.columns.score.null_count"));
        assertNotNull("Score min should be present", enriched.get("_stats.columns.score.min"));
        assertNotNull("Score max should be present", enriched.get("_stats.columns.score.max"));
    }

    public void testStatisticsForStringColumnsAreJdkTypes() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("city")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("pop")
            .named("string_stats_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (String name : List.of("alpha", "bravo", "charlie", "delta")) {
                Group g = factory.newGroup();
                g.add("city", name);
                g.add("pop", 1000);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(storageObject);

        var stats = metadata.statistics().orElseThrow();
        var enriched = org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.embedStatistics(
            metadata.sourceMetadata(),
            stats
        );

        Object minCity = enriched.get("_stats.columns.city.min");
        Object maxCity = enriched.get("_stats.columns.city.max");
        assertNotNull("city min should be present", minCity);
        assertNotNull("city max should be present", maxCity);
        assertThat("min must be a JDK String, not Parquet Binary", minCity, instanceOf(String.class));
        assertThat("max must be a JDK String, not Parquet Binary", maxCity, instanceOf(String.class));
        assertEquals("alpha", minCity);
        assertEquals("delta", maxCity);

        // Also verify per-split stats if we can force multi-row-group
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            for (Map.Entry<String, Object> entry : range.statistics().entrySet()) {
                assertFalse("Split stat value must not be a Parquet Binary: " + entry.getKey(), entry.getValue() instanceof Binary);
            }
        }
    }

    public void testDiscoverSplitRangesMultipleRowGroups() throws Exception {
        byte[] parquetData = createWideMultiRowGroupFile(500);

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Expected multiple ranges for multi-row-group file, got " + ranges.size(), ranges.size() > 1);

        for (RangeAwareFormatReader.SplitRange range : ranges) {
            assertTrue("Range offset must be non-negative", range.offset() >= 0);
            assertTrue("Range length must be positive", range.length() > 0);
            assertNotNull("Per-row-group statistics should be present", range.statistics());
            assertTrue("Statistics should contain row count", range.statistics().containsKey("_stats.row_count"));
        }
    }

    public void testDiscoverSplitRangesSingleRowGroup() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group = factory.newGroup();
            group.add("id", 1L);
            return List.of(group);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertEquals("Single row group file should return one range with stats", 1, ranges.size());
        RangeAwareFormatReader.SplitRange range = ranges.getFirst();
        assertTrue("Range offset must be non-negative", range.offset() >= 0);
        assertTrue("Range length must be positive", range.length() > 0);
        assertNotNull("Statistics should be present", range.statistics());
        assertEquals("Row count should be 1", 1L, range.statistics().get("_stats.row_count"));
    }

    public void testDiscoverSplitRangesEmptyFile() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> List.of());

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Empty file (no row groups) should return empty ranges", ranges.isEmpty());
    }

    public void testDiscoverSplitRangesSingleRowGroupStatsContainColumnInfo() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> rows = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("age", 20 + (i % 50));
                rows.add(g);
            }
            return rows;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertEquals(1, ranges.size());
        Map<String, Object> stats = ranges.getFirst().statistics();
        assertEquals(100L, stats.get("_stats.row_count"));
        assertNotNull("Column null count should be present", stats.get("_stats.columns.id.null_count"));
        assertEquals(0L, stats.get("_stats.columns.id.null_count"));
        assertNotNull("Column min should be present", stats.get("_stats.columns.id.min"));
        assertEquals(0L, stats.get("_stats.columns.id.min"));
        assertNotNull("Column max should be present", stats.get("_stats.columns.id.max"));
        assertEquals(99L, stats.get("_stats.columns.id.max"));
        assertEquals(0L, stats.get("_stats.columns.age.null_count"));
        assertEquals(20, stats.get("_stats.columns.age.min"));
        assertEquals(69, stats.get("_stats.columns.age.max"));
    }

    public void testInvalidParquetOpenGarbageIncludesUriInMessage() throws Exception {
        byte[] garbage = new byte[64];
        Arrays.fill(garbage, (byte) 0x5a);
        StorageObject storageObject = createStorageObject(garbage, "s3://bucket/path/file.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> reader.metadata(storageObject));
        assertThat(
            ex.getMessage(),
            allOf(
                containsString("Could not read [s3://bucket/path/file.parquet] as a Parquet file"),
                containsString("is not a Parquet file. Expected magic number at tail, but found [")
            )
        );
    }

    public void testInvalidParquetOpenEmptyFile() throws Exception {
        StorageObject storageObject = createStorageObject(new byte[0], "memory://empty.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> reader.metadata(storageObject));
        assertThat(
            ex.getMessage(),
            allOf(
                containsString("Could not read [memory://empty.parquet] as a Parquet file:"),
                containsString("is not a Parquet file (length is too low: 0)")
            )
        );
    }

    public void testInvalidParquetOpenTruncated() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        byte[] full = createParquetFile(schema, factory -> {
            Group group = factory.newGroup();
            group.add("id", 1L);
            return List.of(group);
        });
        byte[] truncated = Arrays.copyOf(full, Math.max(1, full.length / 8));
        StorageObject storageObject = createStorageObject(truncated, "https://host/obj.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> reader.metadata(storageObject));
        assertTrue(ex.getMessage(), ex.getMessage().contains("https://host/obj.parquet"));
    }

    public void testCorruptDataPageProducesIllegalArgumentException() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                groups.add(g);
            }
            return groups;
        });

        // Overwrite every byte in the data area (between PAR1 header and footer) so that column
        // data is completely garbled, triggering a decoding error on read(). The footer at the
        // end of the file stays intact so metadata() still succeeds.
        int footerLenOffset = parquetData.length - 8;
        int footerLen = ((parquetData[footerLenOffset] & 0xFF)) | ((parquetData[footerLenOffset + 1] & 0xFF) << 8)
            | ((parquetData[footerLenOffset + 2] & 0xFF) << 16) | ((parquetData[footerLenOffset + 3] & 0xFF) << 24);
        int footerStart = parquetData.length - 8 - footerLen;
        Arrays.fill(parquetData, 4, footerStart, (byte) 0xFF);

        StorageObject storageObject = createStorageObject(parquetData, "https://host/corrupt.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        // metadata() should still succeed (footer is intact)
        SourceMetadata metadata = reader.metadata(storageObject);
        assertNotNull(metadata);
        // read() should fail with IllegalArgumentException (not ElasticsearchException/500)
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 100)) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        assertThat(ex.getMessage(), containsString("id"));
    }

    public void testValidateFooterIntegrityRejectsNullsInRequiredColumn() {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        org.apache.parquet.column.statistics.LongStatistics stats = new org.apache.parquet.column.statistics.LongStatistics();
        stats.setMinMax(1L, 100L);
        stats.setNumNulls(5);
        PrimitiveType type = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id");
        org.apache.parquet.hadoop.metadata.ColumnChunkMetaData ccm = org.apache.parquet.hadoop.metadata.ColumnChunkMetaData.get(
            org.apache.parquet.hadoop.metadata.ColumnPath.get("id"),
            type,
            org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED,
            null,
            java.util.EnumSet.of(org.apache.parquet.column.Encoding.PLAIN),
            stats,
            0L,
            0L,
            100L,
            0L,
            0L
        );
        org.apache.parquet.hadoop.metadata.BlockMetaData block = new org.apache.parquet.hadoop.metadata.BlockMetaData();
        block.setRowCount(100);
        block.addColumn(ccm);

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetFormatReader.validateFooterIntegrity("https://example.com/bad.parquet", schema, List.of(block))
        );
        assertThat(ex.getMessage(), containsString("https://example.com/bad.parquet"));
        assertThat(ex.getMessage(), containsString("column [id] is declared required but row group reports 5 null(s)"));
    }

    public void testValidateFooterIntegrityPassesForOptionalColumnWithNulls() {
        MessageType schema = Types.buildMessage().optional(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        org.apache.parquet.column.statistics.LongStatistics stats = new org.apache.parquet.column.statistics.LongStatistics();
        stats.setMinMax(1L, 100L);
        stats.setNumNulls(5);
        PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("id");
        org.apache.parquet.hadoop.metadata.ColumnChunkMetaData ccm = org.apache.parquet.hadoop.metadata.ColumnChunkMetaData.get(
            org.apache.parquet.hadoop.metadata.ColumnPath.get("id"),
            type,
            org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED,
            null,
            java.util.EnumSet.of(org.apache.parquet.column.Encoding.PLAIN),
            stats,
            0L,
            0L,
            100L,
            0L,
            0L
        );
        org.apache.parquet.hadoop.metadata.BlockMetaData block = new org.apache.parquet.hadoop.metadata.BlockMetaData();
        block.setRowCount(100);
        block.addColumn(ccm);

        ParquetFormatReader.validateFooterIntegrity("https://example.com/ok.parquet", schema, List.of(block));
    }

    public void testValidateFooterIntegrityPassesForRequiredColumnWithZeroNulls() {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        org.apache.parquet.column.statistics.LongStatistics stats = new org.apache.parquet.column.statistics.LongStatistics();
        stats.setMinMax(1L, 100L);
        stats.setNumNulls(0);
        PrimitiveType type = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id");
        org.apache.parquet.hadoop.metadata.ColumnChunkMetaData ccm = org.apache.parquet.hadoop.metadata.ColumnChunkMetaData.get(
            org.apache.parquet.hadoop.metadata.ColumnPath.get("id"),
            type,
            org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED,
            null,
            java.util.EnumSet.of(org.apache.parquet.column.Encoding.PLAIN),
            stats,
            0L,
            0L,
            100L,
            0L,
            0L
        );
        org.apache.parquet.hadoop.metadata.BlockMetaData block = new org.apache.parquet.hadoop.metadata.BlockMetaData();
        block.setRowCount(100);
        block.addColumn(ccm);

        ParquetFormatReader.validateFooterIntegrity("https://example.com/ok.parquet", schema, List.of(block));
    }

    public void testValidParquetZeroRowsMetadata() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> List.of());
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals("id", metadata.schema().get(0).name());
    }

    /**
     * Planner type LONG (widened across globbed files) with INT32 in this file must still decode:
     * widening matches {@link org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter#commonType}.
     */
    public void testPlannerLongCompatibleWithInt32InFileReadRange() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("x").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", 42);
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.LONG));
        try (
            CloseableIterator<Page> iterator = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("x"), 100, 0, parquetData.length, plannerTypes, ErrorPolicy.STRICT)
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(42L, ((LongBlock) page.getBlock(0)).getLong(0));
        }
    }

    /**
     * A signed INT32 value of -1 in Parquet must be sign-extended to -1L when the planner schema
     * declares the column as LONG. Zero-extension would produce 4294967295L — incorrect.
     */
    public void testSignedInt32WideningToLong() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("x").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", -1);
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerSchema = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.LONG));
        try (
            CloseableIterator<Page> iterator = reader.read(
                storageObject,
                FormatReadContext.builder().projectedColumns(List.of("x")).batchSize(10).readSchema(plannerSchema).build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(-1L, ((LongBlock) page.getBlock(0)).getLong(0));
        }
    }

    /**
     * Parquet string-annotated BINARY maps to KEYWORD; planner KEYWORD is still readable (both ESQL strings).
     */
    public void testPlannerKeywordCompatibleWithTextParquetColumnReadRange() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("x")
            .named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", "hello");
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader r = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.KEYWORD));
        try (
            CloseableIterator<Page> it = r.readRange(
                storageObject,
                new RangeReadContext(List.of("x"), 10, 0, parquetData.length, plannerTypes, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertEquals(1, page.getPositionCount());
            assertFalse(page.getBlock(0).isNull(0));
            assertEquals(new BytesRef("hello"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
        }
    }

    public void testSchemaMismatchInt32VsKeywordReturnsNullsOnReadRange() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("x").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", 42);
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData, "s3://b/mismatch1.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.KEYWORD));
        try (
            CloseableIterator<Page> iterator = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("x"), 100, 0, parquetData.length, plannerTypes, ErrorPolicy.STRICT)
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertTrue(page.getBlock(0).isNull(0));
        }
    }

    public void testSchemaMismatchStringVsLongReturnsNullsOnReadRange() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("x")
            .named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", "hello");
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader r = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.LONG));
        try (
            CloseableIterator<Page> it = r.readRange(
                storageObject,
                new RangeReadContext(List.of("x"), 10, 0, parquetData.length, plannerTypes, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertTrue(page.getBlock(0).isNull(0));
        }
    }

    public void testSchemaMismatchBooleanVsDoubleReturnsNullsOnReadRange() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("x").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", true);
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader r = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.DOUBLE));
        try (
            CloseableIterator<Page> it = r.readRange(
                storageObject,
                new RangeReadContext(List.of("x"), 10, 0, parquetData.length, plannerTypes, ErrorPolicy.STRICT)
            )
        ) {
            Page page = it.next();
            assertTrue(page.getBlock(0).isNull(0));
        }
    }

    /**
     * Schema-vs-planner mismatch is a "skip and resume" path: the column is silently null-replaced.
     * Confirm that, on top of the {@code logger.warn} we keep, the reader emits a response Warning
     * header so clients see the same information they get for other recoverable ES|QL warnings.
     */
    public void testSchemaMismatchEmitsResponseWarningHeader() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("x").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", 42);
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData, "s3://bucket/warn.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.KEYWORD));
        try (
            CloseableIterator<Page> iterator = reader.readRange(
                storageObject,
                new RangeReadContext(List.of("x"), 100, 0, parquetData.length, plannerTypes, ErrorPolicy.STRICT)
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertTrue(page.getBlock(0).isNull(0));
        }

        List<String> warnings = drainWarnings();
        // 1 summary + 1 detail
        assertEquals("Expected summary + 1 detail, got: " + warnings, 2, warnings.size());
        assertTrue("Summary should mention the file path, got: " + warnings.get(0), warnings.get(0).contains("s3://bucket/warn.parquet"));
        assertTrue("Detail should mention column [x], got: " + warnings.get(1), warnings.get(1).contains("Column [x]"));
        assertTrue("Detail should mention the planner type, got: " + warnings.get(1), warnings.get(1).contains("KEYWORD"));
        assertTrue(
            "Detail should mention the on-disk type, got: " + warnings.get(1),
            warnings.get(1).contains("INTEGER") || warnings.get(1).contains("LONG")
        );
    }

    private List<String> drainWarnings() {
        List<String> raw = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        List<String> messages = raw.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false)).toList();
        threadContext.stashContext();
        return messages;
    }

    public void testReadRangeSelectsCorrectRowGroups() throws Exception {
        byte[] parquetData = createWideMultiRowGroupFile(500);

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Need at least 2 ranges for this test, got " + ranges.size(), ranges.size() >= 2);

        int totalRowsFromRanges = 0;
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            long rangeStart = range.offset();
            long rangeEnd = rangeStart + range.length();
            try (
                CloseableIterator<Page> iterator = reader.readRange(
                    storageObject,
                    new RangeReadContext(null, 1000, rangeStart, rangeEnd, List.of(), ErrorPolicy.STRICT)
                )
            ) {
                while (iterator.hasNext()) {
                    Page page = iterator.next();
                    totalRowsFromRanges += page.getPositionCount();
                }
            }
        }

        int totalRowsDirect = 0;
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1000)) {
            while (iterator.hasNext()) {
                totalRowsDirect += iterator.next().getPositionCount();
            }
        }
        assertEquals("Reading all ranges should produce the same total as a full read", totalRowsDirect, totalRowsFromRanges);
    }

    /**
     * Regression test for a row over-read bug: row groups must map to exactly one split.
     * Previously {@code SplitRange.length} was built from
     * {@code BlockMetaData.getTotalByteSize()} (uncompressed size), causing ranges to overlap
     * in byte space. Parquet's {@code withRange(rangeStart, rangeEnd)} filter then selected
     * each row group from multiple adjacent splits, producing duplicate rows on read.
     * <p>
     * Constructs a multi-row-group Snappy-compressed Parquet file (where uncompressed is
     * meaningfully larger than compressed), verifies the split set is non-overlapping in
     * byte space, and verifies that reading every split exactly once sums to the true row
     * count with no duplication and no drops.
     */
    public void testReadRangeCoverageIsExactlyOneRowCountTotal() throws Exception {
        int numRows = 5_000;
        byte[] parquetData = createCompressibleMultiRowGroupFile(numRows, CompressionCodecName.SNAPPY);

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Need at least 3 ranges for this regression test, got " + ranges.size(), ranges.size() >= 3);

        // Byte ranges must be sorted and non-overlapping (end_i <= start_{i+1}).
        long lastEnd = Long.MIN_VALUE;
        for (RangeAwareFormatReader.SplitRange r : ranges) {
            assertTrue("Range length must be positive", r.length() > 0);
            assertTrue(
                "Ranges must be sorted and non-overlapping: offset=" + r.offset() + ", previous end=" + lastEnd,
                r.offset() >= lastEnd
            );
            lastEnd = r.offset() + r.length();
        }

        // Sum rows emitted across all splits and compare to the true row count.
        int totalRowsFromRanges = 0;
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            long rangeStart = range.offset();
            long rangeEnd = rangeStart + range.length();
            try (
                CloseableIterator<Page> iterator = reader.readRange(
                    storageObject,
                    new RangeReadContext(null, 1000, rangeStart, rangeEnd, List.of(), ErrorPolicy.STRICT)
                )
            ) {
                while (iterator.hasNext()) {
                    totalRowsFromRanges += iterator.next().getPositionCount();
                }
            }
        }

        assertEquals("Sum of rows across splits must equal the row count written", numRows, totalRowsFromRanges);
    }

    /**
     * Verifies that cross-split footer caching via {@link RangeReadContext#fileContext()} produces
     * correct results. Simulates the {@code AsyncExternalSourceOperatorFactory} pattern: the first
     * split parses the footer and caches it; subsequent splits reuse the cached footer. Row counts
     * and values must match a non-cached full read.
     */
    public void testCrossSlitFooterCachingProducesCorrectResults() throws Exception {
        int numRows = 5_000;
        byte[] parquetData = createCompressibleMultiRowGroupFile(numRows, CompressionCodecName.SNAPPY);
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Need multiple ranges to exercise caching", ranges.size() >= 2);

        Object cachedFileContext = null;
        int cachedTotalRows = 0;
        Set<Long> cachedIds = new HashSet<>();

        for (RangeAwareFormatReader.SplitRange range : ranges) {
            RangeReadContext ctx = new RangeReadContext(
                null,
                1000,
                range.offset(),
                range.offset() + range.length(),
                List.of(),
                ErrorPolicy.STRICT
            );
            if (cachedFileContext != null) {
                ctx.setFileContext(cachedFileContext);
            }
            try (CloseableIterator<Page> iterator = reader.readRange(storageObject, ctx)) {
                while (iterator.hasNext()) {
                    Page page = iterator.next();
                    LongBlock ids = (LongBlock) page.getBlock(0);
                    for (int row = 0; row < page.getPositionCount(); row++) {
                        cachedIds.add(ids.getLong(row));
                    }
                    cachedTotalRows += page.getPositionCount();
                }
            }
            assertNotNull("fileContext should be set after readRange", ctx.fileContext());
            cachedFileContext = ctx.fileContext();
        }

        assertEquals("Footer-cached row count must match written rows", numRows, cachedTotalRows);
        assertEquals("Footer-cached ids must cover all rows (no duplicates, no drops)", numRows, cachedIds.size());
    }

    /**
     * Regression test for #147691: concurrent {@code readRange} calls on a single shared
     * {@link ParquetFormatReader} must not corrupt the codec factory it caches internally.
     * <p>
     * In production {@code AsyncExternalSourceOperatorFactory} hands a single {@code ParquetFormatReader}
     * to multiple driver threads, each reading a distinct row-group split. Every split decodes
     * column chunks via the reader's shared {@link PlainCompressionCodecFactory}, so
     * {@link PlainCompressionCodecFactory#getDecompressor} runs concurrently for the same codec.
     * An earlier implementation backed the lookup with an unsynchronized {@code HashMap}, which
     * raced under load (typically as {@code ConcurrentModificationException} from
     * {@code computeIfAbsent}, but the symptom is non-deterministic).
     * <p>
     * We additionally collect the {@code id} value of every emitted row into a shared set, so a
     * silently-corrupted decode (e.g. plausible-looking but wrong INT64s after a torn snappy
     * block) is caught as a duplicate or missing id rather than only via the row count.
     */
    public void testConcurrentReadRangeAcrossRowGroupsDoesNotCorruptCodecFactory() throws Exception {
        int numRows = 10_000;
        // 8 KB row-group target with highly-compressible payload reliably produces several
        // row groups; the assertion below pins the invariant in case writer defaults change.
        byte[] parquetData = createCompressibleMultiRowGroupFile(numRows, CompressionCodecName.SNAPPY);
        StorageObject storageObject = createStorageObject(parquetData);

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertThat("Test needs multiple row groups to exercise concurrency", ranges.size(), greaterThan(2));

        AtomicInteger totalRows = new AtomicInteger();
        Set<Long> observedIds = new HashSet<>();

        startInParallel(ranges.size(), i -> {
            RangeAwareFormatReader.SplitRange range = ranges.get(i);
            long rangeStart = range.offset();
            long rangeEnd = rangeStart + range.length();
            List<Long> idsForSplit = new ArrayList<>();
            try (
                CloseableIterator<Page> iterator = reader.readRange(
                    storageObject,
                    new RangeReadContext(null, 500, rangeStart, rangeEnd, List.of(), ErrorPolicy.STRICT)
                )
            ) {
                while (iterator.hasNext()) {
                    Page page = iterator.next();
                    LongBlock ids = (LongBlock) page.getBlock(0);
                    for (int row = 0; row < page.getPositionCount(); row++) {
                        idsForSplit.add(ids.getLong(row));
                    }
                    totalRows.addAndGet(page.getPositionCount());
                    page.releaseBlocks();
                }
            } catch (IOException e) {
                throw new AssertionError("readRange failed for split [" + rangeStart + "," + rangeEnd + ")", e);
            }
            // Single bulk synchronization per split rather than per-row contention.
            synchronized (observedIds) {
                for (Long id : idsForSplit) {
                    assertTrue("duplicate id [" + id + "] across splits", observedIds.add(id));
                }
            }
        });

        assertEquals("All splits combined must yield every row exactly once", numRows, totalRows.get());
        assertEquals("Observed ids must cover the full row range", numRows, observedIds.size());
    }

    /**
     * End-to-end test: reads a multi-row-group Parquet file with a filter via {@code read()},
     * then reads via per-range {@code readRange()} with the same filter. Asserts the union of
     * range reads produces identical rows to the full read.
     */
    public void testReadRangeWithFilterProducesCorrectResults() throws Exception {
        byte[] parquetData = createWideMultiRowGroupFile(500);
        StorageObject storageObject = createStorageObject(parquetData);
        FilterPredicate filter = FilterApi.gt(FilterApi.longColumn("id"), -1L);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory).withPushedFilter(FilterCompat.get(filter));

        List<String> fullRows = collectIdPayloadRows(reader.read(storageObject, null, 500));

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Need at least 2 ranges for this test, got " + ranges.size(), ranges.size() >= 2);
        List<String> rangeRows = collectRangeRows(reader, storageObject, ranges);

        assertEquals(fullRows.size(), rangeRows.size());
        Comparator<String> byId = Comparator.comparingLong(s -> Long.parseLong(s.split("\\|", 2)[0]));
        fullRows.sort(byId);
        rangeRows.sort(byId);
        assertEquals(fullRows, rangeRows);
    }

    public void testReadRangeBaselineNoFilter() throws Exception {
        byte[] parquetData = createWideMultiRowGroupFile(500);
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, false);

        List<String> fullRows = collectIdPayloadRows(reader.read(storageObject, null, 500));

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Need at least 2 ranges for this test, got " + ranges.size(), ranges.size() >= 2);
        List<String> rangeRows = collectRangeRows(reader, storageObject, ranges);

        assertEquals(fullRows.size(), rangeRows.size());
        Comparator<String> byId = Comparator.comparingLong(s -> Long.parseLong(s.split("\\|", 2)[0]));
        fullRows.sort(byId);
        rangeRows.sort(byId);
        assertEquals(fullRows, rangeRows);
    }

    public void testReadRangeBaselineWithFilterProducesCorrectResults() throws Exception {
        byte[] parquetData = createWideMultiRowGroupFile(500);
        StorageObject storageObject = createStorageObject(parquetData);
        FilterPredicate filter = FilterApi.gt(FilterApi.longColumn("id"), -1L);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, false).withPushedFilter(FilterCompat.get(filter));

        List<String> fullRows = collectIdPayloadRows(reader.read(storageObject, null, 500));

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Need at least 2 ranges for this test, got " + ranges.size(), ranges.size() >= 2);
        List<String> rangeRows = collectRangeRows(reader, storageObject, ranges);

        assertEquals(fullRows.size(), rangeRows.size());
        Comparator<String> byId = Comparator.comparingLong(s -> Long.parseLong(s.split("\\|", 2)[0]));
        fullRows.sort(byId);
        rangeRows.sort(byId);
        assertEquals(fullRows, rangeRows);
    }

    private static List<String> collectIdPayloadRows(CloseableIterator<Page> iterator) throws IOException {
        List<String> rows = new ArrayList<>();
        try (iterator) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                LongBlock ids = (LongBlock) page.getBlock(0);
                BytesRefBlock payloads = (BytesRefBlock) page.getBlock(1);
                BytesRef scratch = new BytesRef();
                for (int row = 0; row < page.getPositionCount(); row++) {
                    rows.add(ids.getLong(row) + "|" + payloads.getBytesRef(row, scratch).utf8ToString());
                }
            }
        }
        return rows;
    }

    private static List<String> collectRangeRows(
        ParquetFormatReader reader,
        StorageObject storageObject,
        List<RangeAwareFormatReader.SplitRange> ranges
    ) throws IOException {
        List<String> rows = new ArrayList<>();
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            long rangeStart = range.offset();
            long rangeEnd = rangeStart + range.length();
            rows.addAll(
                collectIdPayloadRows(
                    reader.readRange(storageObject, new RangeReadContext(null, 500, rangeStart, rangeEnd, List.of(), ErrorPolicy.STRICT))
                )
            );
        }
        return rows;
    }

    // --- Test helpers ---

    /**
     * Creates a multi-row-group Parquet file whose uncompressed row-group sizes are meaningfully
     * larger than the on-disk compressed sizes. The payload is a highly repetitive string, so
     * codecs such as Snappy compress it dramatically. Used to exercise the split-range length
     * contract (compressed bytes) vs uncompressed byte reporting in Parquet metadata.
     */
    private byte[] createCompressibleMultiRowGroupFile(int numRows, CompressionCodecName codec) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("test_schema");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        // Highly compressible: 512 bytes of a single character.
        String payload = "a".repeat(512);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(codec)
                .withRowGroupSize(8 * 1024L)
                .withPageSize(512)
                .build()
        ) {
            for (int i = 0; i < numRows; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                g.add("payload", payload);
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private static byte[] toFloat16Bytes(float value) {
        short float16 = Float.floatToFloat16(value);
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (float16 & 0xFF);
        bytes[1] = (byte) ((float16 >> 8) & 0xFF);
        return bytes;
    }

    private static byte[] toUuidBytes(UUID uuid) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

    private static byte[] toFixedLenDecimal(long unscaledValue, int fixedLen) {
        byte[] unscaledBytes = BigInteger.valueOf(unscaledValue).toByteArray();
        byte[] padded = new byte[fixedLen];
        byte fill = unscaledValue < 0 ? (byte) 0xFF : (byte) 0x00;
        Arrays.fill(padded, fill);
        System.arraycopy(unscaledBytes, 0, padded, fixedLen - unscaledBytes.length, unscaledBytes.length);
        return padded;
    }

    @FunctionalInterface
    private interface GroupCreator {
        List<Group> create(SimpleGroupFactory factory);
    }

    /**
     * Creates a Parquet file with wide rows (INT64 id + 200-char BINARY payload) and a small
     * row group size to guarantee multiple row groups in the output.
     */
    private byte[] createWideMultiRowGroupFile(int numRows) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("test_schema");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        String padding = "x".repeat(200);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(4 * 1024L)
                .withPageSize(512)
                .build()
        ) {
            for (int i = 0; i < numRows; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                g.add("payload", "row-" + i + "-" + padding);
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private byte[] createParquetFile(MessageType schema, GroupCreator groupCreator) throws IOException {
        return createParquetFile(schema, groupCreator, CompressionCodecName.UNCOMPRESSED);
    }

    private byte[] createParquetFile(MessageType schema, GroupCreator groupCreator, CompressionCodecName codec) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        OutputFile outputFile = createOutputFile(outputStream);

        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        List<Group> groups = groupCreator.create(groupFactory);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(codec)
                .build()
        ) {
            for (Group group : groups) {
                writer.write(group);
            }
        }

        return outputStream.toByteArray();
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        outputStream.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        outputStream.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        outputStream.close();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://test.parquet";
            }
        };
    }

    /**
     * Builds a {@link StorageObject} that completes {@code readBytesAsync} on {@code pool} (never on
     * the executor the caller passes in), so tests can assert the executor thread is released across
     * the "network" read. {@code asyncReadCount} records the number of async dispatches; when
     * {@code reads} is non-null each {@code readBytesAsync} appends a {@code [position, length]} pair
     * so tests can inspect the exact ranges requested.
     */
    private static StorageObject createAsyncStorageObject(
        byte[] data,
        ExecutorService pool,
        AtomicInteger asyncReadCount,
        List<long[]> reads
    ) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor ignored,
                ActionListener<DirectReadBuffer> listener
            ) {
                asyncReadCount.incrementAndGet();
                if (reads != null) {
                    reads.add(new long[] { position, length });
                }
                pool.execute(() -> {
                    try {
                        int pos = (int) position;
                        int len = (int) Math.min(length, data.length - position);
                        byte[] slice = new byte[len];
                        System.arraycopy(data, pos, slice, 0, len);
                        listener.onResponse(new DirectReadBuffer(ByteBuffer.wrap(slice), () -> {}));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                });
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://async-test.parquet");
            }
        };
    }

    /**
     * Like {@link #createAsyncStorageObject} but tracks {@link DirectReadBuffer} lifecycle so leak
     * tests can assert release: {@code allocated} counts buffers handed out and {@code openBuffers} is
     * incremented on allocation and decremented on {@link DirectReadBuffer#close()}. When
     * {@code failReadIndex >= 0} that (0-based) read completes with a failure and allocates no buffer.
     */
    private static StorageObject createBufferTrackingAsyncStorageObject(
        byte[] data,
        ExecutorService pool,
        AtomicInteger openBuffers,
        AtomicInteger allocated,
        int failReadIndex
    ) {
        AtomicInteger readIndex = new AtomicInteger();
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor ignored,
                ActionListener<DirectReadBuffer> listener
            ) {
                int idx = readIndex.getAndIncrement();
                pool.execute(() -> {
                    if (idx == failReadIndex) {
                        listener.onFailure(new IOException("injected read failure at index " + idx));
                        return;
                    }
                    try {
                        int pos = (int) position;
                        int len = (int) Math.min(length, data.length - position);
                        byte[] slice = new byte[len];
                        System.arraycopy(data, pos, slice, 0, len);
                        allocated.incrementAndGet();
                        openBuffers.incrementAndGet();
                        listener.onResponse(new DirectReadBuffer(ByteBuffer.wrap(slice), openBuffers::decrementAndGet));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                });
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://async-leak-test.parquet");
            }
        };
    }

    private StorageObject createStorageObject(byte[] data) {
        return createStorageObject(data, "memory://test.parquet");
    }

    private StorageObject createStorageObject(byte[] data, String locationUri) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of(locationUri);
            }
        };
    }

    /**
     * Regression test for the {@code PageColumnReader} buffer-aliasing bug. Models the producer/
     * consumer boundary deterministically in a single thread by maintaining a queue lookahead
     * larger than the {@link DecodeBuffers} slot count, so every {@link Page} the consumer reads
     * has been alive across several subsequent decodes by the producer - the same shape
     * {@code AsyncExternalSourceBuffer} produces in production. See the {@code Block construction
     * helpers} block in {@link PageColumnReader} for the bug mechanism.
     *
     * <p>All four affected primitive types are covered: longs and doubles (two-slot rotation in
     * {@code DecodeBuffers}), ints and booleans (single slot - aliasing hits at queue depth 2).
     * Without the fix each batch's decode mutates earlier emitted Blocks (typical failure:
     * {@code expected:<0> but was:<160>}); with the fix every value equals its write-time row
     * index.
     */
    public void testEmittedPagesAreNotMutatedAcrossProducerConsumerBoundary() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("v_long")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("v_int")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("v_double")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("v_bool")
            .named("retention_test_pc");

        int batchSize = 37;
        int totalRows = batchSize * 20;
        // Lookahead must exceed the DecodeBuffers slot count (2 for long/double, 1 for
        // int/boolean) so the producer is guaranteed to reuse a buffer the consumer still
        // references. 6 mirrors the typical AsyncExternalSourceBuffer queue depth.
        int lookahead = 6;

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(totalRows);
            for (int i = 0; i < totalRows; i++) {
                Group g = factory.newGroup();
                g.add("v_long", (long) i);
                g.add("v_int", i);
                g.add("v_double", (double) i);
                // (i % 7) < 3 has period 7, which does not divide batchSize=32, so the
                // boolean pattern differs between consecutive batches and aliasing actually
                // shows up as a visible mutation.
                g.add("v_bool", (i % 7) < 3);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        Deque<Page> queue = new ArrayDeque<>();
        int rowOffset = 0;
        int maxObservedDepth = 0;
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, batchSize)) {
            while (true) {
                // Producer: keep the queue full up to `lookahead`, mirroring the async buffer
                // refilling ahead of the consumer.
                while (queue.size() < lookahead && iterator.hasNext()) {
                    queue.addLast(iterator.next());
                }
                if (queue.isEmpty()) break;
                maxObservedDepth = Math.max(maxObservedDepth, queue.size());
                // Consumer: pull the oldest queued page and verify its values. By construction
                // the producer has already decoded `lookahead - 1` more pages into the same
                // PageColumnReader since this page was emitted.
                Page page = queue.removeFirst();
                try {
                    int rows = page.getPositionCount();
                    LongBlock lb = (LongBlock) page.getBlock(0);
                    IntBlock ib = (IntBlock) page.getBlock(1);
                    DoubleBlock db = (DoubleBlock) page.getBlock(2);
                    BooleanBlock bb = (BooleanBlock) page.getBlock(3);
                    for (int r = 0; r < rows; r++) {
                        long expected = rowOffset + r;
                        assertEquals(
                            "long value mutated after queue lookahead (page-rel row=" + r + ", abs=" + expected + ")",
                            expected,
                            lb.getLong(r)
                        );
                        assertEquals(
                            "int value mutated after queue lookahead (page-rel row=" + r + ", abs=" + expected + ")",
                            (int) expected,
                            ib.getInt(r)
                        );
                        assertEquals(
                            "double value mutated after queue lookahead (page-rel row=" + r + ", abs=" + expected + ")",
                            (double) expected,
                            db.getDouble(r),
                            0.0
                        );
                        assertEquals(
                            "boolean value mutated after queue lookahead (page-rel row=" + r + ", abs=" + expected + ")",
                            (expected % 7) < 3,
                            bb.getBoolean(r)
                        );
                    }
                    rowOffset += rows;
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        assertEquals(totalRows, rowOffset);
        assertTrue(
            "expected the queue to grow past the DecodeBuffers slot count to actually exercise the bug, got max depth " + maxObservedDepth,
            maxObservedDepth >= 3
        );
    }

    /**
     * Regression test for the zero-copy {@link BytesRef} optimisation in {@link PlainValueDecoder}
     * and the dictionary cache in {@link DictionaryValueDecoder}. Models the producer/consumer
     * boundary the same way as
     * {@link #testEmittedPagesAreNotMutatedAcrossProducerConsumerBoundary()}: a queue lookahead
     * ensures the consumer reads pages that were emitted several decode batches ago.
     *
     * <p>Two string columns are tested: {@code v_unique} has unique values per row (triggers PLAIN
     * encoding once parquet-mr's dictionary threshold is exceeded), and {@code v_dict} has low
     * cardinality (stays dictionary-encoded). Both exercise the code paths that now return
     * {@link BytesRef} objects sharing backing arrays with the page buffer or the dictionary cache.
     */
    public void testStringColumnsNotCorruptedAcrossProducerConsumerBoundary() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("v_unique")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("v_dict")
            .named("retention_test_string");

        int batchSize = 32;
        int totalRows = batchSize * 20;
        int lookahead = 6;
        String[] dictValues = { "alpha", "bravo", "charlie", "delta", "echo" };

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(totalRows);
            for (int i = 0; i < totalRows; i++) {
                Group g = factory.newGroup();
                g.add("v_unique", "row-" + i);
                g.add("v_dict", dictValues[i % dictValues.length]);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        Deque<Page> queue = new ArrayDeque<>();
        int rowOffset = 0;
        int maxObservedDepth = 0;
        BytesRef scratch = new BytesRef();
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, batchSize)) {
            while (true) {
                while (queue.size() < lookahead && iterator.hasNext()) {
                    queue.addLast(iterator.next());
                }
                if (queue.isEmpty()) break;
                maxObservedDepth = Math.max(maxObservedDepth, queue.size());
                Page page = queue.removeFirst();
                try {
                    int rows = page.getPositionCount();
                    BytesRefBlock uniqueBlock = (BytesRefBlock) page.getBlock(0);
                    BytesRefBlock dictBlock = (BytesRefBlock) page.getBlock(1);
                    for (int r = 0; r < rows; r++) {
                        int absRow = rowOffset + r;
                        assertEquals(
                            "unique string corrupted after queue lookahead (row=" + absRow + ")",
                            new BytesRef("row-" + absRow),
                            uniqueBlock.getBytesRef(r, scratch)
                        );
                        assertEquals(
                            "dict string corrupted after queue lookahead (row=" + absRow + ")",
                            new BytesRef(dictValues[absRow % dictValues.length]),
                            dictBlock.getBytesRef(r, scratch)
                        );
                    }
                    rowOffset += rows;
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        assertEquals(totalRows, rowOffset);
        assertTrue(
            "expected the queue to grow past 3 to exercise the zero-copy BytesRef paths, got max depth " + maxObservedDepth,
            maxObservedDepth >= 3
        );
    }

    public void testWithConfigOptimizedReaderTrue() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of("optimized_reader", true));
        assertSame(reader, configured);
    }

    public void testWithConfigOptimizedReaderFalse() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of("optimized_reader", false));
        assertNotSame(reader, configured);
    }

    public void testWithConfigOptimizedReaderStringTrue() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of("optimized_reader", "true"));
        assertSame(reader, configured);
    }

    public void testWithConfigDefaults() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        assertSame(reader, reader.withConfig(null));
        assertSame(reader, reader.withConfig(Map.of()));
    }

    // --- Nested STRUCT subfield projection tests ---

    public void testNestedStructFlattening() throws Exception {
        MessageType schema = new MessageType(
            "test_schema",
            Types.optionalGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("action")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("outcome")
                .named("event")
        );

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            Group ev = g.addGroup("event");
            ev.append("action", "login");
            ev.append("outcome", "success");
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(createStorageObject(parquetData));
        List<Attribute> attrs = metadata.schema();
        assertEquals(2, attrs.size());
        assertEquals("event.action", attrs.get(0).name());
        assertEquals(DataType.KEYWORD, attrs.get(0).dataType());
        assertEquals("event.outcome", attrs.get(1).name());
        assertEquals(DataType.KEYWORD, attrs.get(1).dataType());
    }

    public void testNestedStructDeepFlattening() throws Exception {
        // three-level nesting a.b.c.x
        MessageType schema = new MessageType(
            "deep",
            Types.optionalGroup()
                .addField(
                    Types.optionalGroup()
                        .addField(Types.optionalGroup().optional(PrimitiveType.PrimitiveTypeName.INT32).named("x").named("c"))
                        .named("b")
                )
                .named("a")
        );
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.addGroup("a").addGroup("b").addGroup("c").append("x", 42);
            return List.of(g);
        });
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(createStorageObject(parquetData));
        assertEquals(1, metadata.schema().size());
        assertEquals("a.b.c.x", metadata.schema().get(0).name());
        assertEquals(DataType.INTEGER, metadata.schema().get(0).dataType());
    }

    public void testNestedStructProjectionSingleSubfield() throws Exception {
        MessageType schema = new MessageType(
            "test_schema",
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
            Types.optionalGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("action")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("outcome")
                .named("event")
        );

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("id", 1L);
            Group ev = g.addGroup("event");
            ev.append("action", "login");
            ev.append("outcome", "success");
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject so = createStorageObject(parquetData);
        try (CloseableIterator<Page> iterator = reader.read(so, List.of("event.action"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getBlockCount());
            assertEquals(1, page.getPositionCount());
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertEquals(new BytesRef("login"), block.getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testNestedStructProjectionTwoSubfieldsSameParent() throws Exception {
        MessageType schema = new MessageType(
            "test_schema",
            Types.optionalGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("action")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("outcome")
                .named("event")
        );

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            Group ev = g.addGroup("event");
            ev.append("action", "login");
            ev.append("outcome", "success");
            return List.of(g);
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject so = createStorageObject(parquetData);
        try (CloseableIterator<Page> iterator = reader.read(so, List.of("event.action", "event.outcome"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getBlockCount());
            assertEquals(new BytesRef("login"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("success"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testNestedStructProjectionMixedTopLevelAndNested() throws Exception {
        MessageType schema = new MessageType(
            "test_schema",
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
            Types.optionalGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("action")
                .named("event")
        );
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("id", 7L);
            g.addGroup("event").append("action", "logout");
            return List.of(g);
        });
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject so = createStorageObject(parquetData);
        try (CloseableIterator<Page> iterator = reader.read(so, List.of("id", "event.action"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getBlockCount());
            assertEquals(7L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("logout"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testLiteralDottedNamePrecedence() throws Exception {
        // Top-level primitive literally named "event.action"; projecting "event.action" must
        // resolve to this field, not to a nested path.
        MessageType schema = new MessageType(
            "test_schema",
            Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("event.action")
        );
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.append("event.action", "literal");
            return List.of(g);
        });
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject so = createStorageObject(parquetData);
        SourceMetadata metadata = reader.metadata(so);
        assertEquals(1, metadata.schema().size());
        assertEquals("event.action", metadata.schema().get(0).name());
        try (CloseableIterator<Page> iterator = reader.read(so, List.of("event.action"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(new BytesRef("literal"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testNestedStructDepthCap() throws Exception {
        // Synthesize a struct that exceeds MAX_STRUCT_FLATTENING_DEPTH; assert the over-cap group
        // surfaces as a single UNSUPPORTED attribute (no infinite loop).
        int over = ParquetFormatReader.MAX_STRUCT_FLATTENING_DEPTH + 4;
        Type field = Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("leaf");
        for (int i = 0; i < over; i++) {
            field = Types.optionalGroup().addField(field).named("g" + i);
        }
        MessageType schema = new MessageType("deep_schema", field);

        // No data needed; we only inspect metadata. Write an empty row group via no rows.
        byte[] parquetData = createParquetFile(schema, factory -> List.of());
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(createStorageObject(parquetData));
        // Once recursion crosses the cap, the over-cap group is emitted as one UNSUPPORTED
        // attribute and recursion stops; deeper leaves never surface. So we expect exactly one
        // attribute, UNSUPPORTED, whose dotted name has cap+1 segments (the cap-exceeding group
        // itself; everything below it is collapsed).
        assertEquals(1, metadata.schema().size());
        Attribute attr = metadata.schema().get(0);
        assertEquals(DataType.UNSUPPORTED, attr.dataType());
        assertEquals(
            "expected name with cap+1 segments (the first over-cap level)",
            ParquetFormatReader.MAX_STRUCT_FLATTENING_DEPTH + 1,
            attr.name().split("\\.").length
        );
    }

    public void testNestedStructFilterPushdownPrunesRowGroups() throws Exception {
        // Two row groups with disjoint event.id ranges. Pushing event.id > 999 must prune the
        // first row group entirely; the surviving row count must reflect only the second.
        MessageType schema = new MessageType(
            "test_schema",
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
            Types.optionalGroup().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("event")
        );

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        // Small row-group size so each batch of writes lands in its own row group, large
        // enough to fit ~50 rows. Disable column index (small files default off) — row group
        // statistics are sufficient for this assertion.
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1024L)
                .withPageSize(256)
                .build()
        ) {
            for (long i = 1; i <= 50; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", i);
                g.addGroup("event").append("id", i);
                writer.write(g);
            }
            for (long i = 1000; i <= 1050; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", i);
                g.addGroup("event").append("id", i);
                writer.write(g);
            }
        }
        byte[] parquetData = outputStream.toByteArray();

        // Sanity: without pushdown, all rows are read.
        ParquetFormatReader baseline = new ParquetFormatReader(blockFactory);
        int totalRows = countRows(baseline, parquetData, List.of("event.id"));
        assertEquals(101, totalRows);

        // Push event.id > 999. The first row group's max is 50 → stats prune it entirely;
        // only the second row group (51 rows) survives.
        org.elasticsearch.xpack.esql.core.expression.Expression filter =
            new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan(
                org.elasticsearch.xpack.esql.core.tree.Source.EMPTY,
                new ReferenceAttribute(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, "event.id", DataType.LONG),
                new org.elasticsearch.xpack.esql.core.expression.Literal(
                    org.elasticsearch.xpack.esql.core.tree.Source.EMPTY,
                    999L,
                    DataType.LONG
                ),
                null
            );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));
        ParquetFormatReader filtered = new ParquetFormatReader(blockFactory).withPushedFilter(pushed);
        int survived = countRows(filtered, parquetData, List.of("event.id"));
        // Strict less-than is the contract: the first row group's 50 rows must be pruned. The
        // exact count is at most 51 (the second row group), since parquet-mr may not perform
        // per-row strict cleanup at the reader level without a record filter.
        assertTrue("expected pruning to drop the first row group (50 rows). survived=" + survived, survived <= 51 && survived < totalRows);
    }

    private int countRows(ParquetFormatReader reader, byte[] parquetData, List<String> projection) throws IOException {
        int total = 0;
        try (CloseableIterator<Page> iter = reader.read(createStorageObject(parquetData), projection, 1024)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                total += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        return total;
    }

    public void testNestedColumnStatistics() throws Exception {
        // Two row groups with distinct min/max for event.id, plus a flat top-level "id" to assert
        // the publication walk does not drop top-level stats when adding nested ones.
        MessageType schema = new MessageType(
            "test_schema",
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
            Types.optionalGroup()
                .optional(PrimitiveType.PrimitiveTypeName.INT64)
                .named("id")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("action")
                .named("event")
        );

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        // Small row-group size so each batch of writes lands in its own row group.
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1024L)
                .withPageSize(256)
                .build()
        ) {
            // Group 1: event.id in [1, 50], event.action = "login"
            for (long i = 1; i <= 50; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", i);
                g.addGroup("event").append("id", i).append("action", "login");
                writer.write(g);
            }
            // Flush by writing more rows that exceed the small row-group budget; group 2
            // has a disjoint event.id range [1000, 1050] and event.action = "logout".
            for (long i = 1000; i <= 1050; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", i);
                g.addGroup("event").append("id", i).append("action", "logout");
                writer.write(g);
            }
        }
        byte[] parquetData = outputStream.toByteArray();

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(createStorageObject(parquetData));

        assertTrue("expected source statistics", metadata.statistics().isPresent());
        var optColStats = metadata.statistics().get().columnStatistics();
        assertTrue("expected per-column statistics", optColStats.isPresent());
        Map<String, ? extends org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics.ColumnStatistics> colStats = optColStats.get();

        // Nested leaves are present with the dotted name.
        assertTrue("event.id stats must be published", colStats.containsKey("event.id"));
        assertTrue("event.action stats must be published", colStats.containsKey("event.action"));
        // Top-level stats remain (no regression).
        assertTrue("top-level id stats still present", colStats.containsKey("id"));

        // event.id: 1 .. 1050 across the two row groups.
        var eventIdStats = colStats.get("event.id");
        assertEquals(Optional.of(1L), eventIdStats.minValue());
        assertEquals(Optional.of(1050L), eventIdStats.maxValue());
        assertEquals(OptionalLong.of(0L), eventIdStats.nullCount());

        // event.action: KEYWORD → min/max are BytesRef-backed; we only assert presence to avoid
        // coupling to parquet-mr's internal Binary representation.
        var eventActionStats = colStats.get("event.action");
        assertTrue("event.action min must be present", eventActionStats.minValue().isPresent());
        assertTrue("event.action max must be present", eventActionStats.maxValue().isPresent());

        // Top-level id stats: 1 .. 1050.
        var topIdStats = colStats.get("id");
        assertEquals(Optional.of(1L), topIdStats.minValue());
        assertEquals(Optional.of(1050L), topIdStats.maxValue());
    }

    /**
     * Regression for the {@code COUNT(<col>)} correctness bug on external Parquet sources: when a
     * column's {@code null_count} footer statistic is absent (a conformant writer may omit it, e.g.
     * an Arrow null-typed / all-null column, reproduced here by disabling statistics for a single
     * column), the reader must report the null count as <b>unknown</b> — {@link OptionalLong#empty()}
     * in {@link org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics.ColumnStatistics#nullCount()}
     * and no {@code null_count} key in the per-split stats — rather than a known zero. A known zero
     * would let the aggregate pushdown answer {@code COUNT(col) = rowCount - 0 = rowCount} instead of
     * the true non-null count, silently returning the row count for an all-null column.
     * <p>
     * The file carries three columns over 200 rows: {@code n} (no nulls, stats on), {@code rare}
     * (196 of 200 null, stats on) and {@code always_null} (all null, stats <b>off</b>). Only
     * {@code always_null} must surface as unknown; {@code n}/{@code rare} keep exact null counts,
     * and {@code always_null} keeps its {@code size_bytes} so it still reads as a present column.
     */
    public void testMissingNullCountStatisticReportedAsUnknown() throws Exception {
        MessageType schema = Types.buildMessage()
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named("n")
            .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("rare")
            .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("always_null")
            .named("test_schema");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                // Disable statistics only for always_null so its footer carries no null_count, while
                // n and rare keep theirs — exactly the mixed situation the bug fires on.
                .withStatisticsEnabled("always_null", false)
                .build()
        ) {
            for (int i = 0; i < 200; i++) {
                Group g = groupFactory.newGroup();
                g.add("n", (long) i);
                if (i < 4) {
                    g.add("rare", (double) i);
                }
                // always_null: never assigned → all 200 rows null.
                writer.write(g);
            }
        }
        byte[] parquetData = outputStream.toByteArray();

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject so = createStorageObject(parquetData);

        // --- extractStatistics path (metadata) ---
        SourceMetadata metadata = reader.metadata(so);
        assertTrue("expected source statistics", metadata.statistics().isPresent());
        assertEquals(OptionalLong.of(200L), metadata.statistics().get().rowCount());
        var colStats = metadata.statistics().get().columnStatistics().orElseThrow();

        assertEquals("no-null column keeps a known zero null count", OptionalLong.of(0L), colStats.get("n").nullCount());
        assertEquals("partially-null column keeps its exact null count", OptionalLong.of(196L), colStats.get("rare").nullCount());

        var alwaysNullStats = colStats.get("always_null");
        assertNotNull("always_null must still be published (it is a present column)", alwaysNullStats);
        assertEquals(
            "missing null_count statistic must be reported as unknown, not zero",
            OptionalLong.empty(),
            alwaysNullStats.nullCount()
        );
        assertTrue("always_null has no non-null values, so no min", alwaysNullStats.minValue().isEmpty());
        assertTrue("always_null has no non-null values, so no max", alwaysNullStats.maxValue().isEmpty());
        assertTrue("always_null column is present, so its size is known", alwaysNullStats.sizeInBytes().isPresent());

        // --- buildRowGroupStats path (discoverSplitRanges) ---
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(so);
        assertFalse("expected at least one split range", ranges.isEmpty());
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            Map<String, Object> stats = range.statistics();
            assertFalse(
                "always_null must not carry a null_count key when the footer omits the statistic",
                stats.containsKey("_stats.columns.always_null.null_count")
            );
            assertTrue(
                "always_null is a present column, so its size_bytes key must be written",
                stats.containsKey("_stats.columns.always_null.size_bytes")
            );
            assertEquals("rare keeps its exact null count in split stats", 196L, stats.get("_stats.columns.rare.null_count"));
            assertEquals("n keeps a known zero null count in split stats", 0L, stats.get("_stats.columns.n.null_count"));
        }
    }

    /**
     * Companion to {@link #testMissingNullCountStatisticReportedAsUnknown} that pins the
     * <b>cross-row-group</b> accumulation in {@code extractStatistics}: the null count must degrade to
     * unknown as soon as <b>any</b> covering row group omits the {@code null_count} statistic, even when
     * other row groups record it. This is the only case that exercises the
     * {@code nullCount != null && unknownNullCounts.contains(name) == false} conjunction &mdash; an
     * all-unknown column trips the {@code != null} guard on its own.
     * <p>
     * A single Parquet writer emits statistics uniformly for a column across all its row groups, so the
     * mixed situation is stitched from two independently written single-row-group files with
     * {@link ParquetFileWriter#appendFile(org.apache.parquet.io.InputFile)}: row group 0 records
     * {@code x}'s null count, row group 1 (statistics disabled for {@code x}) does not. Control column
     * {@code y} keeps statistics in both row groups and must surface the summed null count, proving the
     * accumulation still adds across row groups when every group reports.
     */
    public void testMissingNullCountAcrossRowGroupsReportedAsUnknown() throws Exception {
        MessageType schema = Types.buildMessage()
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named("x")
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named("y")
            .named("test_schema");

        // Row group 0: statistics on for both columns -> x and y both record null_count.
        Path rgWithStats = writeSingleRowGroup(schema, Set.of(), 2, 1);
        // Row group 1: statistics disabled for x only -> x carries no null_count, y still does.
        Path rgWithoutXStats = writeSingleRowGroup(schema, Set.of("x"), 3, 2);

        Path merged = createTempFile();
        try (
            ParquetFileWriter writer = new ParquetFileWriter(
                new LocalOutputFile(merged),
                schema,
                ParquetFileWriter.Mode.OVERWRITE,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.MAX_PADDING_SIZE_DEFAULT
            )
        ) {
            writer.start();
            for (Path source : List.of(rgWithStats, rgWithoutXStats)) {
                // Zero-copy row group transfer preserves each source's footer verbatim, including whether
                // it recorded null_count. Read footers via PlainParquetConfiguration so no Hadoop runtime
                // classes are required — mirrors the reader's own Hadoop-free open path.
                LocalInputFile inputFile = new LocalInputFile(source);
                ParquetReadOptions options = ParquetReadOptions.builder(new PlainParquetConfiguration()).build();
                try (ParquetFileReader fileReader = new ParquetFileReader(inputFile, options)) {
                    List<BlockMetaData> blocks = fileReader.getFooter().getBlocks();
                    try (SeekableInputStream stream = inputFile.newStream()) {
                        writer.appendRowGroups(stream, blocks, false);
                    }
                }
            }
            writer.end(Map.of());
        }
        byte[] parquetData = Files.readAllBytes(merged);

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject so = createStorageObject(parquetData);

        // --- extractStatistics path (metadata): the sticky-across-row-groups accumulation ---
        SourceMetadata metadata = reader.metadata(so);
        assertTrue("expected source statistics", metadata.statistics().isPresent());
        assertEquals("two 5-row row groups", OptionalLong.of(10L), metadata.statistics().get().rowCount());
        var colStats = metadata.statistics().get().columnStatistics().orElseThrow();

        assertEquals(
            "null_count recorded in one row group but omitted in another must degrade to unknown",
            OptionalLong.empty(),
            colStats.get("x").nullCount()
        );
        assertEquals(
            "column with null_count in every row group keeps the summed count",
            OptionalLong.of(3L),
            colStats.get("y").nullCount()
        );

        // --- buildRowGroupStats path (per split): only the row group that records the stat carries the key ---
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(so);
        assertEquals("one split per row group", 2, ranges.size());
        long xNullCountKeys = ranges.stream().filter(r -> r.statistics().containsKey("_stats.columns.x.null_count")).count();
        assertEquals("exactly one row group records x's null_count", 1L, xNullCountKeys);
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            assertTrue("y records null_count in every row group", range.statistics().containsKey("_stats.columns.y.null_count"));
        }
    }

    /**
     * Writes a single-row-group Parquet file (5 rows) with two optional {@code INT64} columns {@code x}
     * and {@code y} to a temp file, disabling footer statistics for the columns named in
     * {@code statsDisabledColumns}. The first {@code xNulls}/{@code yNulls} rows are left null for the
     * respective column. Returned as a {@link Path} so the caller can stitch several such files into a
     * multi-row-group file via {@link ParquetFileWriter#appendFile(org.apache.parquet.io.InputFile)}.
     */
    private Path writeSingleRowGroup(MessageType schema, Set<String> statsDisabledColumns, int xNulls, int yNulls) throws IOException {
        Path path = createTempFile();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new LocalOutputFile(path))
            .withConf(new PlainParquetConfiguration())
            .withCodecFactory(new PlainCompressionCodecFactory())
            .withType(schema)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED);
        for (String col : statsDisabledColumns) {
            builder = builder.withStatisticsEnabled(col, false);
        }
        try (ParquetWriter<Group> writer = builder.build()) {
            for (int i = 0; i < 5; i++) {
                Group g = groupFactory.newGroup();
                if (i >= xNulls) {
                    g.add("x", (long) i);
                }
                if (i >= yNulls) {
                    g.add("y", (long) i);
                }
                writer.write(g);
            }
        }
        return path;
    }

    public void testNestedStructEndToEndWithThreeWayNullPropagation() throws Exception {
        // OPTIONAL event { OPTIONAL action, OPTIONAL outcome }; rows cover:
        // (a) parent null
        // (b) parent non-null, action null
        // (c) both non-null
        MessageType schema = new MessageType(
            "test_schema",
            Types.optionalGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("action")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("outcome")
                .named("event")
        );
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup(); // (a) parent null — no addGroup call
            Group g2 = factory.newGroup(); // (b) parent non-null, action null
            g2.addGroup("event").append("outcome", "success");
            Group g3 = factory.newGroup(); // (c) both non-null
            Group ev3 = g3.addGroup("event");
            ev3.append("action", "logout");
            ev3.append("outcome", "failure");
            return List.of(g1, g2, g3);
        });
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        StorageObject so = createStorageObject(parquetData);
        try (CloseableIterator<Page> iterator = reader.read(so, List.of("event.action"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertTrue("row 0 (parent null) -> child null", block.isNull(0));
            assertTrue("row 1 (parent non-null, child null) -> child null", block.isNull(1));
            assertFalse("row 2 (both non-null) -> non-null", block.isNull(2));
            assertEquals(new BytesRef("logout"), block.getBytesRef(block.getFirstValueIndex(2), new BytesRef()));
            page.releaseBlocks();
        }
    }

    // UINT_32: value 3,000,000,000 overflows signed int to -1,294,967,296
    public void testLargeUint32() throws Exception {
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false)) // unsigned
            .named("object_id")
            .named("test_schema");
        int unsignedBits = 0xFFFFFFFF; // negative in signed int
        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("object_id", unsignedBits)));
        StorageObject storageObject = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        var meta = reader.metadata(storageObject);
        assertEquals("UINT_32 should map to LONG to hold unsigned 32-bit range", DataType.LONG, meta.schema().get(0).dataType());
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(0xFFFFFFFFL, ((LongBlock) page.getBlock(0)).getLong(0));
            assertFalse(iterator.hasNext());
        }
    }

    // UINT_8: value 200 should be read as such
    public void testLargeUint8() throws Exception {
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(8, false)) // unsigned
            .named("object_id")
            .named("test_schema");
        int unsignedBits = 200;
        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("object_id", unsignedBits)));
        StorageObject storageObject = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        var meta = reader.metadata(storageObject);
        assertEquals("UINT_8 should map to INT to hold unsigned 8-bit range", DataType.INTEGER, meta.schema().get(0).dataType());
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(200, ((IntBlock) page.getBlock(0)).getInt(0));
            assertFalse(iterator.hasNext());
        }
    }

    // esql-planning#1030: WHERE pushdown over a uint32 column (physical INT32, widened to
    // ESQL LONG) used to build a Parquet longColumn FilterPredicate, which parquet-mr rejects
    // against the file's INT32 schema — every comparator 500d. This exercises the full read
    // path end to end: the reader must not throw, and must return exactly the matching rows.
    // A sibling uint16 column (stays ESQL INTEGER, never widened) is the issue's suggested
    // "already works" control.
    public void testUint32FilterPushdownDoesNotThrowAndReturnsMatchingRows() throws Exception {
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false)) // unsigned
            .named("u32")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(16, false)) // unsigned control
            .named("u16")
            .named("test_schema");

        // Values straddle Integer.MAX_VALUE, matching the issue's 3,000,000,000-style overflow.
        long[] u32Values = { 50_000L, 100_000L, 200_000L, 3_000_000_000L, 4_000_000_000L };
        int[] u16Values = { 10, 50, 150, 200, 300 };
        byte[] data = createParquetFile(schema, f -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < u32Values.length; i++) {
                Group g = f.newGroup();
                g.append("u32", (int) u32Values[i]);
                g.append("u16", u16Values[i]);
                groups.add(g);
            }
            return groups;
        });
        StorageObject storageObject = createStorageObject(data);

        // The issue's exact reproducer: WHERE u32 > 100000.
        org.elasticsearch.xpack.esql.core.expression.Expression u32Filter =
            new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan(
                Source.EMPTY,
                new ReferenceAttribute(Source.EMPTY, "u32", DataType.LONG),
                new org.elasticsearch.xpack.esql.core.expression.Literal(Source.EMPTY, 100_000L, DataType.LONG),
                null
            );
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory).withPushedFilter(
            new ParquetPushedExpressions(List.of(u32Filter))
        );

        List<Long> survivors = new ArrayList<>();
        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("u32"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                LongBlock block = (LongBlock) page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    survivors.add(block.getLong(i));
                }
                page.releaseBlocks();
            }
        }
        assertEquals(List.of(200_000L, 3_000_000_000L, 4_000_000_000L), survivors);
    }

    // Companion IN/AND coverage, per the issue's "every operator fails" list.
    public void testUint32InAndCombinedAndFilterPushdown() throws Exception {
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false))
            .named("u32")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(16, false))
            .named("u16")
            .named("test_schema");

        long[] u32Values = { 50_000L, 100_000L, 200_000L, 3_000_000_000L, 4_000_000_000L };
        int[] u16Values = { 10, 50, 150, 200, 300 };
        byte[] data = createParquetFile(schema, f -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < u32Values.length; i++) {
                Group g = f.newGroup();
                g.append("u32", (int) u32Values[i]);
                g.append("u16", u16Values[i]);
                groups.add(g);
            }
            return groups;
        });

        // IN (100000, 4000000000)
        org.elasticsearch.xpack.esql.core.expression.Expression inFilter =
            new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In(
                Source.EMPTY,
                new ReferenceAttribute(Source.EMPTY, "u32", DataType.LONG),
                List.of(
                    new org.elasticsearch.xpack.esql.core.expression.Literal(Source.EMPTY, 100_000L, DataType.LONG),
                    new org.elasticsearch.xpack.esql.core.expression.Literal(Source.EMPTY, 4_000_000_000L, DataType.LONG)
                )
            );
        ParquetFormatReader inReader = new ParquetFormatReader(blockFactory).withPushedFilter(
            new ParquetPushedExpressions(List.of(inFilter))
        );
        List<Long> inSurvivors = new ArrayList<>();
        try (CloseableIterator<Page> iterator = inReader.read(createStorageObject(data), List.of("u32"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                LongBlock block = (LongBlock) page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    inSurvivors.add(block.getLong(i));
                }
                page.releaseBlocks();
            }
        }
        assertEquals(List.of(100_000L, 4_000_000_000L), inSurvivors);

        // u32 > 100000 AND u16 > 100 (combined predicate across a widened and a non-widened column)
        org.elasticsearch.xpack.esql.core.expression.Expression u32Gt =
            new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan(
                Source.EMPTY,
                new ReferenceAttribute(Source.EMPTY, "u32", DataType.LONG),
                new org.elasticsearch.xpack.esql.core.expression.Literal(Source.EMPTY, 100_000L, DataType.LONG),
                null
            );
        org.elasticsearch.xpack.esql.core.expression.Expression u16Gt =
            new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan(
                Source.EMPTY,
                new ReferenceAttribute(Source.EMPTY, "u16", DataType.INTEGER),
                new org.elasticsearch.xpack.esql.core.expression.Literal(Source.EMPTY, 100, DataType.INTEGER),
                null
            );
        org.elasticsearch.xpack.esql.core.expression.Expression combined =
            new org.elasticsearch.xpack.esql.expression.predicate.logical.And(Source.EMPTY, u32Gt, u16Gt);
        ParquetFormatReader andReader = new ParquetFormatReader(blockFactory).withPushedFilter(
            new ParquetPushedExpressions(List.of(combined))
        );
        List<Long> andSurvivors = new ArrayList<>();
        try (CloseableIterator<Page> iterator = andReader.read(createStorageObject(data), List.of("u32"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                LongBlock block = (LongBlock) page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    andSurvivors.add(block.getLong(i));
                }
                page.releaseBlocks();
            }
        }
        // u16 > 100 keeps rows [150, 200, 300] -> u32 values [200000, 3000000000, 4000000000];
        // u32 > 100000 further restricts to the same three (100000 itself is excluded by u16=50 anyway).
        assertEquals(List.of(200_000L, 3_000_000_000L, 4_000_000_000L), andSurvivors);
    }

    // esql-planning#1030 follow-up: aggregate (MIN/MAX) pushdown reads a uint32 column's row-group
    // statistics straight off the Parquet footer, which stores the raw INT32 bit pattern. Without
    // widening, a value above Integer.MAX_VALUE (e.g. 4_000_000_000) sign-extends into a negative
    // long, breaking both the SourceStatistics SPI contract (values must match ESQL's in-memory
    // LONG representation) and any downstream consumer (aggregate pushdown, split-skip
    // classification). Exercises both statistics paths: extractStatistics (metadata) and
    // buildRowGroupStats (discoverSplitRanges).
    public void testUint32StatisticsWidenToUnsignedLong() throws Exception {
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false)) // unsigned
            .named("u32")
            .named("test_schema");

        long[] u32Values = { 50_000L, 100_000L, 200_000L, 3_000_000_000L, 4_000_000_000L };
        byte[] data = createParquetFile(schema, f -> {
            List<Group> groups = new ArrayList<>();
            for (long value : u32Values) {
                groups.add(f.newGroup().append("u32", (int) value));
            }
            return groups;
        });
        StorageObject storageObject = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // --- extractStatistics path (metadata) ---
        SourceMetadata metadata = reader.metadata(storageObject);
        var colStats = metadata.statistics().orElseThrow().columnStatistics().orElseThrow().get("u32");
        assertEquals("min must widen to the true unsigned magnitude, not sign-extend", Optional.of(50_000L), colStats.minValue());
        assertEquals("max must widen to the true unsigned magnitude, not sign-extend", Optional.of(4_000_000_000L), colStats.maxValue());

        // --- buildRowGroupStats path (discoverSplitRanges) ---
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertFalse("expected at least one split range", ranges.isEmpty());
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            Map<String, Object> stats = range.statistics();
            assertEquals(50_000L, stats.get("_stats.columns.u32.min"));
            assertEquals(4_000_000_000L, stats.get("_stats.columns.u32.max"));
        }
    }

    // Fast-follow to the uint32 case above: a uint64 column's row-group statistics are also raw
    // physical INT64 values, but unlike uint32 the fix isn't a widen — ESQL's UNSIGNED_LONG is
    // already a 64-bit type, so it stores values sign-flip-encoded (value ^ 2^63) inside a signed
    // LongBlock (see testUnsignedLong64SignFlipEncoding). Row-group stats must go through that same
    // encoding, or a MIN/MAX pushdown answer (and split-skip classification) would compare the raw
    // on-disk bit pattern against an encoded query literal, comparing values from two different
    // domains. Covers both sides of the encoding boundary (0 -> Long.MIN_VALUE, 2^64-1 ->
    // Long.MAX_VALUE) and exercises both statistics paths: extractStatistics (metadata) and
    // buildRowGroupStats (discoverSplitRanges).
    public void testUint64StatisticsSignFlipEncode() throws Exception {
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.intType(64, false)) // unsigned
            .named("u64")
            .named("test_schema");

        // Raw physical INT64 bit patterns as parquet-mr would store them: 2^64-1 round-trips through
        // a Java long as -1L (0xFFFFFFFFFFFFFFFF), the unsigned maximum.
        long[] u64Values = { 0L, 100_000L, Long.MAX_VALUE, -1L };
        byte[] data = createParquetFile(schema, f -> {
            List<Group> groups = new ArrayList<>();
            for (long value : u64Values) {
                groups.add(f.newGroup().append("u64", value));
            }
            return groups;
        });
        StorageObject storageObject = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        long expectedMin = 0L ^ Long.MIN_VALUE;
        long expectedMax = -1L ^ Long.MIN_VALUE;

        // --- extractStatistics path (metadata) ---
        SourceMetadata metadata = reader.metadata(storageObject);
        var colStats = metadata.statistics().orElseThrow().columnStatistics().orElseThrow().get("u64");
        assertEquals("min must be sign-flip encoded, not the raw physical bit pattern", Optional.of(expectedMin), colStats.minValue());
        assertEquals("max must be sign-flip encoded, not the raw physical bit pattern", Optional.of(expectedMax), colStats.maxValue());

        // --- buildRowGroupStats path (discoverSplitRanges) ---
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertFalse("expected at least one split range", ranges.isEmpty());
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            Map<String, Object> stats = range.statistics();
            assertEquals(expectedMin, stats.get("_stats.columns.u64.min"));
            assertEquals(expectedMax, stats.get("_stats.columns.u64.max"));
        }
    }

    public void testLargeUnsignedLong() throws Exception {
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.intType(64, false)) // unsigned
            .named("object_id")
            .named("test_schema");
        long unsignedBits = 0xFFFFFFFFFFFFFFFFL; // negative in signed long
        byte[] data = createParquetFile(schema, f -> List.of(f.newGroup().append("object_id", unsignedBits)));
        StorageObject storageObject = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        var meta = reader.metadata(storageObject);
        assertEquals("UINT_64 should map to UNSIGNED_LONG", DataType.UNSIGNED_LONG, meta.schema().get(0).dataType());
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            // ESQL stores unsigned_long sign-flip-encoded (value ^ 2^63); the output edge decodes it back to the true value.
            assertEquals(unsignedBits ^ Long.MIN_VALUE, ((LongBlock) page.getBlock(0)).getLong(0));
            assertFalse(iterator.hasNext());
        }
    }

    public void testUnsignedLong64SignFlipEncoding() throws Exception {
        assertUnsignedLong64SignFlipEncoding(new ParquetFormatReader(blockFactory, false)); // baseline reader
    }

    public void testUnsignedLong64SignFlipEncodingOptimizedReader() throws Exception {
        assertUnsignedLong64SignFlipEncoding(new ParquetFormatReader(blockFactory, true)); // optimized reader
    }

    /**
     * A 64-bit unsigned column maps to UNSIGNED_LONG, which ESQL stores in a signed LongBlock in sign-flip-encoded form
     * ({@code value ^ 2^63}) so signed-long ordering matches unsigned ordering. The producer (this reader) must emit the
     * encoded form because the output edge unconditionally decodes UNSIGNED_LONG blocks. This asserts the encode is applied
     * across representative values, including {@code 0} and {@code 2^64-1}, on both reader paths.
     */
    private void assertUnsignedLong64SignFlipEncoding(ParquetFormatReader reader) throws Exception {
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.intType(64, false)) // unsigned, bit-width 64
            .named("u64")
            .named("test_schema");
        // The encoding (v ^ 2^63) maps unsigned values in [0, 2^63-1] to negative longs and [2^63, 2^64-1] to non-negative
        // longs. Cover both sides of that boundary: 0, small positive, 2^63-1 (largest value encoding to a negative long),
        // 2^63 (smallest value encoding to a non-negative long), and 2^64-1.
        long[] unsignedValues = { 0L, 100L, 0x7FFFFFFFFFFFFFFFL, 0x8000000000000000L, 0xFFFFFFFFFFFFFFFFL };
        byte[] data = createParquetFile(schema, f -> {
            List<Group> g = new ArrayList<>();
            for (long bits : unsignedValues) {
                g.add(f.newGroup().append("u64", bits));
            }
            return g;
        });
        var so = createStorageObject(data);
        assertEquals(DataType.UNSIGNED_LONG, reader.metadata(so).schema().get(0).dataType());
        try (CloseableIterator<Page> it = reader.read(so, null, 10)) {
            LongBlock block = (LongBlock) it.next().getBlock(0);
            for (int i = 0; i < unsignedValues.length; i++) {
                // contract: a uint64 column is stored sign-flip-encoded (v ^ 2^63 == asLongUnsigned(v))
                assertEquals(unsignedValues[i] ^ Long.MIN_VALUE, block.getLong(i));
            }
        }
    }

    public void testUnsignedLong64ConstantColumnEncoding() throws Exception {
        assertUnsignedLong64ConstantColumnEncoding(new ParquetFormatReader(blockFactory, false), false); // baseline reader
    }

    public void testUnsignedLong64ConstantColumnEncodingOptimizedReader() throws Exception {
        // The optimized reader collapses an all-equal column into a constant block; assert both the encoding and the collapse,
        // which proves the encode is applied before constant detection rather than after.
        assertUnsignedLong64ConstantColumnEncoding(new ParquetFormatReader(blockFactory, true), true);
    }

    /**
     * A constant unsigned_long column must still be sign-flip-encoded. The encode is applied before constant detection,
     * so a collapsed constant block holds the encoded value rather than the raw bits.
     *
     * @param expectConstantCollapse when {@code true} the block is additionally asserted to be a constant vector; only the
     *                               optimized reader performs this collapse, so the baseline reader passes {@code false}.
     */
    private void assertUnsignedLong64ConstantColumnEncoding(ParquetFormatReader reader, boolean expectConstantCollapse) throws Exception {
        var schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.intType(64, false)) // unsigned
            .named("u64")
            .named("test_schema");
        long unsignedBits = 0xFFFFFFFFFFFFFFFFL; // 2^64-1, constant across all rows
        byte[] data = createParquetFile(schema, f -> {
            List<Group> g = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                g.add(f.newGroup().append("u64", unsignedBits));
            }
            return g;
        });
        var so = createStorageObject(data);
        try (CloseableIterator<Page> it = reader.read(so, null, 100)) {
            LongBlock block = (LongBlock) it.next().getBlock(0);
            assertEquals(16, block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                assertEquals(unsignedBits ^ Long.MIN_VALUE, block.getLong(i));
            }
            if (expectConstantCollapse) {
                assertTrue("optimized reader should collapse an all-equal column into a constant vector", block.asVector().isConstant());
            }
        }
    }

    private static MessageType threeColumnSchema() {
        return Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("a")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("b")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("c")
            .named("schema");
    }

    /**
     * Full scan: no filter (filteringRequired == false) and no threshold must emit zero index
     * paths. Regression guard — an earlier version keyed gating off {@code recordFilter == null},
     * but the production record filter is {@code FilterCompat.NOOP} (never null) for an unfiltered
     * read, which silently disabled the gating and fetched every page index.
     */
    public void testComputeIndexColumnPathsFullScanEmitsNothing() {
        ParquetFormatReader.IndexColumnPaths paths = ParquetFormatReader.computeIndexColumnPaths(
            false,
            false,
            null,
            null,
            threeColumnSchema()
        );
        assertNotNull("full scan must gate (non-null sets), not fall back to unrestricted", paths.columnIndexPaths());
        assertNotNull(paths.offsetIndexPaths());
        assertTrue("full scan must not fetch any column index", paths.columnIndexPaths().isEmpty());
        assertTrue("full scan must not fetch any offset index", paths.offsetIndexPaths().isEmpty());
    }

    /**
     * Filtered read: predicate columns get both indexes; projected columns get the offset index
     * (to skip non-surviving pages); non-predicate columns get no column index.
     */
    public void testComputeIndexColumnPathsFilteredQuery() {
        ParquetFormatReader.IndexColumnPaths paths = ParquetFormatReader.computeIndexColumnPaths(
            true,
            true,
            Set.of("a"),
            null,
            threeColumnSchema()
        );
        assertEquals("only the predicate column needs a column index", Set.of("a"), paths.columnIndexPaths());
        assertEquals(
            "every projected column (plus the predicate column) needs an offset index",
            Set.of("a", "b", "c"),
            paths.offsetIndexPaths()
        );
    }

    /**
     * A predicate column that is not projected must still carry both indexes so
     * {@code ColumnIndexRowRangesComputer} can evaluate the predicate against it.
     */
    public void testComputeIndexColumnPathsNonProjectedPredicateColumn() {
        MessageType projected = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("b").named("schema");
        ParquetFormatReader.IndexColumnPaths paths = ParquetFormatReader.computeIndexColumnPaths(true, true, Set.of("a"), null, projected);
        assertEquals(Set.of("a"), paths.columnIndexPaths());
        assertEquals(
            "predicate column a (not projected) and projected column b both need offset index",
            Set.of("a", "b"),
            paths.offsetIndexPaths()
        );
    }

    /**
     * Threshold-only top-N (no filter): only the sort column needs both indexes; projected columns
     * are not added because, without a filter, reads are sequential and need no offset index.
     */
    public void testComputeIndexColumnPathsThresholdOnly() {
        ParquetFormatReader.IndexColumnPaths paths = ParquetFormatReader.computeIndexColumnPaths(
            false,
            false,
            null,
            "a",
            threeColumnSchema()
        );
        assertEquals(Set.of("a"), paths.columnIndexPaths());
        assertEquals(Set.of("a"), paths.offsetIndexPaths());
    }

    /**
     * Legacy FilterPredicateCompat path: a filter is active but its predicate columns cannot be
     * enumerated, so gating is unsafe and both sets must be null (unrestricted preload).
     */
    public void testComputeIndexColumnPathsLegacyFilterIsUnrestricted() {
        ParquetFormatReader.IndexColumnPaths paths = ParquetFormatReader.computeIndexColumnPaths(
            true,
            true,
            null,
            null,
            threeColumnSchema()
        );
        assertNull("legacy filter path must not gate", paths.columnIndexPaths());
        assertNull("legacy filter path must not gate", paths.offsetIndexPaths());
    }

    /**
     * Pushdown that yields no Parquet {@code FilterPredicate} (e.g. a pure {@code WildcardLike}):
     * predicate columns are enumerable but {@code pageRangeFilterActive} is false, so no page-level
     * {@code RowRanges} are ever computed. The predicate-column page indexes must not be fetched -
     * they would be fetched and discarded.
     */
    public void testComputeIndexColumnPathsPushdownWithoutFilterPredicate() {
        ParquetFormatReader.IndexColumnPaths paths = ParquetFormatReader.computeIndexColumnPaths(
            false,
            false,
            Set.of("a"),
            null,
            threeColumnSchema()
        );
        assertNotNull("must gate (non-null sets), not fall back to unrestricted", paths.columnIndexPaths());
        assertNotNull(paths.offsetIndexPaths());
        assertTrue("no FilterPredicate -> predicate column index must not be fetched", paths.columnIndexPaths().isEmpty());
        assertTrue("no FilterPredicate -> no offset index must be fetched", paths.offsetIndexPaths().isEmpty());
    }

    // --- Temporal stats decode regression ---

    /**
     * Verifies that Parquet footer statistics for temporal columns (date32, timestamp[us],
     * timestamp[ms]) are published as epoch-millis, matching the scan-path decode. Before the
     * fix, date32 stats were raw days and timestamp[us] stats were raw microseconds.
     */
    public void testTemporalStatsDecodeToEpochMillis() throws Exception {
        long millis2000 = Instant.parse("2000-01-01T00:00:00Z").toEpochMilli(); // 946684800000
        long millis2020 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli(); // 1577836800000

        int days2000 = (int) (millis2000 / ParquetColumnDecoding.MILLIS_PER_DAY);
        int days2020 = (int) (millis2020 / ParquetColumnDecoding.MILLIS_PER_DAY);
        long micros2000 = millis2000 * 1_000;
        long micros2020 = millis2020 * 1_000;
        // timestamp[us] resolves to DATE_NANOS, so its published stats are epoch-nanos (matching the scan).
        long nanos2000 = millis2000 * 1_000_000;
        long nanos2020 = millis2020 * 1_000_000;

        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.dateType())
            .named("d32")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("tus")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("tms")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("d32", days2000);
            g1.add("tus", micros2000);
            g1.add("tms", millis2000);
            Group g2 = factory.newGroup();
            g2.add("d32", days2020);
            g2.add("tus", micros2020);
            g2.add("tms", millis2020);
            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // --- extractStatistics path (metadata) ---
        SourceMetadata metadata = reader.metadata(storageObject);
        assertTrue("expected source statistics", metadata.statistics().isPresent());
        var colStats = metadata.statistics().get().columnStatistics().get();

        var d32Stats = colStats.get("d32");
        assertEquals("date32 min must be epoch-millis", Optional.of(millis2000), d32Stats.minValue());
        assertEquals("date32 max must be epoch-millis", Optional.of(millis2020), d32Stats.maxValue());

        var tusStats = colStats.get("tus");
        assertEquals("timestamp[us] min must be epoch-nanos (DATE_NANOS)", Optional.of(nanos2000), tusStats.minValue());
        assertEquals("timestamp[us] max must be epoch-nanos (DATE_NANOS)", Optional.of(nanos2020), tusStats.maxValue());

        var tmsStats = colStats.get("tms");
        assertEquals("timestamp[ms] min must be epoch-millis (no double-divide)", Optional.of(millis2000), tmsStats.minValue());
        assertEquals("timestamp[ms] max must be epoch-millis (no double-divide)", Optional.of(millis2020), tmsStats.maxValue());

        // --- buildRowGroupStats path (discoverSplitRanges) ---
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertFalse("expected at least one split range", ranges.isEmpty());

        // buildRowGroupStats is wired separately from extractStatistics, so assert every temporal
        // column here too. date32 and timestamp[ms] publish epoch-millis; timestamp[us] is DATE_NANOS
        // and publishes epoch-nanos.
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            Map<String, Object> stats = range.statistics();
            for (String col : List.of("d32", "tus", "tms")) {
                long lo = col.equals("tus") ? nanos2000 : millis2000;
                long hi = col.equals("tus") ? nanos2020 : millis2020;
                Object min = stats.get("_stats.columns." + col + ".min");
                Object max = stats.get("_stats.columns." + col + ".max");
                if (min != null) {
                    long minVal = ((Number) min).longValue();
                    assertTrue(col + " split min must match scan-path decode", minVal == lo || minVal == hi);
                }
                if (max != null) {
                    long maxVal = ((Number) max).longValue();
                    assertTrue(col + " split max must match scan-path decode", maxVal == lo || maxVal == hi);
                }
            }
        }

        // --- scan-vs-stats parity ---
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 100)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock d32Block = (LongBlock) page.getBlock(0);
            LongBlock tusBlock = (LongBlock) page.getBlock(1);
            LongBlock tmsBlock = (LongBlock) page.getBlock(2);

            long scanD32Min = Long.MAX_VALUE, scanD32Max = Long.MIN_VALUE;
            long scanTusMin = Long.MAX_VALUE, scanTusMax = Long.MIN_VALUE;
            long scanTmsMin = Long.MAX_VALUE, scanTmsMax = Long.MIN_VALUE;
            for (int i = 0; i < page.getPositionCount(); i++) {
                long d = d32Block.getLong(i);
                scanD32Min = Math.min(scanD32Min, d);
                scanD32Max = Math.max(scanD32Max, d);
                long u = tusBlock.getLong(i);
                scanTusMin = Math.min(scanTusMin, u);
                scanTusMax = Math.max(scanTusMax, u);
                long m = tmsBlock.getLong(i);
                scanTmsMin = Math.min(scanTmsMin, m);
                scanTmsMax = Math.max(scanTmsMax, m);
            }
            assertEquals("scan d32 min == stats d32 min", scanD32Min, ((Number) d32Stats.minValue().get()).longValue());
            assertEquals("scan d32 max == stats d32 max", scanD32Max, ((Number) d32Stats.maxValue().get()).longValue());
            assertEquals("scan tus min == stats tus min", scanTusMin, ((Number) tusStats.minValue().get()).longValue());
            assertEquals("scan tus max == stats tus max", scanTusMax, ((Number) tusStats.maxValue().get()).longValue());
            assertEquals("scan tms min == stats tms min", scanTmsMin, ((Number) tmsStats.minValue().get()).longValue());
            assertEquals("scan tms max == stats tms max", scanTmsMax, ((Number) tmsStats.maxValue().get()).longValue());
            page.releaseBlocks();
        }
    }

    /**
     * Direct coverage of {@link ParquetColumnDecoding#decodeTemporalStat} for the timestamp[nanos]
     * and TIME_* branches, which parquet-mr does not always emit footer statistics for (so they are
     * hard to exercise via a written file). Also pins date32/micros/millis, the INT96 opt-out (its
     * footer stats are not chronological, so the helper must return null), and the non-temporal
     * null fall-through.
     */
    public void testDecodeTemporalStatHelper() {
        long millis = Instant.parse("2000-01-01T00:00:00Z").toEpochMilli();
        long days = millis / ParquetColumnDecoding.MILLIS_PER_DAY;

        PrimitiveType date32 = Types.required(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("d");
        assertEquals(Long.valueOf(millis), ParquetColumnDecoding.decodeTemporalStat((int) days, date32));

        // timestamp[us]/[ns] resolve to DATE_NANOS, so their stats decode to epoch-nanos (matching the scan).
        PrimitiveType tsMicros = Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("us");
        assertEquals(Long.valueOf(millis * 1_000_000), ParquetColumnDecoding.decodeTemporalStat(millis * 1_000, tsMicros));
        // A timestamp[us] stat beyond the date_nanos range returns null so the caller falls back to a scan.
        assertNull(ParquetColumnDecoding.decodeTemporalStat(20_000_000_000_000_000L, tsMicros));

        PrimitiveType tsMillis = Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ms");
        assertEquals(Long.valueOf(millis), ParquetColumnDecoding.decodeTemporalStat(millis, tsMillis));

        PrimitiveType tsNanos = Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ns");
        assertEquals(Long.valueOf(millis * 1_000_000), ParquetColumnDecoding.decodeTemporalStat(millis * 1_000_000, tsNanos));

        // TIME must mirror the scan path: TIME_MILLIS (physical INT32) stays raw ms; TIME_MICROS
        // scales x1_000 to nanos; TIME_NANOS is as-is.
        long midMillis = 12 * 3_600_000L; // 12:00:00 of a day
        PrimitiveType timeMillis = Types.required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("tm");
        assertEquals(Long.valueOf(midMillis), ParquetColumnDecoding.decodeTemporalStat((int) midMillis, timeMillis));

        long midMicros = midMillis * 1_000L;
        PrimitiveType timeMicros = Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("tu");
        assertEquals(Long.valueOf(midMicros * 1_000L), ParquetColumnDecoding.decodeTemporalStat(midMicros, timeMicros));

        long midNanos = midMillis * 1_000_000L;
        PrimitiveType timeNanos = Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("tn");
        assertEquals(Long.valueOf(midNanos), ParquetColumnDecoding.decodeTemporalStat(midNanos, timeNanos));

        // INT96 footer min/max are compared as unsigned little-endian bytes, not chronologically, so
        // the helper must opt out (return null) and let the query fall back to a scan.
        int julianDay = (int) (days + 2_440_588);
        byte[] int96 = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(0L).putInt(julianDay).array();
        PrimitiveType int96Type = Types.required(PrimitiveType.PrimitiveTypeName.INT96).named("i96");
        assertNull(ParquetColumnDecoding.decodeTemporalStat(Binary.fromConstantByteArray(int96), int96Type));

        // Non-temporal types return null so the caller falls through to other normalization.
        PrimitiveType plainInt = Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("n");
        assertNull(ParquetColumnDecoding.decodeTemporalStat(5, plainInt));
        PrimitiveType plainLong = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("l");
        assertNull(ParquetColumnDecoding.decodeTemporalStat(5L, plainLong));
    }

    /**
     * A timestamp[us] column that mixes an in-range value with one beyond the date_nanos range: the scan
     * nulls the out-of-range value out, so the physical footer statistics no longer describe the scan.
     * Both min/max and the null count must be poisoned (omitted / unknown) so MIN/MAX/COUNT fall back to a
     * scan instead of trusting a raw micros extremum or an under-counted null count. Covered on both the
     * {@code extractStatistics} (metadata) and {@code buildRowGroupStats} (split ranges) paths.
     */
    public void testOutOfRangeTimestampMicrosPoisonsStats() throws Exception {
        long millis2000 = Instant.parse("2000-01-01T00:00:00Z").toEpochMilli();
        long micros2000 = millis2000 * 1_000;
        // micros * 1000 overflows Long, i.e. beyond the representable date_nanos range; the scan nulls it.
        long microsOverflow = 20_000_000_000_000_000L;

        MessageType schema = Types.buildMessage()
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("tus")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("tus", micros2000);
            Group g2 = factory.newGroup();
            g2.add("tus", microsOverflow);
            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // --- extractStatistics path (metadata): min/max omitted, null count unknown ---
        SourceMetadata metadata = reader.metadata(storageObject);
        assertTrue("expected source statistics", metadata.statistics().isPresent());
        var tusStats = metadata.statistics().get().columnStatistics().get().get("tus");
        assertNotNull("column statistics entry must still exist (size is published)", tusStats);
        assertEquals("out-of-range timestamp[us] min must be omitted", Optional.empty(), tusStats.minValue());
        assertEquals("out-of-range timestamp[us] max must be omitted", Optional.empty(), tusStats.maxValue());
        assertEquals("out-of-range timestamp[us] null count must be unknown", OptionalLong.empty(), tusStats.nullCount());

        // --- buildRowGroupStats path (discoverSplitRanges): min/max + null_count keys dropped ---
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertFalse("expected at least one split range", ranges.isEmpty());
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            Map<String, Object> stats = range.statistics();
            if (stats.containsKey("_stats.columns.tus.min") || stats.containsKey("_stats.columns.tus.max")) {
                // The row group that only holds the in-range value may still publish a usable min/max; the
                // one covering the overflow value must publish neither, and never a raw micros extremum.
                Object min = stats.get("_stats.columns.tus.min");
                Object max = stats.get("_stats.columns.tus.max");
                if (min != null) {
                    assertNotEquals("must never publish raw micros as min", microsOverflow, ((Number) min).longValue());
                }
                if (max != null) {
                    assertNotEquals("must never publish raw micros as max", microsOverflow, ((Number) max).longValue());
                }
            }
        }

        // --- scan parity: the overflow row is nulled, so the scan sees one null the footer never counted ---
        int nullsSeen = 0;
        int rows = 0;
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 100)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                LongBlock block = (LongBlock) page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    rows++;
                    if (block.isNull(i)) {
                        nullsSeen++;
                    }
                }
                page.releaseBlocks();
            }
        }
        assertEquals("both rows scanned", 2, rows);
        assertEquals("the out-of-range value must be nulled on scan", 1, nullsSeen);
    }

    public void testCompareStatExtremumUsesUtf8OrderForStrings() {
        // The cross-row-group keyword MIN/MAX fold must order string extrema by UTF-8 bytes (matching the runtime
        // keyword aggregators and the cross-file fold), not String.compareTo's UTF-16 code units. U+FFFD (BMP)
        // vs U+1F600 (astral): UTF-16 would rank the emoji first (surrogate 0xD83D < 0xFFFD), but UTF-8 orders the
        // replacement char first (0xEF < 0xF0). A regression to String.compareTo would flip both assertions.
        String bmp = "�";
        String astral = "😀";
        assertTrue("string extrema compare in UTF-8 byte order", ParquetFormatReader.compareStatExtremum(bmp, astral) < 0);
        assertTrue(ParquetFormatReader.compareStatExtremum(astral, bmp) > 0);
        // Non-string extrema compare naturally.
        assertTrue("numeric extrema compare naturally", ParquetFormatReader.compareStatExtremum(5L, 10L) < 0);
    }

}
