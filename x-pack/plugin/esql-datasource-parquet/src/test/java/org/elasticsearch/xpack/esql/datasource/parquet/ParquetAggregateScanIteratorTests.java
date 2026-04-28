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
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanSpec;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanSpec.AggOp;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Unit tests for {@link ParquetAggregateScanIterator}. Exercises both the fast path
 * (driven by Parquet column statistics) and the slow path (row-data folded into per-op
 * accumulators), the per-row-group emission contract, and iterator lifecycle.
 * <p>
 * The slow path is forced by writing files with {@code withStatisticsEnabled("col", false)}
 * for the column(s) under test: this leaves {@link org.apache.parquet.column.statistics.Statistics
 * Statistics#isEmpty()} returning {@code true}, so {@link ParquetAggregateScanIterator} drops
 * to the row-data path for any row group whose projected columns are missing stats.
 */
public class ParquetAggregateScanIteratorTests extends ESTestCase {

    /**
     * Each {@link #storageObject(byte[])} call gets a unique URI to dodge the global
     * {@link org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache FooterByteCache}:
     * the cache is keyed on {@code (path, length)} and two test files of identical length
     * would otherwise serve each other's stale footers.
     */
    private static final AtomicLong URI_COUNTER = new AtomicLong();

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    // === Fast-path tests (Parquet column statistics drive the result) ===

    public void testFastPathCountStar() throws IOException {
        MessageType schema = Types.buildMessage().required(INT32).named("v").named("count_star_test");
        byte[] data = writeParquet(schema, Set.of(), 0, 50, (g, i) -> g.append("v", i));

        AggregateScanSpec spec = spec(List.of(new AggOp.CountStar()), List.of(DataType.LONG));
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertTrue("count_star seen", seen(pages.get(0), 0));
            assertEquals(50L, longValue(pages.get(0), 0));
        } finally {
            release(pages);
        }
    }

    public void testFastPathCountFieldUsesNullCount() throws IOException {
        // Column "v" is optional with every 5th row null -> rowCount=50, nullCount=10, COUNT(v)=40.
        MessageType schema = Types.buildMessage().optional(INT32).named("v").named("count_field_test");
        byte[] data = writeParquet(schema, Set.of(), 0, 50, (g, i) -> { if (i % 5 != 0) g.append("v", i); });

        AggregateScanSpec spec = spec(List.of(new AggOp.CountField("v")), List.of(DataType.LONG));
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertTrue(seen(pages.get(0), 0));
            assertEquals(40L, longValue(pages.get(0), 0));
        } finally {
            release(pages);
        }
    }

    public void testFastPathMinMaxInt() throws IOException {
        MessageType schema = Types.buildMessage().required(INT32).named("v").named("min_max_int_test");
        byte[] data = writeParquet(schema, Set.of(), 0, 100, (g, i) -> g.append("v", i * 3));

        AggregateScanSpec spec = spec(
            List.of(new AggOp.MinField("v"), new AggOp.MaxField("v")),
            List.of(DataType.INTEGER, DataType.INTEGER)
        );
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertTrue(seen(pages.get(0), 0));
            assertTrue(seen(pages.get(0), 1));
            assertEquals(0, intValue(pages.get(0), 0));
            assertEquals(99 * 3, intValue(pages.get(0), 1));
        } finally {
            release(pages);
        }
    }

    public void testFastPathMinMaxString() throws IOException {
        // Use values where min/max are unambiguous in either signed or unsigned byte order
        // (parquet-mr has both comparators; "aaa".."ccc" are all in the printable ASCII range).
        MessageType schema = Types.buildMessage()
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("v")
            .named("min_max_str_test");
        String[] values = { "bbb", "ccc", "aaa" };
        byte[] data = writeParquet(schema, Set.of(), 0, values.length, (g, i) -> g.append("v", values[i]));

        AggregateScanSpec spec = spec(
            List.of(new AggOp.MinField("v"), new AggOp.MaxField("v")),
            List.of(DataType.KEYWORD, DataType.KEYWORD)
        );
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertTrue(seen(pages.get(0), 0));
            assertTrue(seen(pages.get(0), 1));
            assertEquals(new BytesRef("aaa"), bytesValue(pages.get(0), 0));
            assertEquals(new BytesRef("ccc"), bytesValue(pages.get(0), 1));
        } finally {
            release(pages);
        }
    }

    public void testFastPathMinMaxAllNullsReturnsUnseen() throws IOException {
        // All rows null -> Statistics.hasNonNullValue() == false -> seen=false on min/max,
        // but COUNT(v) still produces 0 from rowCount - nullCount.
        MessageType schema = Types.buildMessage().optional(INT32).named("v").named("all_nulls_test");
        byte[] data = writeParquet(schema, Set.of(), 0, 10, (g, i) -> {});

        AggregateScanSpec spec = spec(
            List.of(new AggOp.CountField("v"), new AggOp.MinField("v"), new AggOp.MaxField("v")),
            List.of(DataType.LONG, DataType.INTEGER, DataType.INTEGER)
        );
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            Page p = pages.get(0);
            assertTrue("count(v) is always seen", seen(p, 0));
            assertEquals(0L, longValue(p, 0));
            assertFalse("min(v) on all-null column must be unseen", seen(p, 1));
            assertFalse("max(v) on all-null column must be unseen", seen(p, 2));
            // Value blocks for the unseen aggs are constant-null.
            assertTrue(p.getBlock(2).isNull(0));
            assertTrue(p.getBlock(4).isNull(0));
        } finally {
            release(pages);
        }
    }

    public void testFastPathMultipleAggsInOnePage() throws IOException {
        // CountStar + CountField + Min + Max -> 4 ops -> 8 blocks per page (value, seen, ...).
        MessageType schema = Types.buildMessage().optional(INT32).named("v").named("multi_agg_test");
        byte[] data = writeParquet(schema, Set.of(), 0, 20, (g, i) -> { if (i != 7) g.append("v", i + 1); });

        AggregateScanSpec spec = spec(
            List.of(new AggOp.CountStar(), new AggOp.CountField("v"), new AggOp.MinField("v"), new AggOp.MaxField("v")),
            List.of(DataType.LONG, DataType.LONG, DataType.INTEGER, DataType.INTEGER)
        );
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            Page p = pages.get(0);
            assertEquals("8 blocks (4 value + 4 seen)", 8, p.getBlockCount());
            assertEquals(20L, longValue(p, 0));   // CountStar
            assertEquals(19L, longValue(p, 1));   // CountField (one null at i=7)
            assertEquals(1, intValue(p, 2));      // Min
            assertEquals(20, intValue(p, 3));     // Max
            for (int i = 0; i < 4; i++) {
                assertTrue("agg " + i + " seen", seen(p, i));
            }
        } finally {
            release(pages);
        }
    }

    // === Slow-path tests (stats disabled per column -> row decode + accumulators) ===

    public void testSlowPathCountFieldCountsNonNulls() throws IOException {
        MessageType schema = Types.buildMessage().optional(INT32).named("v").named("slow_count_test");
        // Same shape as the fast-path nullcount test, but with stats disabled the iterator must
        // actually walk the column data to derive the count.
        byte[] data = writeParquet(schema, Set.of("v"), 0, 50, (g, i) -> { if (i % 5 != 0) g.append("v", i); });

        AggregateScanSpec spec = spec(List.of(new AggOp.CountField("v")), List.of(DataType.LONG));
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertTrue(seen(pages.get(0), 0));
            assertEquals(40L, longValue(pages.get(0), 0));
        } finally {
            release(pages);
        }
    }

    public void testSlowPathMinMaxInt() throws IOException {
        MessageType schema = Types.buildMessage().required(INT32).named("v").named("slow_int_test");
        byte[] data = writeParquet(schema, Set.of("v"), 0, 100, (g, i) -> g.append("v", i * 3));

        AggregateScanSpec spec = spec(
            List.of(new AggOp.MinField("v"), new AggOp.MaxField("v")),
            List.of(DataType.INTEGER, DataType.INTEGER)
        );
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertEquals(0, intValue(pages.get(0), 0));
            assertEquals(99 * 3, intValue(pages.get(0), 1));
        } finally {
            release(pages);
        }
    }

    public void testSlowPathMinMaxLong() throws IOException {
        MessageType schema = Types.buildMessage().required(INT64).named("v").named("slow_long_test");
        byte[] data = writeParquet(schema, Set.of("v"), 0, 100, (g, i) -> g.append("v", (long) i * 100_000L));

        AggregateScanSpec spec = spec(List.of(new AggOp.MinField("v"), new AggOp.MaxField("v")), List.of(DataType.LONG, DataType.LONG));
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertEquals(0L, longValue(pages.get(0), 0));
            assertEquals(99L * 100_000L, longValue(pages.get(0), 1));
        } finally {
            release(pages);
        }
    }

    public void testSlowPathMinMaxDouble() throws IOException {
        MessageType schema = Types.buildMessage().required(DOUBLE).named("v").named("slow_double_test");
        byte[] data = writeParquet(schema, Set.of("v"), 0, 100, (g, i) -> g.append("v", i * 1.5));

        AggregateScanSpec spec = spec(List.of(new AggOp.MinField("v"), new AggOp.MaxField("v")), List.of(DataType.DOUBLE, DataType.DOUBLE));
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertEquals(0.0, doubleValue(pages.get(0), 0), 0.0);
            assertEquals(99 * 1.5, doubleValue(pages.get(0), 1), 0.0);
        } finally {
            release(pages);
        }
    }

    public void testSlowPathMinMaxBoolean() throws IOException {
        // Boolean MIN is "any false", MAX is "any true". A mixed column gives MIN=false, MAX=true.
        MessageType schema = Types.buildMessage().required(BOOLEAN).named("v").named("slow_bool_test");
        byte[] data = writeParquet(schema, Set.of("v"), 0, 10, (g, i) -> g.append("v", i % 3 == 0));

        AggregateScanSpec spec = spec(
            List.of(new AggOp.MinField("v"), new AggOp.MaxField("v")),
            List.of(DataType.BOOLEAN, DataType.BOOLEAN)
        );
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertFalse("min boolean", boolValue(pages.get(0), 0));
            assertTrue("max boolean", boolValue(pages.get(0), 1));
        } finally {
            release(pages);
        }
    }

    public void testSlowPathMinMaxString() throws IOException {
        MessageType schema = Types.buildMessage().required(BINARY).as(LogicalTypeAnnotation.stringType()).named("v").named("slow_str_test");
        String[] values = { "bbb", "ccc", "aaa" };
        byte[] data = writeParquet(schema, Set.of("v"), 0, values.length, (g, i) -> g.append("v", values[i]));

        AggregateScanSpec spec = spec(
            List.of(new AggOp.MinField("v"), new AggOp.MaxField("v")),
            List.of(DataType.KEYWORD, DataType.KEYWORD)
        );
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertEquals(new BytesRef("aaa"), bytesValue(pages.get(0), 0));
            assertEquals(new BytesRef("ccc"), bytesValue(pages.get(0), 1));
        } finally {
            release(pages);
        }
    }

    public void testSlowPathCountStarMixedWithCountField() throws IOException {
        // CountStar is column-independent; when *any* projected column lacks stats the slow path
        // runs and CountStar must still be filled from the row-group's rowCount, not from a
        // (non-existent) column read. This guards the loop in buildPageFromRowData that calls
        // CountAccumulator#setRowCount on every CountStar op.
        MessageType schema = Types.buildMessage().optional(INT32).named("v").named("slow_count_star_mix_test");
        byte[] data = writeParquet(schema, Set.of("v"), 0, 50, (g, i) -> { if (i % 5 != 0) g.append("v", i); });

        AggregateScanSpec spec = spec(List.of(new AggOp.CountStar(), new AggOp.CountField("v")), List.of(DataType.LONG, DataType.LONG));
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertEquals(1, pages.size());
            assertEquals(50L, longValue(pages.get(0), 0));
            assertEquals(40L, longValue(pages.get(0), 1));
        } finally {
            release(pages);
        }
    }

    // === Multi-row-group / lifecycle tests ===

    public void testMultipleRowGroupsEmitOnePagePerGroup() throws IOException {
        // A small row-group size threshold and a payload column force the writer to flush
        // multiple row groups, even though we only aggregate over a tiny "id" column. Each
        // row group should yield one intermediate page with its own partial count/min/max.
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .named("id")
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("multi_rg_test");

        int rows = 1000;
        String padding = "x".repeat(200);
        byte[] data = writeParquet(schema, Set.of(), 1024, rows, (g, i) -> {
            g.append("id", (long) i);
            g.append("payload", "row-" + i + "-" + padding);
        });

        AggregateScanSpec spec = spec(
            List.of(new AggOp.CountStar(), new AggOp.MinField("id"), new AggOp.MaxField("id")),
            List.of(DataType.LONG, DataType.LONG, DataType.LONG)
        );
        List<Page> pages = drainAndOpen(data, spec);
        try {
            assertTrue("expected multiple row-group pages, got " + pages.size(), pages.size() > 1);

            long rowsSeen = 0;
            long globalMin = Long.MAX_VALUE;
            long globalMax = Long.MIN_VALUE;
            for (Page p : pages) {
                assertEquals(6, p.getBlockCount());
                rowsSeen += longValue(p, 0);
                globalMin = Math.min(globalMin, longValue(p, 1));
                globalMax = Math.max(globalMax, longValue(p, 2));
            }
            assertEquals("sum of per-group counts equals total rows", (long) rows, rowsSeen);
            assertEquals(0L, globalMin);
            assertEquals(rows - 1L, globalMax);
        } finally {
            release(pages);
        }
    }

    public void testEmptyFileEmitsNoPages() throws IOException {
        MessageType schema = Types.buildMessage().required(INT32).named("v").named("empty_test");
        byte[] data = writeParquet(schema, Set.of(), 0, 0, (g, i) -> {});

        AggregateScanSpec spec = spec(List.of(new AggOp.CountStar()), List.of(DataType.LONG));
        try (ParquetAggregateScanIterator iter = openIterator(data, spec)) {
            assertFalse("empty file -> no row groups -> no pages", iter.hasNext());
            expectThrows(NoSuchElementException.class, iter::next);
        }
    }

    public void testCloseStopsIteration() throws IOException {
        // Force >1 row group, drain only the first, then close: hasNext() must return false
        // even though row groups remain. This guards the {@code closed} check in hasNext().
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .named("id")
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("close_test");
        String padding = "x".repeat(200);
        byte[] data = writeParquet(schema, Set.of(), 1024, 1000, (g, i) -> {
            g.append("id", (long) i);
            g.append("payload", "row-" + i + "-" + padding);
        });

        AggregateScanSpec spec = spec(List.of(new AggOp.CountStar()), List.of(DataType.LONG));
        ParquetAggregateScanIterator iter = openIterator(data, spec);
        Page first = null;
        try {
            assertTrue(iter.hasNext());
            first = iter.next();
            iter.close();
            assertFalse("hasNext must be false after close", iter.hasNext());
            // close() must be idempotent (the iterator guards on a {@code closed} flag).
            iter.close();
        } finally {
            if (first != null) first.releaseBlocks();
            iter.close();
        }
    }

    public void testNextThrowsNoSuchElementWhenExhausted() throws IOException {
        MessageType schema = Types.buildMessage().required(INT32).named("v").named("exhaust_test");
        byte[] data = writeParquet(schema, Set.of(), 0, 5, (g, i) -> g.append("v", i));

        AggregateScanSpec spec = spec(List.of(new AggOp.CountStar()), List.of(DataType.LONG));
        try (ParquetAggregateScanIterator iter = openIterator(data, spec)) {
            assertTrue(iter.hasNext());
            iter.next().releaseBlocks();
            assertFalse(iter.hasNext());
            expectThrows(NoSuchElementException.class, iter::next);
        }
    }

    // === Spec / attribute helpers ===

    /** Builds a spec with one value+seen attribute pair per op, of the requested value types. */
    private static AggregateScanSpec spec(List<AggOp> ops, List<DataType> valueTypes) {
        assert ops.size() == valueTypes.size() : "ops and value-types must align 1:1";
        List<Attribute> attrs = new ArrayList<>(ops.size() * 2);
        for (int i = 0; i < ops.size(); i++) {
            attrs.add(new ReferenceAttribute(Source.EMPTY, "agg" + i + "_value", valueTypes.get(i)));
            attrs.add(new ReferenceAttribute(Source.EMPTY, "agg" + i + "_seen", DataType.BOOLEAN));
        }
        return new AggregateScanSpec(ops, attrs);
    }

    // === Page extraction helpers (aggIndex -> block 2*i (value), block 2*i+1 (seen)) ===

    private static long longValue(Page p, int aggIndex) {
        LongBlock b = (LongBlock) p.getBlock(aggIndex * 2);
        return b.getLong(0);
    }

    private static int intValue(Page p, int aggIndex) {
        IntBlock b = (IntBlock) p.getBlock(aggIndex * 2);
        return b.getInt(0);
    }

    private static double doubleValue(Page p, int aggIndex) {
        DoubleBlock b = (DoubleBlock) p.getBlock(aggIndex * 2);
        return b.getDouble(0);
    }

    private static boolean boolValue(Page p, int aggIndex) {
        BooleanBlock b = (BooleanBlock) p.getBlock(aggIndex * 2);
        return b.getBoolean(0);
    }

    private static BytesRef bytesValue(Page p, int aggIndex) {
        BytesRefBlock b = (BytesRefBlock) p.getBlock(aggIndex * 2);
        return b.getBytesRef(0, new BytesRef());
    }

    private static boolean seen(Page p, int aggIndex) {
        BooleanBlock b = (BooleanBlock) p.getBlock(aggIndex * 2 + 1);
        return b.getBoolean(0);
    }

    private static void release(List<Page> pages) {
        for (Page p : pages) {
            p.releaseBlocks();
        }
    }

    // === Iterator construction / draining ===

    private List<Page> drainAndOpen(byte[] data, AggregateScanSpec spec) throws IOException {
        try (ParquetAggregateScanIterator iter = openIterator(data, spec)) {
            return drain(iter);
        }
    }

    private static List<Page> drain(CloseableIterator<Page> iter) {
        List<Page> out = new ArrayList<>();
        while (iter.hasNext()) {
            out.add(iter.next());
        }
        return out;
    }

    private ParquetAggregateScanIterator openIterator(byte[] data, AggregateScanSpec spec) throws IOException {
        StorageObject so = storageObject(data);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(so);
        // Use PlainParquetReadOptions to avoid the default builder pulling in Hadoop XML config
        // (and the woodstox dep) — same approach as ParquetFormatReader#readOptionsBuilder.
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        ParquetFileReader reader = ParquetFileReader.open(adapter, options);
        try {
            MessageType schema = reader.getFileMetaData().getSchema();
            ParquetAggregateScanIterator iter = new ParquetAggregateScanIterator(reader, schema, spec, blockFactory);
            reader = null; // ownership transferred
            return iter;
        } finally {
            if (reader != null) reader.close();
        }
    }

    // === In-memory parquet writer ===

    /**
     * Writes an in-memory parquet file. Set {@code statsDisabledColumns} to force the iterator's
     * slow path on those columns ({@link org.apache.parquet.column.statistics.Statistics
     * Statistics#isEmpty()} returns true on each chunk). A non-zero {@code rowGroupSizeBytes}
     * lets a test force multiple row groups.
     */
    private static byte[] writeParquet(
        MessageType schema,
        Set<String> statsDisabledColumns,
        long rowGroupSizeBytes,
        int numRows,
        BiConsumer<Group, Integer> populator
    ) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(outputFile(out))
            .withConf(new PlainParquetConfiguration())
            .withCodecFactory(new PlainCompressionCodecFactory())
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withPageSize(512);
        if (rowGroupSizeBytes > 0) {
            builder.withRowGroupSize(rowGroupSizeBytes);
        }
        for (String col : statsDisabledColumns) {
            builder.withStatisticsEnabled(col, false);
        }
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = builder.build()) {
            for (int i = 0; i < numRows; i++) {
                Group g = factory.newGroup();
                populator.accept(g, i);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
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
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        out.close();
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
                return "memory://aggregate_scan_test.parquet";
            }
        };
    }

    private static StorageObject storageObject(byte[] data) {
        String uri = "memory://aggregate_scan_test_" + URI_COUNTER.getAndIncrement() + ".parquet";
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
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.EPOCH;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of(uri);
            }
        };
    }
}
