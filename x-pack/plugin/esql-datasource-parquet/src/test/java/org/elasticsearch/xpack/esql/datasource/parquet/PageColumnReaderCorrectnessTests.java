/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Compares the optimized {@link PageColumnReader} path against the baseline row-at-a-time
 * {@code ColumnReader} path to verify they produce identical results.
 * <p>
 * For each test, an in-memory Parquet file is written and read by two readers:
 * the default (optimized) reader and a {@link ParquetFormatReader#withBaselinePath() baseline}
 * reader. Pages from both are compared block-by-block, position-by-position.
 * <p>
 * Explicit tests cover the V1/V2 x compression matrix. A randomized test generates schemas
 * with random column counts and null patterns, catching edge cases in buffer management
 * and encoding dispatch that fixed schemas cannot cover.
 */
public class PageColumnReaderCorrectnessTests extends ESTestCase {

    private static final int NUM_ROWS = 500;
    private static final int BATCH_SIZE = 1000;

    private static final MessageType SCHEMA = Types.buildMessage()
        .required(INT32)
        .named("id")
        .required(INT64)
        .named("big_id")
        .required(DOUBLE)
        .named("score")
        .required(FLOAT)
        .named("weight")
        .required(BOOLEAN)
        .named("flag")
        .required(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("name")
        .required(INT64)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("ts")
        .optional(INT32)
        .named("opt_int")
        .optional(INT64)
        .named("opt_long")
        .optional(DOUBLE)
        .named("opt_double")
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("opt_str")
        .named("correctness_test");

    private static final List<String> COLUMNS = SCHEMA.getFields().stream().map(Type::getName).toList();

    private BlockFactory blockFactory;
    private PlainCompressionCodecFactory codecFactory;

    @Before
    public void initCodecAndClearFooterCache() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        codecFactory = new PlainCompressionCodecFactory();
        // Every test in this class writes to the same in-memory path ("memory://correctness_test.parquet")
        // with a different file body. The JVM-wide FooterByteCache is keyed by (path, length) and would
        // otherwise serve the previous test's footer when the new file happens to land on the same byte
        // length (which testRandomSchema's seeded combinations occasionally do). Clear it before each
        // test so every iteration reads its own footer. Other tests in this package that reuse a single
        // path follow the same pattern (see OptimizedReaderFileVariantTests).
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
    }

    @After
    public void releaseCodecFactory() {
        codecFactory.release();
    }

    // --- Explicit V1/V2 x compression matrix ---

    public void testV1Uncompressed() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_1_0, CompressionCodecName.UNCOMPRESSED);
    }

    public void testV1Snappy() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_1_0, CompressionCodecName.SNAPPY);
    }

    public void testV1Zstd() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_1_0, CompressionCodecName.ZSTD);
    }

    public void testV1Gzip() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_1_0, CompressionCodecName.GZIP);
    }

    public void testV1Lz4Raw() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_1_0, CompressionCodecName.LZ4_RAW);
    }

    public void testV1Lz4HadoopFramed() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_1_0, CompressionCodecName.LZ4);
    }

    public void testV2Uncompressed() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_2_0, CompressionCodecName.UNCOMPRESSED);
    }

    public void testV2Snappy() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_2_0, CompressionCodecName.SNAPPY);
    }

    public void testV2Zstd() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_2_0, CompressionCodecName.ZSTD);
    }

    public void testV2Gzip() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_2_0, CompressionCodecName.GZIP);
    }

    public void testV2Lz4Raw() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_2_0, CompressionCodecName.LZ4_RAW);
    }

    public void testV2Lz4HadoopFramed() throws IOException {
        assertReadersMatch(ParquetProperties.WriterVersion.PARQUET_2_0, CompressionCodecName.LZ4);
    }

    // --- filterBlock tests ---

    public void testFilterBlockLong() {
        long[] values = { 10, 20, 30, 40, 50 };
        Block source = blockFactory.newLongArrayVector(values, values.length).asBlock();
        int[] positions = { 1, 3 };
        Block result = PageColumnReader.filterBlock(source, positions, positions.length, blockFactory);
        try {
            assertEquals(2, result.getPositionCount());
            LongBlock lb = (LongBlock) result;
            assertEquals(20L, lb.getLong(0));
            assertEquals(40L, lb.getLong(1));
        } finally {
            result.close();
        }
    }

    public void testFilterBlockInt() {
        int[] values = { 1, 2, 3, 4 };
        Block source = blockFactory.newIntArrayVector(values, values.length).asBlock();
        int[] positions = { 0, 2, 3 };
        Block result = PageColumnReader.filterBlock(source, positions, positions.length, blockFactory);
        try {
            assertEquals(3, result.getPositionCount());
            IntBlock ib = (IntBlock) result;
            assertEquals(1, ib.getInt(0));
            assertEquals(3, ib.getInt(1));
            assertEquals(4, ib.getInt(2));
        } finally {
            result.close();
        }
    }

    public void testFilterBlockDouble() {
        double[] values = { 1.1, 2.2, 3.3, 4.4 };
        Block source = blockFactory.newDoubleArrayVector(values, values.length).asBlock();
        int[] positions = { 0, 3 };
        Block result = PageColumnReader.filterBlock(source, positions, positions.length, blockFactory);
        try {
            assertEquals(2, result.getPositionCount());
            DoubleBlock db = (DoubleBlock) result;
            assertEquals(1.1, db.getDouble(0), 0.0);
            assertEquals(4.4, db.getDouble(1), 0.0);
        } finally {
            result.close();
        }
    }

    public void testFilterBlockBoolean() {
        boolean[] values = { true, false, true, false, true };
        Block source = blockFactory.newBooleanArrayVector(values, values.length).asBlock();
        int[] positions = { 1, 2, 4 };
        Block result = PageColumnReader.filterBlock(source, positions, positions.length, blockFactory);
        try {
            assertEquals(3, result.getPositionCount());
            BooleanBlock bb = (BooleanBlock) result;
            assertFalse(bb.getBoolean(0));
            assertTrue(bb.getBoolean(1));
            assertTrue(bb.getBoolean(2));
        } finally {
            result.close();
        }
    }

    public void testFilterBlockBytesRef() {
        try (var builder = blockFactory.newBytesRefBlockBuilder(4)) {
            builder.appendBytesRef(new BytesRef("alpha"));
            builder.appendBytesRef(new BytesRef("beta"));
            builder.appendBytesRef(new BytesRef("gamma"));
            builder.appendBytesRef(new BytesRef("delta"));
            Block source = builder.build();
            int[] positions = { 1, 3 };
            Block result = PageColumnReader.filterBlock(source, positions, positions.length, blockFactory);
            try {
                assertEquals(2, result.getPositionCount());
                BytesRefBlock brb = (BytesRefBlock) result;
                assertEquals(new BytesRef("beta"), brb.getBytesRef(0, new BytesRef()));
                assertEquals(new BytesRef("delta"), brb.getBytesRef(1, new BytesRef()));
            } finally {
                result.close();
            }
        }
    }

    public void testFilterBlockWithNulls() {
        try (var builder = blockFactory.newLongBlockBuilder(5)) {
            builder.appendLong(10);
            builder.appendNull();
            builder.appendLong(30);
            builder.appendNull();
            builder.appendLong(50);
            Block source = builder.build();
            int[] positions = { 0, 1, 3 };
            Block result = PageColumnReader.filterBlock(source, positions, positions.length, blockFactory);
            try {
                assertEquals(3, result.getPositionCount());
                assertFalse(result.isNull(0));
                assertTrue(result.isNull(1));
                assertTrue(result.isNull(2));
                LongBlock lb = (LongBlock) result;
                assertEquals(10L, lb.getLong(0));
            } finally {
                result.close();
            }
        }
    }

    public void testFilterBlockAllPositions() {
        long[] values = { 10, 20, 30 };
        Block source = blockFactory.newLongArrayVector(values, values.length).asBlock();
        int[] positions = { 0, 1, 2 };
        Block result = PageColumnReader.filterBlock(source, positions, positions.length, blockFactory);
        try {
            // when all positions are selected, filterBlock returns the source directly
            assertSame(source, result);
            assertEquals(3, result.getPositionCount());
        } finally {
            result.close();
        }
    }

    public void testFilterBlockNoPositions() {
        long[] values = { 10, 20, 30 };
        Block source = blockFactory.newLongArrayVector(values, values.length).asBlock();
        int[] positions = {};
        Block result = PageColumnReader.filterBlock(source, positions, 0, blockFactory);
        try {
            assertEquals(0, result.getPositionCount());
        } finally {
            result.close();
        }
    }

    private void assertReadersMatch(ParquetProperties.WriterVersion version, CompressionCodecName codec) throws IOException {
        byte[] data = writeTestFile(version, codec, SCHEMA, NUM_ROWS, PageColumnReaderCorrectnessTests::populateFixedRow);
        assertOptimizedMatchesBaseline(data, COLUMNS);
    }

    // --- readBatchSparse null-leading / interleaved run coverage (#152592) ---

    private static final int SPARSE_ROWS = 20;
    // Nullable columns: null at rows 0 and 9, values elsewhere. A survivor run starting at row 0
    // therefore decodes to a ConstantNullBlock, which — before the concat fix — poisoned the
    // builder when a later non-null run was appended.
    private static final Set<Integer> SPARSE_NULL_ROWS = Set.of(0, 9);

    private static final MessageType SPARSE_SCHEMA = Types.buildMessage()
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("opt_str")
        .optional(INT32)
        .named("opt_int")
        .named("nullable_sparse_test");

    /**
     * {@link PageColumnReader#readBatchSparse} with a null-leading run followed by a value run for a
     * BytesRef column: survivors {0, 5} produce a ConstantNullBlock chunk then a BytesRef chunk.
     * The three-run case {0, 5, 9} additionally trails with a null run. Reproduces #152592 (threw
     * "can't append non-null values to a null block" pre-fix).
     */
    public void testReadBatchSparseNullLeadingBytesRef() throws IOException {
        byte[] data = sparseFile();
        try (Block b = sparseRead(data, "opt_str", DataType.KEYWORD, new int[] { 0, 5 })) {
            assertEquals(2, b.getPositionCount());
            assertTrue(b.isNull(0));
            assertBytesRefAt(b, 1, "s_5");
        }
        try (Block b = sparseRead(data, "opt_str", DataType.KEYWORD, new int[] { 0, 5, 9 })) {
            assertEquals(3, b.getPositionCount());
            assertTrue(b.isNull(0));
            assertBytesRefAt(b, 1, "s_5");
            assertTrue(b.isNull(2));
        }
    }

    /** Same null-leading / interleaved run shapes for a numeric (INT32) column. */
    public void testReadBatchSparseNullLeadingInt() throws IOException {
        byte[] data = sparseFile();
        try (Block b = sparseRead(data, "opt_int", DataType.INTEGER, new int[] { 0, 5 })) {
            assertEquals(2, b.getPositionCount());
            assertTrue(b.isNull(0));
            assertIntAt(b, 1, 5 * 7);
        }
        try (Block b = sparseRead(data, "opt_int", DataType.INTEGER, new int[] { 0, 5, 9 })) {
            assertEquals(3, b.getPositionCount());
            assertTrue(b.isNull(0));
            assertIntAt(b, 1, 5 * 7);
            assertTrue(b.isNull(2));
        }
    }

    private byte[] sparseFile() throws IOException {
        return writeTestFile(
            ParquetProperties.WriterVersion.PARQUET_1_0,
            CompressionCodecName.UNCOMPRESSED,
            SPARSE_SCHEMA,
            SPARSE_ROWS,
            (g, r) -> {
                if (SPARSE_NULL_ROWS.contains(r) == false) {
                    g.append("opt_str", "s_" + r);
                    g.append("opt_int", r * 7);
                }
            }
        );
    }

    /**
     * Opens a fresh reader over {@code data} and drives {@link PageColumnReader#readBatchSparse} for
     * a single flat column, so each survivor set starts from a clean cursor. The caller owns and
     * closes the returned block.
     */
    private Block sparseRead(byte[] data, String column, DataType dataType, int[] survivors) throws IOException {
        try (ParquetFileReader reader = openSparseReader(data)) {
            BlockMetaData block = reader.getRowGroups().getFirst();
            MessageType schema = reader.getFileMetaData().getSchema();
            ColumnDescriptor desc = schema.getColumnDescription(new String[] { column });
            ColumnInfo info = new ColumnInfo(
                desc,
                desc.getPrimitiveType().getPrimitiveTypeName(),
                dataType,
                desc.getMaxDefinitionLevel(),
                desc.getMaxRepetitionLevel(),
                desc.getPrimitiveType().getLogicalTypeAnnotation()
            );
            PageReadStore store = reader.readNextRowGroup();
            assertNotNull(store);
            int rgRows = Math.toIntExact(block.getRowCount());
            try (PageColumnReader pcr = new PageColumnReader(store.getPageReader(desc), desc, info, RowRanges.all(rgRows))) {
                return pcr.readBatchSparse(rgRows, blockFactory, survivors, survivors.length);
            }
        }
    }

    private ParquetFileReader openSparseReader(byte[] data) throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(storageObject(data), blockFactory.arrowAllocator()),
            PlainParquetReadOptions.builder(codecFactory).build()
        );
    }

    private static void assertBytesRefAt(Block block, int position, String expected) {
        assertFalse("position " + position + " must be non-null", block.isNull(position));
        BytesRefBlock brb = (BytesRefBlock) block;
        assertEquals(new BytesRef(expected), brb.getBytesRef(brb.getFirstValueIndex(position), new BytesRef()));
    }

    private static void assertIntAt(Block block, int position, int expected) {
        assertFalse("position " + position + " must be non-null", block.isNull(position));
        IntBlock ib = (IntBlock) block;
        assertEquals(expected, ib.getInt(ib.getFirstValueIndex(position)));
    }

    // --- Randomized test ---

    public void testRandomSchema() throws IOException {
        int numIntCols = between(1, 4);
        int numLongCols = between(1, 4);
        int numDoubleCols = between(1, 3);
        int numBoolCols = between(0, 2);
        int numStrCols = between(1, 3);
        int numRows = between(100, 2000);

        Types.MessageTypeBuilder builder = Types.buildMessage();
        List<ColSpec> specs = new ArrayList<>();

        addColumns(builder, specs, "i", INT32, null, numIntCols);
        addColumns(builder, specs, "l", INT64, null, numLongCols);
        addColumns(builder, specs, "d", DOUBLE, null, numDoubleCols);
        addColumns(builder, specs, "b", BOOLEAN, null, numBoolCols);
        addColumns(builder, specs, "s", BINARY, LogicalTypeAnnotation.stringType(), numStrCols);

        MessageType schema = builder.named("random_test");
        List<String> columns = schema.getFields().stream().map(Type::getName).toList();

        ParquetProperties.WriterVersion version = randomFrom(
            ParquetProperties.WriterVersion.PARQUET_1_0,
            ParquetProperties.WriterVersion.PARQUET_2_0
        );
        CompressionCodecName codec = randomFrom(
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.ZSTD,
            CompressionCodecName.GZIP,
            CompressionCodecName.LZ4_RAW,
            CompressionCodecName.LZ4
        );

        byte[] data = writeTestFile(version, codec, schema, numRows, (group, row) -> {
            for (ColSpec spec : specs) {
                if (spec.optional && row % spec.nullFreq == 0) {
                    continue;
                }
                switch (spec.type) {
                    case INT32 -> group.append(spec.name, row * spec.seed);
                    case INT64 -> group.append(spec.name, (long) row * spec.seed);
                    case DOUBLE -> group.append(spec.name, row * spec.seed * 0.1);
                    case BOOLEAN -> group.append(spec.name, (row + spec.seed) % 3 == 0);
                    case BINARY -> group.append(spec.name, spec.name + "_" + row);
                    default -> throw new IllegalStateException("unexpected type: " + spec.type);
                }
            }
        });

        assertOptimizedMatchesBaseline(data, columns);
    }

    // --- Column spec for randomized tests ---

    private record ColSpec(
        String name,
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName type,
        boolean optional,
        int nullFreq,
        int seed
    ) {}

    private void addColumns(
        Types.MessageTypeBuilder builder,
        List<ColSpec> specs,
        String prefix,
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName type,
        LogicalTypeAnnotation logicalType,
        int count
    ) {
        for (int i = 0; i < count; i++) {
            boolean optional = randomBoolean();
            String name = prefix + "_" + i;
            var col = optional ? Types.optional(type) : Types.required(type);
            if (logicalType != null) {
                col.as(logicalType);
            }
            builder.addField(col.named(name));
            specs.add(new ColSpec(name, type, optional, optional ? between(2, 7) : 0, between(1, 100)));
        }
    }

    // --- Core comparison logic ---

    private void assertOptimizedMatchesBaseline(byte[] data, List<String> columns) throws IOException {
        List<Page> optimized = readAll(new ParquetFormatReader(blockFactory), data, columns);
        List<Page> baseline = readAll(new ParquetFormatReader(blockFactory).withBaselinePath(), data, columns);

        try {
            assertEquals("page count", baseline.size(), optimized.size());

            int totalRows = 0;
            for (int p = 0; p < baseline.size(); p++) {
                Page opt = optimized.get(p);
                Page base = baseline.get(p);
                assertEquals("position count at page " + p, base.getPositionCount(), opt.getPositionCount());
                assertEquals("block count at page " + p, base.getBlockCount(), opt.getBlockCount());

                for (int col = 0; col < base.getBlockCount(); col++) {
                    assertBlocksEqual(columns.get(col), p, opt.getBlock(col), base.getBlock(col));
                }
                totalRows += opt.getPositionCount();
            }
            assertTrue("expected rows > 0", totalRows > 0);
        } finally {
            optimized.forEach(Page::releaseBlocks);
            baseline.forEach(Page::releaseBlocks);
        }
    }

    // --- Block comparison ---

    private static void assertBlocksEqual(String column, int pageIdx, Block actual, Block expected) {
        String ctx = "[" + column + "] page " + pageIdx;
        assertEquals(ctx + " positions", expected.getPositionCount(), actual.getPositionCount());

        for (int pos = 0; pos < expected.getPositionCount(); pos++) {
            boolean expNull = expected.isNull(pos);
            assertEquals(ctx + " null@" + pos, expNull, actual.isNull(pos));
            if (expNull) continue;

            switch (expected) {
                case IntBlock ei when actual instanceof IntBlock ai -> assertEquals(ctx + "@" + pos, ei.getInt(pos), ai.getInt(pos));
                case LongBlock el when actual instanceof LongBlock al -> assertEquals(ctx + "@" + pos, el.getLong(pos), al.getLong(pos));
                case DoubleBlock ed when actual instanceof DoubleBlock ad -> assertEquals(
                    ctx + "@" + pos,
                    ed.getDouble(pos),
                    ad.getDouble(pos),
                    0.0
                );
                case BooleanBlock eb when actual instanceof BooleanBlock ab -> assertEquals(
                    ctx + "@" + pos,
                    eb.getBoolean(pos),
                    ab.getBoolean(pos)
                );
                case BytesRefBlock exb when actual instanceof BytesRefBlock acb -> assertEquals(
                    ctx + "@" + pos,
                    exb.getBytesRef(pos, new BytesRef()),
                    acb.getBytesRef(pos, new BytesRef())
                );
                default -> fail(
                    ctx + " type mismatch: " + expected.getClass().getSimpleName() + " vs " + actual.getClass().getSimpleName()
                );
            }
        }
    }

    // --- Read helpers ---

    private List<Page> readAll(ParquetFormatReader reader, byte[] data, List<String> columns) throws IOException {
        StorageObject so = storageObject(data);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(so, FormatReadContext.of(columns, BATCH_SIZE))) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }
        return pages;
    }

    // --- Fixed row population for the explicit schema ---

    private static void populateFixedRow(Group g, int i) {
        g.append("id", i)
            .append("big_id", (long) i * 100_000L)
            .append("score", i * 1.5)
            .append("weight", (float) (i * 0.1))
            .append("flag", i % 3 == 0)
            .append("name", "row_" + i)
            .append("ts", 1_700_000_000_000L + i * 60_000L);
        if (i % 5 != 0) g.append("opt_int", i * 7);
        if (i % 4 != 0) g.append("opt_long", (long) i * 31);
        if (i % 3 != 0) g.append("opt_double", i * 2.7);
        if (i % 6 != 0) g.append("opt_str", "val_" + (i % 100));
    }

    // --- Parquet file writing ---

    private byte[] writeTestFile(
        ParquetProperties.WriterVersion version,
        CompressionCodecName codec,
        MessageType schema,
        int numRows,
        BiConsumer<Group, Integer> rowPopulator
    ) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(LegacyLz4HadoopFramedCodecFactory.forCodec(codec))
                .withType(schema)
                .withWriterVersion(version)
                .withCompressionCodec(codec)
                .withRowGroupSize(4096)
                .withPageSize(512)
                .build()
        ) {
            for (int i = 0; i < numRows; i++) {
                Group g = factory.newGroup();
                rowPopulator.accept(g, i);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    // --- In-memory Parquet infrastructure ---

    private static StorageObject storageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
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
                return StoragePath.of("memory://correctness_test.parquet");
            }
        };
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
                return "memory://correctness_test.parquet";
            }
        };
    }
}
