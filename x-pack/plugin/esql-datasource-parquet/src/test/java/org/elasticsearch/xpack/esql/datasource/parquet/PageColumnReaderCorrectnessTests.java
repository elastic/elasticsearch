/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DirectBytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.UninitializedArrays;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.CountingBreaker;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

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

    /**
     * Footer byte cache handed to every adapter this test constructs. In production the owning
     * format reader supplies its instance; a fresh per-test-class cache gives the same sharing
     * within a test and automatic isolation between tests.
     */
    private final FooterByteCache footerByteCache = FooterByteCache.fromSettings(Settings.EMPTY);

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
    public void initCodec() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        codecFactory = new PlainCompressionCodecFactory();
        // Every test in this class writes to the same in-memory path ("memory://correctness_test.parquet")
        // with a different file body. Footer caches are per reader instance and each test constructs
        // its own readers, so no cross-test stale-footer clearing is needed.
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

    /**
     * The values must not move when the conversion is hoisted, so the oracle is the same positions read
     * as one whole batch and picked out. The chosen rows straddle the fixture's null rows, so a run that
     * starts with a null is in play.
     */
    public void testSparseReadOfADeclaredConvertingColumnMatchesTheWholeBatch() throws IOException {
        byte[] data = sparseFile();
        int[] survivors = { 0, 2, 3, 5, 9 };
        try (
            Block sparse = declaredRead(data, "opt_int", DataType.INTEGER, DataType.KEYWORD, survivors);
            Block whole = declaredRead(data, "opt_int", DataType.INTEGER, DataType.KEYWORD, null)
        ) {
            assertEquals(survivors.length, sparse.getPositionCount());
            BytesRefBlock s = (BytesRefBlock) sparse;
            BytesRefBlock w = (BytesRefBlock) whole;
            for (int i = 0; i < survivors.length; i++) {
                int at = survivors[i];
                assertEquals("null-ness at source position " + at, w.isNull(at), s.isNull(i));
                if (w.isNull(at) == false) {
                    assertEquals(
                        "value at source position " + at,
                        w.getBytesRef(w.getFirstValueIndex(at), new BytesRef()),
                        s.getBytesRef(s.getFirstValueIndex(i), new BytesRef())
                    );
                }
            }
        }
    }

    /**
     * Reads a column stored as {@code fileType} and declared as {@code declared}, retyped the way
     * {@code ParquetColumnExtractor} retypes its own, so the conversion branch is live. A null
     * {@code survivors} reads the whole batch, the oracle. The caller closes the block.
     */
    private Block declaredRead(byte[] data, String column, DataType fileType, DataType declared, int[] survivors) throws IOException {
        return declaredRead(data, column, fileType, declared, survivors, blockFactory);
    }

    private Block declaredRead(byte[] data, String column, DataType fileType, DataType declared, int[] survivors, BlockFactory factory)
        throws IOException {
        try (ParquetFileReader reader = openReader(data)) {
            BlockMetaData block = reader.getRowGroups().getFirst();
            MessageType schema = reader.getFileMetaData().getSchema();
            ColumnDescriptor desc = schema.getColumnDescription(new String[] { column });
            ColumnInfo info = new ColumnInfo(
                desc,
                desc.getPrimitiveType().getPrimitiveTypeName(),
                fileType,
                desc.getMaxDefinitionLevel(),
                desc.getMaxRepetitionLevel(),
                desc.getPrimitiveType().getLogicalTypeAnnotation()
            ).targetTyped(declared, null);
            PageReadStore store = reader.readNextRowGroup();
            assertNotNull(store);
            int rgRows = Math.toIntExact(block.getRowCount());
            try (PageColumnReader pcr = new PageColumnReader(store.getPageReader(desc), desc, info, RowRanges.all(rgRows))) {
                return survivors == null
                    ? pcr.readBatch(rgRows, factory)
                    : pcr.readBatchSparse(rgRows, factory, survivors, survivors.length);
            }
        }
    }

    /**
     * The conversion runs once for the batch, not once per run. Allocation is the observable: converting
     * inside each run allocates a block per run, hoisted it allocates one. Eight scattered rows allocate
     * 11 times hoisted and 17 per-run, so the threshold sits between. Forcing {@code coerceOnce} false
     * in {@code readBatchSparse} reddens this.
     */
    public void testSparseReadConvertsOncePerBatchNotOncePerRun() throws IOException {
        byte[] data = sparseFile();
        // Eight isolated non-null rows, so each is its own run and a per-run conversion pays eight times.
        int[] survivors = { 1, 3, 5, 7, 11, 13, 15, 17 };
        assertTrue(
            "the pair must still be one that takes the conversion, or this measures nothing",
            DeclaredTypeCoercions.needsBlockCast(DataType.INTEGER, DataType.KEYWORD, false)
        );
        CountingBreaker breaker = new CountingBreaker();
        BlockFactory counting = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        try (Block b = declaredRead(data, "opt_int", DataType.INTEGER, DataType.KEYWORD, survivors, counting)) {
            assertEquals(survivors.length, b.getPositionCount());
            // The conversion ran: an INT32 column read as keyword comes back as bytes, not ints.
            assertThat(b, instanceOf(BytesRefBlock.class));
        }
        assertThat("a per-run conversion reserves 17 times here; hoisted it reserves 11", breaker.positiveReservations(), lessThan(14));
        assertEquals("every block the sparse read allocated must be released", 0L, breaker.used());
    }

    /**
     * A dictionary-encoded column whose FIRST read is a sparse one -- the sort-and-limit shape this
     * change targets. Decoding a run without loading the dictionary fails, so this reddens if the
     * per-run decode stops going through {@code decodeBatchAs}.
     */
    public void testSparseReadIsTheFirstReadOfADictionaryEncodedConvertingColumn() throws IOException {
        byte[] data = writeTestFile(
            ParquetProperties.WriterVersion.PARQUET_1_0,
            CompressionCodecName.UNCOMPRESSED,
            DICT_CONVERSION_SCHEMA,
            SPARSE_ROWS,
            (g, r) -> g.append("num_str", Integer.toString(r % 4))
        );
        int[] survivors = { 1, 5, 9, 14 };
        try (
            Block sparse = declaredRead(data, "num_str", DataType.KEYWORD, DataType.LONG, survivors);
            Block whole = declaredRead(data, "num_str", DataType.KEYWORD, DataType.LONG, null)
        ) {
            assertEquals(survivors.length, sparse.getPositionCount());
            LongBlock s = (LongBlock) sparse;
            LongBlock w = (LongBlock) whole;
            for (int i = 0; i < survivors.length; i++) {
                assertEquals("value at source position " + survivors[i], w.getLong(survivors[i]), s.getLong(i));
            }
        }
    }

    private static final MessageType DICT_CONVERSION_SCHEMA = Types.buildMessage()
        .required(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("num_str")
        .named("dictionary_conversion_test");

    /**
     * A strict conversion failure on the sparse path, after the chunks have been joined and closed. That
     * boundary is where the coercion could leak the joined block or lose its exception, and neither shows
     * up on a read that succeeds. Strings declared {@code long} fail for every row, at four scattered
     * positions, so the failure happens once the whole batch has decoded.
     */
    public void testStrictConversionFailureAfterTheJoinReleasesEverything() throws IOException {
        byte[] data = sparseFile();
        CountingBreaker breaker = new CountingBreaker();
        BlockFactory counting = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        // opt_str holds "s_<row>", which no row parses as a long, and a null warnings sink is strict.
        // Row 2 leads, and "s_2" is not a prefix of any other chosen row's value, so naming it in the
        // message is unambiguous -- "s_1" would also match s_11 and s_15 and prove nothing about which
        // value failed.
        Exception e = expectThrows(
            Exception.class,
            () -> declaredRead(data, "opt_str", DataType.KEYWORD, DataType.LONG, new int[] { 2, 5, 11, 15 }, counting)
        );
        assertThat("the first row read must be the one that fails", e.getMessage(), containsString("s_2"));
        for (String later : new String[] { "s_5", "s_11", "s_15" }) {
            assertThat("a later row must not be the one reported", e.getMessage(), not(containsString(later)));
        }
        assertEquals("the joined block and every chunk must be released on the failure path", 0L, breaker.used());
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
        try (ParquetFileReader reader = openReader(data)) {
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

    private ParquetFileReader openReader(byte[] data) throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(storageObject(data), footerByteCache, blockFactory.breaker()),
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

    // --- Two-phase page-filtered skip overshoot (skipRows) ---

    private static final MessageType SKIP_OVERSHOOT_SCHEMA = Types.buildMessage().required(INT64).named("v").named("skip_overshoot_repro");

    /**
     * Exercises the two-phase page-filtered skip overshoot in {@link PageColumnReader#skipRows}.
     *
     * <p>When a projection reader carries a survivor {@link RowRanges} that EXCLUDES an early data
     * page (zero survivors on that page) and survivors sit on a later page, {@code loadNextPage}
     * skips the whole excluded page, jumping the physical cursor PAST the caller's skip target. If
     * that overshoot is discarded, the reader is silently ahead of where the caller thinks it is and
     * every later skip/read consumes real survivor rows.
     *
     * <p>The invariant asserted here is coordinate-space-exact and codec/geometry independent: two
     * skip calls that together cover exactly the excluded first page must leave the reader on the
     * first survivor row (source row == first-row-index of page 1), whose value equals its
     * source-row index. With the overshoot bug the first skip jumps to the page boundary and
     * discards the surplus, so the second skip eats into page 1 and the subsequent read returns a
     * survivor row too far along.
     */
    public void testSkipCoveringExcludedPageLandsAtNextPageStart() throws IOException {
        byte[] data = writeTwoPageFile();
        try (ParquetFileReader reader = openReader(data)) {
            BlockMetaData rg = reader.getRowGroups().getFirst();
            long totalRows = rg.getRowCount();
            OffsetIndex oi = reader.readOffsetIndex(rg.getColumns().getFirst());
            assertNotNull("column must have an offset index", oi);
            assertTrue("need at least two pages to reproduce", oi.getPageCount() >= 2);

            // page 0 spans [0, firstPageRows); page 1 starts at firstPageRows.
            int firstPageRows = Math.toIntExact(oi.getFirstRowIndex(1));
            assertTrue(firstPageRows >= 2);

            // Survivor RowRanges cover only page 1 onward, so page 0 is excluded (zero survivors)
            // and survivors sit strictly after it, triggering the skip overshoot.
            RowRanges survivorRanges = RowRanges.of(firstPageRows, totalRows, totalRows);

            ColumnDescriptor desc = SKIP_OVERSHOOT_SCHEMA.getColumns().getFirst();
            ColumnInfo info = new ColumnInfo(
                desc,
                desc.getPrimitiveType().getPrimitiveTypeName(),
                DataType.LONG,
                desc.getMaxDefinitionLevel(),
                desc.getMaxRepetitionLevel(),
                desc.getPrimitiveType().getLogicalTypeAnnotation()
            );

            PageReadStore store = reader.readNextRowGroup();
            assertNotNull(store);

            try (PageColumnReader pcr = new PageColumnReader(store.getPageReader(desc), desc, info, survivorRanges)) {
                // The two-phase caller drains the fully-filtered leading batches with skipRows in
                // source-row coordinates. Split the excluded page into two skips so the first one
                // stops short of the page boundary (count < firstPageRows) and triggers the jump.
                int firstSkip = firstPageRows / 2;
                assertTrue(firstSkip > 0 && firstSkip < firstPageRows);
                pcr.skipRows(firstSkip);
                pcr.skipRows(firstPageRows - firstSkip);

                // We have now skipped exactly the source rows of page 0. The next read must start
                // at the first survivor row, whose value equals its source-row index.
                LongBlock block = (LongBlock) pcr.readBatch(1, blockFactory);
                try {
                    assertEquals(1, block.getPositionCount());
                    assertEquals(
                        "reader must sit on the first survivor row after skipping the excluded page",
                        (long) firstPageRows,
                        block.getLong(0)
                    );
                } finally {
                    block.close();
                }
            }
        }
    }

    /**
     * Writes a single-column {@code {v: long}} file over one row group with plain (dictionary-disabled)
     * encoding and a tiny page size, so {@code v} spans several small data pages. {@code v} is unique
     * per row and equals its source-row index, so a decoded value pins the reader's position.
     */
    private byte[] writeTwoPageFile() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(SKIP_OVERSHOOT_SCHEMA);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(codecFactory)
                .withType(SKIP_OVERSHOOT_SCHEMA)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                // Plain encoding + a tiny page size forces several small pages within one row group.
                .withDictionaryEncoding(false)
                .withRowGroupSize(64L * 1024 * 1024)
                .withPageSize(1024)
                .build()
        ) {
            for (int i = 0; i < 4000; i++) {
                writer.write(factory.newGroup().append("v", (long) i));
            }
        }
        return out.toByteArray();
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

    public void testCompressedUniqueStringsAcrossPages() throws IOException {
        // PARQUET_1_0 PLAIN aliases the reused dest. Fixed-width values keep page uncompressed
        // sizes equal so dest is reused rather than grown (a grow would leave the old array
        // alive and hide a missing copy-out). PARQUET_2_0 DELTA_BYTE_ARRAY copies the page in
        // initDecoders, so it would not catch a missing copy-out. Loop every dest-reuse codec:
        // a destLen vs dest.length bug is codec-specific (Zstd).
        for (CompressionCodecName codec : new CompressionCodecName[] {
            CompressionCodecName.SNAPPY,
            CompressionCodecName.ZSTD,
            CompressionCodecName.GZIP,
            CompressionCodecName.LZ4_RAW }) {
            assertCompressedUniqueStringsAcrossPages(codec);
        }
    }

    private void assertCompressedUniqueStringsAcrossPages(CompressionCodecName codec) throws IOException {
        int rows = 200;
        int batchSize = 128;
        MessageType schema = Types.buildMessage()
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("req")
            .optional(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("opt")
            .named("unique_str_reuse");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(codecFactory)
                .withType(schema)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(codec)
                .withDictionaryEncoding(false)
                .withRowGroupSize(64L * 1024 * 1024)
                .withPageSize(64)
                .build()
        ) {
            for (int i = 0; i < rows; i++) {
                Group g = groupFactory.newGroup().append("req", String.format(Locale.ROOT, "req_%05d", i));
                if (i % 7 != 0) {
                    g.append("opt", String.format(Locale.ROOT, "opt_%05d", i));
                }
                writer.write(g);
            }
        }
        byte[] data = out.toByteArray();
        try (ParquetFileReader parquetReader = openReader(data)) {
            OffsetIndex oi = parquetReader.readOffsetIndex(parquetReader.getRowGroups().getFirst().getColumns().getFirst());
            assertNotNull(codec + " column must have an offset index", oi);
            assertTrue(codec + " need ≥2 data pages so dest reuse can clobber", oi.getPageCount() >= 2);
            assertTrue(codec + " batch must span page 0", batchSize > oi.getFirstRowIndex(1));
        }
        List<String> columns = List.of("req", "opt");
        List<Page> pages = readAll(new ParquetFormatReader(blockFactory), data, columns, batchSize);
        try {
            assertEquals(codec + " first batch", batchSize, pages.getFirst().getPositionCount());
            int pos = 0;
            for (Page page : pages) {
                BytesRefBlock req = (BytesRefBlock) page.getBlock(0);
                BytesRefBlock opt = (BytesRefBlock) page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    assertBytesRefAt(req, i, String.format(Locale.ROOT, "req_%05d", pos));
                    if (pos % 7 == 0) {
                        assertTrue(codec + " opt null@" + pos, opt.isNull(i));
                    } else {
                        assertBytesRefAt(opt, i, String.format(Locale.ROOT, "opt_%05d", pos));
                    }
                    pos++;
                }
            }
            assertEquals(codec + " rows", rows, pos);
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
        assertOptimizedMatchesBaseline(data, columns);
    }

    // --- PLAIN UTF-8 region path ---

    private static final BytesRef FFFD = new BytesRef("\uFFFD");

    public void testPlainUtf8RegionLength127And128() throws IOException {
        byte[] shortOk = bytesOf('x', 127);
        byte[] longOk = bytesOf('y', 128);
        byte[] data = writePlainKeywordFile(false, row -> row == 0 ? shortOk : longOk, 2);
        assertPlainKeywordBlock(data, 2, false, row -> new BytesRef(row == 0 ? shortOk : longOk), "127/128");
    }

    public void testPlainUtf8RegionLength256AsciiPrefix() throws IOException {
        // 256 = 00 01 00 00 LE: all prefix bytes are ASCII, so the region scan must still apply.
        byte[] longOk = bytesOf('z', 256);
        byte[] data = writePlainKeywordFile(false, row -> longOk, 3);
        assertPlainKeywordBlock(data, 3, false, row -> new BytesRef(longOk), "len=256");
    }

    public void testPlainUtf8RegionTruncatedLeadBeforeNextPrefix() throws IOException {
        byte[] truncated = { 'a', (byte) 0xE3 };
        byte[] next = { 'b' };
        byte[] data = writePlainKeywordFile(false, row -> row == 0 ? truncated : next, 2);
        assertPlainKeywordBlock(data, 2, false, row -> row == 0 ? concat(new BytesRef("a"), FFFD) : new BytesRef("b"), "truncated");
    }

    public void testPlainUtf8RegionBadValueFirstMiddleLast() throws IOException {
        byte[] bad = { (byte) 0xFF };
        byte[][] good = {
            "aa".getBytes(StandardCharsets.UTF_8),
            "bb".getBytes(StandardCharsets.UTF_8),
            "cc".getBytes(StandardCharsets.UTF_8) };
        for (int badAt : new int[] { 0, 1, 2 }) {
            int badIndex = badAt;
            byte[] data = writePlainKeywordFile(false, row -> row == badIndex ? bad : good[row], 3);
            assertPlainKeywordBlock(data, 3, false, row -> row == badIndex ? FFFD : new BytesRef(good[row]), "badAt=" + badIndex);
        }
    }

    public void testPlainUtf8SplitSequenceAcrossValuesIsRepaired() throws IOException {
        // Concatenation of C3 + A9 is well-formed "é", but each value is ill-formed. Dest
        // validation must reject continuation-at-start and repair both to U+FFFD.
        byte[] lead = { (byte) 0xC3 };
        byte[] cont = { (byte) 0xA9 };
        byte[] data = writePlainKeywordFile(false, row -> row == 0 ? lead : cont, 2);
        assertPlainKeywordBlock(data, 2, false, row -> FFFD, "split-seq");
    }

    public void testPlainUtf8CyrillicAndMixedAccepted() throws IOException {
        byte[] cyr = "Привет".getBytes(StandardCharsets.UTF_8);
        byte[] data = writePlainKeywordFile(false, row -> cyr, 8);
        assertPlainKeywordBlock(data, 8, false, row -> new BytesRef(cyr), "cyrillic");

        byte[] cafe = "café".getBytes(StandardCharsets.UTF_8);
        byte[] url = "https://example.com/path/to/resource?q=abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        byte[] mixed = writePlainKeywordFile(false, row -> row == 0 ? cafe : url, 6);
        assertPlainKeywordBlock(mixed, 6, false, row -> new BytesRef(row == 0 ? cafe : url), "mixed");
    }

    public void testPlainUtf8RegionWithNulls() throws IOException {
        byte[] bad = { (byte) 0xFF };
        byte[] ok = "ok".getBytes(StandardCharsets.UTF_8);
        byte[] data = writePlainKeywordFile(true, row -> {
            if (row % 3 == 0) {
                return null;
            }
            return row == 1 ? bad : ok;
        }, 6);
        assertPlainKeywordBlock(data, 6, false, row -> {
            if (row % 3 == 0) {
                return null;
            }
            return row == 1 ? FFFD : new BytesRef(ok);
        }, "nulls");
    }

    public void testDictionaryKeywordWithBadEntryStaysOrdinal() throws IOException {
        byte[] bad = { (byte) 0xFF };
        byte[] ok = "ok".getBytes(StandardCharsets.UTF_8);
        byte[] data = writeDictionaryKeywordFile(row -> (row % 2 == 0) ? ok : bad, 64);
        assertPlainKeywordBlock(data, 64, true, row -> (row % 2 == 0) ? new BytesRef(ok) : FFFD, "dict-bad");
    }

    public void testMixedDictionaryThenPlainSanitizesBadDictEntry() throws IOException {
        byte[] bad = { (byte) 0xFF };
        byte[] ok = "ok".getBytes(StandardCharsets.UTF_8);
        // Batch 256 with 200 dictionary rows forces one readBatch to span the dict→PLAIN
        // fallback with a malformed dictionary value in the prefix.
        int dictRows = 200;
        int plainRows = 100;
        int total = dictRows + plainRows;
        byte[] data = writeMixedDictionaryThenPlainKeywordFile(row -> {
            if (row < dictRows) {
                return (row % 2 == 0) ? ok : bad;
            }
            return ("u" + row).getBytes(StandardCharsets.UTF_8);
        }, total, 128);
        boolean sawPlain = false;
        boolean sawFallbackBatch = false;
        int row = 0;
        List<Page> pages = readAll(new ParquetFormatReader(blockFactory), data, List.of("s"), 256);
        try {
            BytesRef scratch = new BytesRef();
            for (Page page : pages) {
                BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
                if (block.asOrdinals() == null) {
                    sawPlain = true;
                    if (row < dictRows && row + page.getPositionCount() > dictRows) {
                        sawFallbackBatch = true;
                    }
                }
                for (int p = 0; p < page.getPositionCount(); p++) {
                    BytesRef expected = row < dictRows ? ((row % 2 == 0) ? new BytesRef(ok) : FFFD) : new BytesRef("u" + row);
                    assertEquals("row " + row, expected, block.getBytesRef(p, scratch));
                    row++;
                }
            }
            assertEquals(total, row);
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
        boolean fileHasDict;
        boolean fileHasPlain;
        try (ParquetFileReader reader = openReader(data)) {
            var encodings = reader.getRowGroups().getFirst().getColumns().getFirst().getEncodings();
            fileHasDict = encodings.contains(Encoding.PLAIN_DICTIONARY) || encodings.contains(Encoding.RLE_DICTIONARY);
            fileHasPlain = encodings.contains(Encoding.PLAIN);
        }
        if (fileHasDict && fileHasPlain) {
            assertTrue("mixed dict+PLAIN file must emit a batch that spans dict-to-PLAIN fallback", sawFallbackBatch);
            assertTrue("mixed dict+PLAIN file must emit a PLAIN remainder", sawPlain);
        }
        assertOptimizedMatchesBaseline(data, List.of("s"));
    }

    // --- Corrupt negative PLAIN BINARY length ---

    /**
     * A corrupt negative PLAIN length reads as an empty KEYWORD value (see {@link PlainValueDecoder}). The full
     * read and a sparse read that skips over it both land on the same following values.
     */
    public void testPlainKeywordNegativeLengthIsEmptyForReadAndSkip() throws IOException {
        byte[] first = "aaa".getBytes(StandardCharsets.UTF_8);
        byte[][] values = { first, new byte[0], "bbb".getBytes(StandardCharsets.UTF_8), "ccc".getBytes(StandardCharsets.UTF_8) };
        byte[] data = writePlainKeywordFile(false, row -> values[row], values.length);
        corruptEmptyPrefixAfter(data, first);

        try (Block full = fullRead(data, "s", DataType.KEYWORD)) {
            assertEquals(4, full.getPositionCount());
            assertBytesRefAt(full, 0, "aaa");
            assertBytesRefAt(full, 1, "");
            assertBytesRefAt(full, 2, "bbb");
            assertBytesRefAt(full, 3, "ccc");
        }
        // Rows 1 and 2 are skipped: the skip must step over the corrupt prefix without moving backwards.
        try (Block sparse = sparseRead(data, "s", DataType.KEYWORD, new int[] { 0, 3 })) {
            assertEquals(2, sparse.getPositionCount());
            assertBytesRefAt(sparse, 0, "aaa");
            assertBytesRefAt(sparse, 1, "ccc");
        }
    }

    /**
     * An empty unscaled value in a BINARY-backed DECIMAL reads as 0 rather than failing, whether a writer stored
     * zero bytes or a corrupt negative PLAIN length decoded as empty; the values after it decode unchanged.
     */
    public void testPlainBinaryDecimalEmptyValueReadsAsZero() throws IOException {
        byte[] first = BigInteger.valueOf(12345).toByteArray();
        byte[][] values = { first, new byte[0], BigInteger.valueOf(-678).toByteArray() };
        byte[] data = writeBinaryFile(LogicalTypeAnnotation.decimalType(2, 9), false, false, 64 * 1024, row -> values[row], values.length);
        boolean corrupt = randomBoolean();
        if (corrupt) {
            corruptEmptyPrefixAfter(data, first);
        }

        try (Block raw = fullRead(data, "s", DataType.DOUBLE)) {
            DoubleBlock block = (DoubleBlock) raw;
            assertEquals(3, block.getPositionCount());
            assertEquals(123.45, block.getDouble(0), 0.0);
            assertEquals("empty unscaled value, corrupt=" + corrupt, 0.0, block.getDouble(1), 0.0);
            assertEquals(-6.78, block.getDouble(2), 0.0);
        }
    }

    /**
     * The baseline reader reads an empty unscaled value in a BINARY-backed DECIMAL as 0, as the optimized reader
     * does, from PLAIN or dictionary pages. Unlike the optimized reader, parquet-java's own PLAIN decoder rejects a
     * negative length, so only a writer-stored empty value is covered here.
     */
    public void testBaselineBinaryDecimalEmptyValueReadsAsZero() throws IOException {
        byte[][] values = { BigInteger.valueOf(12345).toByteArray(), new byte[0], BigInteger.valueOf(-678).toByteArray() };
        boolean dictionary = randomBoolean();
        byte[] data = writeBinaryFile(
            LogicalTypeAnnotation.decimalType(2, 9),
            false,
            dictionary,
            64 * 1024,
            row -> values[row],
            values.length
        );

        List<Page> pages = readAll(new ParquetFormatReader(blockFactory).withBaselinePath(), data, List.of("s"));
        try {
            assertEquals(1, pages.size());
            DoubleBlock block = pages.getFirst().getBlock(0);
            assertEquals(3, block.getPositionCount());
            assertEquals(123.45, block.getDouble(0), 0.0);
            assertEquals("empty unscaled value, dictionary=" + dictionary, 0.0, block.getDouble(1), 0.0);
            assertEquals(-6.78, block.getDouble(2), 0.0);
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
        assertOptimizedMatchesBaseline(data, List.of("s"));
    }

    /**
     * Finds the one PLAIN value {@code before} (length prefix and payload) that is immediately followed by an
     * empty value, and overwrites the empty value's prefix with a random negative length, as a corrupt page
     * would hold. Only an empty value's prefix is rewritten so the rest of the page stays aligned. The written
     * files carry no page checksums that the reader verifies.
     */
    private static void corruptEmptyPrefixAfter(byte[] file, byte[] before) {
        byte[] pattern = new byte[2 * Integer.BYTES + before.length];
        ByteBuffer.wrap(pattern).order(ByteOrder.LITTLE_ENDIAN).putInt(before.length).put(before).putInt(0);
        int at = -1;
        for (int i = 0; i + pattern.length <= file.length; i++) {
            if (Arrays.equals(file, i, i + pattern.length, pattern, 0, pattern.length)) {
                assertEquals("the PLAIN value must occur once in the file", -1, at);
                at = i;
            }
        }
        assertThat("the PLAIN value must occur in the file", at, greaterThan(-1));
        ByteBuffer.wrap(file)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(at + Integer.BYTES + before.length, -randomIntBetween(1, Integer.MAX_VALUE));
    }

    private void assertPlainKeywordBlock(byte[] data, int numRows, boolean expectOrdinal, IntFunction<BytesRef> expected, String ctx)
        throws IOException {
        try (Block raw = fullRead(data, "s", DataType.KEYWORD)) {
            BytesRefBlock block = (BytesRefBlock) raw;
            if (expectOrdinal) {
                assertNotNull(ctx + ": dictionary pages must stay ordinal", block.asOrdinals());
            } else {
                assertNull(ctx + ": PLAIN pages must not emit ordinals", block.asOrdinals());
            }
            assertEquals(ctx + " rows", numRows, block.getPositionCount());
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < numRows; i++) {
                BytesRef exp = expected.apply(i);
                if (exp == null) {
                    assertTrue(ctx + " null at " + i, block.isNull(i));
                } else {
                    assertFalse(ctx + " non-null at " + i, block.isNull(i));
                    assertEquals(ctx + " row " + i, exp, block.getBytesRef(i, scratch));
                }
            }
        }
        assertOptimizedMatchesBaseline(data, List.of("s"));
    }

    private static byte[] bytesOf(char c, int len) {
        byte[] out = new byte[len];
        Arrays.fill(out, (byte) c);
        return out;
    }

    private static BytesRef concat(BytesRef a, BytesRef b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a.bytes, a.offset, out, 0, a.length);
        System.arraycopy(b.bytes, b.offset, out, a.length, b.length);
        return new BytesRef(out);
    }

    private byte[] writePlainKeywordFile(boolean optional, IntFunction<byte[]> valueFor, int numRows) throws IOException {
        return writeKeywordFile(optional, false, 64 * 1024, valueFor, numRows);
    }

    private byte[] writeDictionaryKeywordFile(IntFunction<byte[]> valueFor, int numRows) throws IOException {
        return writeKeywordFile(false, true, 64 * 1024, valueFor, numRows);
    }

    private byte[] writeMixedDictionaryThenPlainKeywordFile(IntFunction<byte[]> valueFor, int numRows, int dictionaryPageSize)
        throws IOException {
        return writeKeywordFile(false, true, dictionaryPageSize, valueFor, numRows);
    }

    private byte[] writeKeywordFile(boolean optional, boolean dictionary, int dictionaryPageSize, IntFunction<byte[]> valueFor, int numRows)
        throws IOException {
        return writeKeywordFile(ParquetProperties.WriterVersion.PARQUET_1_0, optional, dictionary, dictionaryPageSize, valueFor, numRows);
    }

    private byte[] writeKeywordFile(
        ParquetProperties.WriterVersion version,
        boolean optional,
        boolean dictionary,
        int dictionaryPageSize,
        IntFunction<byte[]> valueFor,
        int numRows
    ) throws IOException {
        return writeBinaryFile(version, LogicalTypeAnnotation.stringType(), optional, dictionary, dictionaryPageSize, valueFor, numRows);
    }

    /** Writes one uncompressed V1 BINARY column {@code s} annotated with {@code logical}. */
    private byte[] writeBinaryFile(
        LogicalTypeAnnotation logical,
        boolean optional,
        boolean dictionary,
        int dictionaryPageSize,
        IntFunction<byte[]> valueFor,
        int numRows
    ) throws IOException {
        return writeBinaryFile(
            ParquetProperties.WriterVersion.PARQUET_1_0,
            logical,
            optional,
            dictionary,
            dictionaryPageSize,
            valueFor,
            numRows
        );
    }

    /** Writes one uncompressed BINARY column {@code s} annotated with {@code logical}, with data pages of {@code version}. */
    private byte[] writeBinaryFile(
        ParquetProperties.WriterVersion version,
        LogicalTypeAnnotation logical,
        boolean optional,
        boolean dictionary,
        int dictionaryPageSize,
        IntFunction<byte[]> valueFor,
        int numRows
    ) throws IOException {
        Types.MessageTypeBuilder schemaBuilder = Types.buildMessage();
        var col = optional ? Types.optional(BINARY) : Types.required(BINARY);
        col.as(logical);
        schemaBuilder.addField(col.named("s"));
        MessageType schema = schemaBuilder.named("plain_utf8");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", Boolean.toString(dictionary));
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(conf)
                .withCodecFactory(codecFactory)
                .withType(schema)
                .withWriterVersion(version)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(dictionary)
                .withDictionaryPageSize(dictionaryPageSize)
                .withRowGroupSize(64L * 1024 * 1024)
                .withPageSize(4 * 1024)
                .build()
        ) {
            for (int i = 0; i < numRows; i++) {
                Group g = factory.newGroup();
                byte[] v = valueFor.apply(i);
                if (v != null) {
                    g.append("s", Binary.fromConstantByteArray(Arrays.copyOf(v, v.length)));
                }
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    // --- Flat (DirectBytesRefVector) KEYWORD path ---

    private static final int FLAT_ROWS = 2000;

    /** One required PLAIN page read as one batch lands, uncopied, in a {@link DirectBytesRefVector}. */
    public void testFlatRequiredSinglePageIsDirectVector() throws IOException {
        int rows = 20;
        byte[] data = writePlainKeywordFile(false, PageColumnReaderCorrectnessTests::distinctKeyword, rows);
        assertEquals("fixture must be a single page", 1, pageCount(data));
        assertFlatRead(data, rows, rows, DEFAULT_CAP, row -> new BytesRef(distinctKeyword(row)), BlockKind.DIRECT);
    }

    /**
     * A batch spanning several pages grows the flat bytes once per page and must still come out in order. A random
     * batch size also puts page boundaries at arbitrary points of a batch; a batch that starts with a one-value
     * chunk enters the constant state and leaves it on the next chunk.
     */
    public void testFlatRequiredBatchSpanningPages() throws IOException {
        byte[] data = writePlainKeywordFile(false, PageColumnReaderCorrectnessTests::distinctKeyword, FLAT_ROWS);
        assertThat("fixture must span pages", pageCount(data), greaterThan(1));
        IntFunction<BytesRef> expected = row -> new BytesRef(distinctKeyword(row));
        assertFlatRead(data, FLAT_ROWS, FLAT_ROWS, DEFAULT_CAP, expected, BlockKind.DIRECT);
        assertFlatRead(data, FLAT_ROWS, randomIntBetween(2, FLAT_ROWS - 1), DEFAULT_CAP, expected, BlockKind.DIRECT);
    }

    /**
     * Equal values across every page are a constant block, also when the batch is larger than the cap: while a
     * batch is constant the flat tail is only scratch, so the cap must not push it to the slow path.
     */
    public void testFlatRequiredConstantBatchIsConstantBlock() throws IOException {
        byte[] value = "same-value".getBytes(StandardCharsets.UTF_8);
        byte[] data = writePlainKeywordFile(false, row -> value, FLAT_ROWS);
        assertThat("fixture must span pages", pageCount(data), greaterThan(1));
        for (long cap : new long[] { DEFAULT_CAP, 64 }) {
            assertFlatRead(data, FLAT_ROWS, FLAT_ROWS, cap, row -> new BytesRef(value), BlockKind.CONSTANT);
        }
    }

    /**
     * A constant run covering whole pages, then a change: the run is materialized in front of the changing
     * chunk. Under the default cap that lands in the flat vector; under a small cap on the slow path.
     */
    public void testFlatRequiredConstantRunThenChange() throws IOException {
        int runLength = 1500;
        byte[] same = "same".getBytes(StandardCharsets.UTF_8);
        IntFunction<byte[]> valueFor = row -> row < runLength ? same : distinctKeyword(row);
        byte[] data = writePlainKeywordFile(false, valueFor, FLAT_ROWS);
        assertThat("the run must cover at least one whole page", firstPageRows(data), lessThan((long) runLength));
        IntFunction<BytesRef> expected = row -> new BytesRef(valueFor.apply(row));
        assertFlatRead(data, FLAT_ROWS, FLAT_ROWS, DEFAULT_CAP, expected, BlockKind.DIRECT);
        assertFlatRead(data, FLAT_ROWS, FLAT_ROWS, offsetsBytes(FLAT_ROWS) + 1024, expected, BlockKind.SLOW);
    }

    /**
     * Malformed UTF-8 moves the batch to the slow path, with the values already in the flat arrays copied over
     * first, and sanitizes the bad chunk as before: U+FFFD per maximal ill-formed subsequence.
     */
    public void testFlatRequiredMalformedFallsBackAndSanitizes() throws IOException {
        byte[] bad = { (byte) 0xFF };
        byte[] truncated = { 'a', (byte) 0xE3 };
        for (int badRow : new int[] { 0, FLAT_ROWS / 2, FLAT_ROWS - 1 }) {
            boolean useTruncated = randomBoolean();
            IntFunction<byte[]> valueFor = row -> row == badRow ? (useTruncated ? truncated : bad) : distinctKeyword(row);
            byte[] data = writePlainKeywordFile(false, valueFor, FLAT_ROWS);
            BytesRef sanitized = useTruncated ? concat(new BytesRef("a"), FFFD) : FFFD;
            IntFunction<BytesRef> expected = row -> row == badRow ? sanitized : new BytesRef(distinctKeyword(row));
            assertFlatRead(data, FLAT_ROWS, FLAT_ROWS, DEFAULT_CAP, expected, BlockKind.SLOW);
            assertOptimizedMatchesBaseline(data, List.of("s"));
        }
    }

    /**
     * A batch whose bytes plus offsets outgrow {@link BlockFactory#maxPrimitiveArrayBytes()} moves to the slow
     * path: after a few pages (a cap that fits the first page), or at once (a cap below the offsets alone).
     */
    public void testFlatRequiredOverCapFallsBack() throws IOException {
        byte[] data = writePlainKeywordFile(false, PageColumnReaderCorrectnessTests::distinctKeyword, FLAT_ROWS);
        assertThat("fixture must span pages", pageCount(data), greaterThan(2));
        IntFunction<BytesRef> expected = row -> new BytesRef(distinctKeyword(row));
        for (long cap : new long[] { offsetsBytes(FLAT_ROWS) + 6 * 1024, 16 }) {
            assertFlatRead(data, FLAT_ROWS, FLAT_ROWS, cap, expected, BlockKind.SLOW);
        }
    }

    /** An optional column with no nulls takes the flat path too. */
    public void testFlatOptionalNullFreeIsDirectVector() throws IOException {
        byte[] data = writePlainKeywordFile(true, PageColumnReaderCorrectnessTests::distinctKeyword, FLAT_ROWS);
        assertThat("fixture must span pages", pageCount(data), greaterThan(1));
        assertFlatRead(data, FLAT_ROWS, FLAT_ROWS, DEFAULT_CAP, row -> new BytesRef(distinctKeyword(row)), BlockKind.DIRECT);
    }

    /**
     * Optional batches that leave the flat path: nulls after flat values (copied into the builder first), leading
     * nulls (never flat), malformed UTF-8 in a null-free batch, and a batch over the cap.
     */
    public void testFlatOptionalFallsBackToBuilder() throws IOException {
        IntFunction<byte[]> trailingNulls = row -> row >= 1500 && row % 7 == 0 ? null : distinctKeyword(row);
        IntFunction<byte[]> leadingNulls = row -> row < 10 ? null : distinctKeyword(row);
        assertOptionalFallsBack(trailingNulls, row -> asBytesRef(trailingNulls.apply(row)), DEFAULT_CAP);
        assertOptionalFallsBack(leadingNulls, row -> asBytesRef(leadingNulls.apply(row)), DEFAULT_CAP);

        int badRow = randomIntBetween(0, FLAT_ROWS - 1);
        IntFunction<byte[]> malformed = row -> row == badRow ? new byte[] { (byte) 0xFF } : distinctKeyword(row);
        assertOptionalFallsBack(malformed, row -> row == badRow ? FFFD : new BytesRef(distinctKeyword(row)), DEFAULT_CAP);

        IntFunction<byte[]> nullFree = PageColumnReaderCorrectnessTests::distinctKeyword;
        assertOptionalFallsBack(nullFree, row -> new BytesRef(distinctKeyword(row)), offsetsBytes(FLAT_ROWS) + 6 * 1024);
    }

    /** UUID columns keep the per-value formatting path and never take the flat one. */
    public void testFlatUuidStaysOnSlowPath() throws IOException {
        int rows = 50;
        UUID[] uuids = new UUID[rows];
        for (int i = 0; i < rows; i++) {
            uuids[i] = new UUID(randomLong(), randomLong());
        }
        MessageType schema = Types.buildMessage()
            .required(FIXED_LEN_BYTE_ARRAY)
            .length(16)
            .as(LogicalTypeAnnotation.uuidType())
            .named("s")
            .named("uuid_flat");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(codecFactory)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                // PLAIN, so the batch reaches the PLAIN branch that UUID must still bypass.
                .withDictionaryEncoding(false)
                .build()
        ) {
            for (UUID uuid : uuids) {
                ByteBuffer bytes = ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
                writer.write(factory.newGroup().append("s", Binary.fromConstantByteArray(bytes.array())));
            }
        }
        assertFlatRead(out.toByteArray(), rows, rows, DEFAULT_CAP, row -> new BytesRef(uuids[row].toString()), BlockKind.SLOW);
    }

    // --- Dictionary chunk that falls back to PLAIN ---

    private static final int MIXED_ROWS = 4000;
    /** Rows below this hold one of four values, so their pages stay dictionary-encoded; later rows are all distinct. */
    private static final int LOW_CARDINALITY_ROWS = 2000;
    /** Rows below this may be null in the optional variant; all of them land on dictionary pages. */
    private static final int NULLABLE_ROWS = 500;

    /**
     * parquet-mr switches a chunk from dictionary to PLAIN once the dictionary outgrows its size limit, so a
     * high-cardinality column has a few dictionary pages followed by many PLAIN pages. A batch that starts on one of
     * those PLAIN pages must take the flat path, as in a dictionary-less chunk, rather than the ordinal path's
     * per-value fallback. Batches on the dictionary pages stay ordinal, and the one batch that straddles the switch
     * stays correct. Read with batches aligned to the switch and with a random batch size.
     */
    public void testDictionaryFallbackPlainPagesUseFlatPath() throws IOException {
        for (boolean optional : new boolean[] { false, true }) {
            // Nulls only early on, on the dictionary pages: they exercise the ordinal and straddling paths while the
            // PLAIN pages stay null-free and so eligible for the flat path.
            IntFunction<byte[]> valueFor = row -> optional && row < NULLABLE_ROWS && row % 5 == 0 ? null : mixedKeyword(row);
            byte[] data = writeKeywordFile(optional, true, 256, valueFor, MIXED_ROWS);
            int dictRows = dictionaryEncodedRows(data, Encoding.PLAIN);
            assertThat("nulls must stay on the dictionary pages", dictRows, greaterThan(NULLABLE_ROWS));
            IntFunction<BytesRef> expected = row -> asBytesRef(valueFor.apply(row));
            assertMixedRead(data, dictRows, 0, dictRows, expected, -1, BlockKind.DIRECT);
            assertMixedRead(data, dictRows, 0, randomIntBetween(2, MIXED_ROWS), expected, -1, BlockKind.DIRECT);
            // A sparse read whose first page, past the skipped ones, is a PLAIN page.
            int sparseStart = randomFrom(fallbackPageStarts(data, dictRows));
            assertMixedRead(data, dictRows, sparseStart, randomIntBetween(2, MIXED_ROWS), expected, -1, BlockKind.DIRECT);
            assertOptimizedMatchesBaseline(data, List.of("s"));
        }
    }

    /**
     * With the V2 writer, parquet-mr falls back from dictionary to DELTA_BYTE_ARRAY rather than PLAIN. A batch that
     * starts on such a page leaves the ordinal path for the required/optional readers, whose slow path decodes the
     * delta pages. Batches on the dictionary pages stay ordinal, the batch that straddles the switch stays correct,
     * and so does a sparse read whose first page, past the skipped ones, is a fallback page.
     */
    public void testDictionaryFallbackDeltaPagesV2() throws IOException {
        for (boolean optional : new boolean[] { false, true }) {
            IntFunction<byte[]> valueFor = row -> optional && row < NULLABLE_ROWS && row % 5 == 0 ? null : mixedKeyword(row);
            byte[] data = writeKeywordFile(ParquetProperties.WriterVersion.PARQUET_2_0, optional, true, 256, valueFor, MIXED_ROWS);
            int dictRows = dictionaryEncodedRows(data, Encoding.DELTA_BYTE_ARRAY);
            assertThat("nulls must stay on the dictionary pages", dictRows, greaterThan(NULLABLE_ROWS));
            IntFunction<BytesRef> expected = row -> asBytesRef(valueFor.apply(row));
            assertMixedRead(data, dictRows, 0, dictRows, expected, -1, BlockKind.SLOW);
            assertMixedRead(data, dictRows, 0, randomIntBetween(2, MIXED_ROWS), expected, -1, BlockKind.SLOW);
            int sparseStart = randomFrom(fallbackPageStarts(data, dictRows));
            assertMixedRead(data, dictRows, sparseStart, randomIntBetween(2, MIXED_ROWS), expected, -1, BlockKind.SLOW);
            assertOptimizedMatchesBaseline(data, List.of("s"));
        }
    }

    /**
     * Malformed UTF-8 on the PLAIN pages of a dictionary chunk is sanitized on the flat path's slow mode, both in a
     * batch that starts on a PLAIN page and in the PLAIN suffix of the batch that straddles the switch. Other batches
     * that start on PLAIN pages stay direct.
     */
    public void testDictionaryFallbackPlainPagesSanitizeMalformed() throws IOException {
        for (boolean optional : new boolean[] { false, true }) {
            byte[] probe = writeKeywordFile(optional, true, 256, PageColumnReaderCorrectnessTests::mixedKeyword, MIXED_ROWS);
            int dictRows = dictionaryEncodedRows(probe, Encoding.PLAIN);
            // Where the writer switches depends only on the dictionary, which the PLAIN region's bytes do not change.
            int badRow = randomIntBetween(dictRows, MIXED_ROWS - 1);
            byte[] bad = { 'x', (byte) 0xFF };
            IntFunction<byte[]> valueFor = row -> row == badRow ? bad : mixedKeyword(row);
            byte[] data = writeKeywordFile(optional, true, 256, valueFor, MIXED_ROWS);
            assertEquals("the switch must not move", dictRows, dictionaryEncodedRows(data, Encoding.PLAIN));
            IntFunction<BytesRef> expected = row -> row == badRow ? concat(new BytesRef("x"), FFFD) : new BytesRef(mixedKeyword(row));
            assertMixedRead(data, dictRows, 0, dictRows, expected, badRow, BlockKind.DIRECT);
            // A first batch that straddles the switch and holds the bad row in its PLAIN suffix.
            int straddling = badRow + 1 + randomIntBetween(0, MIXED_ROWS - badRow - 1);
            assertMixedRead(data, dictRows, 0, straddling, expected, badRow, BlockKind.DIRECT);
            assertMixedRead(data, dictRows, 0, randomIntBetween(2, MIXED_ROWS), expected, badRow, BlockKind.DIRECT);
            assertOptimizedMatchesBaseline(data, List.of("s"));
        }
    }

    private static byte[] mixedKeyword(int row) {
        return row < LOW_CARDINALITY_ROWS ? ("low-" + (row % 4)).getBytes(StandardCharsets.UTF_8) : distinctKeyword(row);
    }

    /**
     * Reads column {@code s} of a dictionary chunk whose first {@code dictRows} rows are on dictionary pages, in batches
     * of {@code batchSize}, checking every value and that the breaker returns to zero. A {@code firstRow} above 0 must
     * start a page: the read is then sparse, with {@link RowRanges} that skip every page before it. A batch inside the
     * dictionary pages must be ordinal or constant. A multi-row batch that starts on a fallback page must be of kind
     * {@code fallbackKind}, unless it holds the malformed {@code badRow} ({@code -1} for none), which moves it to the
     * slow path. Without a malformed row, PLAIN batches aligned to the switch must produce at least one direct batch.
     */
    private void assertMixedRead(
        byte[] data,
        int dictRows,
        int firstRow,
        int batchSize,
        IntFunction<BytesRef> expected,
        int badRow,
        BlockKind fallbackKind
    ) throws IOException {
        CountingBreaker breaker = new CountingBreaker();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker.service());
        BlockFactory factory = BlockFactory.builder(bigArrays).breaker(breaker).build();
        List<BytesRefBlock> blocks = readKeywordBatches(data, firstRow, batchSize, factory);
        try {
            BytesRef scratch = new BytesRef();
            int row = firstRow;
            int directBatches = 0;
            for (BytesRefBlock block : blocks) {
                int start = row;
                int end = start + block.getPositionCount();
                String ctx = "batch [" + start + ", " + end + ") of " + batchSize + ", switch at " + dictRows;
                if (end <= dictRows) {
                    assertTrue(ctx + " must stay ordinal", block.asOrdinals() != null || kindOf(block) == BlockKind.CONSTANT);
                } else if (start >= dictRows && block.getPositionCount() > 1) {
                    boolean malformed = start <= badRow && badRow < end;
                    BlockKind kind = malformed ? BlockKind.SLOW : fallbackKind;
                    assertEquals(ctx, kind, kindOf(block));
                    directBatches += kind == BlockKind.DIRECT ? 1 : 0;
                }
                for (int p = 0; p < block.getPositionCount(); p++, row++) {
                    BytesRef exp = expected.apply(row);
                    if (exp == null) {
                        assertTrue("null at row " + row, block.isNull(p));
                    } else {
                        assertFalse("non-null at row " + row, block.isNull(p));
                        assertEquals("row " + row, exp, block.getBytesRef(p, scratch));
                    }
                }
            }
            assertEquals(MIXED_ROWS, row);
            if (fallbackKind == BlockKind.DIRECT && badRow < 0 && batchSize == dictRows && MIXED_ROWS - dictRows > 1) {
                assertThat("batches aligned to the switch must start on PLAIN pages", directBatches, greaterThan(0));
            }
        } finally {
            Releasables.close(blocks);
        }
        assertEquals("every byte the batches reserved must be released", 0L, breaker.used());
    }

    /**
     * Rows on the dictionary-encoded pages of column {@code s}, which must all come before the pages the writer fell
     * back to, all encoded with {@code fallback}. Fails unless the chunk really mixes both, so the fixture cannot
     * silently degrade to a single encoding.
     */
    private int dictionaryEncodedRows(byte[] data, Encoding fallback) throws IOException {
        try (ParquetFileReader reader = openReader(data)) {
            assertEquals(1, reader.getRowGroups().size());
            ColumnDescriptor desc = reader.getFileMetaData().getSchema().getColumnDescription(new String[] { "s" });
            PageReadStore store = reader.readNextRowGroup();
            PageReader pages = store.getPageReader(desc);
            assertNotNull("the chunk must have a dictionary", pages.readDictionaryPage());
            int dictRows = 0;
            int fallbackPages = 0;
            for (DataPage page = pages.readPage(); page != null; page = pages.readPage()) {
                Encoding encoding = switch (page) {
                    case DataPageV1 v1 -> v1.getValueEncoding();
                    case DataPageV2 v2 -> v2.getDataEncoding();
                    default -> throw new AssertionError("unexpected page " + page);
                };
                if (encoding.usesDictionary()) {
                    assertEquals("dictionary pages must precede fallback ones", 0, fallbackPages);
                    dictRows += page.getValueCount();
                } else {
                    assertEquals(fallback, encoding);
                    fallbackPages++;
                }
            }
            assertThat("the chunk must start with dictionary pages", dictRows, greaterThan(0));
            assertThat("the chunk must fall back to several pages", fallbackPages, greaterThan(1));
            return dictRows;
        }
    }

    /** The first row of every page of column {@code s} at or past {@code dictRows}, the pages the writer fell back to. */
    private List<Integer> fallbackPageStarts(byte[] data, int dictRows) throws IOException {
        try (ParquetFileReader reader = openReader(data)) {
            OffsetIndex oi = reader.readOffsetIndex(reader.getRowGroups().getFirst().getColumns().getFirst());
            assertNotNull("column must have an offset index", oi);
            List<Integer> starts = new ArrayList<>();
            for (int p = 0; p < oi.getPageCount(); p++) {
                int firstRow = Math.toIntExact(oi.getFirstRowIndex(p));
                if (firstRow >= dictRows) {
                    starts.add(firstRow);
                }
            }
            assertThat("the chunk must fall back to several pages", starts.size(), greaterThan(1));
            return starts;
        }
    }

    private void assertOptionalFallsBack(IntFunction<byte[]> valueFor, IntFunction<BytesRef> expected, long cap) throws IOException {
        byte[] data = writePlainKeywordFile(true, valueFor, FLAT_ROWS);
        assertFlatRead(data, FLAT_ROWS, FLAT_ROWS, cap, expected, BlockKind.SLOW);
        assertFlatRead(data, FLAT_ROWS, randomIntBetween(2, FLAT_ROWS - 1), cap, expected, null);
        assertOptimizedMatchesBaseline(data, List.of("s"));
    }

    private static final long DEFAULT_CAP = BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes();

    /** How a batch came out: the flat path's {@link DirectBytesRefVector}, a constant, or the slow path's array or builder. */
    private enum BlockKind {
        DIRECT,
        CONSTANT,
        SLOW
    }

    /**
     * Reads column {@code s} in batches of {@code batchSize} through a factory whose breaker counts every byte and
     * whose plain-array cap is {@code maxPrimitiveArrayBytes}, checks every value, checks that each multi-row batch
     * is of kind {@code kind} (unless {@code null}), and that closing the blocks returns the breaker to zero.
     */
    private void assertFlatRead(
        byte[] data,
        int rows,
        int batchSize,
        long maxPrimitiveArrayBytes,
        IntFunction<BytesRef> expected,
        @Nullable BlockKind kind
    ) throws IOException {
        CountingBreaker breaker = new CountingBreaker();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker.service());
        BlockFactory factory = BlockFactory.builder(bigArrays).breaker(breaker).maxPrimitiveArraySize(maxPrimitiveArrayBytes).build();
        List<BytesRefBlock> blocks = readKeywordBatches(data, batchSize, factory);
        try {
            BytesRef scratch = new BytesRef();
            int row = 0;
            for (BytesRefBlock block : blocks) {
                if (kind != null && block.getPositionCount() > 1) {
                    assertEquals("batch at row " + row, kind, kindOf(block));
                }
                for (int p = 0; p < block.getPositionCount(); p++, row++) {
                    BytesRef exp = expected.apply(row);
                    if (exp == null) {
                        assertTrue("null at row " + row, block.isNull(p));
                    } else {
                        assertFalse("non-null at row " + row, block.isNull(p));
                        assertEquals("row " + row, exp, block.getBytesRef(p, scratch));
                    }
                }
            }
            assertEquals(rows, row);
        } finally {
            Releasables.close(blocks);
        }
        assertEquals("every byte the batches reserved must be released", 0L, breaker.used());
    }

    private static BlockKind kindOf(BytesRefBlock block) {
        BytesRefVector vector = block.asVector();
        if (vector != null && vector.isConstant()) {
            return BlockKind.CONSTANT;
        }
        return vector instanceof DirectBytesRefVector ? BlockKind.DIRECT : BlockKind.SLOW;
    }

    private List<BytesRefBlock> readKeywordBatches(byte[] data, int batchSize, BlockFactory factory) throws IOException {
        return readKeywordBatches(data, 0, batchSize, factory);
    }

    /**
     * Reads column {@code s} from {@code firstRow} to the end of the row group in batches of {@code batchSize}. A
     * {@code firstRow} above 0 must start a page: {@link RowRanges} then skip every page before it.
     */
    private List<BytesRefBlock> readKeywordBatches(byte[] data, int firstRow, int batchSize, BlockFactory factory) throws IOException {
        List<BytesRefBlock> blocks = new ArrayList<>();
        boolean success = false;
        try (ParquetFileReader reader = openReader(data)) {
            int rgRows = Math.toIntExact(reader.getRowGroups().getFirst().getRowCount());
            ColumnDescriptor desc = reader.getFileMetaData().getSchema().getColumnDescription(new String[] { "s" });
            ColumnInfo info = new ColumnInfo(
                desc,
                desc.getPrimitiveType().getPrimitiveTypeName(),
                DataType.KEYWORD,
                desc.getMaxDefinitionLevel(),
                desc.getMaxRepetitionLevel(),
                desc.getPrimitiveType().getLogicalTypeAnnotation()
            );
            PageReadStore store = reader.readNextRowGroup();
            assertNotNull(store);
            RowRanges ranges = firstRow == 0 ? RowRanges.all(rgRows) : RowRanges.of(firstRow, rgRows, rgRows);
            try (PageColumnReader pcr = new PageColumnReader(store.getPageReader(desc), desc, info, ranges)) {
                int read = firstRow;
                while (read < rgRows) {
                    Block block = pcr.readBatch(Math.min(batchSize, rgRows - read), factory);
                    blocks.add((BytesRefBlock) block);
                    assertThat("a batch must make progress", block.getPositionCount(), greaterThan(0));
                    read += block.getPositionCount();
                }
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(blocks);
            }
        }
        return blocks;
    }

    private int pageCount(byte[] data) throws IOException {
        try (ParquetFileReader reader = openReader(data)) {
            OffsetIndex oi = reader.readOffsetIndex(reader.getRowGroups().getFirst().getColumns().getFirst());
            assertNotNull("column must have an offset index", oi);
            return oi.getPageCount();
        }
    }

    private long firstPageRows(byte[] data) throws IOException {
        try (ParquetFileReader reader = openReader(data)) {
            OffsetIndex oi = reader.readOffsetIndex(reader.getRowGroups().getFirst().getColumns().getFirst());
            assertNotNull("column must have an offset index", oi);
            assertThat(oi.getPageCount(), greaterThan(1));
            return oi.getFirstRowIndex(1);
        }
    }

    /** What the flat offsets array alone costs for a batch of {@code rows}. */
    private static long offsetsBytes(int rows) {
        return (rows + 1L) * Integer.BYTES;
    }

    private static byte[] distinctKeyword(int row) {
        return String.format(Locale.ROOT, "value-%05d", row).getBytes(StandardCharsets.UTF_8);
    }

    private static BytesRef asBytesRef(byte[] bytes) {
        return bytes == null ? null : new BytesRef(bytes);
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
        return readAll(reader, data, columns, BATCH_SIZE);
    }

    private List<Page> readAll(ParquetFormatReader reader, byte[] data, List<String> columns, int batchSize) throws IOException {
        StorageObject so = storageObject(data);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(so, FormatReadContext.of(columns, batchSize))) {
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

    /**
     * At least one cell for every decode arm, because each writes its kept values differently and so leaves the
     * position it skips differently. Not a bijection: the long and double arms each serve two cells, since an
     * unsigned long and a float reach them by a different route.
     */
    private static final MessageType NULLABLE_TYPES_SCHEMA = Types.buildMessage()
        .optional(BOOLEAN)
        .named("opt_bool")
        .optional(INT32)
        .named("opt_int")
        .optional(INT64)
        .named("opt_long")
        .optional(DOUBLE)
        .named("opt_double")
        // An unsigned column sign-flip-encodes or widens each value it keeps, so its write loop is not the one
        // the signed cells above exercise; the slot it leaves for a null is the same slot.
        .optional(INT64)
        .as(LogicalTypeAnnotation.intType(64, false))
        .named("opt_ulong")
        .optional(INT32)
        .as(LogicalTypeAnnotation.intType(32, false))
        .named("opt_uint")
        .optional(FLOAT)
        .named("opt_float")
        .optional(INT64)
        .as(LogicalTypeAnnotation.decimalType(4, 18))
        .named("opt_decimal")
        .optional(INT64)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
        .named("opt_datetime")
        .optional(INT64)
        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
        .named("opt_datenanos")
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("opt_str")
        .optional(FIXED_LEN_BYTE_ARRAY)
        .length(2)
        .as(LogicalTypeAnnotation.float16Type())
        .named("opt_f16")
        .optional(INT96)
        .named("opt_i96")
        .named("nullable_types_test");

    private static final int DETERMINISM_ROWS = 1024;
    private static final int DETERMINISM_READS = 400;

    /**
     * Reading the same nullable column twice must produce identical bytes. A value array filled only at the
     * positions carrying a value leaves masked positions holding prior memory, which is serialized even though
     * every consumer honours the mask and the results are correct. Asserting on bytes catches that for every
     * element type, not just boolean, where deserialization happens to reject a non-canonical byte. Repeated
     * because any single pair of reads can coincide.
     */
    public void testNullableColumnSerializesDeterministically() throws IOException {
        UninitializedArrays.ensureUnsafeEnabled();
        byte[] data = writeTestFile(
            ParquetProperties.WriterVersion.PARQUET_1_0,
            CompressionCodecName.UNCOMPRESSED,
            NULLABLE_TYPES_SCHEMA,
            DETERMINISM_ROWS,
            (g, r) -> {
                if (r % 5 != 0) {
                    g.append("opt_bool", r % 4 == 0);
                    g.append("opt_int", r);
                    g.append("opt_long", (long) r);
                    g.append("opt_double", r * 1.5);
                    // Values above the signed maxima, so a dropped sign-flip or a sign-extension is visible too.
                    g.append("opt_ulong", Long.MIN_VALUE + r);
                    g.append("opt_uint", Integer.MIN_VALUE + r);
                    g.append("opt_float", r * 0.25f);
                    g.append("opt_decimal", (long) r * 101);
                    g.append("opt_datetime", 1_700_000_000_000L + r);
                    g.append("opt_datenanos", 1_700_000_000_000_000L + r);
                    // Few enough distinct entries that the keyword arm takes its dictionary/ordinals path.
                    g.append("opt_str", "v" + (r % 16));
                    g.append("opt_f16", Binary.fromConstantByteArray(toFloat16Bytes(r * 0.25f)));
                    g.append("opt_i96", new NanoTime(2_451_545, 43_200_000_000_000L + r));
                }
            }
        );
        assertColumnSerializesDeterministically(data, "opt_bool", DataType.BOOLEAN);
        assertColumnSerializesDeterministically(data, "opt_int", DataType.INTEGER);
        assertColumnSerializesDeterministically(data, "opt_long", DataType.LONG);
        assertColumnSerializesDeterministically(data, "opt_double", DataType.DOUBLE);
        assertColumnSerializesDeterministically(data, "opt_ulong", DataType.UNSIGNED_LONG);
        assertColumnSerializesDeterministically(data, "opt_uint", DataType.LONG);
        assertColumnSerializesDeterministically(data, "opt_float", DataType.DOUBLE);
        assertColumnSerializesDeterministically(data, "opt_decimal", DataType.DOUBLE);
        assertColumnSerializesDeterministically(data, "opt_datetime", DataType.DATETIME);
        assertColumnSerializesDeterministically(data, "opt_datenanos", DataType.DATE_NANOS);
        assertColumnSerializesDeterministically(data, "opt_str", DataType.KEYWORD);
        assertColumnSerializesDeterministically(data, "opt_f16", DataType.DOUBLE);
        assertColumnSerializesDeterministically(data, "opt_i96", DataType.DATETIME);
    }

    private void assertColumnSerializesDeterministically(byte[] data, String column, DataType dataType) throws IOException {
        byte[] expected = null;
        for (int read = 0; read < DETERMINISM_READS; read++) {
            try (Block block = fullRead(data, column, dataType)) {
                // One batch is one data page, not the row group; a fresh reader starts at row 0, so position p is row p.
                int positions = block.getPositionCount();
                assertThat(column + " read nothing", positions, greaterThan(0));
                for (int p = 0; p < positions; p++) {
                    boolean isNull = p % 5 == 0;
                    assertEquals(column + " null at " + p, isNull, block.isNull(p));
                    if (isNull) {
                        // Comparing reads can only false-pass: memory that happens to be reproducible agrees
                        // with itself. Read the masked element itself, which is deterministic when zeroed.
                        assertMaskedPositionIsDefined(column, dataType, block, p);
                    }
                }
                byte[] actual = serializedBytes(block);
                if (expected == null) {
                    expected = actual;
                } else {
                    assertArrayEquals(column + " serialized form changed on read " + read, expected, actual);
                }
            }
        }
    }

    /**
     * The backing element at a masked position, read directly. Single-valued here, so the position indexes the
     * array. A bytes column keeps its values in a {@link org.apache.lucene.util.BytesRef} array rather than a
     * primitive one, so there is no slot to read and the byte comparison is the only oracle for it.
     */
    private static void assertMaskedPositionIsDefined(String column, DataType dataType, Block block, int p) {
        String where = column + " masked position " + p;
        assertEquals(where + " must index the array directly", p, block.getFirstValueIndex(p));
        if (block instanceof BooleanBlock b) {
            assertFalse(where, b.getBoolean(p));
        } else if (block instanceof IntBlock b) {
            assertEquals(where, 0, b.getInt(p));
        } else if (block instanceof LongBlock b) {
            // An unsigned column sign-flip-encodes its whole batch after decoding, masked positions included,
            // so a defined zero arrives here as the encoding of zero rather than as zero.
            long expected = dataType == DataType.UNSIGNED_LONG ? ParquetColumnDecoding.encodeUnsignedLong(0L) : 0L;
            assertEquals(where, expected, b.getLong(p));
        } else if (block instanceof DoubleBlock b) {
            assertEquals(where, 0.0, b.getDouble(p), 0.0);
        }
    }

    /** Little-endian IEEE half, the layout a Parquet FLOAT16 column stores. */
    private static byte[] toFloat16Bytes(float value) {
        short half = Float.floatToFloat16(value);
        return new byte[] { (byte) (half & 0xFF), (byte) ((half >> 8) & 0xFF) };
    }

    /** Serialized bytes, copied out before close returns the stream's pages to the recycler. */
    private static byte[] serializedBytes(Block block) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Block.writeTypedBlock(block, out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    /** Reads {@code column} as a single full batch through a fresh reader. The caller owns the block. */
    private Block fullRead(byte[] data, String column, DataType dataType) throws IOException {
        try (ParquetFileReader reader = openReader(data)) {
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
                return pcr.readBatch(rgRows, blockFactory);
            }
        }
    }

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

    private static final AtomicInteger STORAGE_SEQ = new AtomicInteger();

    private static StorageObject storageObject(byte[] data) {
        // FooterByteCache keys (identity, path, length, version). Same-length files in one test
        // must not share a path or the second read is served the first file's cached footer.
        String id = Integer.toString(STORAGE_SEQ.incrementAndGet());
        return new StorageObject() {
            @Override
            public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

            @Override
            public String version() {
                return id;
            }

            @Override
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

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
                return StoragePath.of("memory://correctness_test-" + id + ".parquet");
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
