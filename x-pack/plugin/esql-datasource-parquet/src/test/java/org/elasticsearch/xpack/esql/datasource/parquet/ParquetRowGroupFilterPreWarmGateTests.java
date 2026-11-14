/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntPredicate;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * parquet-mr's DICTIONARY row-group filter level reads dictionary pages through the reader's
 * synchronous stream. {@code PreloadedRowGroupMetadata} pre-warms those pages when the footer
 * says where they are, but files from parquet-mr 1.10-era writers (Spark 3.1) omit
 * {@code dictionary_page_offset} and the page index, so nothing is pre-warmed and every row
 * group costs one blocking window-sized range GET that prunes nothing when the statistics
 * already said "might match". {@code computeSurvivingRowGroups} now drops the DICTIONARY level for
 * such row groups; these tests pin that down on both file shapes.
 * <p>
 * The fixture is a 16-value dictionary {@code value} column (the predicate column), an
 * {@code id} column and an unprojected random {@code pad} column sized so each row group is
 * larger than one adapter window, which keeps every dictionary read on a fresh window. The
 * "legacy" variant rewrites the footer the way 1.10 wrote it: {@code data_page_offset} points at
 * the dictionary page, {@code dictionary_page_offset} is unset, no column/offset index.
 */
public class ParquetRowGroupFilterPreWarmGateTests extends ESTestCase {

    private static final int DICT_CARDINALITY = 16;
    private static final int ROWS = 12_288;
    /** Rows carrying {@link #RARE_CODE} sit in {@code [RARE_FROM, RARE_TO)}, well inside the middle row group. */
    private static final int RARE_CODE = 7;
    private static final int RARE_FROM = ROWS / 2 - 256;
    private static final int RARE_TO = ROWS / 2 + 256;
    private static final long ROW_GROUP_SIZE = 6L * 1024 * 1024;
    /** About 1.5 KiB of unprojected random padding per row: ~4096 rows per 6 MiB row group, 3 row groups. */
    private static final int PAD_PER_ROW = (int) (ROW_GROUP_SIZE / 4096);
    private static final List<String> PROJECTION = List.of("value", "id");
    /** Encoding stats of a chunk whose data pages are all dictionary encoded: the one shape {@code DictionaryFilter} reads. */
    private static final EncodingStats PURE_DICTIONARY_STATS = new EncodingStats.Builder().addDictEncoding(Encoding.PLAIN)
        .addDataEncoding(Encoding.PLAIN_DICTIONARY)
        .build();

    private ExecutorService asyncIo;
    private LimitedBreaker breaker;
    private BlockFactory blockFactory;

    @Before
    public void startAsyncIo() {
        asyncIo = Executors.newFixedThreadPool(4, EsExecutors.daemonThreadFactory("test", "prewarm-gate"));
        breaker = new LimitedBreaker("prewarm-gate", ByteSizeValue.ofGb(2));
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
    }

    @After
    public void stopAsyncIo() throws Exception {
        terminate(asyncIo);
    }

    /**
     * The issue's shape: a range predicate every row group's statistics admit, on a legacy file.
     * The DICTIONARY level is skipped, so the filtered open issues no more synchronous GETs than
     * an unfiltered read of the same file, and the per-row predicate still yields the exact rows.
     */
    public void testLegacyFileRangePredicateIssuesNoSyncDictionaryReads() throws Exception {
        byte[] file = legacyFooter(parquetFile());
        assertLegacyShape(file);
        long baselineSync = readAll(file, null).syncGets();
        ReadResult filtered = readAll(file, valueGreaterThan(codeToValue(5)));
        assertEquals(expectedRows(c -> c > 5), filtered.rows());
        assertEquals("statistics admit every row group", filtered.status().rowGroupsTotal(), filtered.status().rowGroupsKept());
        assertThat(filtered.status().rowGroupsTotal(), greaterThanOrEqualTo(3L));
        assertEquals(
            "the dictionary level is skipped for every row group",
            filtered.status().rowGroupsTotal(),
            filtered.status().rowGroupsDictionaryFilterSkipped()
        );
        assertThat(
            "filtered open must not add a synchronous dictionary GET per row group; baseline=" + baselineSync,
            filtered.syncGets(),
            lessThanOrEqualTo(baselineSync)
        );
    }

    /**
     * Same predicate on the unmodified file: dictionaries are pre-warmed, so the level runs
     * from memory and still adds no synchronous GETs.
     */
    public void testModernFileRangePredicateIssuesNoSyncDictionaryReads() throws Exception {
        byte[] file = parquetFile();
        long baselineSync = readAll(file, null).syncGets();
        ReadResult filtered = readAll(file, valueGreaterThan(codeToValue(5)));
        assertEquals(expectedRows(c -> c > 5), filtered.rows());
        assertEquals(0L, filtered.status().rowGroupsDictionaryFilterSkipped());
        assertThat(filtered.syncGets(), lessThanOrEqualTo(baselineSync));
    }

    /**
     * Dictionary pruning is kept where it is free: on the unmodified file an equality on a value
     * present in only one row group's dictionary (identical min/max everywhere) still prunes.
     */
    public void testModernFileEqualityStillPrunesByDictionary() throws Exception {
        byte[] file = parquetFile();
        long baselineSync = readAll(file, null).syncGets();
        ReadResult filtered = readAll(file, valueEquals(codeToValue(RARE_CODE)));
        assertEquals(expectedRows(c -> c == RARE_CODE), filtered.rows());
        assertThat(filtered.status().rowGroupsTotal(), greaterThanOrEqualTo(3L));
        assertThat(
            "the dictionary level must prune the row groups without the rare value",
            filtered.status().rowGroupsKept(),
            lessThan(filtered.status().rowGroupsTotal())
        );
        assertEquals(0L, filtered.status().rowGroupsDictionaryFilterSkipped());
        assertThat(filtered.syncGets(), lessThanOrEqualTo(baselineSync));
    }

    /**
     * The same equality on the legacy file: pruning is given up (every row group survives), the
     * row-level predicate keeps the result exact and no synchronous dictionary GET is issued.
     */
    public void testLegacyFileEqualityKeepsAllRowGroupsWithoutSyncReads() throws Exception {
        byte[] file = legacyFooter(parquetFile());
        assertLegacyShape(file);
        long baselineSync = readAll(file, null).syncGets();
        ReadResult filtered = readAll(file, valueEquals(codeToValue(RARE_CODE)));
        assertEquals(expectedRows(c -> c == RARE_CODE), filtered.rows());
        assertEquals(filtered.status().rowGroupsTotal(), filtered.status().rowGroupsKept());
        assertEquals(filtered.status().rowGroupsTotal(), filtered.status().rowGroupsDictionaryFilterSkipped());
        assertThat(filtered.syncGets(), lessThanOrEqualTo(baselineSync));
    }

    public void testDictionaryLevelAppliesWhenNoPredicateColumnHasDictionary() {
        BlockMetaData block = block(
            column("value", 1_000L, 0L, Set.of(Encoding.PLAIN), null),
            column("id", 5_000L, 4_000L, dictEncodings(), PURE_DICTIONARY_STATS)
        );
        assertTrue(ParquetFormatReader.dictionaryFilterLevelApplies(block, Set.of("value"), new TreeMap<>()));
        assertTrue(ParquetFormatReader.dictionaryFilterLevelApplies(block, Set.of("missing"), new TreeMap<>()));
        assertFalse(ParquetFormatReader.dictionaryFilterLevelApplies(block, Set.of("id"), new TreeMap<>()));
    }

    public void testDictionaryLevelRequiresChunkCoveringStartingPos() {
        // Explicit dictionary offset: the read starts at the dictionary page.
        BlockMetaData explicit = block(column("value", 1_500L, 1_000L, dictEncodings(), PURE_DICTIONARY_STATS));
        assertFalse(ParquetFormatReader.dictionaryFilterLevelApplies(explicit, Set.of("value"), chunks(0L, 1_000L)));
        assertFalse(ParquetFormatReader.dictionaryFilterLevelApplies(explicit, Set.of("value"), chunks(1_001L, 10L)));
        assertTrue(ParquetFormatReader.dictionaryFilterLevelApplies(explicit, Set.of("value"), chunks(1_000L, 500L)));
        assertTrue(ParquetFormatReader.dictionaryFilterLevelApplies(explicit, Set.of("value"), chunks(900L, 101L)));
        // Omitted dictionary offset (legacy writers): the read starts at the first data page offset.
        BlockMetaData omitted = block(column("value", 1_000L, 0L, dictEncodings(), PURE_DICTIONARY_STATS));
        assertFalse(ParquetFormatReader.dictionaryFilterLevelApplies(omitted, Set.of("value"), chunks(1_500L, 10L)));
        assertTrue(ParquetFormatReader.dictionaryFilterLevelApplies(omitted, Set.of("value"), chunks(1_000L, 1L)));
    }

    /** {@code DictionaryFilter} answers "might match" for chunks with non-dictionary pages without reading the dictionary. */
    public void testDictionaryLevelAppliesWhenEncodingStatsShowNonDictionaryPages() {
        EncodingStats mixed = new EncodingStats.Builder().addDictEncoding(Encoding.PLAIN)
            .addDataEncoding(Encoding.PLAIN_DICTIONARY)
            .addDataEncoding(Encoding.PLAIN)
            .build();
        assertTrue(
            ParquetFormatReader.dictionaryFilterLevelApplies(
                block(column("value", 1_500L, 1_000L, dictEncodings(), mixed)),
                Set.of("value"),
                new TreeMap<>()
            )
        );
        assertFalse(
            ParquetFormatReader.dictionaryFilterLevelApplies(
                block(column("value", 1_500L, 1_000L, dictEncodings(), PURE_DICTIONARY_STATS)),
                Set.of("value"),
                new TreeMap<>()
            )
        );
    }

    /**
     * Without encoding stats (pre-1.9 writers) {@code DictionaryFilter} reads the dictionary only when
     * the encodings set is {@code PLAIN_DICTIONARY} plus level encodings; any other set, and V2
     * {@code RLE_DICTIONARY}, is answered without a read.
     */
    public void testDictionaryLevelWithoutEncodingStatsFollowsEncodingsSet() {
        BlockMetaData pureV1 = block(
            column("value", 1_500L, 1_000L, Set.of(Encoding.PLAIN_DICTIONARY, Encoding.RLE, Encoding.BIT_PACKED), null)
        );
        assertFalse(ParquetFormatReader.dictionaryFilterLevelApplies(pureV1, Set.of("value"), new TreeMap<>()));
        assertTrue(ParquetFormatReader.dictionaryFilterLevelApplies(pureV1, Set.of("value"), chunks(1_000L, 500L)));
        BlockMetaData mixedV1 = block(column("value", 1_500L, 1_000L, dictEncodings(), null));
        assertTrue(ParquetFormatReader.dictionaryFilterLevelApplies(mixedV1, Set.of("value"), new TreeMap<>()));
        BlockMetaData v2 = block(column("value", 1_500L, 1_000L, Set.of(Encoding.RLE_DICTIONARY, Encoding.RLE), null));
        assertTrue(ParquetFormatReader.dictionaryFilterLevelApplies(v2, Set.of("value"), new TreeMap<>()));
    }

    private record ReadResult(int rows, long syncGets, ParquetReaderStatus status) {}

    private ReadResult readAll(byte[] file, Expression pushed) throws IOException {
        CountingStorage storage = new CountingStorage(file, asyncIo);
        ParquetReaderCounters counters = new ParquetReaderCounters();
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withIoWatermark(
            new ParquetIoWatermark(1024L * 1024 * 1024)
        );
        if (pushed != null) {
            reader = reader.withPushedFilter(new ParquetPushedExpressions(List.of(pushed)));
        }
        List<Attribute> schema = List.of(valueAttribute(), new ReferenceAttribute(Source.EMPTY, "id", DataType.INTEGER));
        FormatReadContext context = FormatReadContext.builder()
            .projectedColumns(PROJECTION)
            .readSchema(schema)
            .batchSize(256)
            .readCounters(counters)
            .build();
        int rows;
        try (CloseableIterator<Page> iter = reader.read(storage, context)) {
            rows = drain(iter);
        }
        assertEquals("breaker refunds everything after close", 0L, breaker.getUsed());
        return new ReadResult(rows, storage.syncGets.get(), counters.snapshot());
    }

    private static int drain(CloseableIterator<Page> iter) {
        int rows = 0;
        while (true) {
            SubscribableListener<Void> ready = iter.waitForReady();
            if (ready.isDone() == false) {
                safeAwait(ready, TimeValue.timeValueSeconds(30));
                continue;
            }
            Page page = iter.tryAdvance();
            if (page == null) {
                if (iter.waitForReady().isDone() == false) {
                    continue;
                }
                page = iter.tryAdvance();
                if (page == null) {
                    if (iter.waitForReady().isDone() == false) {
                        continue;
                    }
                    return rows;
                }
            }
            rows += page.getPositionCount();
            page.releaseBlocks();
        }
    }

    private static ReferenceAttribute valueAttribute() {
        return new ReferenceAttribute(Source.EMPTY, "value", DataType.DOUBLE);
    }

    private static Expression valueGreaterThan(double literal) {
        return new GreaterThan(Source.EMPTY, valueAttribute(), new Literal(Source.EMPTY, literal, DataType.DOUBLE), null);
    }

    private static Expression valueEquals(double literal) {
        return new Equals(Source.EMPTY, valueAttribute(), new Literal(Source.EMPTY, literal, DataType.DOUBLE), null);
    }

    /** Wei-scale values like the issue's {@code value > 1e18} on an ETH {@code double} column. */
    private static double codeToValue(int code) {
        return code * 1e18;
    }

    private static int codeFor(int row) {
        int code = row % DICT_CARDINALITY;
        if (code == RARE_CODE && (row < RARE_FROM || row >= RARE_TO)) {
            return RARE_CODE + 1;
        }
        return code;
    }

    private static int expectedRows(IntPredicate codeMatches) {
        int n = 0;
        for (int i = 0; i < ROWS; i++) {
            if (codeMatches.test(codeFor(i))) {
                n++;
            }
        }
        return n;
    }

    private static BlockMetaData block(ColumnChunkMetaData... columns) {
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(100);
        for (ColumnChunkMetaData column : columns) {
            block.addColumn(column);
        }
        return block;
    }

    /** What a V1 writer records for a dictionary column: {@code PLAIN} for the dictionary page itself. */
    private static Set<Encoding> dictEncodings() {
        return Set.of(Encoding.PLAIN, Encoding.PLAIN_DICTIONARY);
    }

    private static ColumnChunkMetaData column(
        String name,
        long firstDataPageOffset,
        long dictionaryPageOffset,
        Set<Encoding> encodings,
        EncodingStats encodingStats
    ) {
        PrimitiveType type = Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
        return ColumnChunkMetaData.get(
            ColumnPath.get(name),
            type,
            CompressionCodecName.UNCOMPRESSED,
            encodingStats,
            encodings,
            Statistics.createStats(type),
            firstDataPageOffset,
            dictionaryPageOffset,
            100,
            2_000,
            2_000
        );
    }

    private static NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks(long offset, long length) {
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> map = new TreeMap<>();
        map.put(offset, new ColumnChunkPrefetcher.PrefetchedChunk(offset, length, ByteBuffer.allocate((int) length)));
        return map;
    }

    /**
     * Asserts {@link #legacyFooter} produced what parquet-mr 1.10 wrote: no usable dictionary page
     * offset (so {@code getStartingPos()} is the first data page offset, which is the dictionary
     * page), dictionary pages nevertheless present and exclusively used, and no page index.
     */
    private static void assertLegacyShape(byte[] file) throws IOException {
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(
                    new CountingStorage(file, null),
                    FooterByteCache.fromSettings(Settings.EMPTY),
                    NoopCircuitBreaker.INSTANCE
                ),
                options
            )
        ) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            assertThat(blocks.size(), greaterThanOrEqualTo(3));
            for (BlockMetaData block : blocks) {
                for (ColumnChunkMetaData col : block.getColumns()) {
                    assertNull(col.getColumnIndexReference());
                    assertNull(col.getOffsetIndexReference());
                    assertTrue(
                        "dictionary offset must be unset or equal to the data page offset, was " + col.getDictionaryPageOffset(),
                        col.getDictionaryPageOffset() == 0L || col.getDictionaryPageOffset() == col.getFirstDataPageOffset()
                    );
                    assertEquals(col.getFirstDataPageOffset(), col.getStartingPos());
                    if (col.getPath().toDotString().equals("value")) {
                        assertTrue(col.hasDictionaryPage());
                        assertNotNull(col.getEncodingStats());
                        assertFalse(col.getEncodingStats().hasNonDictionaryEncodedPages());
                    }
                }
            }
            for (int i = 1; i < blocks.size(); i++) {
                assertThat(
                    "row groups must be more than one window apart",
                    blocks.get(i).getStartingPos() - blocks.get(i - 1).getStartingPos(),
                    greaterThan((long) ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE)
                );
            }
        }
    }

    /**
     * Rewrites the footer the way parquet-mr 1.10 wrote it: {@code data_page_offset} moves back to
     * the dictionary page (1.18 points it past the dictionary), {@code dictionary_page_offset} is
     * either unset or, as some old writers did, left equal to {@code data_page_offset} (both resolve
     * to the same {@code getStartingPos()} and neither is pre-warmed), and the column/offset index
     * references are dropped. Encoding stats, statistics and {@code created_by} are left alone.
     */
    private static byte[] legacyFooter(byte[] parquetBytes) throws IOException {
        boolean unsetDictionaryOffset = randomBoolean();
        int len = parquetBytes.length;
        int footerLen = ByteBuffer.wrap(parquetBytes, len - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int footerStart = len - 8 - footerLen;
        FileMetaData thriftMeta;
        try (var in = new ByteArrayInputStream(parquetBytes, footerStart, footerLen)) {
            thriftMeta = Util.readFileMetaData(in);
        }
        for (RowGroup rowGroup : thriftMeta.getRow_groups()) {
            for (ColumnChunk cc : rowGroup.getColumns()) {
                ColumnMetaData meta = cc.getMeta_data();
                if (meta.isSetDictionary_page_offset()) {
                    meta.setData_page_offset(meta.getDictionary_page_offset());
                    if (unsetDictionaryOffset) {
                        meta.unsetDictionary_page_offset();
                    }
                }
                cc.unsetColumn_index_offset();
                cc.unsetColumn_index_length();
                cc.unsetOffset_index_offset();
                cc.unsetOffset_index_length();
            }
        }
        ByteArrayOutputStream newFooterBuf = new ByteArrayOutputStream();
        Util.writeFileMetaData(thriftMeta, newFooterBuf);
        byte[] newFooter = newFooterBuf.toByteArray();
        ByteArrayOutputStream result = new ByteArrayOutputStream(footerStart + newFooter.length + 8);
        result.write(parquetBytes, 0, footerStart);
        result.write(newFooter);
        result.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(newFooter.length).array());
        result.write(parquetBytes, len - 4, 4);
        return result.toByteArray();
    }

    private static byte[] parquetFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("pad")
            .named("prewarm_gate");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withDictionaryEncoding("pad", false)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withPageSize(8 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .build()
        ) {
            // Uncompressed, so the content does not affect sizes; one constant array keeps the
            // writer's statistics (which hold on to constant binaries) valid.
            byte[] pad = new byte[PAD_PER_ROW];
            random().nextBytes(pad);
            for (int i = 0; i < ROWS; i++) {
                writer.write(
                    groups.newGroup()
                        .append("id", i)
                        .append("value", codeToValue(codeFor(i)))
                        .append("pad", Binary.fromConstantByteArray(pad))
                );
            }
        }
        return out.toByteArray();
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return out.size();
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
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
                return "memory://prewarm-gate.parquet";
            }
        };
    }

    /**
     * In-memory storage with native async reads so the optimized iterator and the metadata preload
     * take the async path, while synchronous ranged {@link #newStream(long, long)} opens -- the
     * ones parquet-mr's {@code RowGroupFilter} dictionary reads end up in -- are counted.
     */
    private static final class CountingStorage extends AbstractTestStorageObject {
        private final byte[] data;
        private final Executor asyncIo;
        private final AtomicLong syncGets = new AtomicLong();

        private CountingStorage(byte[] data, Executor asyncIo) {
            this.data = data;
            this.asyncIo = asyncIo;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            syncGets.incrementAndGet();
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
            return StoragePath.of("memory://prewarm-gate.parquet");
        }

        @Override
        public boolean supportsNativeAsync() {
            return asyncIo != null;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            if (asyncIo == null) {
                listener.onFailure(new IllegalStateException("async reads are not supported by this storage"));
                return;
            }
            DirectReadBuffer drb;
            try {
                drb = factory.allocateWritableWindow((int) length);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            asyncIo.execute(() -> {
                try {
                    ByteBuffer buffer = drb.buffer();
                    buffer.put(data, (int) position, (int) Math.min(length, data.length - position));
                    buffer.flip();
                    listener.onResponse(drb);
                } catch (Exception e) {
                    drb.close();
                    listener.onFailure(e);
                }
            });
        }
    }
}
