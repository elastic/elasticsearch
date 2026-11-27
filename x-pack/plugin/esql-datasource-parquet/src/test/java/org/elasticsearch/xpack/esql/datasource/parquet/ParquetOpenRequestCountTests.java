/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
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
import org.elasticsearch.compute.data.IntBlock;
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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntPredicate;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * GET-count pins for skipping dictionary/bloom preload when those levels cannot drop a row
 * group after STATISTICS. Footer is primed first so the 64 KiB tail is cached; remaining
 * {@code readBytesAsync} calls are the contract (coalesced byte identity is not).
 * <p>
 * Repeat the random identity method with {@code -Dtests.iters=N} on this class.
 */
public class ParquetOpenRequestCountTests extends ESTestCase {

    private static final int FOOTER_TAIL = ParquetFormatReader.FOOTER_TAIL_PREFETCH_BYTES;
    private static final int PAD_PER_ROW = 512;
    private static final int ROWS_1RG = 250;
    private static final int ROWS_PER_RG = 64;
    private static final int NUM_RG = 3;
    private static final long ONE_RG_BYTES = 64L * 1024 * 1024;
    private static final long SMALL_RG_BYTES = 16L * 1024;
    private static final int MAX_EMPTY_READS_IN_A_ROW = 64;
    private static final List<RowGroupFilter.FilterLevel> ALL_LEVELS = List.of(
        RowGroupFilter.FilterLevel.STATISTICS,
        RowGroupFilter.FilterLevel.DICTIONARY,
        RowGroupFilter.FilterLevel.BLOOMFILTER
    );

    private ExecutorService asyncIo;
    private LimitedBreaker breaker;
    private BlockFactory blockFactory;

    @Before
    public void startAsyncIo() {
        asyncIo = Executors.newFixedThreadPool(4, EsExecutors.daemonThreadFactory("test", "open-request-count"));
        breaker = new LimitedBreaker("open-request-count", ByteSizeValue.ofGb(2));
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
    }

    @After
    public void stopAsyncIo() throws Exception {
        terminate(asyncIo);
    }

    /**
     * VPC shape: 1 RG, dict-encoded {@code action}, range on {@code start} plus
     * {@code action != "-"}. NotEq with existing min/max is not dictionary-useful, so preload
     * issues no GET and the data path issues one coalesced GET.
     */
    public void testVpcShapeSkipsDictPreload() throws Exception {
        byte[] file = vpcFile();
        assertThat("dict at offset 4 must sit outside the cached footer tail", (long) file.length, greaterThan((long) FOOTER_TAIL));
        List<Expression> pushed = List.of(gte(startAttr(), 0), lt(startAttr(), 10_000), neqAction("-"));
        OpenResult result = open(file, new ParquetPushedExpressions(pushed), List.of("start", "action"), vpcSchema(), true);
        List<Integer> expected = expectedStarts(i -> actionFor(i).equals("-") == false);
        assertEquals(expected, result.ids());
        assertEquals(
            "NotEq with existing min/max is not dictionary-useful, so the DICTIONARY level is dropped",
            result.status().rowGroupsTotal(),
            result.status().rowGroupsDictionaryFilterSkipped()
        );
        assertEquals(0L, result.syncGets());
        assertThat(result.getsAfterDrain(), greaterThanOrEqualTo(1));
    }

    public void testAllMissFileIssuesNoPreloadGet() throws Exception {
        byte[] file = vpcFile();
        // Eq on dict-encoded action would be dictionary-useful if STATISTICS admitted the group.
        OpenResult result = open(
            file,
            new ParquetPushedExpressions(List.of(gt(startAttr(), 100_000), eqAction("index"))),
            List.of("start", "action"),
            vpcSchema(),
            true
        );
        assertEquals(List.of(), result.ids());
        assertEquals(0L, result.status().rowGroupsKept());
        assertEquals(result.status().rowGroupsTotal(), result.status().rowGroupsDictionaryFilterSkipped());
        assertEquals(0L, result.syncGets());
    }

    public void testSomeMissGroupsSkipDictBloomRanges() throws Exception {
        byte[] file = multiRgFile();
        assertThat(rowGroupCount(file), greaterThanOrEqualTo(2));
        int firstGroupRows = firstRowGroupRows(file);
        OpenResult result = open(
            file,
            new ParquetPushedExpressions(List.of(gt(startAttr(), firstGroupRows - 1), eqAction("index"))),
            List.of("start", "action"),
            vpcSchema(),
            true
        );
        int rows = NUM_RG * ROWS_PER_RG;
        List<Integer> expected = expectedStarts(rows, i -> i > firstGroupRows - 1 && actionFor(i).equals("index"));
        assertEquals(expected, result.ids());
        assertThat(result.status().rowGroupsTotal(), greaterThan(result.status().rowGroupsKept()));
        assertThat(result.status().rowGroupsKept(), greaterThan(0L));
        assertEquals(
            "MISS groups must not pre-warm dictionary pages",
            result.status().rowGroupsTotal() - result.status().rowGroupsKept(),
            result.status().rowGroupsDictionaryFilterSkipped()
        );
        assertEquals(0L, result.syncGets());
    }

    /**
     * Eq on a dictionary column whose min/max are AMBIGUOUS and whose dictionary does not
     * contain the value: preload still runs, the group is dropped, survivors are not wider.
     */
    public void testDictEqAbsentValueStillPreloadsAndDropsGroup() throws Exception {
        byte[] file = valueFile(true, false, new int[] { 0, 2, 3 });
        OpenResult result = open(file, new ParquetPushedExpressions(List.of(eqValue(1))), List.of("value"), List.of(valueAttr()), true);
        assertEquals(List.of(), result.ids());
        assertEquals(0L, result.status().rowGroupsKept());
        assertThat(result.getsAfterDrain(), greaterThanOrEqualTo(1));
        assertEquals(0L, result.status().rowGroupsDictionaryFilterSkipped());
        assertEquals(0, parquetMrKept(file, FilterApi.eq(FilterApi.intColumn("value"), 1)));
    }

    public void testBloomEqAbsentValuePreloadsAndDropsWithoutSyncBloomRead() throws Exception {
        // parquet-mr skips bloom on dictionary-encoded pages, so this file is PLAIN + bloom.
        byte[] file = valueFile(false, true, new int[] { 0, 2, 3 });
        OpenResult result = open(file, new ParquetPushedExpressions(List.of(eqValue(1))), List.of("value"), List.of(valueAttr()), true);
        assertEquals(List.of(), result.ids());
        assertEquals(0L, result.status().rowGroupsKept());
        assertThat(result.getsAfterDrain(), greaterThanOrEqualTo(1));
        assertEquals(0L, result.syncGets());
    }

    public void testPlainFallbackIssuesNoDictPreload() throws Exception {
        byte[] file = valueFile(false, false, new int[] { 0, 2, 3 });
        OpenResult result = open(file, new ParquetPushedExpressions(List.of(eqValue(1))), List.of("value"), List.of(valueAttr()), true);
        assertEquals(List.of(), result.ids());
        assertEquals(1L, result.status().rowGroupsKept());
        assertEquals(0L, result.syncGets());
        assertFalse(ParquetFormatReader.dictionaryFilterReadsDictionary(firstValueColumn(file)));
    }

    public void testUnknownLengthBloomDropsLevelWithZeroSyncBloomRead() throws Exception {
        byte[] file = unsetBloomFilterLength(valueFile(false, true, new int[] { 0, 2, 3 }));
        OpenResult result = open(file, new ParquetPushedExpressions(List.of(eqValue(1))), List.of("value"), List.of(valueAttr()), true);
        assertEquals(List.of(), result.ids());
        assertEquals(1L, result.status().rowGroupsKept());
        assertEquals(0L, result.syncGets());
    }

    /**
     * Legacy {@code FilterCompat.get} path does not run the visitor; rows stay correct even
     * if parquet-mr still issues a synchronous dictionary read.
     */
    public void testLegacyFilterCompatPathReturnsCorrectRows() throws Exception {
        byte[] file = valueFile(true, false, new int[] { 0, 2, 3 });
        OpenResult result = open(
            file,
            FilterCompat.get(FilterApi.eq(FilterApi.intColumn("value"), 1)),
            List.of("value"),
            List.of(valueAttr()),
            false
        );
        assertEquals(List.of(), result.ids());
        assertEquals(0L, result.status().rowGroupsKept());
    }

    /**
     * Random files + predicates: row identity vs the unpushed reader, and survivors never
     * shrink versus parquet-mr running every filter level. {@code -Dtests.iters} on this class.
     */
    public void testRandomFilesRowIdentityAndSurvivorsNeverShrink() throws Exception {
        int rows = randomIntBetween(24, 96);
        int groups = randomIntBetween(1, 3);
        boolean dict = randomBoolean();
        int[] values = new int[] { 0, 1, 2, 3, 4 };
        long rgBytes = Math.max(256L, (long) Math.ceil((double) rows / groups) * 64);
        byte[] file = valueFile(rows, rgBytes, dict, false, values);
        int needle = randomIntBetween(0, 6);
        int kind = randomIntBetween(0, 2);
        Expression pushed = switch (kind) {
            case 0 -> eqValue(needle);
            case 1 -> gt(valueAttr(), needle);
            default -> new NotEquals(Source.EMPTY, valueAttr(), litInt(needle), null);
        };
        IntPredicate javaPred = switch (kind) {
            case 0 -> v -> v == needle;
            case 1 -> v -> v > needle;
            default -> v -> v != needle;
        };
        OpenResult pushedRead = open(file, new ParquetPushedExpressions(List.of(pushed)), List.of("value"), List.of(valueAttr()), true);
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            int v = valueFor(i, values);
            if (javaPred.test(v)) {
                expected.add(v);
            }
        }
        assertEquals(expected, pushedRead.ids());
        FilterPredicate parquetPred = new ParquetPushedExpressions(List.of(pushed)).toFilterPredicate(valueFileSchema());
        assertNotNull(parquetPred);
        int parquetMrKept = parquetMrKept(file, parquetPred);
        assertThat(
            "skipping a filter level must only widen survivors",
            pushedRead.status().rowGroupsKept(),
            greaterThanOrEqualTo((long) parquetMrKept)
        );
    }

    private record OpenResult(int getsAfterDrain, List<Integer> ids, ParquetReaderStatus status, long syncGets) {}

    private OpenResult open(byte[] file, Object pushed, List<String> projection, List<Attribute> schema, boolean tripwire)
        throws IOException {
        CountingStorage storage = new CountingStorage(file, asyncIo, tripwire);
        ParquetFormatReader base = new ParquetFormatReader(blockFactory, true).withIoWatermark(new ParquetIoWatermark(1024L * 1024 * 1024));
        FormatReadContext context = FormatReadContext.builder().projectedColumns(projection).readSchema(schema).batchSize(64).build();
        try (CloseableIterator<Page> prime = base.read(storage, context)) {
            drain(prime, null);
        }
        storage.resetGets();
        storage.armTripwire();
        ParquetReaderCounters counters = new ParquetReaderCounters();
        FormatReadContext filteredContext = FormatReadContext.builder()
            .projectedColumns(projection)
            .readSchema(schema)
            .batchSize(64)
            .readCounters(counters)
            .build();
        ParquetFormatReader filtered = base.withPushedFilter(pushed);
        List<Integer> ids = new ArrayList<>();
        OpenResult result;
        try (CloseableIterator<Page> iter = filtered.read(storage, filteredContext)) {
            drain(iter, ids);
            result = new OpenResult(storage.asyncGets.get(), ids, counters.snapshot(), storage.syncGets.get());
        }
        assertEquals("breaker refunds everything after close", 0L, breaker.getUsed());
        return result;
    }

    private static void drain(CloseableIterator<Page> iter, List<Integer> ids) {
        int emptyReads = 0;
        while (true) {
            SubscribableListener<Void> ready = iter.waitForReady();
            if (ready.isDone() == false) {
                safeAwait(ready, TimeValue.timeValueSeconds(30));
                emptyReads = 0;
                continue;
            }
            Page page = iter.tryAdvance();
            if (page == null) {
                if (iter.waitForReady().isDone() == false) {
                    emptyReads = 0;
                    continue;
                }
                if (iter.isExhausted()) {
                    return;
                }
                if (++emptyReads >= MAX_EMPTY_READS_IN_A_ROW) {
                    throw new AssertionError("iterator stayed ready, empty and not exhausted for " + emptyReads + " reads in a row");
                }
                continue;
            }
            emptyReads = 0;
            if (ids != null) {
                IntBlock block = page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    if (block.isNull(i) == false) {
                        ids.add(block.getInt(i));
                    }
                }
            }
            page.releaseBlocks();
        }
    }

    private int rowGroupCount(byte[] file) throws IOException {
        try (ParquetFileReader reader = openMeta(file)) {
            return reader.getRowGroups().size();
        }
    }

    private int firstRowGroupRows(byte[] file) throws IOException {
        try (ParquetFileReader reader = openMeta(file)) {
            return (int) reader.getRowGroups().get(0).getRowCount();
        }
    }

    private ColumnChunkMetaData firstValueColumn(byte[] file) throws IOException {
        try (ParquetFileReader reader = openMeta(file)) {
            for (ColumnChunkMetaData col : reader.getRowGroups().get(0).getColumns()) {
                if (col.getPath().toDotString().equals("value")) {
                    return col;
                }
            }
        }
        throw new AssertionError("no value column");
    }

    private ParquetFileReader openMeta(byte[] file) throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(
                new ByteArrayStorage(file),
                FooterByteCache.fromSettings(Settings.EMPTY),
                NoopCircuitBreaker.INSTANCE
            ),
            PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
        );
    }

    private int parquetMrKept(byte[] file, FilterPredicate predicate) throws IOException {
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(
                    new CountingStorage(file, null, false),
                    FooterByteCache.fromSettings(Settings.EMPTY),
                    NoopCircuitBreaker.INSTANCE
                ),
                options
            )
        ) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            return RowGroupFilter.filterRowGroups(ALL_LEVELS, FilterCompat.get(predicate), blocks, reader).size();
        }
    }

    private static List<Integer> expectedStarts(IntPredicate keep) {
        return expectedStarts(ROWS_1RG, keep);
    }

    private static List<Integer> expectedStarts(int rows, IntPredicate keep) {
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            if (keep.test(i)) {
                expected.add(i);
            }
        }
        return expected;
    }

    private static ReferenceAttribute startAttr() {
        return new ReferenceAttribute(Source.EMPTY, "start", DataType.INTEGER);
    }

    private static ReferenceAttribute actionAttr() {
        return new ReferenceAttribute(Source.EMPTY, "action", DataType.KEYWORD);
    }

    private static ReferenceAttribute valueAttr() {
        return new ReferenceAttribute(Source.EMPTY, "value", DataType.INTEGER);
    }

    private static List<Attribute> vpcSchema() {
        return List.of(startAttr(), actionAttr());
    }

    private static MessageType valueFileSchema() {
        return Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("pad")
            .named("value_file");
    }

    private static Literal litInt(int v) {
        return new Literal(Source.EMPTY, v, DataType.INTEGER);
    }

    private static Expression eqValue(int v) {
        return new Equals(Source.EMPTY, valueAttr(), litInt(v), null);
    }

    private static Expression gt(ReferenceAttribute attr, int v) {
        return new GreaterThan(Source.EMPTY, attr, litInt(v), null);
    }

    private static Expression gte(ReferenceAttribute attr, int v) {
        return new GreaterThanOrEqual(Source.EMPTY, attr, litInt(v), null);
    }

    private static Expression lt(ReferenceAttribute attr, int v) {
        return new LessThan(Source.EMPTY, attr, litInt(v), null);
    }

    private static Expression neqAction(String value) {
        return new NotEquals(Source.EMPTY, actionAttr(), new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD), null);
    }

    private static Expression eqAction(String value) {
        return new Equals(Source.EMPTY, actionAttr(), new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD), null);
    }

    private static String actionFor(int row) {
        return row % 5 == 0 ? "-" : "index";
    }

    private static Binary uniquePad(byte[] template, int row) {
        byte[] copy = template.clone();
        copy[0] = (byte) row;
        copy[1] = (byte) (row >> 8);
        copy[2] = (byte) (row >> 16);
        copy[3] = (byte) (row >> 24);
        return Binary.fromConstantByteArray(copy);
    }

    private static int valueFor(int row, int[] values) {
        return values[row % values.length];
    }

    private static byte[] vpcFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("start")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("action")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("pad")
            .named("vpc");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        byte[] pad = new byte[PAD_PER_ROW];
        random().nextBytes(pad);
        try (ParquetWriter<Group> writer = writer(out, schema, ONE_RG_BYTES, true, false, "action")) {
            for (int i = 0; i < ROWS_1RG; i++) {
                writer.write(groups.newGroup().append("start", i).append("action", actionFor(i)).append("pad", uniquePad(pad, i)));
            }
        }
        return out.toByteArray();
    }

    private static byte[] multiRgFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("start")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("action")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("pad")
            .named("multi");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        byte[] pad = new byte[PAD_PER_ROW];
        random().nextBytes(pad);
        int rows = NUM_RG * ROWS_PER_RG;
        try (ParquetWriter<Group> writer = writer(out, schema, SMALL_RG_BYTES, true, false, "action")) {
            for (int i = 0; i < rows; i++) {
                writer.write(groups.newGroup().append("start", i).append("action", actionFor(i)).append("pad", uniquePad(pad, i)));
            }
        }
        return out.toByteArray();
    }

    private static byte[] valueFile(boolean dictionary, boolean bloom, int[] values) throws IOException {
        return valueFile(ROWS_1RG, ONE_RG_BYTES, dictionary, bloom, values);
    }

    private static byte[] valueFile(int rows, long rowGroupBytes, boolean dictionary, boolean bloom, int[] values) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("pad")
            .named("value_file");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        int padSize = rows >= ROWS_1RG ? PAD_PER_ROW : 8;
        byte[] pad = new byte[padSize];
        random().nextBytes(pad);
        try (ParquetWriter<Group> writer = writer(out, schema, rowGroupBytes, dictionary, bloom, "value")) {
            for (int i = 0; i < rows; i++) {
                writer.write(groups.newGroup().append("value", valueFor(i, values)).append("pad", uniquePad(pad, i)));
            }
        }
        return out.toByteArray();
    }

    private static ParquetWriter<Group> writer(
        ByteArrayOutputStream out,
        MessageType schema,
        long rowGroupBytes,
        boolean dictionary,
        boolean bloom,
        String dictOrBloomCol
    ) throws IOException {
        var builder = ExampleParquetWriter.builder(outputFile(out))
            .withConf(new PlainParquetConfiguration())
            .withCodecFactory(new PlainCompressionCodecFactory())
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
            .withRowGroupSize(rowGroupBytes)
            .withPageSize(8 * 1024)
            .withDictionaryPageSize(64 * 1024)
            .withDictionaryEncoding(dictionary);
        builder = builder.withDictionaryEncoding("pad", false);
        if (dictionary && dictOrBloomCol != null) {
            builder = builder.withDictionaryEncoding(dictOrBloomCol, true);
        }
        if (bloom && dictOrBloomCol != null) {
            builder = builder.withBloomFilterEnabled(dictOrBloomCol, true).withBloomFilterNDV(dictOrBloomCol, 16L);
        }
        return builder.build();
    }

    private static byte[] unsetBloomFilterLength(byte[] parquetBytes) throws IOException {
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
                if (meta.isSetBloom_filter_length()) {
                    meta.unsetBloom_filter_length();
                }
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
                return "memory://open-request-count.parquet";
            }
        };
    }

    /**
     * In-memory storage with native async reads. After {@link #armTripwire()}, a synchronous
     * ranged {@link #newStream(long, long)} that overlaps a dictionary page or bloom filter
     * fails — that is the RowGroupFilter sync-read tripwire.
     */
    private static final class CountingStorage extends AbstractTestStorageObject {
        private final byte[] data;
        private final Executor asyncIo;
        private final StoragePath path;
        private final AtomicInteger asyncGets = new AtomicInteger();
        private final AtomicLong syncGets = new AtomicLong();
        private final List<long[]> dictBloomRanges;
        private volatile boolean tripwire;

        private CountingStorage(byte[] data, Executor asyncIo, boolean enableTripwireRanges) {
            this.data = data;
            this.asyncIo = asyncIo;
            this.path = StoragePath.of("memory://open-request-count-" + System.identityHashCode(data) + "-" + data.length + ".parquet");
            this.dictBloomRanges = enableTripwireRanges ? dictBloomRanges(data) : List.of();
        }

        @Override
        public String version() {
            return "v1";
        }

        private void resetGets() {
            asyncGets.set(0);
            syncGets.set(0);
        }

        private void armTripwire() {
            if (dictBloomRanges.isEmpty() == false) {
                tripwire = true;
            }
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            syncGets.incrementAndGet();
            if (tripwire && isFooterOrWholeFile(position, length, data.length) == false) {
                assertNoOverlap("newStream", position, length, dictBloomRanges);
            }
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
            return path;
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
            asyncGets.incrementAndGet();
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

    private static boolean isFooterOrWholeFile(long position, long length, long fileLength) {
        if (position == 0 && length >= fileLength) {
            return true;
        }
        return position >= Math.max(0L, fileLength - FOOTER_TAIL);
    }

    private static void assertNoOverlap(String op, long position, long length, List<long[]> ranges) {
        for (long[] range : ranges) {
            if (position < range[0] + range[1] && range[0] < position + length) {
                throw new AssertionError(
                    op
                        + " ["
                        + position
                        + ","
                        + length
                        + ") overlapped dict/bloom ["
                        + range[0]
                        + ","
                        + range[1]
                        + ") during RowGroupFilter"
                );
            }
        }
    }

    static List<long[]> dictBloomRanges(byte[] file) {
        List<long[]> ranges = new ArrayList<>();
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(
                    new ByteArrayStorage(file),
                    FooterByteCache.fromSettings(Settings.EMPTY),
                    NoopCircuitBreaker.INSTANCE
                ),
                options
            )
        ) {
            for (BlockMetaData block : reader.getRowGroups()) {
                for (ColumnChunkMetaData col : block.getColumns()) {
                    CoalescedRangeReader.ByteRange dict = ColumnChunkPrefetcher.dictionaryPageRange(col, col.getFirstDataPageOffset());
                    if (dict != null) {
                        ranges.add(new long[] { dict.offset(), dict.length() });
                    } else if (col.hasDictionaryPage()) {
                        // Legacy writers omit dictionary_page_offset; parquet-mr still reads from
                        // getStartingPos() with a window-sized GET.
                        long start = col.getStartingPos();
                        long length = Math.min(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE, Math.max(1L, col.getTotalSize()));
                        ranges.add(new long[] { start, length });
                    }
                    long bloomOffset = col.getBloomFilterOffset();
                    if (bloomOffset > 0) {
                        int bloomLength = col.getBloomFilterLength();
                        long length = bloomLength > 0 ? bloomLength : ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE;
                        ranges.add(new long[] { bloomOffset, length });
                    }
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return ranges;
    }

    private static final class ByteArrayStorage extends AbstractTestStorageObject {
        private final byte[] data;

        private ByteArrayStorage(byte[] data) {
            this.data = data;
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
            return StoragePath.of("memory://open-request-count-meta.parquet");
        }

        @Override
        public String version() {
            return "v1";
        }
    }
}
