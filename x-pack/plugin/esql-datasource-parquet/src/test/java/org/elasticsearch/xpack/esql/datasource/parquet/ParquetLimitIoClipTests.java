/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Unfiltered LIMIT must stop later Parquet row groups, release heap I/O when the budget is
 * exhausted, and (when OffsetIndex exists) fetch only prefix pages of the first covering groups.
 */
public class ParquetLimitIoClipTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    public void testRangeReadContextDefaultsToNoLimit() {
        RangeReadContext ctx = new RangeReadContext(List.of("id"), 10, 0, 100, List.of(), ErrorPolicy.STRICT);
        assertThat(ctx.rowLimit(), equalTo(FormatReader.NO_LIMIT));
    }

    public void testComputeIndexColumnPathsUnfilteredLimitAddsOffsetIndexOnly() {
        MessageType schema = threeColumnSchema();
        ParquetFormatReader.IndexColumnPaths unlimited = ParquetFormatReader.computeIndexColumnPaths(
            false,
            false,
            null,
            null,
            schema,
            FormatReader.NO_LIMIT
        );
        assertTrue(unlimited.columnIndexPaths().isEmpty());
        assertTrue(unlimited.offsetIndexPaths().isEmpty());

        ParquetFormatReader.IndexColumnPaths limited = ParquetFormatReader.computeIndexColumnPaths(false, false, null, null, schema, 1);
        assertTrue("LIMIT must not fetch ColumnIndex", limited.columnIndexPaths().isEmpty());
        assertEquals(Set.of("a", "b", "c"), limited.offsetIndexPaths());
    }

    public void testUnfilteredLimitHelperMatchesIteratorAndReader() {
        assertTrue(ParquetFormatReader.unfilteredLimit(1, false, false, false, false));
        assertFalse(ParquetFormatReader.unfilteredLimit(FormatReader.NO_LIMIT, false, false, false, false));
        assertFalse(ParquetFormatReader.unfilteredLimit(1, true, false, false, false));
        assertFalse(ParquetFormatReader.unfilteredLimit(1, false, true, false, false));
        assertFalse(ParquetFormatReader.unfilteredLimit(1, false, false, true, false));
        assertFalse(ParquetFormatReader.unfilteredLimit(1, false, false, false, true));
    }

    public void testReadRangeHonorsRowLimit() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(2000, 2048);
        RecordingStorageObject storage = new RecordingStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        try (
            CloseableIterator<Page> iter = reader.readRange(
                storage,
                new RangeReadContext(List.of("id"), 64, 0, parquetData.length, List.of(), ErrorPolicy.STRICT, null, 1)
            )
        ) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertThat(page.getPositionCount(), equalTo(1));
            page.releaseBlocks();
            assertFalse(iter.hasNext());
        }
    }

    public void testLimitDoesNotPrefetchLaterRowGroups() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(4000, 2048);
        List<BlockMetaData> blocks = rowGroupsOf(parquetData);
        int k = ParquetFormatReader.coveringRowGroupLimit(blocks, 1);
        assertThat(blocks.size(), greaterThan(k));

        RecordingStorageObject limited = new RecordingStorageObject(parquetData);
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                limited,
                FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(256).rowLimit(1).build()
            )
        ) {
            assertTrue(iter.hasNext());
            iter.next().releaseBlocks();
            assertFalse(iter.hasNext());
        }
        assertNoGetsOverlapRowGroupsFrom(limited, blocks, k, parquetData);
    }

    public void testLimitReleasesPrefetchBuffersBeforeClose() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(3000, 2048);
        RecordingStorageObject storage = new RecordingStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        CloseableIterator<Page> iter = reader.read(
            storage,
            FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(64).rowLimit(1).build()
        );
        try {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            page.releaseBlocks();
            assertFalse(iter.hasNext());
            assertThat("prefetch buffers must drop when LIMIT is exhausted, before close()", storage.liveAsyncBytes.get(), equalTo(0));
        } finally {
            iter.close();
        }
        assertThat(storage.liveAsyncBytes.get(), equalTo(0));
    }

    public void testLimitFirstRowMatchesFullScan() throws Exception {
        byte[] parquetData = createTallFile(80, 32, true);
        long fullFirst;
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                new RecordingStorageObject(parquetData),
                FormatReadContext.of(List.of("id", "payload"), 64)
            )
        ) {
            Page page = iter.next();
            fullFirst = ((LongBlock) page.getBlock(0)).getLong(0);
            page.releaseBlocks();
        }
        long limitFirst;
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                new RecordingStorageObject(parquetData),
                FormatReadContext.builder().projectedColumns(List.of("id", "payload")).batchSize(64).rowLimit(1).build()
            )
        ) {
            Page page = iter.next();
            limitFirst = ((LongBlock) page.getBlock(0)).getLong(0);
            page.releaseBlocks();
        }
        assertThat(limitFirst, equalTo(fullFirst));
    }

    public void testOffsetIndexLimitClipsFirstWindowAndUnlimitedFetchesNoPageIndex() throws Exception {
        // Payload must exceed DEFAULT_WINDOW_SIZE. Files that fit in the window are filled with one
        // GET of [0, length), which overlaps OffsetIndex and would make both unlimited and LIMIT 1
        // look like they fetched page-index bytes.
        byte[] parquetData = createTallFile(800, 8192, true);
        assertThat(parquetData.length, greaterThan(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE));
        IndexLayout layout = indexLayout(parquetData);
        assertThat("parquet-mr default writer must emit OffsetIndex for this clip to apply", layout.offsetIndex.length, greaterThan(0));
        long firstGroupSpan = firstRowGroupSpan(parquetData);
        assertThat(firstGroupSpan, greaterThan(16 * 1024L));

        RecordingStorageObject unlimited = new RecordingStorageObject(parquetData);
        int unlimitedRows = 0;
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                unlimited,
                FormatReadContext.of(List.of("id", "payload"), 256)
            )
        ) {
            while (iter.hasNext()) {
                Page page = iter.next();
                unlimitedRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertThat(unlimitedRows, equalTo(800));
        assertThat("unlimited scan must not fetch page-index bytes", indexGetCount(unlimited, layout), equalTo(0L));

        RecordingStorageObject limited = new RecordingStorageObject(parquetData);
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                limited,
                FormatReadContext.builder().projectedColumns(List.of("id", "payload")).batchSize(32).rowLimit(1).build()
            )
        ) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertThat(page.getPositionCount(), equalTo(1));
            page.releaseBlocks();
            assertFalse(iter.hasNext());
        }
        long indexGets = indexGetCount(limited, layout);
        assertThat("LIMIT 1 should fetch OffsetIndex for the first covering group", indexGets, greaterThan(0L));
        long dataBytes = dataGetBytes(limited, parquetData, layout);
        assertThat(dataBytes, greaterThan(0L));
        long prefixPages = prefixPageBytes(parquetData, 1);
        assertThat(
            "LIMIT 1 data GETs must stay within prefix pages plus one coalesce gap, not the full first group",
            dataBytes,
            lessThanOrEqualTo(prefixPages + CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP)
        );
        assertThat(firstGroupSpan, greaterThan(CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP));
    }

    public void testLimitWithoutOffsetIndexStillCorrect() throws Exception {
        byte[] parquetData = createTallFile(4000, 32, false);
        List<BlockMetaData> blocks = rowGroupsOf(parquetData);
        int k = ParquetFormatReader.coveringRowGroupLimit(blocks, 1);
        assertThat(blocks.size(), greaterThan(k));

        RecordingStorageObject limited = new RecordingStorageObject(parquetData);
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                limited,
                FormatReadContext.builder().projectedColumns(List.of("id", "payload")).batchSize(32).rowLimit(1).build()
            )
        ) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertThat(page.getPositionCount(), equalTo(1));
            page.releaseBlocks();
            assertFalse(iter.hasNext());
        }
        assertNoGetsOverlapRowGroupsFrom(limited, blocks, k, parquetData);
    }

    public void testCoveringRowGroupLimitAndPreloadCapsOffsetIndexToFirstK() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(4000, 2048);
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        RecordingStorageObject storage = new RecordingStorageObject(parquetData);
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(storage, blockFactory.breaker()), options)) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            assertThat(blocks.size(), greaterThan(1));
            assertThat(
                "test file must carry OffsetIndex so the cap is observable",
                blocks.get(0).getColumns().get(0).getOffsetIndexReference() != null
                    && blocks.get(0).getColumns().get(0).getOffsetIndexReference().getLength() > 0,
                equalTo(true)
            );
            assertThat(ParquetFormatReader.coveringRowGroupLimit(blocks, FormatReader.NO_LIMIT), equalTo(blocks.size()));
            assertThat(ParquetFormatReader.coveringRowGroupLimit(blocks, 1), equalTo(1));

            Set<String> allPaths = Set.of("id");
            int k = ParquetFormatReader.coveringRowGroupLimit(blocks, 1);
            storage.gets.clear();
            try (
                PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(
                    reader,
                    storage,
                    Set.of(),
                    Set.of(),
                    allPaths,
                    k,
                    blockFactory.breaker()
                )
            ) {
                assertNotNull(metadata.getOffsetIndex(0, "id"));
                if (blocks.size() > k) {
                    assertNull("OffsetIndex past K must not be preloaded", metadata.getOffsetIndex(k, "id"));
                }
            }
            long laterOiGets = 0;
            for (int rg = k; rg < blocks.size(); rg++) {
                for (ColumnChunkMetaData col : blocks.get(rg).getColumns()) {
                    var oi = col.getOffsetIndexReference();
                    if (oi == null || oi.getLength() <= 0) {
                        continue;
                    }
                    long start = oi.getOffset();
                    long end = start + oi.getLength();
                    for (long[] get : storage.gets) {
                        if (get[0] < end && get[0] + get[1] > start) {
                            laterOiGets++;
                        }
                    }
                }
            }
            assertThat("preload must not GET OffsetIndex for rgIdx >= K", laterOiGets, equalTo(0L));
        }
    }

    public void testLimitSpanningTwoRowGroupsDoesNotFetchLaterGroups() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(4000, 2048);
        List<BlockMetaData> blocks = rowGroupsOf(parquetData);
        assertThat(blocks.size(), greaterThan(2));
        int limit = (int) (blocks.get(0).getRowCount() + blocks.get(1).getRowCount() / 2);
        int k = ParquetFormatReader.coveringRowGroupLimit(blocks, limit);
        assertThat(k, greaterThan(1));
        assertThat(blocks.size(), greaterThan(k));

        RecordingStorageObject limited = new RecordingStorageObject(parquetData);
        int rows = 0;
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                limited,
                FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(64).rowLimit(limit).build()
            )
        ) {
            while (iter.hasNext()) {
                Page page = iter.next();
                rows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertThat(rows, equalTo(limit));
        assertNoGetsOverlapRowGroupsFrom(limited, blocks, k, parquetData);
    }

    public void testFilteredLimitStillPrefetchesLaterRowGroups() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(4000, 2048);
        List<BlockMetaData> blocks = rowGroupsOf(parquetData);
        int k = ParquetFormatReader.coveringRowGroupLimit(blocks, 1);
        assertThat(blocks.size(), greaterThan(k));

        RecordingStorageObject filtered = new RecordingStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory).withPushedFilter(
            FilterCompat.get(FilterApi.gtEq(FilterApi.longColumn("id"), 0L))
        );
        try (
            CloseableIterator<Page> iter = reader.read(
                filtered,
                FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(64).rowLimit(1).build()
            )
        ) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertThat(page.getPositionCount(), equalTo(1));
            page.releaseBlocks();
            assertFalse(iter.hasNext());
        }
        assertTrue(
            "filtered LIMIT must not treat source row counts as survivors; later groups stay eligible",
            getsOverlapRowGroupsFrom(filtered, blocks, k, parquetData)
        );
    }

    public void testWhereLimitStopsPredicateDecodeInFatFirstGroup() throws Exception {
        byte[] parquetData = createTallFile(2000, 4096, true);
        List<BlockMetaData> blocks = rowGroupsOf(parquetData);
        assertThat(blocks.get(0).getRowCount(), greaterThan(100L));

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        // Not stats-trivial: group max is 1999, so TriviallyPassesChecker cannot skip Phase-1.
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 1990L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        RecordingStorageObject storage = new RecordingStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        int rows = 0;
        CloseableIterator<Page> iter = reader.read(
            storage,
            FormatReadContext.builder().projectedColumns(List.of("id", "payload")).batchSize(16).rowLimit(5).build()
        );
        try {
            while (iter.hasNext()) {
                Page page = iter.next();
                rows += page.getPositionCount();
                page.releaseBlocks();
            }
            assertThat(rows, equalTo(5));
            assertFalse(iter.hasNext());
            assertThat(storage.liveAsyncBytes.get(), equalTo(0));
        } finally {
            iter.close();
        }
        IndexLayout layout = indexLayout(parquetData);
        long dataBytes = dataGetBytes(storage, parquetData, layout);
        long firstGroupSpan = firstRowGroupSpan(parquetData);
        assertThat(firstGroupSpan, greaterThan(CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP));
        assertThat(
            "Phase-1 LIMIT stop must not fetch the unread tail of the fat first group",
            dataBytes,
            lessThanOrEqualTo(prefixPageBytes(parquetData, 5) + CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP)
        );
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

    private static long dataGetBytes(RecordingStorageObject storage, byte[] data, IndexLayout layout) {
        long bytes = 0;
        for (long[] get : storage.gets) {
            if (isFooterGet(get[0], get[1], data) || overlapsAny(get[0], get[1], layout.all())) {
                continue;
            }
            bytes += get[1];
        }
        return bytes;
    }

    private static long indexGetCount(RecordingStorageObject storage, IndexLayout layout) {
        long count = 0;
        for (long[] get : storage.gets) {
            if (overlapsAny(get[0], get[1], layout.all())) {
                count++;
            }
        }
        return count;
    }

    private List<BlockMetaData> rowGroupsOf(byte[] parquetData) throws IOException {
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(new RecordingStorageObject(parquetData), blockFactory.breaker()),
                options
            )
        ) {
            return List.copyOf(reader.getRowGroups());
        }
    }

    private static boolean getsOverlapRowGroupsFrom(RecordingStorageObject storage, List<BlockMetaData> blocks, int fromRg, byte[] data) {
        for (long[] get : storage.gets) {
            if (isFooterGet(get[0], get[1], data)) {
                continue;
            }
            long getEnd = get[0] + get[1];
            for (int rg = fromRg; rg < blocks.size(); rg++) {
                for (ColumnChunkMetaData col : blocks.get(rg).getColumns()) {
                    long start = col.getStartingPos();
                    long end = start + col.getTotalSize();
                    if (get[0] < end && getEnd > start) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static void assertNoGetsOverlapRowGroupsFrom(
        RecordingStorageObject storage,
        List<BlockMetaData> blocks,
        int fromRg,
        byte[] data
    ) {
        assertFalse(
            "LIMIT must not GET later row-group chunks from [" + fromRg + "]",
            getsOverlapRowGroupsFrom(storage, blocks, fromRg, data)
        );
    }

    private static boolean isFooterGet(long offset, long length, byte[] data) {
        long start = footerStart(data);
        return offset < data.length && offset + length > start;
    }

    private static long footerStart(byte[] data) {
        int footerLength = ByteBuffer.wrap(data, data.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return data.length - 8L - footerLength;
    }

    private long prefixPageBytes(byte[] parquetData, int rowLimit) throws IOException {
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(new RecordingStorageObject(parquetData), blockFactory.breaker()),
                options
            )
        ) {
            long bytes = 0;
            long remaining = rowLimit;
            for (BlockMetaData block : reader.getRowGroups()) {
                if (remaining <= 0) {
                    break;
                }
                long rgRows = block.getRowCount();
                long end = Math.min(remaining, rgRows);
                for (ColumnChunkMetaData col : block.getColumns()) {
                    OffsetIndex oi = reader.readOffsetIndex(col);
                    if (oi == null) {
                        bytes += col.getTotalSize();
                        continue;
                    }
                    long firstData = oi.getOffset(0);
                    if (col.getDictionaryPageOffset() > 0 && col.getDictionaryPageOffset() < firstData) {
                        bytes += firstData - col.getDictionaryPageOffset();
                    }
                    int pageCount = oi.getPageCount();
                    for (int p = 0; p < pageCount; p++) {
                        long pageStart = oi.getFirstRowIndex(p);
                        long pageEnd = (p + 1 < pageCount) ? oi.getFirstRowIndex(p + 1) : rgRows;
                        if (pageStart < end && pageEnd > 0) {
                            bytes += oi.getCompressedPageSize(p);
                        }
                    }
                }
                remaining -= rgRows;
            }
            return bytes;
        }
    }

    private static boolean overlapsAny(long offset, long length, long[][] ranges) {
        long end = offset + length;
        for (long[] r : ranges) {
            if (offset < r[1] && end > r[0]) {
                return true;
            }
        }
        return false;
    }

    private IndexLayout indexLayout(byte[] parquetData) throws IOException {
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(new RecordingStorageObject(parquetData), blockFactory.breaker()),
                options
            )
        ) {
            List<long[]> ci = new ArrayList<>();
            List<long[]> oi = new ArrayList<>();
            for (BlockMetaData block : reader.getRowGroups()) {
                for (ColumnChunkMetaData col : block.getColumns()) {
                    var ciRef = col.getColumnIndexReference();
                    if (ciRef != null && ciRef.getLength() > 0) {
                        ci.add(new long[] { ciRef.getOffset(), ciRef.getOffset() + ciRef.getLength() });
                    }
                    var oiRef = col.getOffsetIndexReference();
                    if (oiRef != null && oiRef.getLength() > 0) {
                        oi.add(new long[] { oiRef.getOffset(), oiRef.getOffset() + oiRef.getLength() });
                    }
                }
            }
            return new IndexLayout(ci.toArray(new long[0][]), oi.toArray(new long[0][]));
        }
    }

    private long firstRowGroupSpan(byte[] parquetData) throws IOException {
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(new RecordingStorageObject(parquetData), blockFactory.breaker()),
                options
            )
        ) {
            BlockMetaData first = reader.getRowGroups().get(0);
            long span = 0;
            for (ColumnChunkMetaData col : first.getColumns()) {
                span += col.getTotalSize();
            }
            return span;
        }
    }

    private byte[] createMultiRowGroupFile(int rowCount, int rowGroupSize) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(outputStream))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(256)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private byte[] createTallFile(int rowCount, int payloadBytes, boolean pageIndex) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("test_schema");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        String payload = "x".repeat(payloadBytes);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(createOutputFile(outputStream))
            .withConf(new PlainParquetConfiguration())
            .withCodecFactory(new PlainCompressionCodecFactory())
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withDictionaryEncoding(false)
            .withRowGroupSize(pageIndex ? 8 * 1024 * 1024L : 2048)
            .withPageSize(1024);
        if (pageIndex == false) {
            // parquet-mr 1.17 rejects truncate-length 0. PARQUET_1_0 still writes OffsetIndex
            // refs on this writer; later-group stop does not depend on prefix clip.
            builder = builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0);
        }
        try (ParquetWriter<Group> writer = builder.build()) {
            for (int i = 0; i < rowCount; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                g.add("payload", i + payload);
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return outputStream.size();
                    }

                    @Override
                    public void write(int b) {
                        outputStream.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        outputStream.write(b, off, len);
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
        };
    }

    private record IndexLayout(long[][] columnIndex, long[][] offsetIndex) {
        long[][] all() {
            long[][] both = new long[columnIndex.length + offsetIndex.length][];
            System.arraycopy(columnIndex, 0, both, 0, columnIndex.length);
            System.arraycopy(offsetIndex, 0, both, columnIndex.length, offsetIndex.length);
            return both;
        }
    }

    private static final class RecordingStorageObject implements StorageObject {
        private final byte[] data;
        final List<long[]> gets = new CopyOnWriteArrayList<>();
        final AtomicInteger liveAsyncBytes = new AtomicInteger();

        RecordingStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            gets.add(new long[] { position, length });
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
            return StoragePath.of("memory://limit-io-clip.parquet");
        }

        @Override
        public boolean supportsNativeAsync() {
            return true;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            gets.add(new long[] { position, length });
            executor.execute(() -> {
                try {
                    int pos = (int) position;
                    int len = (int) Math.min(length, data.length - position);
                    DirectReadBuffer allocated = factory.allocate(len);
                    ByteBuffer buffer = allocated.buffer();
                    buffer.put(data, pos, len);
                    buffer.flip();
                    liveAsyncBytes.addAndGet(len);
                    listener.onResponse(new DirectReadBuffer(buffer, () -> {
                        liveAsyncBytes.addAndGet(-len);
                        allocated.close();
                    }));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }
}
