/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies that {@link PreloadedRowGroupMetadata#preload(ParquetFileReader, StorageObject, Set, BufferAllocator)}
 * pre-fetches dictionary page bytes for predicate columns into a coalesced batch fetch and that
 * the resulting {@code preWarmedChunks} map can be installed on the adapter so subsequent reads
 * are served from memory.
 *
 * <p>The dominant goal is regression coverage for the optimization that eliminates per-row-group
 * synchronous range GETs during {@code RowGroupFilter} on remote storage.
 */
public class PreloadedRowGroupMetadataTests extends ESTestCase {

    private BlockFactory blockFactory;
    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        allocator = blockFactory.arrowAllocator();
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
    }

    /**
     * When the file has a dictionary-encoded predicate column, pre-warm chunks must be populated
     * with one entry per row group covering the dictionary page byte range.
     */
    public void testDictionaryPagesPreFetchedForPredicateColumn() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("v").named("schema");

        // Use a small dictionary alphabet (16 values) so the writer keeps dictionary encoding
        // for every row group. Row count is sized to comfortably exceed the writer's small row
        // group budget.
        int rows = 65_536;
        long[] values = new long[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = i % 16;
        }
        byte[] parquetData = writeDictionaryEncodedInt64Parquet(schema, values);
        StorageObject storage = createRangeReadStorageObject(parquetData);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(storage, allocator), options)) {
            int rgCount = reader.getRowGroups().size();
            assertTrue("Test setup must produce at least one row group", rgCount >= 1);

            PreloadedRowGroupMetadata withPrewarm = PreloadedRowGroupMetadata.preload(reader, storage, Set.of("v"), allocator);
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = withPrewarm.preWarmedChunks();

            // Count how many of the row groups actually have a dictionary page; only those
            // contribute a pre-warm entry. Unique values may cause the writer to drop dictionary
            // encoding for some row groups when the dictionary grows too large.
            int rowGroupsWithDictionary = 0;
            for (int rg = 0; rg < rgCount; rg++) {
                if (reader.getRowGroups().get(rg).getColumns().get(0).hasDictionaryPage()) {
                    rowGroupsWithDictionary++;
                }
            }
            assertTrue("Expected at least one dictionary-encoded row group", rowGroupsWithDictionary > 0);
            assertEquals(
                "Pre-warm chunk count must equal the number of row groups with a dictionary",
                rowGroupsWithDictionary,
                chunks.size()
            );

            // Each chunk must cover the dictionary page byte range of its row group.
            for (int rg = 0; rg < rgCount; rg++) {
                var col = reader.getRowGroups().get(rg).getColumns().get(0);
                if (col.hasDictionaryPage() == false) {
                    continue;
                }
                long expectedOffset = col.getDictionaryPageOffset();
                long expectedLength = col.getFirstDataPageOffset() - col.getDictionaryPageOffset();
                ColumnChunkPrefetcher.PrefetchedChunk chunk = chunks.get(expectedOffset);
                assertNotNull("Missing pre-warmed chunk for rg " + rg, chunk);
                assertEquals(expectedLength, chunk.length());
            }
        }
    }

    /**
     * Calling {@link PreloadedRowGroupMetadata#close()} twice must be a no-op. The underlying
     * releasable wraps refcounted {@link org.apache.arrow.memory.ArrowBuf}s whose
     * {@code close()} throws when the reference count reaches zero a second time; we want
     * callers (e.g. an iterator with overlapping cleanup paths) to be able to close defensively
     * without tripping on that.
     */
    public void testCloseIsIdempotent() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("v").named("schema");
        int rows = 65_536;
        long[] values = new long[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = i % 16;
        }
        byte[] parquetData = writeDictionaryEncodedInt64Parquet(schema, values);
        StorageObject storage = createRangeReadStorageObject(parquetData);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(storage, allocator), options)) {
            PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(reader, storage, Set.of("v"), allocator);
            assertFalse("Pre-warm map must be populated to exercise the ArrowBuf-backed releasable", metadata.preWarmedChunks().isEmpty());
            metadata.close();
            // Without the idempotency guard this second close throws on ArrowBuf double-decrement.
            metadata.close();
        }
    }

    /**
     * Without predicate column names, pre-warm chunks must remain empty so the optimization is
     * disabled — this preserves the existing behavior for callers that don't have a filter.
     */
    public void testNoPredicateColumnsLeavesPreWarmEmpty() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("v").named("schema");
        byte[] parquetData = writeDictionaryEncodedInt64Parquet(schema, new long[] { 1, 2, 3, 4, 5 });
        StorageObject storage = createRangeReadStorageObject(parquetData);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(storage, allocator), options)) {
            try (PreloadedRowGroupMetadata withoutPrewarm = PreloadedRowGroupMetadata.preload(reader, storage, allocator)) {
                assertTrue("Default preload must not pre-warm dictionary pages", withoutPrewarm.preWarmedChunks().isEmpty());
            }

            try (PreloadedRowGroupMetadata withEmptyPredicates = PreloadedRowGroupMetadata.preload(reader, storage, Set.of(), allocator)) {
                assertTrue("Empty predicate set must disable pre-warm", withEmptyPredicates.preWarmedChunks().isEmpty());
            }

            try (PreloadedRowGroupMetadata withNullPredicates = PreloadedRowGroupMetadata.preload(reader, storage, null, allocator)) {
                assertTrue("Null predicate set must disable pre-warm", withNullPredicates.preWarmedChunks().isEmpty());
            }
        }
    }

    /**
     * After installing the pre-warm map on the adapter and opening a fresh
     * {@link ParquetFileReader}, dictionary reads issued by parquet-mr must be served from the
     * pre-warmed buffers — the storage object must observe zero range GETs against the
     * dictionary-page byte ranges. This is the end-to-end behavior that powers the optimization.
     */
    public void testInstalledPreWarmEliminatesDictionaryRangeReads() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("v").named("schema");
        long[] values = new long[65_536];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 16;
        }
        byte[] parquetData = writeDictionaryEncodedInt64Parquet(schema, values);

        // First, learn each row group's dictionary range so the test can assert on those exact reads.
        long[][] dictRanges;
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(createRangeReadStorageObject(parquetData), allocator),
                options
            )
        ) {
            int rgCount = reader.getRowGroups().size();
            List<long[]> tmp = new ArrayList<>();
            for (int rg = 0; rg < rgCount; rg++) {
                var col = reader.getRowGroups().get(rg).getColumns().get(0);
                if (col.hasDictionaryPage() == false) {
                    continue;
                }
                tmp.add(new long[] { col.getDictionaryPageOffset(), col.getFirstDataPageOffset() });
            }
            dictRanges = tmp.toArray(new long[0][]);
            assertTrue("Test must exercise at least one dictionary page", dictRanges.length > 0);
        }

        // Counting harness: track range reads that overlap any dictionary page range.
        AtomicInteger dictionaryRangeReads = new AtomicInteger();
        StorageObject countingStorage = createRangeReadCountingStorageObject(parquetData, (pos, len) -> {
            for (long[] r : dictRanges) {
                long dictStart = r[0];
                long dictEnd = r[1];
                long readStart = pos;
                long readEnd = pos + len;
                if (readStart < dictEnd && readEnd > dictStart) {
                    dictionaryRangeReads.incrementAndGet();
                    return;
                }
            }
        });

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(countingStorage, allocator);
        try (ParquetFileReader reader = ParquetFileReader.open(adapter, options)) {
            // Pre-fetch + install: exactly the production wiring.
            try (PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(reader, countingStorage, Set.of("v"), allocator)) {
                assertFalse("Pre-warm map must be populated", metadata.preWarmedChunks().isEmpty());
                int dictReadsAfterPreload = dictionaryRangeReads.get();

                adapter.installPreWarmedChunks(metadata.preWarmedChunks());

                // Trigger reads of every row group's dictionary page through parquet-mr's reader.
                // This mirrors what RowGroupFilter does internally during DICTIONARY-level filtering.
                ColumnDescriptor desc = reader.getFileMetaData().getSchema().getColumns().get(0);
                for (int rg = 0; rg < reader.getRowGroups().size(); rg++) {
                    var col = reader.getRowGroups().get(rg).getColumns().get(0);
                    if (col.hasDictionaryPage() == false) {
                        continue;
                    }
                    try (DictionaryPageReadStore dictStore = reader.getDictionaryReader(reader.getRowGroups().get(rg))) {
                        DictionaryPage dictPage = dictStore.readDictionaryPage(desc);
                        assertNotNull("Dictionary page must be readable for rg " + rg, dictPage);
                    }
                }

                // The pre-warm install must catch all subsequent dictionary reads.
                assertEquals(
                    "After installing the pre-warm map, dictionary reads must not trigger any new range GETs",
                    dictReadsAfterPreload,
                    dictionaryRangeReads.get()
                );
            }
        }
    }

    /**
     * Simulates the async-dispatch behavior of native async storage backends like S3, where every
     * {@code readBytesAsync} call is completed on a worker thread rather than inline. Verifies
     * that the pre-warm map is still populated correctly when reads complete out of order on
     * different threads, and that subsequent parquet-mr dictionary reads against the installed
     * pre-warm cache are served from memory without triggering any new range GETs.
     *
     * <p>Note: the coalesced reader merges all dictionary pages within a configurable gap into a
     * single batched request — for a small test file this can collapse to one merged range. The
     * parallelism on S3 only matters when the file is large enough that dictionary ranges from
     * different row groups exceed the coalesce gap; here we exercise the async completion path
     * itself rather than asserting on observed parallelism.
     */
    public void testPreWarmFetchedCorrectlyOnAsyncStorage() throws IOException, InterruptedException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("v").named("schema");
        long[] values = new long[65_536];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 16;
        }
        byte[] parquetData = writeDictionaryEncodedInt64Parquet(schema, values);

        ExecutorService ioPool = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "test-async-io");
            t.setDaemon(true);
            return t;
        });
        try {
            AtomicInteger asyncReadCount = new AtomicInteger();
            StorageObject asyncStorage = createAsyncRangeReadStorageObject(parquetData, ioPool, asyncReadCount);

            ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
            ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(asyncStorage, allocator);
            try (ParquetFileReader reader = ParquetFileReader.open(adapter, options)) {
                try (PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(reader, asyncStorage, Set.of("v"), allocator)) {
                    NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = metadata.preWarmedChunks();
                    assertFalse("Pre-warm map must contain dictionary chunks even when reads complete async", chunks.isEmpty());
                    assertTrue("Async reads must have been dispatched", asyncReadCount.get() > 0);

                    int asyncReadsAfterPreload = asyncReadCount.get();
                    adapter.installPreWarmedChunks(chunks);

                    // Every dictionary page should now be served from the pre-warm map — no new
                    // async reads should be dispatched for those byte ranges.
                    ColumnDescriptor desc = reader.getFileMetaData().getSchema().getColumns().get(0);
                    for (int rg = 0; rg < reader.getRowGroups().size(); rg++) {
                        var col = reader.getRowGroups().get(rg).getColumns().get(0);
                        if (col.hasDictionaryPage() == false) {
                            continue;
                        }
                        try (DictionaryPageReadStore dictStore = reader.getDictionaryReader(reader.getRowGroups().get(rg))) {
                            DictionaryPage dictPage = dictStore.readDictionaryPage(desc);
                            assertNotNull("Dictionary page must be readable from pre-warm for rg " + rg, dictPage);
                        }
                    }

                    assertEquals(
                        "Dictionary reads after pre-warm install must not trigger any new async range reads",
                        asyncReadsAfterPreload,
                        asyncReadCount.get()
                    );
                }
            }
        } finally {
            ioPool.shutdownNow();
            assertTrue("Async io pool failed to terminate", ioPool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Full scan (no predicate, no threshold): the caller passes empty column/offset index sets, so
     * the preload must fetch zero page-index byte ranges and expose no column/offset indexes. This
     * is the core optimization in esql-planning#817 — page indexes that no plan consumes are no
     * longer fetched and decoded.
     */
    public void testFullScanEmitsZeroPageIndexRanges() throws IOException {
        MessageType schema = threeColumnInt64Schema();
        byte[] parquetData = writeThreeColumnInt64Parquet(schema, 65_536);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();

        long[][] indexRanges;
        try (
            ParquetFileReader probe = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(createRangeReadStorageObject(parquetData), allocator),
                options
            )
        ) {
            indexRanges = collectIndexRanges(probe);
            assertTrue("Test file must contain page indexes for the gating assertion to be meaningful", indexRanges.length > 0);
        }

        AtomicInteger indexRangeReads = new AtomicInteger();
        StorageObject countingStorage = createRangeReadCountingStorageObject(parquetData, (pos, len) -> {
            for (long[] r : indexRanges) {
                if (pos < r[1] && pos + len > r[0]) {
                    indexRangeReads.incrementAndGet();
                    return;
                }
            }
        });

        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(countingStorage, allocator), options)) {
            // Footer reads happen during open and never overlap the index ranges; capture the count
            // afterwards so the assertion isolates the preload's contribution.
            int readsBeforePreload = indexRangeReads.get();
            try (
                PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(
                    reader,
                    countingStorage,
                    Set.of(),
                    Set.of(),
                    Set.of(),
                    allocator
                )
            ) {
                assertFalse("Full scan must not preload any column index", metadata.hasColumnIndexes());
                assertFalse("Full scan must not preload any offset index", metadata.hasOffsetIndexes());
                assertEquals("Full scan must not fetch any page-index byte ranges", readsBeforePreload, indexRangeReads.get());
            }
        }
    }

    /**
     * End-to-end guard for the production wiring in {@code ParquetFormatReader.createOptimizedIterator}.
     * A full-scan read through the optimized reader must fetch zero page-index bytes. The direct-API
     * test above cannot catch a miswired call site (the original bug keyed gating off
     * {@code recordFilter == null}, but the production record filter is {@code FilterCompat.NOOP}),
     * so this drives the real {@code read(...)} path and counts byte ranges overlapping the indexes.
     */
    public void testOptimizedFullScanReadFetchesNoPageIndexBytes() throws IOException {
        MessageType schema = threeColumnInt64Schema();
        int rows = 65_536;
        byte[] parquetData = writeThreeColumnInt64Parquet(schema, rows);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        long[][] indexRanges;
        try (
            ParquetFileReader probe = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(createRangeReadStorageObject(parquetData), allocator),
                options
            )
        ) {
            indexRanges = collectIndexRanges(probe);
            assertTrue("Test file must contain page indexes for the gating assertion to be meaningful", indexRanges.length > 0);
        }

        AtomicInteger indexRangeReads = new AtomicInteger();
        StorageObject countingStorage = createRangeReadCountingStorageObject(parquetData, (pos, len) -> {
            for (long[] r : indexRanges) {
                if (pos < r[1] && pos + len > r[0]) {
                    indexRangeReads.incrementAndGet();
                    return;
                }
            }
        });

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true);
        int totalRows = 0;
        try (CloseableIterator<Page> iterator = reader.read(countingStorage, FormatReadContext.of(List.of("a", "b", "c"), 1024))) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }

        assertEquals("Full scan must return every row", rows, totalRows);
        assertEquals("Optimized full-scan read must not fetch any page-index byte ranges", 0, indexRangeReads.get());
    }

    /**
     * Single-column predicate: only column "a" is requested, so its column and offset indexes must
     * be preloaded while columns "b" and "c" carry none.
     */
    public void testSingleColumnPredicateGatesPageIndexToThatColumn() throws IOException {
        MessageType schema = threeColumnInt64Schema();
        byte[] parquetData = writeThreeColumnInt64Parquet(schema, 65_536);
        StorageObject storage = createRangeReadStorageObject(parquetData);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(storage, allocator), options)) {
            assertHasPageIndexReferences(reader, "a", "b", "c");
            try (
                PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(
                    reader,
                    storage,
                    Set.of("a"),
                    Set.of("a"),
                    Set.of("a"),
                    allocator
                )
            ) {
                assertColumnPreloaded(metadata, reader.getRowGroups().size(), "a", true);
                assertColumnPreloaded(metadata, reader.getRowGroups().size(), "b", false);
                assertColumnPreloaded(metadata, reader.getRowGroups().size(), "c", false);
            }
        }
    }

    /**
     * Multi-column predicate: columns "a" and "c" are requested, so their indexes are preloaded
     * while column "b" (not a predicate column) carries none.
     */
    public void testMultiColumnPredicateGatesPageIndexToPredicateColumns() throws IOException {
        MessageType schema = threeColumnInt64Schema();
        byte[] parquetData = writeThreeColumnInt64Parquet(schema, 65_536);
        StorageObject storage = createRangeReadStorageObject(parquetData);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(storage, allocator), options)) {
            assertHasPageIndexReferences(reader, "a", "b", "c");
            Set<String> predicates = Set.of("a", "c");
            try (
                PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(
                    reader,
                    storage,
                    predicates,
                    predicates,
                    predicates,
                    allocator
                )
            ) {
                assertColumnPreloaded(metadata, reader.getRowGroups().size(), "a", true);
                assertColumnPreloaded(metadata, reader.getRowGroups().size(), "c", true);
                assertColumnPreloaded(metadata, reader.getRowGroups().size(), "b", false);
            }
        }
    }

    /**
     * Regression guard for the consumption-aware split between column and offset index gating: a
     * filtered query needs the OffsetIndex of every projected column (to skip non-surviving pages)
     * but only the ColumnIndex of the predicate column. Here the predicate is "a" while "b" and "c"
     * are projected — so all three carry an offset index, but only "a" carries a column index.
     */
    public void testOffsetIndexFetchedForProjectedColumnsButColumnIndexOnlyForPredicate() throws IOException {
        MessageType schema = threeColumnInt64Schema();
        byte[] parquetData = writeThreeColumnInt64Parquet(schema, 65_536);
        StorageObject storage = createRangeReadStorageObject(parquetData);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(storage, allocator), options)) {
            assertHasPageIndexReferences(reader, "a", "b", "c");
            try (
                PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(
                    reader,
                    storage,
                    Set.of("a"),
                    Set.of("a"),
                    Set.of("a", "b", "c"),
                    allocator
                )
            ) {
                int rgCount = reader.getRowGroups().size();
                boolean anyColumnIndexForA = false;
                boolean anyOffsetIndexForB = false;
                boolean anyOffsetIndexForC = false;
                for (int rg = 0; rg < rgCount; rg++) {
                    if (metadata.getColumnIndex(rg, "a") != null) {
                        anyColumnIndexForA = true;
                    }
                    assertNull("Column index for projected-only column b must not be preloaded", metadata.getColumnIndex(rg, "b"));
                    assertNull("Column index for projected-only column c must not be preloaded", metadata.getColumnIndex(rg, "c"));
                    if (metadata.getOffsetIndex(rg, "b") != null) {
                        anyOffsetIndexForB = true;
                    }
                    if (metadata.getOffsetIndex(rg, "c") != null) {
                        anyOffsetIndexForC = true;
                    }
                }
                assertTrue("Column index for predicate column a must be preloaded", anyColumnIndexForA);
                assertTrue("Offset index for projected column b must be preloaded", anyOffsetIndexForB);
                assertTrue("Offset index for projected column c must be preloaded", anyOffsetIndexForC);
            }
        }
    }

    /**
     * Asserts that every named column has both a column index and an offset index reference in the
     * file, so the gating tests above are not vacuously satisfied by an absent index.
     */
    private static void assertHasPageIndexReferences(ParquetFileReader reader, String... columnPaths) {
        for (BlockMetaData block : reader.getRowGroups()) {
            for (ColumnChunkMetaData col : block.getColumns()) {
                String path = col.getPath().toDotString();
                for (String wanted : columnPaths) {
                    if (path.equals(wanted)) {
                        assertNotNull("Expected a column index reference for " + wanted, col.getColumnIndexReference());
                        assertNotNull("Expected an offset index reference for " + wanted, col.getOffsetIndexReference());
                    }
                }
            }
        }
    }

    /**
     * Asserts whether a column's page indexes were preloaded across all row groups. When
     * {@code expectPreloaded} is true at least one row group must expose both the column and offset
     * index for the column; when false no row group may expose either.
     */
    private static void assertColumnPreloaded(
        PreloadedRowGroupMetadata metadata,
        int rowGroupCount,
        String columnPath,
        boolean expectPreloaded
    ) {
        boolean anyColumnIndex = false;
        boolean anyOffsetIndex = false;
        for (int rg = 0; rg < rowGroupCount; rg++) {
            if (metadata.getColumnIndex(rg, columnPath) != null) {
                anyColumnIndex = true;
            }
            if (metadata.getOffsetIndex(rg, columnPath) != null) {
                anyOffsetIndex = true;
            }
        }
        if (expectPreloaded) {
            assertTrue("Expected a preloaded column index for " + columnPath, anyColumnIndex);
            assertTrue("Expected a preloaded offset index for " + columnPath, anyOffsetIndex);
        } else {
            assertFalse("Did not expect a preloaded column index for " + columnPath, anyColumnIndex);
            assertFalse("Did not expect a preloaded offset index for " + columnPath, anyOffsetIndex);
        }
    }

    /**
     * Measures how many ColumnIndex/OffsetIndex bytes the gated preload fetches old (unrestricted)
     * vs new (consumption-aware) across the representative ClickBench query shapes. Page indexes are
     * fetched per column per row group, so on a wide table a full scan that consumes none of them
     * used to pay for every column's indexes; this quantifies exactly how much that is.
     *
     * <p>This is a deterministic, hermetic stand-in for an S3 latency benchmark: it counts index
     * bytes rather than timing them, so there is zero run-to-run variance. The per-scenario numbers
     * are logged (run with {@code -Dtests.output=always}); the assertions only lock the invariants
     * the gate guarantees (full scan fetches nothing, no gated shape fetches more than unrestricted).
     */
    public void testPageIndexBytesByScenario() throws IOException {
        int cols = 105;
        int rows = 16_384;
        MessageType schema = wideInt64Schema(cols);
        byte[] parquetData = writeWideInt64Parquet(schema, cols, rows);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        PageIndexRanges ranges;
        try (
            ParquetFileReader probe = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(createRangeReadStorageObject(parquetData), allocator),
                options
            )
        ) {
            ranges = collectPageIndexRanges(probe);
            assertTrue("wide file must carry page indexes for the measurement to be meaningful", ranges.totalCount() > 0);
        }

        String c0 = columnName(0);
        Set<String> projected5 = Set.of(columnName(0), columnName(1), columnName(2), columnName(3), columnName(4));

        // label, predicate columns (drive dict/bloom pre-warm only), columnIndex paths, offsetIndex
        // paths. A null path set means "unrestricted" — the legacy, pre-PR behavior that fetched
        // every column's page index.
        record Scenario(String label, Set<String> predicate, Set<String> columnIndex, Set<String> offsetIndex) {}
        List<Scenario> scenarios = List.of(
            new Scenario("unrestricted_OLD", Set.of(), null, null),
            new Scenario("full_scan_NEW", Set.of(), Set.of(), Set.of()),
            new Scenario("topN_threshold", Set.of(), Set.of(c0), Set.of(c0)),
            new Scenario("single_predicate", Set.of(c0), Set.of(c0), Set.of(c0)),
            new Scenario("filtered_projection", Set.of(c0), Set.of(c0), projected5)
        );

        Map<String, Long> totals = new LinkedHashMap<>();
        logger.info("page-index byte measurement: {} columns x {} rows, {} index ranges", cols, rows, ranges.totalCount());
        logger.info(
            String.format(
                Locale.ROOT,
                "%-22s %12s %12s %12s %10s %16s",
                "scenario",
                "ci_bytes",
                "oi_bytes",
                "total",
                "idx_reads",
                "saved_vs_OLD"
            )
        );
        for (Scenario s : scenarios) {
            ParquetStorageObjectAdapter.clearFooterCacheForTests();
            AtomicLong ciBytes = new AtomicLong();
            AtomicLong oiBytes = new AtomicLong();
            AtomicInteger idxReads = new AtomicInteger();
            StorageObject counting = createRangeReadCountingStorageObject(parquetData, (pos, len) -> {
                long ci = overlapBytes(pos, len, ranges.columnIndex());
                long oi = overlapBytes(pos, len, ranges.offsetIndex());
                if (ci > 0) {
                    ciBytes.addAndGet(ci);
                }
                if (oi > 0) {
                    oiBytes.addAndGet(oi);
                }
                if (ci > 0 || oi > 0) {
                    idxReads.incrementAndGet();
                }
            });
            try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(counting, allocator), options)) {
                // Footer reads during open never overlap the index ranges; snapshot afterwards so the
                // measurement isolates the preload's contribution.
                long ciAfterOpen = ciBytes.get();
                long oiAfterOpen = oiBytes.get();
                int readsAfterOpen = idxReads.get();
                try (
                    PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(
                        reader,
                        counting,
                        s.predicate(),
                        s.columnIndex(),
                        s.offsetIndex(),
                        allocator
                    )
                ) {
                    assertNotNull(metadata);
                    long ci = ciBytes.get() - ciAfterOpen;
                    long oi = oiBytes.get() - oiAfterOpen;
                    long total = ci + oi;
                    int reads = idxReads.get() - readsAfterOpen;
                    totals.put(s.label(), total);
                    long old = totals.getOrDefault("unrestricted_OLD", 0L);
                    String saved = old > 0 ? String.format(Locale.ROOT, "%d (%.1f%%)", old - total, 100.0 * (old - total) / old) : "-";
                    logger.info(String.format(Locale.ROOT, "%-22s %12d %12d %12d %10d %16s", s.label(), ci, oi, total, reads, saved));
                }
            }
        }

        long old = totals.get("unrestricted_OLD");
        assertTrue("unrestricted preload must fetch some page-index bytes", old > 0);
        assertEquals("full scan must fetch zero page-index bytes", 0L, (long) totals.get("full_scan_NEW"));
        for (Map.Entry<String, Long> e : totals.entrySet()) {
            assertTrue("gated scenario " + e.getKey() + " must not fetch more than unrestricted", e.getValue() <= old);
        }
    }

    private static byte[] writeDictionaryEncodedInt64Parquet(MessageType schema, long[] values) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(out);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withPageSize(4 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .withRowGroupSize(64 * 1024L)
                .build()
        ) {
            for (long v : values) {
                Group g = groupFactory.newGroup();
                g.add("v", v);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    /**
     * Builds a three-column INT64 schema (a, b, c) used by the page-index gating tests.
     */
    private static MessageType threeColumnInt64Schema() {
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
     * Writes a three-column INT64 parquet file with small pages so the writer emits a column index
     * and offset index per column. Used to assert that the gated preload only fetches the page
     * indexes of the requested columns. Each column gets the same {@code rows} count of values
     * derived from the row index.
     */
    private static byte[] writeThreeColumnInt64Parquet(MessageType schema, int rows) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(out);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                // Small page size forces multiple pages per column chunk so the offset index and
                // column index carry more than a single trivial entry.
                .withPageSize(4 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .withRowGroupSize(256 * 1024L)
                .build()
        ) {
            for (int i = 0; i < rows; i++) {
                Group g = groupFactory.newGroup();
                g.add("a", (long) (i % 16));
                g.add("b", (long) (i % 32));
                g.add("c", (long) (i % 64));
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    /**
     * Collects the [start, end) byte ranges of every column index and offset index in the file so
     * a counting storage harness can detect fetches that overlap them.
     */
    private static long[][] collectIndexRanges(ParquetFileReader reader) {
        List<long[]> tmp = new ArrayList<>();
        for (BlockMetaData block : reader.getRowGroups()) {
            for (ColumnChunkMetaData col : block.getColumns()) {
                var ci = col.getColumnIndexReference();
                if (ci != null && ci.getLength() > 0) {
                    tmp.add(new long[] { ci.getOffset(), ci.getOffset() + ci.getLength() });
                }
                var oi = col.getOffsetIndexReference();
                if (oi != null && oi.getLength() > 0) {
                    tmp.add(new long[] { oi.getOffset(), oi.getOffset() + oi.getLength() });
                }
            }
        }
        return tmp.toArray(new long[0][]);
    }

    /**
     * Builds a wide INT64 schema with {@code cols} columns named c000..cNNN, approximating the
     * column count of the ClickBench hits table so a full scan's eliminated page-index volume is
     * representative rather than a three-column toy.
     */
    private static MessageType wideInt64Schema(int cols) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < cols; i++) {
            builder.required(PrimitiveType.PrimitiveTypeName.INT64).named(columnName(i));
        }
        return builder.named("schema");
    }

    private static String columnName(int index) {
        return String.format(Locale.ROOT, "c%03d", index);
    }

    /**
     * Writes a wide INT64 parquet file with small (4 KB) pages so every column chunk emits a
     * multi-entry ColumnIndex and OffsetIndex. Per-column value cardinality is varied so the page
     * indexes are non-trivial.
     */
    private static byte[] writeWideInt64Parquet(MessageType schema, int cols, int rows) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(out);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withPageSize(4 * 1024)
                .withDictionaryPageSize(64 * 1024)
                // Large row group so the wide file stays a couple of row groups (not hundreds),
                // while the small page size still forces many pages per column chunk.
                .withRowGroupSize(8 * 1024 * 1024L)
                .build()
        ) {
            for (int i = 0; i < rows; i++) {
                Group g = groupFactory.newGroup();
                for (int c = 0; c < cols; c++) {
                    g.add(columnName(c), (long) (i % (8 + c)));
                }
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    /**
     * The [start, end) byte ranges of every ColumnIndex and OffsetIndex in the file, kept separate
     * so a measurement can attribute fetched bytes to the column index vs the offset index.
     */
    private record PageIndexRanges(long[][] columnIndex, long[][] offsetIndex) {
        int totalCount() {
            return columnIndex.length + offsetIndex.length;
        }
    }

    /**
     * Collects ColumnIndex and OffsetIndex byte ranges separately across all row groups.
     */
    private static PageIndexRanges collectPageIndexRanges(ParquetFileReader reader) {
        List<long[]> ci = new ArrayList<>();
        List<long[]> oi = new ArrayList<>();
        for (BlockMetaData block : reader.getRowGroups()) {
            for (ColumnChunkMetaData col : block.getColumns()) {
                var c = col.getColumnIndexReference();
                if (c != null && c.getLength() > 0) {
                    ci.add(new long[] { c.getOffset(), c.getOffset() + c.getLength() });
                }
                var o = col.getOffsetIndexReference();
                if (o != null && o.getLength() > 0) {
                    oi.add(new long[] { o.getOffset(), o.getOffset() + o.getLength() });
                }
            }
        }
        return new PageIndexRanges(ci.toArray(new long[0][]), oi.toArray(new long[0][]));
    }

    /**
     * Sums the bytes of a read {@code [position, position + length)} that overlap any of the given
     * index ranges, so coalesced reads that pull adjacent data are attributed only their index bytes.
     */
    private static long overlapBytes(long position, long length, long[][] indexRanges) {
        long end = position + length;
        long sum = 0;
        for (long[] r : indexRanges) {
            long lo = Math.max(position, r[0]);
            long hi = Math.min(end, r[1]);
            if (hi > lo) {
                sum += hi - lo;
            }
        }
        return sum;
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionOutputStream(out);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return positionOutputStream(out);
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
                return "memory://preload-test.parquet";
            }
        };
    }

    private static PositionOutputStream positionOutputStream(ByteArrayOutputStream out) {
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

    private static StorageObject createRangeReadStorageObject(byte[] data) {
        return createRangeReadCountingStorageObject(data, (pos, len) -> {});
    }

    @FunctionalInterface
    private interface RangeReadObserver {
        void onRangeRead(long position, long length);
    }

    /**
     * Builds a {@link StorageObject} that completes {@code readBytesAsync} on the supplied pool.
     * The {@code asyncReadCount} counter records how many async dispatches actually happened so
     * the test can assert that pre-warm install eliminates further async reads.
     */
    private static StorageObject createAsyncRangeReadStorageObject(byte[] data, ExecutorService pool, AtomicInteger asyncReadCount) {
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
                return StoragePath.of("memory://preload-async-test.parquet");
            }
        };
    }

    private static StorageObject createRangeReadCountingStorageObject(byte[] data, RangeReadObserver observer) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                observer.onRangeRead(position, length);
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
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://preload-test.parquet");
            }
        };
    }
}
