/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that {@link PreloadedRowGroupMetadata#preload(ParquetFileReader, StorageObject, Set)}
 * pre-fetches dictionary page bytes for predicate columns into a coalesced batch fetch and that
 * the resulting {@code preWarmedChunks} map can be installed on the adapter so subsequent reads
 * are served from memory.
 *
 * <p>The dominant goal is regression coverage for the optimization that eliminates per-row-group
 * synchronous range GETs during {@code RowGroupFilter} on remote storage.
 */
public class PreloadedRowGroupMetadataTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
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
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(storage), options)) {
            int rgCount = reader.getRowGroups().size();
            assertTrue("Test setup must produce at least one row group", rgCount >= 1);

            PreloadedRowGroupMetadata withPrewarm = PreloadedRowGroupMetadata.preload(reader, storage, Set.of("v"));
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
     * Without predicate column names, pre-warm chunks must remain empty so the optimization is
     * disabled — this preserves the existing behavior for callers that don't have a filter.
     */
    public void testNoPredicateColumnsLeavesPreWarmEmpty() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("v").named("schema");
        byte[] parquetData = writeDictionaryEncodedInt64Parquet(schema, new long[] { 1, 2, 3, 4, 5 });
        StorageObject storage = createRangeReadStorageObject(parquetData);

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader reader = ParquetFileReader.open(new ParquetStorageObjectAdapter(storage), options)) {
            PreloadedRowGroupMetadata withoutPrewarm = PreloadedRowGroupMetadata.preload(reader, storage);
            assertTrue("Default preload must not pre-warm dictionary pages", withoutPrewarm.preWarmedChunks().isEmpty());

            PreloadedRowGroupMetadata withEmptyPredicates = PreloadedRowGroupMetadata.preload(reader, storage, Set.of());
            assertTrue("Empty predicate set must disable pre-warm", withEmptyPredicates.preWarmedChunks().isEmpty());

            PreloadedRowGroupMetadata withNullPredicates = PreloadedRowGroupMetadata.preload(reader, storage, null);
            assertTrue("Null predicate set must disable pre-warm", withNullPredicates.preWarmedChunks().isEmpty());
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
                new ParquetStorageObjectAdapter(createRangeReadStorageObject(parquetData)),
                options
            )
        ) {
            int rgCount = reader.getRowGroups().size();
            java.util.List<long[]> tmp = new java.util.ArrayList<>();
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

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(countingStorage);
        try (ParquetFileReader reader = ParquetFileReader.open(adapter, options)) {
            // Pre-fetch + install: exactly the production wiring.
            PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(reader, countingStorage, Set.of("v"));
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
            ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(asyncStorage);
            try (ParquetFileReader reader = ParquetFileReader.open(adapter, options)) {
                PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(reader, asyncStorage, Set.of("v"));
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
        } finally {
            ioPool.shutdownNow();
            assertTrue("Async io pool failed to terminate", ioPool.awaitTermination(5, TimeUnit.SECONDS));
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
                java.util.concurrent.Executor ignored,
                ActionListener<ByteBuffer> listener
            ) {
                asyncReadCount.incrementAndGet();
                pool.execute(() -> {
                    try {
                        int pos = (int) position;
                        int len = (int) Math.min(length, data.length - position);
                        byte[] slice = new byte[len];
                        System.arraycopy(data, pos, slice, 0, len);
                        listener.onResponse(ByteBuffer.wrap(slice));
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
