/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@link ColumnChunkPrefetcher}: byte range computation, parallel dispatch,
 * and async prefetch with failure handling.
 */
public class ColumnChunkPrefetcherTests extends ESTestCase {

    private CircuitBreaker breaker;

    @Before
    public void initAllocator() {
        breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
    }

    @After
    public void assertNoOutstandingMemory() {
        assertEquals("circuit breaker still holds bytes at teardown", 0L, breaker.getUsed());
    }

    public void testComputeColumnChunkRangesAllColumns() {
        BlockMetaData block = createBlockWithColumns(
            new ColMeta("col_a", 100, 500),
            new ColMeta("col_b", 700, 300),
            new ColMeta("col_c", 1100, 200)
        );

        List<CoalescedRangeReader.ByteRange> ranges = ColumnChunkPrefetcher.computeColumnChunkRanges(block, null);

        assertThat(ranges.size(), equalTo(3));
        assertThat(ranges.get(0), equalTo(new CoalescedRangeReader.ByteRange(100, 500)));
        assertThat(ranges.get(1), equalTo(new CoalescedRangeReader.ByteRange(700, 300)));
        assertThat(ranges.get(2), equalTo(new CoalescedRangeReader.ByteRange(1100, 200)));
    }

    public void testComputeColumnChunkRangesWithProjection() {
        BlockMetaData block = createBlockWithColumns(
            new ColMeta("col_a", 100, 500),
            new ColMeta("col_b", 700, 300),
            new ColMeta("col_c", 1100, 200)
        );

        List<CoalescedRangeReader.ByteRange> ranges = ColumnChunkPrefetcher.computeColumnChunkRanges(block, Set.of("col_a", "col_c"));

        assertThat(ranges.size(), equalTo(2));
        assertThat(ranges.get(0), equalTo(new CoalescedRangeReader.ByteRange(100, 500)));
        assertThat(ranges.get(1), equalTo(new CoalescedRangeReader.ByteRange(1100, 200)));
    }

    public void testComputeColumnChunkRangesEmptyProjection() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_a", 100, 500));

        List<CoalescedRangeReader.ByteRange> ranges = ColumnChunkPrefetcher.computeColumnChunkRanges(block, Set.of("nonexistent"));

        assertThat(ranges.size(), equalTo(0));
    }

    public void testFetchSyncReturnsCorrectDataAndReleasesCharge() {
        byte[] fileData = new byte[2000];
        for (int i = 0; i < fileData.length; i++) {
            fileData[i] = (byte) (i & 0xFF);
        }

        StorageObject storage = createStorageObject(fileData);
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_a", 100, 50), new ColMeta("col_b", 200, 60));

        ColumnChunkPrefetcher.PrefetchedChunks prefetched = ColumnChunkPrefetcher.fetchSync(storage, block, null, breaker);
        try {
            assertEquals(160L, breaker.getUsed());
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> result = prefetched.chunks();
            assertThat(result.size(), greaterThanOrEqualTo(2));

            ColumnChunkPrefetcher.PrefetchedChunk chunkA = result.get(100L);
            assertThat(chunkA, notNullValue());
            assertThat(chunkA.covers(100, 50), equalTo(true));
            byte[] expected = new byte[50];
            System.arraycopy(fileData, 100, expected, 0, 50);
            byte[] actual = new byte[50];
            chunkA.data().duplicate().get(actual);
            assertArrayEquals(expected, actual);
        } finally {
            prefetched.release().close();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testPrefetchAsyncReturnsCorrectData() throws Exception {
        byte[] fileData = new byte[1000];
        for (int i = 0; i < fileData.length; i++) {
            fileData[i] = (byte) (i & 0xFF);
        }

        StorageObject storage = createStorageObject(fileData);
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_x", 50, 100));

        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> future = ColumnChunkPrefetcher.prefetchAsync(
            storage,
            block,
            null,
            breaker
        );

        ColumnChunkPrefetcher.PrefetchedChunks prefetched = future.get();
        try {
            assertThat(prefetched.chunks().isEmpty(), equalTo(false));
        } finally {
            prefetched.release().close();
        }
    }

    /**
     * Heap buffers from {@code readBytesAsync} stay heap. Production factories already allocate
     * heap; this stub forces a wrapped {@code byte[]} so the prefetcher cannot accidentally
     * promote back to direct.
     */
    public void testPrefetchKeepsHeapBuffer() throws Exception {
        byte[] fileData = new byte[1000];
        for (int i = 0; i < fileData.length; i++) {
            fileData[i] = (byte) (i & 0xFF);
        }

        StorageObject storage = new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(fileData);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, fileData.length - position);
                return new ByteArrayInputStream(fileData, pos, len);
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                executor.execute(() -> {
                    int pos = (int) position;
                    int len = (int) Math.min(length, fileData.length - position);
                    byte[] copy = new byte[len];
                    System.arraycopy(fileData, pos, copy, 0, len);
                    // Intentionally heap-backed: ColumnChunkPrefetcher must not promote this to direct.
                    listener.onResponse(new DirectReadBuffer(ByteBuffer.wrap(copy), () -> {}));
                });
            }

            @Override
            public long length() {
                return fileData.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("test://heap.parquet");
            }
        };

        BlockMetaData block = createBlockWithColumns(new ColMeta("col_a", 100, 50));

        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> future = ColumnChunkPrefetcher.prefetchAsync(
            storage,
            block,
            null,
            breaker
        );

        ColumnChunkPrefetcher.PrefetchedChunks prefetched = future.get();
        try {
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> result = prefetched.chunks();
            ColumnChunkPrefetcher.PrefetchedChunk chunk = result.get(100L);
            assertThat(chunk, notNullValue());
            assertFalse("PrefetchedChunk must stay heap", chunk.data().isDirect());

            byte[] expected = new byte[50];
            System.arraycopy(fileData, 100, expected, 0, 50);
            byte[] actual = new byte[50];
            chunk.data().duplicate().get(actual);
            assertArrayEquals(expected, actual);
        } finally {
            prefetched.release().close();
        }
    }

    public void testPrefetchConcurrentReadCalls() throws Exception {
        AtomicInteger concurrentReads = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        byte[] fileData = new byte[10000];
        StorageObject storage = new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(fileData);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, fileData.length - position);
                return new ByteArrayInputStream(fileData, pos, len);
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                int current = concurrentReads.incrementAndGet();
                maxConcurrent.updateAndGet(m -> Math.max(m, current));
                executor.execute(() -> {
                    try (InputStream stream = newStream(position, length)) {
                        byte[] bytes = stream.readAllBytes();
                        listener.onResponse(new DirectReadBuffer(ByteBuffer.wrap(bytes), () -> {}));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    } finally {
                        concurrentReads.decrementAndGet();
                    }
                });
            }

            @Override
            public long length() {
                return fileData.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("test://concurrent.parquet");
            }
        };

        BlockMetaData block = createBlockWithColumns(new ColMeta("a", 100, 500), new ColMeta("b", 2000, 500), new ColMeta("c", 5000, 500));

        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> future = ColumnChunkPrefetcher.prefetchAsync(
            storage,
            block,
            null,
            breaker
        );

        ColumnChunkPrefetcher.PrefetchedChunks prefetched = future.get();
        try {
            assertThat(prefetched.chunks().isEmpty(), equalTo(false));
        } finally {
            prefetched.release().close();
        }
    }

    public void testPrefetchFailureCompletesExceptionally() {
        StorageObject failingStorage = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new IOException("Simulated failure");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                throw new IOException("Simulated failure");
            }

            @Override
            public long length() {
                return 10000;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("test://failing.parquet");
            }
        };

        BlockMetaData block = createBlockWithColumns(new ColMeta("col", 100, 500));

        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> future = ColumnChunkPrefetcher.prefetchAsync(
            failingStorage,
            block,
            null,
            breaker
        );

        assertTrue(future.isCompletedExceptionally());
    }

    /**
     * Cancelling the prefetch wrapper future must cancel the in-flight backend GET, not leave
     * it running until the object-store response arrives.
     */
    public void testPrefetchCancelCancelsBackendRead() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CompletableFuture<Void> backendGet = new CompletableFuture<>();
        StorageObject storage = new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(new byte[1000]);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(new byte[(int) length]);
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                started.countDown();
                backendGet.whenComplete((ignored, error) -> {
                    if (backendGet.isCancelled() || error instanceof CancellationException) {
                        listener.onFailure(new CancellationException("backend GET cancelled"));
                        return;
                    }
                    listener.onFailure(new IOException("backend GET completed without cancel"));
                });
            }

            @Override
            public Releasable startReadBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                readBytesAsync(position, length, factory, executor, listener);
                return () -> backendGet.cancel(true);
            }

            @Override
            public long length() {
                return 1000;
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
                return StoragePath.of("test://cancel.parquet");
            }
        };

        BlockMetaData block = createBlockWithColumns(new ColMeta("col", 100, 500));
        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> future = ColumnChunkPrefetcher.prefetchAsync(
            storage,
            block,
            null,
            breaker
        );
        assertTrue("backend GET must start", started.await(10, TimeUnit.SECONDS));
        assertTrue(future.cancel(true));
        assertTrue("cancelling the prefetch wrapper must cancel the backend GET, not only the wrapper future", backendGet.isCancelled());
    }

    public void testFetchSyncIoFailureIsClient400() {
        IOException injected = new IOException("injected storage read failure");
        BlockMetaData block = createBlockWithColumns(new ColMeta("id", 100, 50));

        RuntimeException exception = expectThrows(
            RuntimeException.class,
            () -> ColumnChunkPrefetcher.fetchSync(throwingOnRead(injected), block, Set.of("id"), breaker)
        );

        assertThat(exception, instanceOf(ExternalClientException.class));
        assertFalse(exception instanceof IllegalArgumentException);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(exception)));
        assertSame(injected, exception.getCause());
    }

    public void testFetchSyncExternalUnavailableStays503() {
        ExternalUnavailableException injected = new ExternalUnavailableException("store 503", new IOException("pool"));
        BlockMetaData block = createBlockWithColumns(new ColMeta("id", 100, 50));

        ExternalUnavailableException exception = expectThrows(
            ExternalUnavailableException.class,
            () -> ColumnChunkPrefetcher.fetchSync(throwingOnRead(injected), block, Set.of("id"), breaker)
        );

        assertSame(injected, exception);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(exception));
    }

    public void testFetchSyncCircuitBreakingExceptionPassesThrough() {
        LimitedBreaker tightBreaker = new LimitedBreaker("tight", ByteSizeValue.ofBytes(1));
        BlockMetaData block = createBlockWithColumns(new ColMeta("id", 100, 50));

        expectThrows(
            CircuitBreakingException.class,
            () -> ColumnChunkPrefetcher.fetchSync(createStorageObject(new byte[200]), block, Set.of("id"), tightBreaker)
        );
        assertEquals(0L, tightBreaker.getUsed());
    }

    public void testFetchSyncShortReadReleasesCharge() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("id", 100, 50));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ColumnChunkPrefetcher.fetchSync(createStorageObject(new byte[110]), block, Set.of("id"), breaker)
        );

        assertThat(exception.getMessage(), containsString("Short read"));
        assertEquals(0L, breaker.getUsed());
    }

    public void testFetchSyncFillsAcrossShortReads() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("id", 100, 50));
        AtomicInteger reads = new AtomicInteger();
        StorageObject shortReads = new TestStorageObject() {
            @Override
            public int readBytes(long position, ByteBuffer target) {
                int read = Math.min(7, target.remaining());
                for (int i = 0; i < read; i++) {
                    target.put((byte) ((position + i) & 0xFF));
                }
                reads.incrementAndGet();
                return read;
            }
        };

        ColumnChunkPrefetcher.PrefetchedChunks fetched = ColumnChunkPrefetcher.fetchSync(shortReads, block, Set.of("id"), breaker);
        try {
            assertTrue(reads.get() > 1);
            ByteBuffer data = fetched.chunks().get(100L).data().duplicate();
            assertEquals(50, data.remaining());
            for (int i = 0; i < data.remaining(); i++) {
                assertEquals((byte) ((100 + i) & 0xFF), data.get(i));
            }
        } finally {
            fetched.release().close();
        }
    }

    public void testFetchSyncRejectsZeroProgressAndReleasesCharge() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("id", 100, 50));
        StorageObject zeroProgress = new TestStorageObject() {
            @Override
            public int readBytes(long position, ByteBuffer target) {
                return 0;
            }
        };

        RuntimeException exception = expectThrows(
            RuntimeException.class,
            () -> ColumnChunkPrefetcher.fetchSync(zeroProgress, block, Set.of("id"), breaker)
        );

        assertThat(exception, instanceOf(ExternalClientException.class));
        assertThat(exception.getMessage(), containsString("Read made no progress"));
        assertEquals(0L, breaker.getUsed());
    }

    public void testFetchSyncRejectsOversizedWholeChunkWithColumnName() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("huge", 100, (long) Integer.MAX_VALUE + 1));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ColumnChunkPrefetcher.fetchSync(new TestStorageObject(), block, Set.of("huge"), breaker)
        );

        assertThat(exception.getMessage(), containsString("column [huge]"));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(exception)));
    }

    public void testFilteredFetchSyncRejectsOversizedChunkWithoutOffsetIndex() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("huge", 100, (long) Integer.MAX_VALUE + 1));
        try (PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.empty()) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> ColumnChunkPrefetcher.fetchSync(
                    new TestStorageObject(),
                    block,
                    Set.of("huge"),
                    RowRanges.of(0, 1, block.getRowCount()),
                    metadata,
                    0,
                    block.getRowCount(),
                    breaker
                )
            );

            assertThat(exception.getMessage(), containsString("column [huge]"));
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(exception)));
        }
    }

    public void testFilteredFetchSyncAcceptsSelectiveReadFromOversizedChunk() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("huge", 100, (long) Integer.MAX_VALUE + 1));
        ColumnChunkMetaData column = block.getColumns().getFirst();
        OffsetIndex offsetIndex = ParquetMetadataConverter.fromParquetOffsetIndex(
            new org.apache.parquet.format.OffsetIndex(List.of(new PageLocation(1000, 32, 0)))
        );
        var schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("huge").named("test");
        try (
            PreloadedRowGroupMetadata metadata = new PreloadedRowGroupMetadata(
                Map.of(),
                Map.of(PreloadedRowGroupMetadata.key(0, column), offsetIndex),
                schema
            )
        ) {
            ColumnChunkPrefetcher.PrefetchedChunks fetched = ColumnChunkPrefetcher.fetchSync(
                new TestStorageObject(),
                block,
                Set.of("huge"),
                RowRanges.of(0, 1, block.getRowCount()),
                metadata,
                0,
                block.getRowCount(),
                breaker
            );
            try {
                assertEquals(1, fetched.chunks().size());
                assertNotNull(fetched.chunks().get(1000L));
                assertEquals(32L, breaker.getUsed());
            } finally {
                fetched.release().close();
            }
        }
    }

    public void testComputePrefetchBytesAllColumns() {
        // All three ranges merge into [100, 1300) since gaps (100, 100) are within 512KB coalesce threshold
        BlockMetaData block = createBlockWithColumns(
            new ColMeta("col_a", 100, 500),
            new ColMeta("col_b", 700, 300),
            new ColMeta("col_c", 1100, 200)
        );
        assertThat(ColumnChunkPrefetcher.computePrefetchBytes(block, null), equalTo(1200L));
    }

    public void testComputePrefetchBytesWithProjection() {
        // col_a [100,600) and col_c [1100,1300): gap of 500 < 512KB, merged to [100, 1300) = 1200
        BlockMetaData block = createBlockWithColumns(
            new ColMeta("col_a", 100, 500),
            new ColMeta("col_b", 700, 300),
            new ColMeta("col_c", 1100, 200)
        );
        assertThat(ColumnChunkPrefetcher.computePrefetchBytes(block, Set.of("col_a", "col_c")), equalTo(1200L));
    }

    public void testComputePrefetchBytesNoMatchingProjection() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_a", 100, 500));
        assertThat(ColumnChunkPrefetcher.computePrefetchBytes(block, Set.of("nonexistent")), equalTo(0L));
    }

    public void testComputePrefetchBytesNullProjection() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_a", 100, 500));
        assertThat(ColumnChunkPrefetcher.computePrefetchBytes(block, null), equalTo(500L));
    }

    public void testComputePrefetchBytesSkipsZeroSizeColumns() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_a", 100, 500), new ColMeta("col_b", 700, 0));
        assertThat(ColumnChunkPrefetcher.computePrefetchBytes(block, null), equalTo(500L));
    }

    public void testComputePrefetchBytesIncludesCoalescingGaps() {
        // Two columns separated by a gap smaller than DEFAULT_MAX_COALESCE_GAP (512KB).
        // The merged range should include the gap bytes, not just the column data.
        BlockMetaData block = createBlockWithColumns(new ColMeta("col_a", 0, 100), new ColMeta("col_b", 200, 100));
        long prefetchBytes = ColumnChunkPrefetcher.computePrefetchBytes(block, null);
        // Merged range: [0, 300) = 300 bytes (includes the 100-byte gap)
        assertThat(prefetchBytes, equalTo(300L));
    }

    public void testComputePrefetchBytesEmptyBlock() {
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(0);
        assertThat(ColumnChunkPrefetcher.computePrefetchBytes(block, null), equalTo(0L));
    }

    public void testDictionaryPageRangeUnsetOffsetUsesStartingPos() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("min_fl", 4, 0, 200, Set.of(Encoding.RLE_DICTIONARY, Encoding.PLAIN)));
        ColumnChunkMetaData column = block.getColumns().getFirst();
        assertEquals(0L, column.getDictionaryPageOffset());
        assertEquals(4L, column.getStartingPos());
        assertTrue(column.hasDictionaryPage());

        CoalescedRangeReader.ByteRange range = ColumnChunkPrefetcher.dictionaryPageRange(column, 100);
        assertEquals(new CoalescedRangeReader.ByteRange(4, 96), range);
    }

    public void testDictionaryPageRangeUnsetOffsetSkippedWhenNoGap() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("min_fl", 4, 0, 200, Set.of(Encoding.RLE_DICTIONARY, Encoding.PLAIN)));
        ColumnChunkMetaData column = block.getColumns().getFirst();

        assertNull(ColumnChunkPrefetcher.dictionaryPageRange(column, 4));
        assertNull("must not treat Thrift-omitted dict offset as file offset 0", ColumnChunkPrefetcher.dictionaryPageRange(column, 0));
    }

    public void testDictionaryPageRangeExplicitOffsetBeforeFirstDataPage() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("col", 100, 50, 200, Set.of(Encoding.RLE_DICTIONARY, Encoding.PLAIN)));
        ColumnChunkMetaData column = block.getColumns().getFirst();
        assertEquals(50L, column.getDictionaryPageOffset());
        assertEquals(50L, column.getStartingPos());

        CoalescedRangeReader.ByteRange range = ColumnChunkPrefetcher.dictionaryPageRange(column, 100);
        assertEquals(new CoalescedRangeReader.ByteRange(50, 50), range);
    }

    public void testDictionaryPageRangeAbsentWithoutDictionaryEncodings() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("col", 4, 200));
        ColumnChunkMetaData column = block.getColumns().getFirst();
        assertFalse(column.hasDictionaryPage());
        assertNull(ColumnChunkPrefetcher.dictionaryPageRange(column, 100));
    }

    public void testComputeFilteredPageRangesPrefetchesUnsetDictionaryGap() {
        BlockMetaData block = createBlockWithColumns(new ColMeta("min_fl", 4, 0, 200, Set.of(Encoding.RLE_DICTIONARY, Encoding.PLAIN)));
        ColumnChunkMetaData column = block.getColumns().getFirst();
        OffsetIndex offsetIndex = ParquetMetadataConverter.fromParquetOffsetIndex(
            new org.apache.parquet.format.OffsetIndex(List.of(new PageLocation(100, 32, 0)))
        );
        var schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("min_fl").named("test");
        try (
            PreloadedRowGroupMetadata metadata = new PreloadedRowGroupMetadata(
                Map.of(),
                Map.of(PreloadedRowGroupMetadata.key(0, column), offsetIndex),
                schema
            )
        ) {
            List<CoalescedRangeReader.ByteRange> ranges = ColumnChunkPrefetcher.computeFilteredPageRanges(
                block,
                RowRanges.of(0, 10, block.getRowCount()),
                metadata,
                0,
                Set.of("min_fl"),
                block.getRowCount()
            );
            assertTrue("must not prefetch PAR1 at offset 0", ranges.stream().noneMatch(r -> r.offset() == 0));
            assertTrue(
                "filtered prefetch must cover the dictionary gap [4, 100)",
                ranges.stream().anyMatch(r -> r.offset() <= 4 && r.end() >= 100)
            );
        }
    }

    public void testPrefetchedChunkCovers() {
        ByteBuffer data = ByteBuffer.allocate(100);
        ColumnChunkPrefetcher.PrefetchedChunk chunk = new ColumnChunkPrefetcher.PrefetchedChunk(200, 100, data);

        assertTrue(chunk.covers(200, 50));
        assertTrue(chunk.covers(200, 100));
        assertTrue(chunk.covers(250, 50));
        assertFalse(chunk.covers(199, 50));
        assertFalse(chunk.covers(250, 51));
        assertFalse(chunk.covers(300, 1));
    }

    public void testFetchSyncEmptyRanges() {
        byte[] fileData = new byte[1000];
        StorageObject storage = createStorageObject(fileData);
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(0);

        ColumnChunkPrefetcher.PrefetchedChunks prefetched = ColumnChunkPrefetcher.fetchSync(storage, block, null, breaker);
        try {
            assertThat(prefetched.chunks().isEmpty(), equalTo(true));
        } finally {
            prefetched.release().close();
        }
    }

    // --- helpers ---

    private record ColMeta(String name, long firstDataPageOffset, long dictionaryPageOffset, long totalSize, Set<Encoding> encodings) {
        ColMeta(String name, long startPos, long totalSize) {
            this(name, startPos, 0L, totalSize, Set.of(Encoding.PLAIN));
        }
    }

    private static StorageObject throwingOnRead(Exception failure) {
        return new TestStorageObject() {
            @Override
            public InputStream newStream(long position, long length) throws IOException {
                if (failure instanceof IOException ioException) {
                    throw ioException;
                }
                if (failure instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                throw new IOException(failure);
            }
        };
    }

    private static class TestStorageObject implements StorageObject {
        @Override
        public InputStream newStream(long position, long length) throws IOException {
            return new ByteArrayInputStream(new byte[(int) length]);
        }

        @Override
        public long length() {
            return Long.MAX_VALUE;
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
            return StoragePath.of("test://synthetic.parquet");
        }
    }

    @SuppressWarnings("deprecation")
    private static BlockMetaData createBlockWithColumns(ColMeta... cols) {
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(100);
        for (ColMeta col : cols) {
            ColumnChunkMetaData chunk = ColumnChunkMetaData.get(
                ColumnPath.get(col.name),
                PrimitiveType.PrimitiveTypeName.INT64,
                CompressionCodecName.UNCOMPRESSED,
                col.encodings,
                org.apache.parquet.column.statistics.Statistics.createStats(
                    Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(col.name)
                ),
                col.firstDataPageOffset,
                col.dictionaryPageOffset,
                100,
                col.totalSize,
                col.totalSize
            );
            block.addColumn(chunk);
        }
        return block;
    }

    private StorageObject createStorageObject(byte[] data) {
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
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("test://test.parquet");
            }
        };
    }
}
