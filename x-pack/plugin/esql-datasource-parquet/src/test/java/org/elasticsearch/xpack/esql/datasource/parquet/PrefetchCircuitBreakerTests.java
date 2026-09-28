/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

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
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tests that the optimized Parquet reader's prefetch pipeline correctly integrates with
 * the circuit breaker: reserving memory before async I/O, releasing on clear/cancel/failure,
 * and retrying failed prefetches synchronously through the same breaker-accounted path.
 */
public class PrefetchCircuitBreakerTests extends ESTestCase {

    /**
     * Footer byte cache handed to every adapter this test constructs. In production the owning
     * format reader supplies its instance; a fresh per-test-class cache gives the same sharing
     * within a test and automatic isolation between tests.
     */
    private final FooterByteCache footerByteCache = FooterByteCache.fromSettings(Settings.EMPTY);

    private static final MessageType SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .named("id")
        .required(PrimitiveType.PrimitiveTypeName.INT32)
        .named("value")
        .named("test_schema");

    /**
     * Reads a multi-row-group file with a limited breaker and verifies the breaker
     * returns to zero after full iteration completes.
     */
    public void testPrefetchReservesAndReleasesBreaker() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(3000, 2048);
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createAsyncStorageObject(parquetData);
        int totalRows = 0;
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertTrue("Should have read rows", totalRows > 0);
        assertTrue("Breaker should have been used for prefetch", breaker.peakUsed.get() > 0);
        assertEquals("Breaker should return to zero after iteration", 0, breaker.getUsed());
    }

    /**
     * Verifies the optimized reader works correctly with a tight breaker limit. The prefetch
     * competes with Parquet-mr decode allocations and ESQL block creation for the same breaker
     * budget. The query may either complete normally or throw a CircuitBreakingException from
     * asynchronous prefetch, its synchronous fallback, or decode allocations. Any outcome must
     * return the breaker to zero.
     */
    public void testPrefetchWithTightBreakerLimit() throws Exception {
        MessageType wideSchema = buildWideSchema(10);
        byte[] parquetData = createMultiRowGroupFile(wideSchema, 5000, 50 * 1024);
        // Cover the clamped window (file length, or 4 MiB if the object is larger) and leave ~2 MB
        // so decode or prefetch allocations may still trip the breaker.
        long windowCharge = Math.min(parquetData.length, ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofBytes(windowCharge + 2 * 1024 * 1024));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createAsyncStorageObject(parquetData);
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            try {
                while (iter.hasNext()) {
                    Page page = iter.next();
                    page.releaseBlocks();
                }
            } catch (CircuitBreakingException e) {
                // The tight limit may reject either a prefetch/fallback buffer or decode output.
            }
        }
        assertEquals("Breaker should return to zero", 0, breaker.getUsed());
    }

    /**
     * Uses a failing storage object to test that the breaker reservation is released
     * when async prefetch I/O fails.
     */
    public void testPrefetchReleasesOnIOFailure() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(2000, 2048);
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createFailingAsyncStorageObject(parquetData);
        int totalRows = 0;
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            long storageReadBaseline = breaker.reservedBytes("storage read buffer");
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
            assertTrue(
                "sync fallback must reserve breaker-accounted storage buffers",
                breaker.reservedBytes("storage read buffer") > storageReadBaseline
            );
        }
        assertTrue("Should have read rows via sync fallback", totalRows > 0);
        assertEquals("Breaker should return to zero after iteration with failures", 0, breaker.getUsed());
    }

    public void testForcedFallbackCanBeRefusedByBreaker() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(2000, 2048);
        var breaker = new SwitchableStorageReadBreaker("test", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        StorageObject storage = createFailingAsyncStorageObject(parquetData);

        try (
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true);
            CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 1024))
        ) {
            breaker.rejectStorageReads = true;
            expectThrows(CircuitBreakingException.class, iter::hasNext);
        }
        assertEquals("rejected fallback and iterator close must release all bytes", 0L, breaker.getUsed());
    }

    /**
     * Tracks the maximum breaker usage during iteration and verifies it stays bounded
     * to approximately one row group's worth of prefetch data.
     */
    public void testBreakerUsageDuringIteration() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(5000, 2048);
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createAsyncStorageObject(parquetData);
        int totalRows = 0;
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertTrue("Should have read rows", totalRows > 0);
        assertEquals("Breaker should return to zero", 0, breaker.getUsed());
        // Peak includes the clamped window (file length when the object fits, else 4 MiB) plus
        // prefetch/decode. Bound against that window, not the historical 4 MiB floor.
        long windowCharge = Math.min(parquetData.length, ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        assertTrue(
            "Peak prefetch breaker usage should be bounded (was " + breaker.peakUsed + " bytes)",
            breaker.peakUsed.get() < parquetData.length + windowCharge
        );
    }

    /**
     * Closes the iterator mid-iteration (before exhausting all row groups) and verifies
     * the breaker returns to zero — catches leak paths on early close / query abort.
     */
    public void testPrefetchReleasedOnEarlyClose() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(5000, 2048);
        var breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(50));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        StorageObject storage = createAsyncStorageObject(parquetData);
        try (CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(storage, FormatReadContext.of(null, 1024))) {
            if (iter.hasNext()) {
                Page page = iter.next();
                page.releaseBlocks();
            }
        }
        assertEquals("Breaker should return to zero after early close", 0, breaker.getUsed());
    }

    /**
     * N concurrent optimized readers share one breaker and one Arrow allocator. After every
     * thread finishes iterating, both accounts must return to baseline. Uses zstd so the
     * decompress path is exercised under concurrency, not only uncompressed prefetch.
     * Peak is not compared to N times a single-reader peak: N concurrent windows can
     * legitimately reach about N times one window. Sharing is the accounts returning to zero.
     */
    public void testConcurrentReadersShareBreaker() throws Exception {
        int readers = 4;
        ConcurrentPipeline pipeline = newConcurrentPipeline(3000);

        startInParallel(readers, i -> {
            int totalRows = 0;
            try {
                try (
                    CloseableIterator<Page> iter = new ParquetFormatReader(pipeline.blockFactory, true).read(
                        pipeline.storage(),
                        FormatReadContext.of(null, 1024)
                    )
                ) {
                    while (iter.hasNext()) {
                        Page page = iter.next();
                        totalRows += page.getPositionCount();
                        page.releaseBlocks();
                    }
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            assertTrue("Should have read rows", totalRows > 0);
        });
        assertEquals("Breaker should return to zero after concurrent readers finish", 0, pipeline.breaker.getUsed());
        assertEquals(
            "Arrow allocator should return to baseline after concurrent readers finish",
            pipeline.allocBaseline,
            pipeline.blockFactory.arrowAllocator().getAllocatedMemory()
        );
        assertTrue("Prefetch should have reserved breaker bytes", pipeline.breaker.peakUsed.get() > 0);
    }

    /**
     * Concurrent early close of the optimized iterator after a synchronized partial read.
     * Each worker consumes one page, waits so all hold an open iterator, then closes on the
     * iterating thread. This is not {@code AsyncExternalSourceBuffer.discardPages} cancel
     * (covered in {@code AsyncExternalSourceBufferTests}) and not other-thread close.
     */
    public void testConcurrentEarlyCloseReclaimsMemory() throws Exception {
        int readers = 4;
        ConcurrentPipeline pipeline = newConcurrentPipeline(5000);

        CyclicBarrier hold = new CyclicBarrier(readers);
        startInParallel(readers, i -> {
            try {
                try (
                    CloseableIterator<Page> iter = new ParquetFormatReader(pipeline.blockFactory, true).read(
                        pipeline.storage(),
                        FormatReadContext.of(null, 1024)
                    )
                ) {
                    assertTrue("optimized reader must produce a page before early close", iter.hasNext());
                    Page page = iter.next();
                    assertTrue("page must contain rows", page.getPositionCount() > 0);
                    page.releaseBlocks();
                    hold.await(30, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertEquals("Breaker should return to zero after concurrent early close", 0, pipeline.breaker.getUsed());
        assertEquals(
            "Arrow allocator should return to baseline after concurrent early close",
            pipeline.allocBaseline,
            pipeline.blockFactory.arrowAllocator().getAllocatedMemory()
        );
        assertTrue("Prefetch should have reserved breaker bytes before early close", pipeline.breaker.peakUsed.get() > 0);
    }

    public void testFallbackWaitsForSkippedInFlightPrefetchRelease() throws Exception {
        byte[] parquetData = createTwoLargeRowGroupFile();
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofMb(128));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        ExecutorService executor = Executors.newCachedThreadPool();
        BarrierStorageObject storage = new BarrierStorageObject(parquetData, executor);
        SharedNumericThreshold channel = new SharedNumericThreshold.Supplier(false, false).get();
        DynamicThreshold threshold = new DynamicThreshold("id", ElementType.LONG, false, false, channel);
        try {
            try (
                threshold;
                ParquetFormatReader reader = (ParquetFormatReader) new ParquetFormatReader(blockFactory, true).withDynamicThreshold(
                    threshold
                );
                CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 1024))
            ) {
                OptimizedParquetColumnIterator optimized = (OptimizedParquetColumnIterator) iter;
                assertTrue("fixture must seed more than one prefetch", optimized.prefetchDepth() > 1);
                assertEquals("both stale and expected ordinals must be queued", List.of(0, 1), optimized.pendingPrefetchOrdinals());
                assertTrue(storage.prefetchesStarted.await(10, TimeUnit.SECONDS));
                assertTrue(storage.staleReservationAcquired.await(10, TimeUnit.SECONDS));
                assertTrue(storage.expectedFailureDelivered.await(10, TimeUnit.SECONDS));

                // The descending bound drops row group 0 (id=0) but keeps row group 1 (id=1000).
                // Both were queued before the bound existed, so ordinal 0 becomes stale while
                // ordinal 1 supplies the expected failed prefetch that forces synchronous fallback.
                channel.offer(500L);
                storage.checkFallbackBarrier.set(true);
                CountDownLatch advanceStarted = new CountDownLatch(1);
                Future<Boolean> advance = executor.submit(() -> {
                    advanceStarted.countDown();
                    return iter.hasNext();
                });
                assertTrue(advanceStarted.await(10, TimeUnit.SECONDS));
                assertFalse("row-group advance must wait at the release barrier", advance.isDone());

                storage.allowStaleCompletion.countDown();
                assertTrue(advance.get(10, TimeUnit.SECONDS));
                assertTrue(storage.syncFallbackStarted.await(10, TimeUnit.SECONDS));
                assertFalse("synchronous fallback started before the stale reservation was released", storage.fallbackBeforeRelease.get());
                assertTrue(storage.staleReleasedAt.get() > 0);
                assertTrue(storage.fallbackReadAt.get() > storage.staleReleasedAt.get());
            }
        } finally {
            storage.allowStaleCompletion.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals("iterator close must release every breaker reservation", 0L, breaker.getUsed());
    }

    public void testPhaseTwoFallbackWaitsForPendingPhaseOneRelease() throws Exception {
        byte[] parquetData = createTwoLargeRowGroupFile();
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofMb(128));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        ExecutorService executor = Executors.newCachedThreadPool();
        List<long[]> predicateRanges = columnChunkRanges(parquetData, "id");
        List<long[]> projectionRanges = columnChunkRanges(parquetData, "payload");
        assertEquals(2, predicateRanges.size());
        assertEquals(2, projectionRanges.size());
        PhaseTwoBarrierStorageObject storage = new PhaseTwoBarrierStorageObject(
            parquetData,
            executor,
            predicateRanges.get(1),
            projectionRanges.get(0)
        );
        ReferenceAttribute id = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(
            List.of(new GreaterThanOrEqual(Source.EMPTY, id, new Literal(Source.EMPTY, 0L, DataType.LONG), null))
        );
        try {
            try (
                ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
                CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 1024))
            ) {
                OptimizedParquetColumnIterator optimized = (OptimizedParquetColumnIterator) iter;
                assertEquals("fixture must queue two Phase-1 prefetches", 2, optimized.prefetchDepth());
                assertEquals(List.of(0, 1), optimized.pendingPrefetchOrdinals());
                assertTrue(storage.pendingPhaseOneReservationAcquired.await(10, TimeUnit.SECONDS));

                Future<Boolean> advance = executor.submit(iter::hasNext);
                assertTrue(storage.phaseTwoFailureDelivered.await(10, TimeUnit.SECONDS));
                storage.allowPendingPhaseOneCompletion.countDown();

                assertTrue(advance.get(10, TimeUnit.SECONDS));
                assertTrue(storage.syncFallbackStarted.await(10, TimeUnit.SECONDS));
                assertTrue(storage.pendingPhaseOneReleasedAt.get() > 0);
                assertTrue(storage.fallbackReadAt.get() > storage.pendingPhaseOneReleasedAt.get());
                Page page = iter.next();
                page.releaseBlocks();
            }
        } finally {
            storage.allowPendingPhaseOneCompletion.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals("iterator close must release every Phase-1 and Phase-2 reservation", 0L, breaker.getUsed());
    }

    public void testDrainPendingPrefetchesWaitsForReleaseBeforeReturning() throws Exception {
        ArrayDeque<OptimizedParquetColumnIterator.PendingPrefetch> queue = new ArrayDeque<>();
        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> pending = new CompletableFuture<>();
        queue.add(new OptimizedParquetColumnIterator.PendingPrefetch(0, pending));
        AtomicInteger sequence = new AtomicInteger();
        AtomicInteger releasedAt = new AtomicInteger();
        CountDownLatch drainStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> drain = executor.submit(() -> {
                drainStarted.countDown();
                OptimizedParquetColumnIterator.drainPendingPrefetches(queue);
            });
            assertTrue(drainStarted.await(10, TimeUnit.SECONDS));
            assertFalse(drain.isDone());
            pending.complete(new ColumnChunkPrefetcher.PrefetchedChunks(new TreeMap<>(), () -> releasedAt.set(sequence.incrementAndGet())));
            drain.get(10, TimeUnit.SECONDS);

            int fallbackReadAt = sequence.incrementAndGet();
            assertEquals(1, releasedAt.get());
            assertEquals(2, fallbackReadAt);
            assertTrue(queue.isEmpty());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testDrainPendingPrefetchesContinuesAfterReleaseFailure() {
        ArrayDeque<OptimizedParquetColumnIterator.PendingPrefetch> queue = new ArrayDeque<>();
        AtomicInteger releases = new AtomicInteger();
        queue.add(
            new OptimizedParquetColumnIterator.PendingPrefetch(
                0,
                CompletableFuture.completedFuture(new ColumnChunkPrefetcher.PrefetchedChunks(new TreeMap<>(), () -> {
                    releases.incrementAndGet();
                    throw new IllegalStateException("injected release failure");
                }))
            )
        );
        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> failedPrefetch = new CompletableFuture<>();
        failedPrefetch.completeExceptionally(new IOException("expected speculative failure"));
        queue.add(new OptimizedParquetColumnIterator.PendingPrefetch(1, failedPrefetch));
        AssertionError prefetchError = new AssertionError("injected speculative error");
        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> erroredPrefetch = new CompletableFuture<>();
        erroredPrefetch.completeExceptionally(prefetchError);
        queue.add(new OptimizedParquetColumnIterator.PendingPrefetch(2, erroredPrefetch));
        queue.add(
            new OptimizedParquetColumnIterator.PendingPrefetch(
                3,
                CompletableFuture.completedFuture(new ColumnChunkPrefetcher.PrefetchedChunks(new TreeMap<>(), () -> {
                    releases.incrementAndGet();
                    throw new IllegalArgumentException("injected later release failure");
                }))
            )
        );

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> OptimizedParquetColumnIterator.drainPendingPrefetches(queue)
        );

        assertEquals("injected release failure", exception.getMessage());
        assertEquals("later results must still be released", 2, releases.get());
        assertEquals(2, exception.getSuppressed().length);
        assertSame(prefetchError, exception.getSuppressed()[0]);
        assertEquals("injected later release failure", exception.getSuppressed()[1].getMessage());
        assertTrue(queue.isEmpty());
    }

    public void testCancelPendingPrefetchesContinuesAfterReleaseFailure() {
        ArrayDeque<OptimizedParquetColumnIterator.PendingPrefetch> queue = new ArrayDeque<>();
        AtomicInteger releases = new AtomicInteger();
        queue.add(
            new OptimizedParquetColumnIterator.PendingPrefetch(
                0,
                CompletableFuture.completedFuture(new ColumnChunkPrefetcher.PrefetchedChunks(new TreeMap<>(), () -> {
                    releases.incrementAndGet();
                    throw new IllegalStateException("injected cancellation release failure");
                }))
            )
        );
        queue.add(
            new OptimizedParquetColumnIterator.PendingPrefetch(
                1,
                CompletableFuture.completedFuture(new ColumnChunkPrefetcher.PrefetchedChunks(new TreeMap<>(), () -> {
                    releases.incrementAndGet();
                    throw new IllegalArgumentException("injected later cancellation release failure");
                }))
            )
        );

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> OptimizedParquetColumnIterator.cancelPendingPrefetches(queue)
        );

        assertEquals("injected cancellation release failure", exception.getMessage());
        assertEquals("later staged results must still be canceled and released", 2, releases.get());
        assertEquals(1, exception.getSuppressed().length);
        assertEquals("injected later cancellation release failure", exception.getSuppressed()[0].getMessage());
        assertTrue(queue.isEmpty());
    }

    // --- Helpers ---

    /**
     * Breaker that tracks peak usage for assertions.
     */
    static class TrackingBreaker extends LimitedBreaker {
        final AtomicLong peakUsed = new AtomicLong(0);
        private final ConcurrentHashMap<String, LongAdder> reservedByLabel = new ConcurrentHashMap<>();

        TrackingBreaker(String name, ByteSizeValue limit) {
            super(name, limit);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            super.addEstimateBytesAndMaybeBreak(bytes, label);
            reservedByLabel.computeIfAbsent(label, ignored -> new LongAdder()).add(bytes);
            peakUsed.updateAndGet(peak -> Math.max(peak, getUsed()));
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            super.addWithoutBreaking(bytes);
            if (bytes > 0) {
                peakUsed.updateAndGet(peak -> Math.max(peak, getUsed()));
            }
        }

        long reservedBytes(String label) {
            LongAdder reserved = reservedByLabel.get(label);
            return reserved == null ? 0L : reserved.sum();
        }
    }

    private static final class SwitchableStorageReadBreaker extends TrackingBreaker {
        private volatile boolean rejectStorageReads;

        private SwitchableStorageReadBreaker(String name, ByteSizeValue limit) {
            super(name, limit);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            if (rejectStorageReads && "storage read buffer".equals(label)) {
                throw new CircuitBreakingException("forced storage-read rejection", bytes, 0, CircuitBreaker.Durability.TRANSIENT);
            }
            super.addEstimateBytesAndMaybeBreak(bytes, label);
        }
    }

    private StorageObject createAsyncStorageObject(byte[] data) {
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
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://breaker-test.parquet");
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
                    try {
                        int pos = (int) position;
                        int len = (int) Math.min(length, data.length - position);
                        ByteBuffer buffer = ByteBuffer.allocate(len);
                        buffer.put(data, pos, len);
                        buffer.flip();
                        listener.onResponse(new DirectReadBuffer(buffer, () -> {}));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                });
            }
        };
    }

    /**
     * Storage object whose async reads always fail, forcing the prefetch pipeline
     * to fall back to synchronous I/O. Sync reads work normally.
     */
    private StorageObject createFailingAsyncStorageObject(byte[] data) {
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
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://failing-async-test.parquet");
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                executor.execute(() -> listener.onFailure(new IOException("Simulated async I/O failure")));
            }
        };
    }

    /**
     * Completes row group 0's Phase-1 prefetch, holds row group 1's Phase-1 reservation, then
     * fails row group 0's large Phase-2 request. The synchronous retry records whether the held
     * Phase-1 result was released first.
     */
    private static final class PhaseTwoBarrierStorageObject implements StorageObject {
        private final byte[] data;
        private final ExecutorService executor;
        private final long[] pendingPhaseOneRange;
        private final long[] failingPhaseTwoRange;
        private final AtomicInteger sequence = new AtomicInteger();
        private final CountDownLatch pendingPhaseOneReservationAcquired = new CountDownLatch(1);
        private final CountDownLatch allowPendingPhaseOneCompletion = new CountDownLatch(1);
        private final CountDownLatch phaseTwoFailureDelivered = new CountDownLatch(1);
        private final CountDownLatch syncFallbackStarted = new CountDownLatch(1);
        private final AtomicInteger pendingPhaseOneReleasedAt = new AtomicInteger();
        private final AtomicInteger fallbackReadAt = new AtomicInteger();

        private PhaseTwoBarrierStorageObject(
            byte[] data,
            ExecutorService executor,
            long[] pendingPhaseOneRange,
            long[] failingPhaseTwoRange
        ) {
            this.data = data;
            this.executor = executor;
            this.pendingPhaseOneRange = pendingPhaseOneRange;
            this.failingPhaseTwoRange = failingPhaseTwoRange;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            if (inRange(position, failingPhaseTwoRange)) {
                syncFallbackStarted.countDown();
                fallbackReadAt.compareAndSet(0, sequence.incrementAndGet());
            }
            return new ByteArrayInputStream(data, Math.toIntExact(position), Math.toIntExact(Math.min(length, data.length - position)));
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
            return StoragePath.of("memory://phase-two-fallback-release-barrier.parquet");
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
            Executor ignored,
            ActionListener<DirectReadBuffer> listener
        ) {
            if (inRange(position, failingPhaseTwoRange)) {
                phaseTwoFailureDelivered.countDown();
                listener.onFailure(new IOException("injected Phase-2 prefetch failure"));
                return;
            }
            if (inRange(position, pendingPhaseOneRange)) {
                completePendingPhaseOne(position, length, factory, listener);
            } else {
                completeAsyncRead(position, length, factory, listener);
            }
        }

        private void completePendingPhaseOne(
            long position,
            long length,
            DirectBufferFactory factory,
            ActionListener<DirectReadBuffer> listener
        ) {
            executor.execute(() -> {
                DirectReadBuffer allocated = null;
                try {
                    allocated = allocateAndFill(position, length, factory);
                    pendingPhaseOneReservationAcquired.countDown();
                    allowPendingPhaseOneCompletion.await();
                    DirectReadBuffer owner = allocated;
                    listener.onResponse(new DirectReadBuffer(owner.buffer(), () -> {
                        try {
                            owner.close();
                        } finally {
                            pendingPhaseOneReleasedAt.compareAndSet(0, sequence.incrementAndGet());
                        }
                    }));
                    allocated = null;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    listener.onFailure(new IOException("interrupted pending Phase-1 prefetch", e));
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    if (allocated != null) {
                        allocated.close();
                    }
                }
            });
        }

        private void completeAsyncRead(long position, long length, DirectBufferFactory factory, ActionListener<DirectReadBuffer> listener) {
            executor.execute(() -> {
                DirectReadBuffer allocated = null;
                try {
                    allocated = allocateAndFill(position, length, factory);
                    listener.onResponse(allocated);
                    allocated = null;
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    if (allocated != null) {
                        allocated.close();
                    }
                }
            });
        }

        private DirectReadBuffer allocateAndFill(long position, long length, DirectBufferFactory factory) throws IOException {
            int offset = Math.toIntExact(position);
            int bytes = Math.toIntExact(Math.min(length, data.length - position));
            DirectReadBuffer allocated = factory.allocateWritableWindow(bytes);
            try {
                allocated.buffer().put(data, offset, bytes).flip();
                return allocated;
            } catch (RuntimeException | Error e) {
                allocated.close();
                throw e;
            }
        }

        private static boolean inRange(long position, long[] range) {
            return position >= range[0] && position < range[1];
        }
    }

    /**
     * Holds the first large async read after allocating its breaker-accounted buffer, fails the
     * second, and ignores cancellation by letting the first backend operation finish only when
     * the test releases its latch.
     */
    private static final class BarrierStorageObject implements StorageObject {
        private static final long LARGE_PREFETCH_BYTES = 8_000_000L;

        private final byte[] data;
        private final ExecutorService executor;
        private final AtomicInteger largeAsyncReads = new AtomicInteger();
        private final AtomicInteger sequence = new AtomicInteger();
        private final CountDownLatch prefetchesStarted = new CountDownLatch(2);
        private final CountDownLatch staleReservationAcquired = new CountDownLatch(1);
        private final CountDownLatch expectedFailureDelivered = new CountDownLatch(1);
        private final CountDownLatch allowStaleCompletion = new CountDownLatch(1);
        private final CountDownLatch syncFallbackStarted = new CountDownLatch(1);
        private final AtomicBoolean checkFallbackBarrier = new AtomicBoolean();
        private final AtomicBoolean staleReleased = new AtomicBoolean();
        private final AtomicBoolean fallbackBeforeRelease = new AtomicBoolean();
        private final AtomicInteger staleReleasedAt = new AtomicInteger();
        private final AtomicInteger fallbackReadAt = new AtomicInteger();

        private BarrierStorageObject(byte[] data, ExecutorService executor) {
            this.data = data;
            this.executor = executor;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            if (checkFallbackBarrier.get() && length > LARGE_PREFETCH_BYTES) {
                syncFallbackStarted.countDown();
                if (staleReleased.get() == false) {
                    fallbackBeforeRelease.set(true);
                }
                fallbackReadAt.compareAndSet(0, sequence.incrementAndGet());
            }
            return new ByteArrayInputStream(data, Math.toIntExact(position), Math.toIntExact(Math.min(length, data.length - position)));
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
            return StoragePath.of("memory://fallback-release-barrier.parquet");
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
            Executor ignored,
            ActionListener<DirectReadBuffer> listener
        ) {
            if (length <= LARGE_PREFETCH_BYTES) {
                completeAsyncRead(position, length, factory, listener);
                return;
            }
            int prefetch = largeAsyncReads.getAndIncrement();
            prefetchesStarted.countDown();
            if (prefetch == 0) {
                completeStalePrefetch(position, length, factory, listener);
            } else if (prefetch == 1) {
                executor.execute(() -> {
                    try {
                        listener.onFailure(new IOException("injected expected-prefetch failure"));
                    } finally {
                        expectedFailureDelivered.countDown();
                    }
                });
            } else {
                completeAsyncRead(position, length, factory, listener);
            }
        }

        private void completeStalePrefetch(
            long position,
            long length,
            DirectBufferFactory factory,
            ActionListener<DirectReadBuffer> listener
        ) {
            executor.execute(() -> {
                DirectReadBuffer allocated = null;
                try {
                    allocated = allocateAndFill(position, length, factory);
                    staleReservationAcquired.countDown();
                    allowStaleCompletion.await();
                    DirectReadBuffer owner = allocated;
                    listener.onResponse(new DirectReadBuffer(owner.buffer(), () -> {
                        try {
                            owner.close();
                        } finally {
                            staleReleased.set(true);
                            staleReleasedAt.compareAndSet(0, sequence.incrementAndGet());
                        }
                    }));
                    allocated = null;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    listener.onFailure(new IOException("interrupted stale prefetch", e));
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    if (allocated != null) {
                        allocated.close();
                    }
                }
            });
        }

        private void completeAsyncRead(long position, long length, DirectBufferFactory factory, ActionListener<DirectReadBuffer> listener) {
            executor.execute(() -> {
                DirectReadBuffer allocated = null;
                try {
                    allocated = allocateAndFill(position, length, factory);
                    listener.onResponse(allocated);
                    allocated = null;
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    if (allocated != null) {
                        allocated.close();
                    }
                }
            });
        }

        private DirectReadBuffer allocateAndFill(long position, long length, DirectBufferFactory factory) throws IOException {
            int offset = Math.toIntExact(position);
            int bytes = Math.toIntExact(Math.min(length, data.length - position));
            DirectReadBuffer allocated = factory.allocateWritableWindow(bytes);
            try {
                allocated.buffer().put(data, offset, bytes).flip();
                return allocated;
            } catch (RuntimeException | Error e) {
                allocated.close();
                throw e;
            }
        }
    }

    private static MessageType buildWideSchema(int numColumns) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int c = 0; c < numColumns; c++) {
            builder.required(PrimitiveType.PrimitiveTypeName.INT64).named("col_" + c);
        }
        return builder.named("wide_schema");
    }

    /**
     * Allocator-backed in-memory storage: default {@code readBytesAsync} allocates through
     * {@link DirectBufferFactory}. Distinct from {@link #createAsyncStorageObject}, which uses a
     * heap {@code ByteBuffer} and a no-op closer so older breaker tests do not charge the breaker for
     * prefetch bytes. Do not merge the two stubs.
     */
    private static final class InMemoryStorageObject implements StorageObject {
        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
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
            return StoragePath.of("memory://breaker-concurrent.parquet");
        }
    }

    private ConcurrentPipeline newConcurrentPipeline(int rowCount) throws IOException {
        byte[] parquetData = createMultiRowGroupFile(buildWideSchema(8), rowCount, 2048, CompressionCodecName.ZSTD);
        var breaker = new TrackingBreaker("test", ByteSizeValue.ofMb(256));
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        return new ConcurrentPipeline(parquetData, breaker, blockFactory, blockFactory.arrowAllocator().getAllocatedMemory());
    }

    private record ConcurrentPipeline(byte[] parquetData, TrackingBreaker breaker, BlockFactory blockFactory, long allocBaseline) {
        StorageObject storage() {
            return new InMemoryStorageObject(parquetData);
        }
    }

    private byte[] createMultiRowGroupFile(int rowCount, int rowGroupSize) throws IOException {
        return createMultiRowGroupFile(SCHEMA, rowCount, rowGroupSize, CompressionCodecName.UNCOMPRESSED);
    }

    private List<long[]> columnChunkRanges(byte[] parquetData, String columnName) throws IOException {
        PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(
                    new InMemoryStorageObject(parquetData),
                    footerByteCache,
                    new NoopCircuitBreaker("phase-two-barrier-ranges")
                ),
                PlainParquetReadOptions.builder(codecFactory).build()
            )
        ) {
            List<long[]> ranges = new ArrayList<>();
            for (BlockMetaData block : reader.getRowGroups()) {
                for (ColumnChunkMetaData column : block.getColumns()) {
                    if (columnName.equals(column.getPath().toDotString())) {
                        ranges.add(new long[] { column.getStartingPos(), column.getStartingPos() + column.getTotalSize() });
                    }
                }
            }
            return ranges;
        } finally {
            codecFactory.release();
        }
    }

    private byte[] createTwoLargeRowGroupFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("payload")
            .named("fallback_barrier_schema");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(outputStream))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false)
                .withRowGroupSize(1)
                .withRowGroupRowCountLimit(1)
                .withPageSize(10 * 1024 * 1024)
                .build()
        ) {
            for (long id : new long[] { 0L, 1_000L }) {
                // Each row must exceed SHALLOW_PREFETCH_BYTES so computePrefetchDepth queues both
                // row groups and the barrier can hold one while the other falls back.
                byte[] payload = new byte[9 * 1024 * 1024];
                payload[0] = (byte) id;
                writer.write(groupFactory.newGroup().append("id", id).append("payload", Binary.fromConstantByteArray(payload)));
            }
        }
        return outputStream.toByteArray();
    }

    private byte[] createMultiRowGroupFile(MessageType schema, int rowCount, int rowGroupSize) throws IOException {
        return createMultiRowGroupFile(schema, rowCount, rowGroupSize, CompressionCodecName.UNCOMPRESSED);
    }

    private byte[] createMultiRowGroupFile(MessageType schema, int rowCount, int rowGroupSize, CompressionCodecName codec)
        throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        String[] columns = new String[schema.getFieldCount()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = schema.getFieldName(i);
        }
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(codec)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(256)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = groupFactory.newGroup();
                for (String col : columns) {
                    switch (schema.getType(col).asPrimitiveType().getPrimitiveTypeName()) {
                        case INT64 -> g.add(col, (long) i);
                        case INT32 -> g.add(col, i * 10);
                        default -> throw new IllegalArgumentException("Unsupported type");
                    }
                }
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

            @Override
            public String getPath() {
                return "memory://breaker-test.parquet";
            }
        };
    }
}
