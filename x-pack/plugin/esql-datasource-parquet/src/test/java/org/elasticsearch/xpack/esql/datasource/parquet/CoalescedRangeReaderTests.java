/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.parquet.CoalescedRangeReader.ByteRange;
import org.elasticsearch.xpack.esql.datasource.parquet.CoalescedRangeReader.CoalescedRangeResult;
import org.elasticsearch.xpack.esql.datasource.parquet.CoalescedRangeReader.MergedRange;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

public class CoalescedRangeReaderTests extends ESTestCase {

    private CircuitBreaker breaker;
    private BufferAllocator allocator;
    private BlockFactory blockFactory;

    @Before
    public void initAllocator() {
        // A real (limited) breaker rather than a noop one: BlockFactory wires it into the Arrow
        // allocator via CircuitBreakerAllocationListener, so every coalesced buffer is charged on
        // allocate and uncharged on close. breaker.getUsed() is therefore the ground-truth leak
        // signal - a NoopCircuitBreaker reports 0 unconditionally and would hide a leak.
        breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        allocator = blockFactory.arrowAllocator();
    }

    @After
    public void assertNoOutstandingMemory() {
        // Global leak net: whatever each test did, no coalesced buffer may remain charged.
        assertEquals("circuit breaker still holds bytes at teardown", 0L, breaker.getUsed());
        assertEquals("allocator still holds bytes at teardown", 0L, allocator.getAllocatedMemory());
    }

    public void testMergeAdjacentRanges() {
        List<ByteRange> ranges = List.of(new ByteRange(0, 100), new ByteRange(100, 200), new ByteRange(300, 50));
        // gap=0 means adjacent ranges (gap==0) are merged; all three are contiguous
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 0);
        assertEquals(1, merged.size());
        assertEquals(0, merged.get(0).offset());
        assertEquals(350, merged.get(0).length());
        assertEquals(3, merged.get(0).constituents().size());
    }

    public void testMergeOverlappingRanges() {
        List<ByteRange> ranges = List.of(new ByteRange(0, 150), new ByteRange(100, 200));
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 0);
        assertEquals(1, merged.size());
        assertEquals(0, merged.get(0).offset());
        assertEquals(300, merged.get(0).length());
    }

    public void testMergeWithGapBelowThreshold() {
        List<ByteRange> ranges = List.of(new ByteRange(0, 100), new ByteRange(200, 100));
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 200);
        assertEquals(1, merged.size());
        assertEquals(0, merged.get(0).offset());
        assertEquals(300, merged.get(0).length());
    }

    public void testMergeWithGapAboveThreshold() {
        List<ByteRange> ranges = List.of(new ByteRange(0, 100), new ByteRange(200, 100));
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 50);
        assertEquals(2, merged.size());
    }

    public void testMergeSingleRange() {
        List<ByteRange> ranges = List.of(new ByteRange(500, 200));
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 1024);
        assertEquals(1, merged.size());
        assertEquals(500, merged.get(0).offset());
        assertEquals(200, merged.get(0).length());
    }

    public void testMergeUnsortedRanges() {
        List<ByteRange> ranges = List.of(new ByteRange(300, 50), new ByteRange(0, 100), new ByteRange(100, 200));
        // All three are contiguous [0,100) [100,300) [300,350) -> one merged range
        List<MergedRange> merged = CoalescedRangeReader.mergeRanges(ranges, 0);
        assertEquals(1, merged.size());
        assertEquals(0, merged.get(0).offset());
        assertEquals(350, merged.get(0).length());
    }

    public void testReadCoalescedParallelDispatch() throws Exception {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }

        AtomicInteger asyncCallCount = new AtomicInteger();
        StorageObject storageObject = new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) length);
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
                return StoragePath.of("memory://test.parquet");
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                asyncCallCount.incrementAndGet();
                StorageObject.super.readBytesAsync(position, length, factory, executor, listener);
            }
        };

        // [0,100) and [100,300) are adjacent; [500,600) has gap=200 which exceeds maxCoalesceGap=50
        List<ByteRange> ranges = List.of(new ByteRange(0, 100), new ByteRange(100, 200), new ByteRange(500, 100));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<CoalescedRangeResult> resultRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        CoalescedRangeReader.readCoalesced(storageObject, ranges, 50, allocator, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(CoalescedRangeResult result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(failureRef.get());

        CoalescedRangeResult coalescedResult = resultRef.get();
        assertNotNull(coalescedResult);
        Map<ByteRange, ByteBuffer> results = coalescedResult.ranges();
        Releasable release = coalescedResult.release();
        assertEquals(3, results.size());

        // Adjacent ranges [0,100) and [100,300) should be merged into one async call
        // Range [500,600) is separate -> 2 async calls total
        assertEquals(2, asyncCallCount.get());

        ByteBuffer buf0 = results.get(new ByteRange(0, 100));
        assertNotNull(buf0);
        assertEquals(100, buf0.remaining());
        assertEquals((byte) 0, buf0.get(0));

        ByteBuffer buf1 = results.get(new ByteRange(100, 200));
        assertNotNull(buf1);
        assertEquals(200, buf1.remaining());
        assertEquals((byte) 100, buf1.get(0));

        ByteBuffer buf2 = results.get(new ByteRange(500, 100));
        assertNotNull(buf2);
        assertEquals(100, buf2.remaining());
        assertEquals((byte) (500 & 0xFF), buf2.get(0));
        release.close();
    }

    public void testReadCoalescedEmptyRanges() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<CoalescedRangeResult> resultRef = new AtomicReference<>();

        // null StorageObject is safe here: the empty-ranges path returns before any I/O
        CoalescedRangeReader.readCoalesced(null, List.of(), 0, allocator, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(CoalescedRangeResult result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(resultRef.get());
        assertTrue(resultRef.get().ranges().isEmpty());
        resultRef.get().release().close();
    }

    public void testReadCoalescedFailure() throws Exception {
        StorageObject failingObject = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new IOException("test failure");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                throw new IOException("test failure");
            }

            @Override
            public long length() {
                return 1000;
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
                return StoragePath.of("memory://fail.parquet");
            }
        };

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        CoalescedRangeReader.readCoalesced(
            failingObject,
            List.of(new ByteRange(0, 100)),
            0,
            allocator,
            Runnable::run,
            new ActionListener<>() {
                @Override
                public void onResponse(CoalescedRangeResult result) {
                    result.release().close();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            }
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(failureRef.get());
        assertThat(failureRef.get().getMessage(), org.hamcrest.Matchers.containsString("test failure"));
    }

    /**
     * Slicing the far constituent of a coalesced buffer that only received a short read must throw
     * rather than hand out a truncated view. Here 100 bytes were requested but only 10 arrived, so
     * positioning at the second constituent (offset 90) runs past the delivered limit.
     */
    public void testSliceConstituentsShortReadThrows() {
        ByteBuffer shortBuffer = ByteBuffer.allocate(100);
        shortBuffer.position(0).limit(10);
        MergedRange mr = new MergedRange(0, 100, List.of(new ByteRange(0, 10), new ByteRange(90, 10)));
        Map<ByteRange, ByteBuffer> results = new HashMap<>();
        expectThrows(IllegalArgumentException.class, () -> CoalescedRangeReader.sliceConstituents(shortBuffer, mr, results));
    }

    /**
     * Gate for the hang/leak fix: when a merged-range read is delivered as a short read, slicing a
     * far constituent throws inside {@code onResponse}. The reader must fail the listener and return
     * the coalesced buffer's native memory to the allocator. Before the fix, the throw escaped
     * {@code onResponse}, the terminal {@code complete()} was skipped, the listener hung forever, and
     * the coalesced buffer stayed charged against the allocator.
     */
    public void testShortReadFailsListenerAndReleasesMemory() throws Exception {
        long breakerBaseline = breaker.getUsed();

        // [0,10) and [90,10) are 80 bytes apart; maxCoalesceGap=1024 merges them into a single
        // 100-byte range with two constituents.
        List<ByteRange> ranges = List.of(new ByteRange(0, 10), new ByteRange(90, 10));

        StorageObject shortReadObject = new StorageObject() {
            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException("async path only");
            }

            @Override
            public long length() {
                return 100;
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
                return StoragePath.of("memory://short.parquet");
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
                    final DirectReadBuffer drb;
                    try {
                        drb = factory.allocate((int) length);
                    } catch (IOException e) {
                        listener.onFailure(e);
                        return;
                    }
                    // Simulate a short read: only 10 of the requested bytes arrived.
                    drb.buffer().position(0).limit(10);
                    try {
                        listener.onResponse(drb);
                    } catch (Exception e) {
                        // Faithful to StorageObject#readBytesAsync: close the buffer and rethrow if the
                        // listener throws. With the fix, onResponse folds the slice failure internally
                        // and never throws, so this branch is not exercised.
                        drb.close();
                        throw e;
                    }
                });
            }
        };

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<CoalescedRangeResult> resultRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        CoalescedRangeReader.readCoalesced(shortReadObject, ranges, 1024, allocator, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(CoalescedRangeResult result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
                latch.countDown();
            }
        });

        assertTrue("listener never completed - short read hung the coalesced read", latch.await(10, TimeUnit.SECONDS));
        if (resultRef.get() != null) {
            resultRef.get().release().close();
        }
        assertNotNull(failureRef.get());
        assertThat(failureRef.get(), instanceOf(IllegalArgumentException.class));
        // The coalesced buffer was charged against the circuit breaker on allocate; the failure path
        // must uncharge it. Without the fix the throw skips complete(), the buffer stays charged, and
        // this assertion fails (the earlier hang is caught by the latch timeout above).
        assertEquals("coalesced buffer stayed charged to the breaker after the short-read failure", breakerBaseline, breaker.getUsed());
    }

    /**
     * Loop-pressure regression: across many iterations each merged range is randomly delivered as a
     * success, a short read (slice throws), or a backend failure (buffer already released). Whatever
     * the outcome, the allocator must return to its baseline once the coalesced result is released,
     * proving no path leaks a coalesced buffer. Uses a real thread pool so the completion race
     * between siblings is exercised; the pool is shut down in a finally so the test runner does not
     * flag a leaked thread.
     */
    public void testReadCoalescedReturnsToBaselineUnderInjectedFailures() throws Exception {
        long breakerBaseline = breaker.getUsed();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            int iterations = 256;
            for (int i = 0; i < iterations; i++) {
                // Two merged ranges: [0,10)+[90,10) (two constituents, short read can throw) and a far
                // [5000,10) (its own single-constituent merged range).
                List<ByteRange> ranges = List.of(new ByteRange(0, 10), new ByteRange(90, 10), new ByteRange(5000, 10));

                StorageObject injecting = new StorageObject() {
                    @Override
                    public InputStream newStream(long position, long length) {
                        throw new UnsupportedOperationException("async path only");
                    }

                    @Override
                    public long length() {
                        return 6000;
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
                        return StoragePath.of("memory://inject.parquet");
                    }

                    @Override
                    public void readBytesAsync(
                        long position,
                        long length,
                        DirectBufferFactory factory,
                        Executor exec,
                        ActionListener<DirectReadBuffer> listener
                    ) {
                        // Choose the outcome on the calling (test) thread so the shared randomness
                        // source is never touched from a pool thread.
                        int mode = randomIntBetween(0, 2);
                        exec.execute(() -> {
                            if (mode == 0) {
                                // Backend failure: the buffer was never handed out (or already released),
                                // matching the onFailure contract.
                                listener.onFailure(new IOException("injected backend failure"));
                                return;
                            }
                            final DirectReadBuffer drb;
                            try {
                                drb = factory.allocate((int) length);
                            } catch (IOException e) {
                                listener.onFailure(e);
                                return;
                            }
                            // mode == 1: full read; mode == 2: short read (only 10 bytes).
                            int delivered = mode == 1 ? (int) length : 10;
                            drb.buffer().position(0).limit(delivered);
                            try {
                                listener.onResponse(drb);
                            } catch (Exception e) {
                                drb.close();
                                throw e;
                            }
                        });
                    }
                };

                CountDownLatch latch = new CountDownLatch(1);
                AtomicReference<CoalescedRangeResult> resultRef = new AtomicReference<>();
                AtomicReference<Exception> failureRef = new AtomicReference<>();

                CoalescedRangeReader.readCoalesced(injecting, ranges, 1024, allocator, executor, new ActionListener<>() {
                    @Override
                    public void onResponse(CoalescedRangeResult result) {
                        resultRef.set(result);
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        failureRef.set(e);
                        latch.countDown();
                    }
                });

                assertTrue("iteration " + i + " never completed", latch.await(10, TimeUnit.SECONDS));
                CoalescedRangeResult result = resultRef.get();
                if (result != null) {
                    result.release().close();
                }
                assertEquals("iteration " + i + " leaked breaker-charged memory", breakerBaseline, breaker.getUsed());
            }
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }
}
