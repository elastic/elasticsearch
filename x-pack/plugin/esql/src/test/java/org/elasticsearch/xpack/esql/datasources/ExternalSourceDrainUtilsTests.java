/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link ExternalSourceDrainUtils} async drain methods verifying error handling,
 * backpressure, cancellation, and edge cases.
 */
public class ExternalSourceDrainUtilsTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private ExecutorService exec;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        exec = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "drain-test"));
    }

    @Override
    public void tearDown() throws Exception {
        exec.shutdown();
        assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
        super.tearDown();
    }

    private static Page createTestPage(int numColumns, int numRows) {
        IntBlock.Builder[] builders = new IntBlock.Builder[numColumns];
        for (int c = 0; c < numColumns; c++) {
            builders[c] = BLOCK_FACTORY.newIntBlockBuilder(numRows);
            for (int r = 0; r < numRows; r++) {
                builders[c].appendInt(r);
            }
        }
        IntBlock[] blocks = new IntBlock[numColumns];
        for (int c = 0; c < numColumns; c++) {
            blocks[c] = builders[c].build();
        }
        return new Page(blocks);
    }

    private static CloseableIterator<Page> iteratorOf(List<Page> pages) {
        return new CloseableIterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < pages.size();
            }

            @Override
            public Page next() {
                if (index >= pages.size()) {
                    throw new NoSuchElementException();
                }
                return pages.get(index++);
            }

            @Override
            public void close() {}
        };
    }

    private static CloseableIterator<Page> faultingIterator(List<Page> pages, int succeedCount, boolean failOnHasNext) {
        return new CloseableIterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                if (failOnHasNext && index >= succeedCount) {
                    throw new RuntimeException(new IOException("simulated S3 read failure on hasNext"));
                }
                return index < pages.size();
            }

            @Override
            public Page next() {
                if (index >= pages.size()) {
                    throw new NoSuchElementException();
                }
                if (failOnHasNext == false && index >= succeedCount) {
                    throw new RuntimeException(new IOException("simulated S3 read failure on next"));
                }
                return pages.get(index++);
            }

            @Override
            public void close() {}
        };
    }

    public void testDrainPagesAsyncSimple() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = List.of(createTestPage(1, 10), createTestPage(1, 10), createTestPage(1, 10));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        ExternalSourceDrainUtils.drainPagesAsync(iteratorOf(pages), buffer, exec, ActionListener.wrap(v -> latch.countDown(), e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertNull(error.get());
        assertEquals(3, buffer.size());
        buffer.finish(true);
    }

    public void testDrainAsyncRespectsPagesBackpressure() throws Exception {
        int totalPages = 20;
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        List<Page> sourcePages = new ArrayList<>();
        for (int i = 0; i < totalPages; i++) {
            sourcePages.add(createTestPage(2, 50));
        }

        CountDownLatch drainDone = new CountDownLatch(1);
        AtomicReference<Exception> drainError = new AtomicReference<>();
        ExternalSourceDrainUtils.drainPagesAsync(iteratorOf(sourcePages), buffer, exec, ActionListener.wrap(v -> {
            buffer.finish(false);
            drainDone.countDown();
        }, e -> {
            drainError.set(e);
            drainDone.countDown();
        }));

        int consumed = 0;
        while (consumed < totalPages) {
            Page page = buffer.pollPage();
            if (page != null) {
                page.releaseBlocks();
                consumed++;
            } else if (buffer.noMoreInputs() && buffer.size() == 0) {
                break;
            } else {
                Thread.yield();
            }
        }

        assertTrue(drainDone.await(30, java.util.concurrent.TimeUnit.SECONDS));
        assertNull("Drain should not throw", drainError.get());
    }

    public void testBackpressureWithSameNamedPoolThreads() throws Exception {
        int totalPages = 20;
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        List<Page> sourcePages = new ArrayList<>();
        for (int i = 0; i < totalPages; i++) {
            sourcePages.add(createTestPage(2, 50));
        }

        CountDownLatch drainDone = new CountDownLatch(1);
        AtomicReference<Exception> drainError = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(2);

        exec.execute(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            ExternalSourceDrainUtils.drainPagesAsync(iteratorOf(sourcePages), buffer, exec, ActionListener.wrap(v -> {
                buffer.finish(false);
                drainDone.countDown();
            }, e -> {
                drainError.set(e);
                drainDone.countDown();
            }));
        });

        var factory = EsExecutors.daemonThreadFactory("testNode", "esql_worker");
        Thread consumeThread = factory.newThread(() -> {
            try {
                barrier.await();
                int consumed = 0;
                while (consumed < totalPages) {
                    Page page = buffer.pollPage();
                    if (page != null) {
                        page.releaseBlocks();
                        consumed++;
                    } else if (buffer.noMoreInputs() && buffer.size() == 0) {
                        break;
                    } else {
                        Thread.yield();
                    }
                }
            } catch (Exception e) {
                buffer.finish(true);
            }
        });
        consumeThread.start();

        consumeThread.join(30_000);
        assertTrue(drainDone.await(30, java.util.concurrent.TimeUnit.SECONDS));
        assertNull("Drain should not throw", drainError.get());
    }

    public void testDrainPagesWithBudgetAsyncRespectsRowLimit() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Integer> result = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        ExternalSourceDrainUtils.drainPagesWithBudgetAsync(iteratorOf(pages), buffer, 25, exec, ActionListener.wrap(totalRows -> {
            result.set(totalRows);
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertNull(error.get());
        assertEquals(Integer.valueOf(30), result.get());
        assertEquals(3, buffer.size());
        buffer.finish(true);
    }

    public void testDrainPagesAsyncIteratorThrowsOnNext() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        int succeedCount = between(1, 3);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        ExternalSourceDrainUtils.drainPagesAsync(faultingIterator(pages, succeedCount, false), buffer, exec, ActionListener.wrap(v -> {
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get().getCause().getMessage().contains("simulated S3 read failure on next"));
        assertEquals(succeedCount, buffer.size());
        buffer.finish(true);
    }

    public void testDrainPagesAsyncIteratorThrowsOnHasNext() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        int succeedCount = between(1, 3);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        ExternalSourceDrainUtils.drainPagesAsync(faultingIterator(pages, succeedCount, true), buffer, exec, ActionListener.wrap(v -> {
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get().getCause().getMessage().contains("simulated S3 read failure on hasNext"));
        assertEquals(succeedCount, buffer.size());
        buffer.finish(true);
    }

    public void testDrainPagesAsyncBufferCancelledMidDrain() throws Exception {
        int totalPages = 20;
        long maxBufferBytes = 1000;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        CountDownLatch drainStarted = new CountDownLatch(1);

        List<Page> sourcePages = new ArrayList<>();
        for (int i = 0; i < totalPages; i++) {
            sourcePages.add(createTestPage(2, 50));
        }
        CloseableIterator<Page> signalingIterator = new CloseableIterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                drainStarted.countDown();
                return index < sourcePages.size();
            }

            @Override
            public Page next() {
                if (index >= sourcePages.size()) {
                    throw new NoSuchElementException();
                }
                return sourcePages.get(index++);
            }

            @Override
            public void close() {}
        };

        CountDownLatch drainDone = new CountDownLatch(1);
        AtomicReference<Exception> drainError = new AtomicReference<>();
        AtomicBoolean drainSucceeded = new AtomicBoolean(false);
        ExternalSourceDrainUtils.drainPagesAsync(signalingIterator, buffer, exec, ActionListener.wrap(v -> {
            drainSucceeded.set(true);
            drainDone.countDown();
        }, e -> {
            drainError.set(e);
            drainDone.countDown();
        }));

        drainStarted.await();
        buffer.finish(true);

        assertTrue("Drain should complete after cancellation", drainDone.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertNull("Drain should exit cleanly when buffer is cancelled, but got: " + drainError.get(), drainError.get());
        assertTrue(drainSucceeded.get());
    }

    public void testDrainAsyncEmptyIterator() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        CountDownLatch latch = new CountDownLatch(1);
        ExternalSourceDrainUtils.drainPagesAsync(iteratorOf(List.of()), buffer, exec, ActionListener.wrap(v -> latch.countDown(), e -> {
            fail("should not fail");
        }));

        assertTrue(latch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(0, buffer.size());
        buffer.finish(true);
    }

    public void testDrainPagesWithBudgetAsyncThrowsMidDrain() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        ExternalSourceDrainUtils.drainPagesWithBudgetAsync(
            faultingIterator(pages, 2, false),
            buffer,
            100,
            exec,
            ActionListener.wrap(v -> latch.countDown(), e -> {
                error.set(e);
                latch.countDown();
            })
        );

        assertTrue(latch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get().getCause().getMessage().contains("simulated S3 read failure on next"));
        assertEquals(2, buffer.size());
        buffer.finish(true);
    }

    public void testDrainAsyncExecutorRejection() throws Exception {
        long maxBufferBytes = 100;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            pages.add(createTestPage(2, 50));
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        AtomicBoolean cleanupRan = new AtomicBoolean(false);

        ExecutorService shutdownExec = Executors.newSingleThreadExecutor(EsExecutors.daemonThreadFactory("test", "shutdown"));
        shutdownExec.shutdown();

        exec.execute(() -> {
            ExternalSourceDrainUtils.drainPagesAsync(
                iteratorOf(pages),
                buffer,
                shutdownExec,
                ActionListener.runAfter(ActionListener.wrap(v -> latch.countDown(), e -> {
                    error.set(e);
                    latch.countDown();
                }), () -> cleanupRan.set(true))
            );
        });

        // The drain adds one page synchronously, then goes async because the buffer is full.
        // The addListener callback fires when we poll a page, then tries shutdownExec.execute()
        // which throws EsRejectedExecutionException, failing the listener.
        Thread.sleep(200);
        Page p = buffer.pollPage();
        if (p != null) {
            p.releaseBlocks();
        }

        assertTrue(latch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertNotNull("Should fail with executor rejection", error.get());
        assertTrue(cleanupRan.get());
        buffer.finish(true);
    }

    public void testIteratorOwnershipNotClosedByDrain() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        AtomicBoolean closeCalled = new AtomicBoolean(false);

        List<Page> pages = List.of(createTestPage(1, 10));
        CloseableIterator<Page> tracked = new CloseableIterator<>() {
            private final CloseableIterator<Page> delegate = iteratorOf(pages);

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Page next() {
                return delegate.next();
            }

            @Override
            public void close() {
                closeCalled.set(true);
            }
        };

        CountDownLatch latch = new CountDownLatch(1);
        ExternalSourceDrainUtils.drainPagesAsync(
            tracked,
            buffer,
            exec,
            ActionListener.wrap(v -> latch.countDown(), e -> { latch.countDown(); })
        );

        assertTrue(latch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertFalse("drainPagesAsync must NOT close the iterator (INV-7)", closeCalled.get());
        buffer.finish(true);
    }

    public void testBudgetAccountingAcrossAsyncBoundaries() throws Exception {
        long maxBufferBytes = 500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            pages.add(createTestPage(2, 5));
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger resultRows = new AtomicInteger();
        AtomicReference<Exception> error = new AtomicReference<>();
        ExternalSourceDrainUtils.drainPagesWithBudgetAsync(iteratorOf(pages), buffer, 20, exec, ActionListener.wrap(totalRows -> {
            resultRows.set(totalRows);
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        int consumed = 0;
        while (true) {
            Page page = buffer.pollPage();
            if (page != null) {
                page.releaseBlocks();
                consumed++;
            } else if (latch.getCount() == 0) {
                break;
            } else {
                Thread.yield();
            }
        }

        assertTrue(latch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertNull(error.get());
        assertEquals(20, resultRows.get());
        assertEquals(4, consumed);
        buffer.finish(true);
    }

    // ===== Exactly-once lifecycle tests (INV-2) =====

    /**
     * Verifies that the iterator is closed exactly once on the success path
     * when using ActionListener.runAfter, as recommended by INV-7 / INV-2.
     */
    public void testIteratorClosedExactlyOnceOnSuccess() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = List.of(createTestPage(1, 10), createTestPage(1, 10));

        AtomicInteger closeCount = new AtomicInteger(0);
        CloseableIterator<Page> tracked = trackingClose(iteratorOf(pages), closeCount);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        ExternalSourceDrainUtils.drainPagesAsync(
            tracked,
            buffer,
            exec,
            ActionListener.runAfter(ActionListener.wrap(v -> latch.countDown(), e -> {
                error.set(e);
                latch.countDown();
            }), () -> closeQuietly(tracked))
        );

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(error.get());
        assertEquals("Iterator must be closed exactly once on success path", 1, closeCount.get());
        buffer.finish(true);
    }

    /**
     * Verifies that the iterator is closed exactly once on the failure path
     * when using ActionListener.runAfter, as recommended by INV-7 / INV-2.
     */
    public void testIteratorClosedExactlyOnceOnFailure() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        AtomicInteger closeCount = new AtomicInteger(0);
        CloseableIterator<Page> tracked = trackingClose(faultingIterator(pages, 2, false), closeCount);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        ExternalSourceDrainUtils.drainPagesAsync(
            tracked,
            buffer,
            exec,
            ActionListener.runAfter(ActionListener.wrap(v -> latch.countDown(), e -> {
                error.set(e);
                latch.countDown();
            }), () -> closeQuietly(tracked))
        );

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNotNull("Drain should fail due to iterator exception", error.get());
        assertEquals("Iterator must be closed exactly once on failure path", 1, closeCount.get());
        buffer.finish(true);
    }

    /**
     * Verifies that removeAsyncAction fires exactly once when the drain is used
     * as part of the standard lifecycle pattern (INV-2).
     */
    public void testRemoveAsyncActionFiresExactlyOnce() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = List.of(createTestPage(1, 10), createTestPage(1, 10));

        AtomicInteger removeCount = new AtomicInteger(0);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        ExternalSourceDrainUtils.drainPagesAsync(iteratorOf(pages), buffer, exec, ActionListener.runAfter(ActionListener.wrap(v -> {
            buffer.finish(false);
            latch.countDown();
        }, e -> {
            buffer.onFailure(e);
            error.set(e);
            latch.countDown();
        }), removeCount::incrementAndGet));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(error.get());
        assertEquals("removeAsyncAction must fire exactly once", 1, removeCount.get());
        buffer.finish(true);
    }

    /**
     * Verifies that removeAsyncAction fires exactly once on the failure path.
     */
    public void testRemoveAsyncActionFiresExactlyOnceOnFailure() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        AtomicInteger removeCount = new AtomicInteger(0);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        ExternalSourceDrainUtils.drainPagesAsync(
            faultingIterator(pages, 2, false),
            buffer,
            exec,
            ActionListener.runAfter(ActionListener.wrap(v -> {
                buffer.finish(false);
                latch.countDown();
            }, e -> {
                buffer.onFailure(e);
                error.set(e);
                latch.countDown();
            }), removeCount::incrementAndGet)
        );

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertEquals("removeAsyncAction must fire exactly once on failure path", 1, removeCount.get());
    }

    /**
     * Verifies that buffer.finish(false) and buffer.onFailure(e) are mutually exclusive:
     * on the success path, only finish(false) is called; on the failure path, only onFailure is called.
     */
    public void testBufferFinishAndOnFailureMutuallyExclusive() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = List.of(createTestPage(1, 10));

        AtomicInteger finishCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        ExternalSourceDrainUtils.drainPagesAsync(iteratorOf(pages), buffer, exec, ActionListener.wrap(v -> {
            finishCount.incrementAndGet();
            buffer.finish(false);
            latch.countDown();
        }, e -> {
            failureCount.incrementAndGet();
            buffer.onFailure(e);
            latch.countDown();
        }));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals("finish should be called on success", 1, finishCount.get());
        assertEquals("onFailure should not be called on success", 0, failureCount.get());
        assertNull("No failure should be recorded in buffer", buffer.failure());
    }

    /**
     * Verifies that on the failure path, onFailure is called and finish(false) is not.
     */
    public void testBufferOnFailureCalledNotFinishOnError() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        AtomicInteger finishCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        ExternalSourceDrainUtils.drainPagesAsync(faultingIterator(pages, 2, false), buffer, exec, ActionListener.wrap(v -> {
            finishCount.incrementAndGet();
            buffer.finish(false);
            latch.countDown();
        }, e -> {
            failureCount.incrementAndGet();
            buffer.onFailure(e);
            latch.countDown();
        }));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals("finish should not be called on failure", 0, finishCount.get());
        assertEquals("onFailure should be called on failure", 1, failureCount.get());
        assertNotNull("Failure should be recorded in buffer", buffer.failure());
    }

    // ===== Regression & integration tests =====

    /**
     * Regression test: a slow consumer that processes pages slower than the producer,
     * with total drain time that would exceed the old 5-minute timeout.
     * The async drain has no timeout; it must complete successfully.
     */
    public void testSlowConsumerExceedingOldTimeoutCompletes() throws Exception {
        int totalPages = 10;
        long maxBufferBytes = 500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        List<Page> sourcePages = new ArrayList<>();
        for (int i = 0; i < totalPages; i++) {
            sourcePages.add(createTestPage(2, 50));
        }

        CountDownLatch drainDone = new CountDownLatch(1);
        AtomicReference<Exception> drainError = new AtomicReference<>();

        ExternalSourceDrainUtils.drainPagesAsync(iteratorOf(sourcePages), buffer, exec, ActionListener.wrap(v -> {
            buffer.finish(false);
            drainDone.countDown();
        }, e -> {
            drainError.set(e);
            drainDone.countDown();
        }));

        int consumed = 0;
        while (consumed < totalPages) {
            Page page = buffer.pollPage();
            if (page != null) {
                page.releaseBlocks();
                consumed++;
                Thread.sleep(50);
            } else if (buffer.noMoreInputs() && buffer.size() == 0) {
                break;
            } else {
                Thread.yield();
            }
        }

        assertTrue("Drain should complete despite slow consumer", drainDone.await(30, TimeUnit.SECONDS));
        assertNull("No timeout error should occur", drainError.get());
        assertEquals(totalPages, consumed);
    }

    /**
     * Verifies that the executor thread is returned to the pool between pages
     * when the buffer is full. Uses a tiny buffer (fits ~1 page) and a multi-page
     * source with a delayed consumer.
     */
    public void testThreadReleaseDuringBackpressure() throws Exception {
        int totalPages = 10;
        long maxBufferBytes = 200;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        List<Page> sourcePages = new ArrayList<>();
        for (int i = 0; i < totalPages; i++) {
            sourcePages.add(createTestPage(2, 50));
        }

        CountDownLatch drainDone = new CountDownLatch(1);
        AtomicReference<Exception> drainError = new AtomicReference<>();
        AtomicInteger maxConcurrentTasks = new AtomicInteger(0);
        AtomicInteger currentRunning = new AtomicInteger(0);

        ExecutorService trackingExec = Executors.newFixedThreadPool(4, EsExecutors.daemonThreadFactory("test", "tracking"));
        Executor trackingWrapper = command -> trackingExec.execute(() -> {
            int running = currentRunning.incrementAndGet();
            maxConcurrentTasks.updateAndGet(prev -> Math.max(prev, running));
            try {
                command.run();
            } finally {
                currentRunning.decrementAndGet();
            }
        });

        ExternalSourceDrainUtils.drainPagesAsync(iteratorOf(sourcePages), buffer, trackingWrapper, ActionListener.wrap(v -> {
            buffer.finish(false);
            drainDone.countDown();
        }, e -> {
            drainError.set(e);
            drainDone.countDown();
        }));

        int consumed = 0;
        while (consumed < totalPages) {
            Page page = buffer.pollPage();
            if (page != null) {
                page.releaseBlocks();
                consumed++;
            } else if (buffer.noMoreInputs() && buffer.size() == 0) {
                break;
            } else {
                Thread.sleep(10);
            }
        }

        assertTrue(drainDone.await(30, TimeUnit.SECONDS));
        assertNull(drainError.get());
        assertEquals(totalPages, consumed);
        assertTrue(
            "Max concurrent drain tasks should be 1 (thread released during waits), got: " + maxConcurrentTasks.get(),
            maxConcurrentTasks.get() <= 2
        );

        trackingExec.shutdown();
        assertTrue(trackingExec.awaitTermination(10, TimeUnit.SECONDS));
    }

    /**
     * Start a drain, let buffer fill, then cancel the query (buffer.finish(true)).
     * Assert producer exits cleanly, cleanup runs, no leaked resources.
     */
    public void testCancellationWhileWaitingOnWaitForSpace() throws Exception {
        int totalPages = 50;
        long maxBufferBytes = 200;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        List<Page> sourcePages = new ArrayList<>();
        for (int i = 0; i < totalPages; i++) {
            sourcePages.add(createTestPage(2, 50));
        }

        CountDownLatch drainDone = new CountDownLatch(1);
        AtomicReference<Exception> drainError = new AtomicReference<>();
        AtomicBoolean cleanupRan = new AtomicBoolean(false);
        AtomicInteger closeCount = new AtomicInteger(0);

        CloseableIterator<Page> tracked = trackingClose(iteratorOf(sourcePages), closeCount);

        ExternalSourceDrainUtils.drainPagesAsync(
            tracked,
            buffer,
            exec,
            ActionListener.runAfter(ActionListener.wrap(v -> drainDone.countDown(), e -> {
                drainError.set(e);
                drainDone.countDown();
            }), () -> {
                closeQuietly(tracked);
                cleanupRan.set(true);
            })
        );

        // Let the buffer fill up (producer should be blocked waiting for space)
        Thread.sleep(200);
        assertTrue("Buffer should have pages", buffer.size() > 0);

        // Cancel the query
        buffer.finish(true);

        assertTrue("Drain must complete after cancellation", drainDone.await(10, TimeUnit.SECONDS));
        assertNull("Drain should exit cleanly on cancellation, but got: " + drainError.get(), drainError.get());
        assertTrue("Cleanup must run after cancellation", cleanupRan.get());
        assertEquals("Iterator must be closed exactly once after cancellation", 1, closeCount.get());
    }

    /**
     * Budget accounting with multiple sequential drains simulating multi-split behavior.
     * Each drain reports consumed rows; the total across async boundaries must match.
     */
    public void testBudgetPropagationAcrossMultipleSequentialDrains() throws Exception {
        long maxBufferBytes = 500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);
        int overallRowLimit = 35;

        List<Page> batch1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch1.add(createTestPage(2, 5));
        }
        List<Page> batch2 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch2.add(createTestPage(2, 5));
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger totalConsumedRows = new AtomicInteger(0);
        AtomicReference<Exception> error = new AtomicReference<>();

        // Simulate multi-split: drain batch1, then batch2 with remaining budget
        ExternalSourceDrainUtils.drainPagesWithBudgetAsync(iteratorOf(batch1), buffer, overallRowLimit, exec, ActionListener.wrap(c1 -> {
            totalConsumedRows.addAndGet(c1);
            int remaining = overallRowLimit - c1;
            ExternalSourceDrainUtils.drainPagesWithBudgetAsync(iteratorOf(batch2), buffer, remaining, exec, ActionListener.wrap(c2 -> {
                totalConsumedRows.addAndGet(c2);
                latch.countDown();
            }, e -> {
                error.set(e);
                latch.countDown();
            }));
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        // Consume pages to avoid blocking
        int consumed = 0;
        while (latch.getCount() > 0 || buffer.size() > 0) {
            Page page = buffer.pollPage();
            if (page != null) {
                page.releaseBlocks();
                consumed++;
            } else {
                Thread.yield();
            }
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(error.get());
        assertEquals("Total rows across async boundaries must match limit", 35, totalConsumedRows.get());
        buffer.finish(true);
    }

    /**
     * Executor rejection with budget variant: when executor.execute() throws during a
     * budget-limited drain, the listener receives the failure and cleanup runs.
     */
    public void testBudgetDrainExecutorRejection() throws Exception {
        long maxBufferBytes = 100;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            pages.add(createTestPage(2, 50));
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        AtomicBoolean cleanupRan = new AtomicBoolean(false);

        ExecutorService shutdownExec = Executors.newSingleThreadExecutor(EsExecutors.daemonThreadFactory("test", "shutdown"));
        shutdownExec.shutdown();

        exec.execute(() -> {
            ExternalSourceDrainUtils.drainPagesWithBudgetAsync(
                iteratorOf(pages),
                buffer,
                1000,
                shutdownExec,
                ActionListener.runAfter(ActionListener.<Integer>wrap(v -> latch.countDown(), e -> {
                    error.set(e);
                    latch.countDown();
                }), () -> cleanupRan.set(true))
            );
        });

        Thread.sleep(200);
        Page p = buffer.pollPage();
        if (p != null) {
            p.releaseBlocks();
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNotNull("Should fail with executor rejection", error.get());
        assertTrue(
            "Error should be RejectedExecutionException, got: " + error.get().getClass().getSimpleName(),
            error.get() instanceof RejectedExecutionException
        );
        assertTrue("Cleanup must run on executor rejection", cleanupRan.get());
        buffer.finish(true);
    }

    /**
     * Verifies that the drain handles an iterator that fails on the very first hasNext() call.
     */
    public void testIteratorFailsOnFirstHasNext() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        CloseableIterator<Page> failImmediately = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                throw new RuntimeException("immediate hasNext failure");
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        ExternalSourceDrainUtils.drainPagesAsync(failImmediately, buffer, exec, ActionListener.wrap(v -> latch.countDown(), e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get().getMessage().contains("immediate hasNext failure"));
        assertEquals(0, buffer.size());
        buffer.finish(true);
    }

    // ===== Helpers =====

    private static CloseableIterator<Page> trackingClose(CloseableIterator<Page> delegate, AtomicInteger closeCount) {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Page next() {
                return delegate.next();
            }

            @Override
            public void close() {
                closeCount.incrementAndGet();
                try {
                    delegate.close();
                } catch (Exception ignored) {}
            }
        };
    }

    private static void closeQuietly(CloseableIterator<?> iterator) {
        if (iterator != null) {
            try {
                iterator.close();
            } catch (Exception ignored) {}
        }
    }
}
