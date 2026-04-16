/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link ExternalSourceDrainUtils} verifying error handling, backpressure,
 * and edge cases during page draining.
 */
public class ExternalSourceDrainUtilsTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

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

    /**
     * Iterator that throws after delivering a configurable number of pages.
     * The exception can be thrown from either {@code hasNext()} or {@code next()}.
     */
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

    public void testDrainPagesSimple() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        List<Page> pages = List.of(createTestPage(1, 10), createTestPage(1, 10), createTestPage(1, 10));

        ExternalSourceDrainUtils.drainPages(iteratorOf(pages), buffer);
        assertEquals(3, buffer.size());

        buffer.finish(true);
    }

    public void testDrainRespectsPagesBackpressure() throws Exception {
        int totalPages = 20;
        // ~3 pages worth of bytes: each page is 2 cols × 50 ints ≈ 400+ bytes
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        List<Page> sourcePages = new ArrayList<>();
        for (int i = 0; i < totalPages; i++) {
            sourcePages.add(createTestPage(2, 50));
        }

        AtomicReference<Exception> drainError = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(2);

        Thread drainThread = new Thread(() -> {
            try {
                barrier.await();
                ExternalSourceDrainUtils.drainPages(iteratorOf(sourcePages), buffer);
                buffer.finish(false);
            } catch (Exception e) {
                drainError.set(e);
                buffer.onFailure(e);
            }
        });

        Thread consumeThread = new Thread(() -> {
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

        drainThread.start();
        consumeThread.start();

        drainThread.join(30_000);
        consumeThread.join(30_000);

        assertNull("Drain should not throw", drainError.get());
    }

    public void testDrainPagesWithBudgetRespectsRowLimit() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        int totalRows = ExternalSourceDrainUtils.drainPagesWithBudget(iteratorOf(pages), buffer, 25);
        assertEquals(30, totalRows);
        assertEquals(3, buffer.size());

        buffer.finish(true);
    }

    public void testDrainPagesIteratorThrowsOnNext() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        int succeedCount = between(1, 3);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        try {
            RuntimeException ex = expectThrows(
                RuntimeException.class,
                () -> ExternalSourceDrainUtils.drainPages(faultingIterator(pages, succeedCount, false), buffer)
            );
            assertTrue(ex.getCause().getMessage().contains("simulated S3 read failure on next"));
            assertEquals(succeedCount, buffer.size());
        } finally {
            buffer.finish(true);
        }
    }

    public void testDrainPagesIteratorThrowsOnHasNext() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        int succeedCount = between(1, 3);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        try {
            RuntimeException ex = expectThrows(
                RuntimeException.class,
                () -> ExternalSourceDrainUtils.drainPages(faultingIterator(pages, succeedCount, true), buffer)
            );
            assertTrue(ex.getCause().getMessage().contains("simulated S3 read failure on hasNext"));
            assertEquals(succeedCount, buffer.size());
        } finally {
            buffer.finish(true);
        }
    }

    public void testDrainPagesBufferCancelledMidDrain() throws Exception {
        int totalPages = 20;
        // ~2 pages worth of bytes: each page is 2 cols × 50 ints ≈ 400+ bytes
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

        AtomicReference<Exception> drainError = new AtomicReference<>();

        Thread drainThread = new Thread(() -> {
            try {
                ExternalSourceDrainUtils.drainPages(signalingIterator, buffer);
            } catch (Exception e) {
                drainError.set(e);
            }
        });

        drainThread.start();
        drainStarted.await();
        buffer.finish(true);

        drainThread.join(10_000);
        assertFalse("Drain thread should have exited", drainThread.isAlive());
        assertNull("Drain should exit cleanly when buffer is cancelled, but got: " + drainError.get(), drainError.get());
    }

    public void testDrainEmptyIterator() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

        ExternalSourceDrainUtils.drainPages(iteratorOf(List.of()), buffer);
        assertEquals(0, buffer.size());

        buffer.finish(true);
    }

    public void testDrainPagesWithBudgetThrowsMidDrain() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        try {
            RuntimeException ex = expectThrows(
                RuntimeException.class,
                () -> ExternalSourceDrainUtils.drainPagesWithBudget(faultingIterator(pages, 2, false), buffer, 100)
            );
            assertTrue(ex.getCause().getMessage().contains("simulated S3 read failure on next"));
            assertEquals(2, buffer.size());
        } finally {
            buffer.finish(true);
        }
    }

    public void testDrainPagesWithExplicitTimeout() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = List.of(createTestPage(1, 10), createTestPage(1, 10));

        ExternalSourceDrainUtils.drainPages(iteratorOf(pages), buffer, TimeValue.timeValueMinutes(1));
        assertEquals(2, buffer.size());

        buffer.finish(true);
    }

    public void testDrainPagesWithBudgetAndExplicitTimeout() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        int totalRows = ExternalSourceDrainUtils.drainPagesWithBudget(iteratorOf(pages), buffer, 25, TimeValue.timeValueMinutes(1));
        assertEquals(30, totalRows);
        assertEquals(3, buffer.size());

        buffer.finish(true);
    }

    public void testDefaultDrainTimeoutConstant() {
        assertEquals(TimeValue.timeValueMinutes(5), ExternalSourceDrainUtils.DEFAULT_DRAIN_TIMEOUT);
    }
}
