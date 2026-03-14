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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link ExternalSourceDrainUtils} verifying byte-based backpressure
 * during page draining.
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

    public void testDrainRespectsByteBackpressure() throws Exception {
        int totalPages = 20;
        Page samplePage = createTestPage(2, 50);
        long singlePageBytes = samplePage.ramBytesUsedByBlocks();
        samplePage.releaseBlocks();

        long maxBufferBytes = singlePageBytes * 3;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        List<Page> sourcePages = new ArrayList<>();
        for (int i = 0; i < totalPages; i++) {
            sourcePages.add(createTestPage(2, 50));
        }

        CloseableIterator<Page> iterator = new CloseableIterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < sourcePages.size();
            }

            @Override
            public Page next() {
                if (index >= sourcePages.size()) {
                    throw new NoSuchElementException();
                }
                Page page = sourcePages.get(index++);
                page.allowPassingToDifferentDriver();
                return page;
            }

            @Override
            public void close() {}
        };

        AtomicLong maxObservedBytes = new AtomicLong(0);
        AtomicReference<Exception> drainError = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(2);

        Thread drainThread = new Thread(() -> {
            try {
                barrier.await();
                ExternalSourceDrainUtils.drainPages(iterator, buffer);
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
                        long currentBytes = buffer.bytesInBuffer();
                        maxObservedBytes.updateAndGet(prev -> Math.max(prev, currentBytes));
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
        assertTrue(
            "Max observed bytes ("
                + maxObservedBytes.get()
                + ") should be bounded by maxBufferBytes + one page ("
                + (maxBufferBytes + singlePageBytes)
                + ")",
            maxObservedBytes.get() <= maxBufferBytes + singlePageBytes
        );
    }

    public void testDrainPagesSimple() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        List<Page> pages = List.of(createTestPage(1, 10), createTestPage(1, 10), createTestPage(1, 10));

        CloseableIterator<Page> iterator = new CloseableIterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < pages.size();
            }

            @Override
            public Page next() {
                if (index >= pages.size()) throw new NoSuchElementException();
                return pages.get(index++);
            }

            @Override
            public void close() {}
        };

        ExternalSourceDrainUtils.drainPages(iterator, buffer);
        assertEquals(3, buffer.size());
        assertTrue(buffer.bytesInBuffer() > 0);

        buffer.finish(true);
    }

    public void testDrainPagesWithBudgetRespectsRowLimit() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(createTestPage(1, 10));
        }

        CloseableIterator<Page> iterator = new CloseableIterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < pages.size();
            }

            @Override
            public Page next() {
                if (index >= pages.size()) throw new NoSuchElementException();
                return pages.get(index++);
            }

            @Override
            public void close() {}
        };

        int totalRows = ExternalSourceDrainUtils.drainPagesWithBudget(iterator, buffer, 25);
        assertEquals(30, totalRows);
        assertEquals(3, buffer.size());

        buffer.finish(true);
    }
}
