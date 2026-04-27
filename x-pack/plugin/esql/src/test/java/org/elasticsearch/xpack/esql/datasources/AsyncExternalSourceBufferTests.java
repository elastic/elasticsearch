/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link AsyncExternalSourceBuffer} backpressure via {@link AsyncExternalSourceBuffer#waitForSpace()}.
 */
public class AsyncExternalSourceBufferTests extends ESTestCase {

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

    public void testWaitForSpaceReturnsCompletedWhenBufferHasRoom() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        SubscribableListener<Void> space = buffer.waitForSpace();
        assertTrue("waitForSpace should be immediately done when buffer is empty", space.isDone());
        buffer.finish(true);
    }

    public void testWaitForSpaceReturnsPendingWhenBufferFull() {
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        while (buffer.bytesInBuffer() < maxBufferBytes) {
            buffer.addPage(createTestPage(2, 50));
        }
        assertTrue(buffer.bytesInBuffer() >= maxBufferBytes);

        SubscribableListener<Void> space = buffer.waitForSpace();
        assertFalse("waitForSpace should NOT be done when buffer is full", space.isDone());

        buffer.pollPage().releaseBlocks();
        assertTrue("waitForSpace should complete after pollPage frees space", space.isDone());

        buffer.finish(true);
    }

    public void testWaitForSpaceCompletesOnFinish() {
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        while (buffer.bytesInBuffer() < maxBufferBytes) {
            buffer.addPage(createTestPage(2, 50));
        }

        SubscribableListener<Void> space = buffer.waitForSpace();
        assertFalse(space.isDone());

        buffer.finish(true);
        assertTrue("waitForSpace should complete when buffer is finished (cancelled)", space.isDone());
    }

    public void testBufferConsistentAfterFullAndDrain() {
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        long expectedBytes = 0;
        while (buffer.bytesInBuffer() < maxBufferBytes) {
            Page p = createTestPage(2, 50);
            expectedBytes += p.ramBytesUsedByBlocks();
            buffer.addPage(p);
        }
        assertTrue(buffer.bytesInBuffer() >= maxBufferBytes);
        int sizeBeforeWait = buffer.size();

        assertEquals(sizeBeforeWait, buffer.size());
        assertEquals(expectedBytes, buffer.bytesInBuffer());
        assertFalse(buffer.noMoreInputs());

        for (int i = 0; i < sizeBeforeWait; i++) {
            Page p = buffer.pollPage();
            assertNotNull(p);
            p.releaseBlocks();
        }
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.bytesInBuffer());
        buffer.finish(true);
    }

    /**
     * Regression test for a lost-wakeup race between {@link AsyncExternalSourceBuffer#addPage(Page)}
     * and {@link AsyncExternalSourceBuffer#pollPage()}.
     * <p>
     * The prior implementation guarded {@code notifyNotEmpty()} on a snapshot of {@code bytesInBuffer}
     * taken BEFORE the page was inserted into the queue, and guarded {@code notifyNotFull()} on a
     * threshold-crossing check using that same snapshot. A consumer that drained the queue and
     * installed a {@code notEmptyFuture} in the tiny window between a producer's {@code getAndAdd}
     * and {@code queue.add} would be orphaned — the producer would see {@code prevBytes != 0} and
     * skip the wakeup. On buffers with a low max capacity this deadlocks both sides.
     * <p>
     * This test runs tens of thousands of add/poll interleavings against a small buffer. Pre-fix it
     * reliably hangs; post-fix it completes in well under the safety deadline.
     */
    public void testNoLostWakeupUnderConcurrentAddAndPoll() throws Exception {
        // Tight buffer to force frequent back-pressure flips. Each test page is ~224 B (2 cols x 1 row
        // IntBlocks), so 3 * 8224 comfortably fits a handful of pages, with producer regularly waiting.
        final long maxBufferBytes = 3L * 8224;
        final int iterations = 50;
        final int pagesPerIteration = 5_000;
        final long deadlineNanos = TimeUnit.SECONDS.toNanos(30);

        for (int iter = 0; iter < iterations; iter++) {
            AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);
            AtomicInteger pagesAdded = new AtomicInteger();
            AtomicInteger pagesConsumed = new AtomicInteger();
            AtomicReference<Throwable> producerError = new AtomicReference<>();

            Thread producer = new Thread(() -> {
                try {
                    for (int i = 0; i < pagesPerIteration; i++) {
                        SubscribableListener<Void> space = buffer.waitForSpace();
                        if (space.isDone() == false) {
                            PlainActionFuture<Void> fut = new PlainActionFuture<>();
                            space.addListener(fut);
                            fut.actionGet(TimeValue.timeValueSeconds(10));
                        }
                        buffer.addPage(createTestPage(2, 1));
                        pagesAdded.incrementAndGet();
                    }
                    buffer.finish(false);
                } catch (Throwable t) {
                    producerError.set(t);
                }
            }, "async-buffer-producer");
            producer.setDaemon(true);
            producer.start();

            long deadline = System.nanoTime() + deadlineNanos;
            while (buffer.isFinished() == false) {
                Page p = buffer.pollPage();
                if (p != null) {
                    p.releaseBlocks();
                    pagesConsumed.incrementAndGet();
                } else {
                    IsBlockedResult blk = buffer.waitForReading();
                    if (blk.listener().isDone() == false) {
                        PlainActionFuture<Void> fut = new PlainActionFuture<>();
                        blk.listener().addListener(fut);
                        try {
                            fut.actionGet(TimeValue.timeValueSeconds(10));
                        } catch (Exception e) {
                            throw new AssertionError(
                                "consumer stuck waiting for data (lost wakeup) iter="
                                    + iter
                                    + " addedSoFar="
                                    + pagesAdded.get()
                                    + " consumedSoFar="
                                    + pagesConsumed.get()
                                    + " queueSize="
                                    + buffer.size()
                                    + " bytesInBuffer="
                                    + buffer.bytesInBuffer(),
                                e
                            );
                        }
                    }
                }
                if (System.nanoTime() > deadline) {
                    throw new AssertionError(
                        "test deadline exceeded iter="
                            + iter
                            + " added="
                            + pagesAdded.get()
                            + " consumed="
                            + pagesConsumed.get()
                            + " queueSize="
                            + buffer.size()
                            + " bytesInBuffer="
                            + buffer.bytesInBuffer()
                    );
                }
            }

            producer.join(TimeUnit.SECONDS.toMillis(5));
            assertFalse("producer thread should have exited", producer.isAlive());
            assertNull("producer threw", producerError.get());
            assertEquals("all pages should have been added", pagesPerIteration, pagesAdded.get());
            assertEquals("all pages should have been consumed", pagesPerIteration, pagesConsumed.get());
            assertEquals(0, buffer.size());
            assertEquals(0, buffer.bytesInBuffer());
        }
    }
}
