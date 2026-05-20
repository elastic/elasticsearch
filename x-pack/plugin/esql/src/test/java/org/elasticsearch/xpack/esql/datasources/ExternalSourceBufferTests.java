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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link ExternalSourceBuffer} backpressure via {@link ExternalSourceBuffer#waitForSpace()}.
 */
public class ExternalSourceBufferTests extends ESTestCase {

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
        ExternalSourceBuffer buffer = new ExternalSourceBuffer(1024 * 1024);
        SubscribableListener<Void> space = buffer.waitForSpace();
        assertTrue("waitForSpace should be immediately done when buffer is empty", space.isDone());
        buffer.finish(true);
    }

    public void testWaitForSpaceReturnsPendingWhenBufferFull() {
        long maxBufferBytes = 1500;
        ExternalSourceBuffer buffer = new ExternalSourceBuffer(maxBufferBytes);

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
        ExternalSourceBuffer buffer = new ExternalSourceBuffer(maxBufferBytes);

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
        ExternalSourceBuffer buffer = new ExternalSourceBuffer(maxBufferBytes);

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
     * Regression test for a lost-wakeup race between {@link ExternalSourceBuffer#addPage(Page)}
     * and {@link ExternalSourceBuffer#pollPage()}.
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
            ExternalSourceBuffer buffer = new ExternalSourceBuffer(maxBufferBytes);
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

    public void testFormatReaderStatusGetterMatchesLastRecorded() {
        ExternalSourceBuffer buffer = new ExternalSourceBuffer(1024);
        assertEquals(Map.of(), buffer.formatReaderStatus());

        buffer.recordFormatReaderStatus(Map.of("row_groups_read", 3L));
        assertEquals(Map.of("row_groups_read", 3L), buffer.formatReaderStatus());

        // Latest snapshot replaces (does not merge) the prior one.
        buffer.recordFormatReaderStatus(Map.of("row_groups_read", 5L, "rows_filtered", 17L));
        assertEquals(Map.of("row_groups_read", 5L, "rows_filtered", 17L), buffer.formatReaderStatus());

        // Null is normalized to an empty map.
        buffer.recordFormatReaderStatus(null);
        assertEquals(Map.of(), buffer.formatReaderStatus());
    }

    public void testBytesReadAccumulatesPositiveDeltas() {
        ExternalSourceBuffer buffer = new ExternalSourceBuffer(1024);
        assertEquals(0L, buffer.bytesRead());

        buffer.addBytesRead(100);
        buffer.addBytesRead(250);
        assertEquals(350L, buffer.bytesRead());

        // Non-positive deltas are ignored.
        buffer.addBytesRead(0);
        buffer.addBytesRead(-50);
        assertEquals(350L, buffer.bytesRead());
    }

    public void testSplitTrackingTriplet() {
        ExternalSourceBuffer buffer = new ExternalSourceBuffer(1024);
        assertEquals(0, buffer.splitsTotal());
        assertEquals(0, buffer.splitsProcessed());
        assertEquals(0, buffer.currentSplit());

        buffer.setSplitsTotal(4);
        assertEquals(4, buffer.splitsTotal());

        buffer.setCurrentSplit(1);
        buffer.incSplitsProcessed();
        assertEquals(1, buffer.currentSplit());
        assertEquals(1, buffer.splitsProcessed());

        buffer.setCurrentSplit(2);
        buffer.incSplitsProcessed();
        assertEquals(2, buffer.currentSplit());
        assertEquals(2, buffer.splitsProcessed());
    }
}
