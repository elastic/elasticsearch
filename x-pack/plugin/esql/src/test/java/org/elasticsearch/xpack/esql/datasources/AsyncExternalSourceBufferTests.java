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
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonReaderStatus;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
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

    /**
     * Regression test for a leak where a page buffered before {@link AsyncExternalSourceBuffer#onFailure}
     * is never released.
     * <p>
     * {@code onFailure} deliberately leaves already-queued pages in place so the driver can drain them
     * via {@code getOutput()}/{@link AsyncExternalSourceBuffer#pollPage()} before the failure surfaces.
     * But {@link AsyncExternalSourceOperator#close()} always calls {@code finish(true)}, and the prior
     * implementation gated {@code AsyncExternalSourceBuffer#discardPages} behind the {@code noMoreInputs}
     * CAS transition — which {@code onFailure} had already performed, so {@code finish(true)} always lost
     * the race and skipped the discard. A close that arrives without the driver ever draining the queue
     * (e.g. cross-driver task cancellation cutting this operator's poll loop before it runs) leaked the
     * page's blocks forever.
     */
    public void testFinishAfterFailureStillDiscardsQueuedPages() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        Page page = createTestPage(2, 5);
        IntBlock block = page.getBlock(0);
        buffer.addPage(page);
        assertEquals(1, buffer.size());

        buffer.onFailure(new RuntimeException("simulated read failure"));
        assertEquals("onFailure must not itself discard: the driver may still drain via pollPage()", 1, buffer.size());
        assertFalse(block.isReleased());

        // Simulates AsyncExternalSourceOperator#close() -> finish() arriving without the driver ever
        // polling this page out (e.g. abrupt cancellation before the operator's own poll loop ran).
        boolean transitioned = buffer.finish(true);
        assertFalse("onFailure already performed the transition", transitioned);
        assertEquals("finish(true) must discard the page onFailure left queued", 0, buffer.size());
        assertEquals(0, buffer.bytesInBuffer());
        assertTrue("the page's blocks must be released, not leaked", block.isReleased());
    }

    /**
     * Companion to {@link #testFinishAfterFailureStillDiscardsQueuedPages} for the non-failure case: a
     * prior {@code finish(false)} (natural producer EOF, called before the driver drained every page)
     * must not prevent a later {@code finish(true)} — e.g. from {@code AsyncExternalSourceOperator#close()}
     * — from discarding whatever is still queued.
     */
    public void testFinishTrueAfterFinishFalseStillDiscardsQueuedPages() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        Page page = createTestPage(2, 5);
        IntBlock block = page.getBlock(0);
        buffer.addPage(page);
        assertEquals(1, buffer.size());

        // Producer reached natural EOF before the driver polled this page out.
        boolean firstTransitioned = buffer.finish(false);
        assertTrue(firstTransitioned);
        assertEquals(1, buffer.size());
        assertFalse(block.isReleased());

        boolean secondTransitioned = buffer.finish(true);
        assertFalse("finish(false) already performed the transition", secondTransitioned);
        assertEquals("finish(true) must discard the page finish(false) left queued", 0, buffer.size());
        assertEquals(0, buffer.bytesInBuffer());
        assertTrue("the page's blocks must be released, not leaked", block.isReleased());
    }

    public void testFormatReaderStatusGetterMatchesLastRecorded() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        assertNull(buffer.formatReaderStatus());

        buffer.recordFormatReaderStatus(new NdJsonReaderStatus(3L, 0L, 0L));
        assertEquals(new NdJsonReaderStatus(3L, 0L, 0L), buffer.formatReaderStatus());

        // Latest snapshot replaces (does not merge) the prior one.
        buffer.recordFormatReaderStatus(new NdJsonReaderStatus(5L, 17L, 0L));
        assertEquals(new NdJsonReaderStatus(5L, 17L, 0L), buffer.formatReaderStatus());

        // Null clears the recorded snapshot.
        buffer.recordFormatReaderStatus(null);
        assertNull(buffer.formatReaderStatus());
    }

    public void testBytesReadAccumulatesPositiveDeltas() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
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
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
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

    public void testRecordInformationalWarningDoesNotFlipPartial() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        buffer.recordInformationalWarning("null-filled row 3");
        assertFalse("an informational reader warning is not a partial-result signal", buffer.isPartial());
        assertEquals("null-filled row 3", buffer.pollWarning());
        assertNull(buffer.pollWarning());
    }

    public void testRecordInformationalWarningSharesQueueWithPartialResultsWarnings() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        buffer.recordWarning("truncated at max_record_size");
        buffer.recordInformationalWarning("null-filled row 3");
        assertTrue(buffer.isPartial());
        assertEquals("truncated at max_record_size", buffer.pollWarning());
        assertEquals("null-filled row 3", buffer.pollWarning());
        assertNull(buffer.pollWarning());
    }

    /**
     * The whole point of the central cap: many independent per-segment/per-chunk {@link SkipWarnings}
     * instances feeding the same buffer must not multiply the header count by the segment/chunk count.
     * <p>
     * Uses the same real-thread-contention pattern as {@link #testNoLostWakeupUnderConcurrentAddAndPoll}
     * rather than a sequential loop, so the cap is exercised under genuine concurrent access to the
     * shared counter — matching how independent parse-worker threads actually drive
     * {@link AsyncExternalSourceBuffer#recordInformationalWarning} for one chunk/segment each. Regression
     * coverage for the streaming per-chunk flood.
     */
    public void testRecordInformationalWarningAppliesOneGlobalCapAcrossManyCallers() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        // One thread per chunk, as 50 independent SkipWarnings instances would drive this method
        // concurrently on the streaming-parallel path.
        int chunks = 50;
        CyclicBarrier barrier = new CyclicBarrier(chunks);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread[] threads = new Thread[chunks];
        for (int c = 0; c < chunks; c++) {
            int chunk = c;
            threads[c] = new Thread(() -> {
                try {
                    barrier.await();
                    buffer.recordInformationalWarning("chunk " + chunk + " summary");
                    buffer.recordInformationalWarning("chunk " + chunk + " detail");
                } catch (Throwable t) {
                    error.set(t);
                }
            }, "informational-warning-chunk-" + chunk);
            threads[c].setDaemon(true);
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse("chunk thread should have exited", t.isAlive());
        }
        assertNull("chunk thread threw", error.get());

        List<String> drained = new ArrayList<>();
        String w;
        while ((w = buffer.pollWarning()) != null) {
            drained.add(w);
        }

        int maxInformationalWarnings = SkipWarnings.MAX_ADDED_WARNINGS + 2;
        assertEquals(
            "total lines must be bounded regardless of how many chunk threads raced to contribute",
            maxInformationalWarnings,
            drained.size()
        );
        assertTrue("the last line must note suppression", drained.get(drained.size() - 1).contains("further reader warnings suppressed"));
        assertFalse(buffer.isPartial());
    }

    /**
     * A hard-cutting {@code finish(true)} that wins the running→finishing transition (task cancel / async DELETE /
     * LIMIT teardown while the producer is still reading) must arm {@link AsyncExternalSourceBuffer#readCancelled()}
     * so the runtime read's {@code StorageRetryCancellation} scope aborts a parked storage backoff.
     */
    public void testFinishTrueArmsReadCancelledWhenTransitioning() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        assertFalse("a fresh buffer is not cancelled", buffer.readCancelled());
        assertTrue("first finish(true) makes the transition", buffer.finish(true));
        assertTrue("a transitioning draining finish is a hard cut and must arm read cancellation", buffer.readCancelled());
    }

    /**
     * Async STOP is {@code finish(false)}: it keeps buffered pages for a partial response and must NOT arm read
     * cancellation, otherwise an in-flight read would be aborted mid-backoff and the STOP contract (return what was
     * already produced) would degrade into a hard failure.
     */
    public void testFinishFalseDoesNotArmReadCancelled() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        assertTrue("first finish(false) makes the transition", buffer.finish(false));
        assertFalse("STOP must not arm read cancellation", buffer.readCancelled());
    }

    /**
     * Natural EOF: the producer's own {@code finish(false)} wins the transition, so the driver's later
     * {@code finish(true)} on operator close no longer transitions and must not be mistaken for a cancel.
     */
    public void testNaturalCompletionThenCloseDoesNotArmReadCancelled() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        assertTrue("producer's finish(false) wins the transition at natural EOF", buffer.finish(false));
        assertFalse("the driver's later close is a no-op transition and must not arm cancellation", buffer.finish(true));
        assertFalse("natural completion is not a cancel", buffer.readCancelled());
    }

    /**
     * A producer that fails on its own (real read error) sets {@code noMoreInputs} via {@link
     * AsyncExternalSourceBuffer#onFailure}; a subsequent close's {@code finish(true)} then loses the transition, so
     * read cancellation stays disarmed — there is no other in-flight read to abort.
     */
    public void testOnFailureThenCloseDoesNotArmReadCancelled() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        buffer.onFailure(new RuntimeException("boom"));
        assertFalse("close after a producer failure does not transition", buffer.finish(true));
        assertFalse("a producer's own failure is not a read cancellation", buffer.readCancelled());
    }

    /**
     * End-to-end coverage of the runtime producer read wiring. {@code AsyncExternalSourceOperatorFactory}'s
     * {@code openUnitThenDrain} / {@code drainCurrentUnit} / {@code startSyncWrapperRead} all install the buffer's
     * hard-cancel signal as the ambient {@link StorageRetryCancellation} scope exactly this way —
     * {@code callWithCancellation(buffer::readCancelled, <read>)} — so the read work is what this test reproduces
     * (the real factory adds only the two-executor producer loop around the same seam). A read parked in
     * retry/throttle backoff is modeled by {@link StorageRetryCancellation#sleepWithCancellationChecks}. A hard-cut
     * {@code finish(true)} (task cancel / async DELETE / LIMIT teardown) must arm {@code readCancelled()} so the
     * scoped backoff aborts promptly with {@link TaskCancelledException} instead of sleeping out its budget — the
     * long-lived post-cancel read this change fixes. The scope is the only thing that cuts a backoff already
     * entered; cooperative {@code noMoreInputs} only fires between pulls.
     */
    public void testHardCancelAbortsScopedReadBackoff() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        CountDownLatch readParked = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        long backoffMillis = TimeUnit.SECONDS.toMillis(60);
        long startNanos = System.nanoTime();

        Thread producer = new Thread(() -> {
            try {
                // Mirrors the factory: the read runs inside a scope keyed to buffer::readCancelled.
                StorageRetryCancellation.runWithCancellation(buffer::readCancelled, () -> {
                    readParked.countDown();
                    StorageRetryCancellation.sleepWithCancellationChecks(backoffMillis);
                });
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                done.countDown();
            }
        }, "async-buffer-read-backoff-hard-cancel");
        producer.setDaemon(true);
        producer.start();

        assertTrue("the read must have parked in backoff", readParked.await(10, TimeUnit.SECONDS));
        // Hard cut while the producer is parked in backoff.
        assertTrue("hard cancel wins the running->finishing transition", buffer.finish(true));

        assertTrue("the scoped backoff must abort after a hard cancel", done.await(10, TimeUnit.SECONDS));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        assertTrue(
            "abort must not wait out the backoff budget (took " + elapsedMillis + "ms of " + backoffMillis + "ms)",
            elapsedMillis < backoffMillis / 2
        );
        assertTrue(
            "a cancelled read must surface as TaskCancelledException, got " + thrown.get(),
            thrown.get() instanceof TaskCancelledException
        );
        producer.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse("the producer thread must have exited", producer.isAlive());
    }

    /**
     * Companion to {@link #testHardCancelAbortsScopedReadBackoff} proving the runtime scope is keyed to the
     * hard-cut signal only. Async STOP is {@code finish(false)}: it leaves {@code readCancelled()} disarmed, so the
     * same scoped read runs its backoff to completion rather than aborting mid-flight. Arming on STOP would turn a
     * graceful partial-result stop into a {@link TaskCancelledException}; the accepted cost is that STOP waits out
     * the in-flight backoff (bounded by the retry budget) before the producer exits on {@code noMoreInputs}.
     */
    public void testStopDoesNotAbortScopedReadBackoff() throws Exception {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        CountDownLatch readParked = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        // A short real backoff the read is expected to sleep out: STOP must not cut it short.
        long backoffMillis = 200;

        Thread producer = new Thread(() -> {
            try {
                StorageRetryCancellation.runWithCancellation(buffer::readCancelled, () -> {
                    readParked.countDown();
                    StorageRetryCancellation.sleepWithCancellationChecks(backoffMillis);
                });
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                done.countDown();
            }
        }, "async-buffer-read-backoff-stop");
        producer.setDaemon(true);
        producer.start();

        assertTrue("the read must have parked in backoff", readParked.await(10, TimeUnit.SECONDS));
        // Graceful STOP: keeps buffered pages, must not arm read cancellation.
        assertTrue("STOP wins the running->finishing transition", buffer.finish(false));
        assertFalse("STOP must not arm read cancellation", buffer.readCancelled());

        assertTrue("the read completes its own backoff under STOP", done.await(10, TimeUnit.SECONDS));
        assertNull("STOP must not surface the read as a cancellation failure", thrown.get());
        producer.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse("the producer thread must have exited", producer.isAlive());
    }
}
