/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe buffer for async external source data.
 * Modeled after {@link org.elasticsearch.compute.operator.exchange.ExchangeBuffer}.
 *
 * This buffer provides:
 * - Thread-safe page queue for cross-thread communication
 * - Byte-based backpressure control proportional to actual memory usage
 * - Notification via {@link SubscribableListener} when data becomes available
 * - Lifecycle management (finished state tracking)
 */
public final class AsyncExternalSourceBuffer {

    /**
     * Default byte limit for the buffer, preserving the original "10 normal-sized pages" intent.
     */
    public static final long DEFAULT_MAX_BUFFER_BYTES = 10L * Operator.TARGET_PAGE_SIZE;

    private final Queue<Page> queue = new ConcurrentLinkedQueue<>();
    // uses a separate counter for size for CAS; and ConcurrentLinkedQueue#size is not a constant time operation.
    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicLong bytesInBuffer = new AtomicLong();
    private final long maxBufferBytes;

    private final Object notEmptyLock = new Object();
    private SubscribableListener<Void> notEmptyFuture = null;

    private final Object notFullLock = new Object();
    private SubscribableListener<Void> notFullFuture = null;

    private final SubscribableListener<Void> completionFuture = new SubscribableListener<>();

    private volatile boolean noMoreInputs = false;
    private volatile Throwable failure = null;

    public AsyncExternalSourceBuffer(long maxBufferBytes) {
        if (maxBufferBytes < 1) {
            throw new IllegalArgumentException("max_buffer_bytes must be at least one; got=" + maxBufferBytes);
        }
        this.maxBufferBytes = maxBufferBytes;
    }

    /**
     * Add a page to the buffer. Called by the background reader thread.
     */
    public void addPage(Page page) {
        if (failure != null) {
            // Reject the page without touching buffer state, so the trailing invariantsHold()
            // call is intentionally bypassed: nothing was mutated for it to check.
            page.releaseBlocks();
            return;
        }
        long pageBytes = page.ramBytesUsedByBlocks();
        bytesInBuffer.addAndGet(pageBytes);
        queue.add(page);
        queueSize.incrementAndGet();
        // Always notify: the conditional guard on prevBytes==0 previously caused a lost-wakeup race
        // when a consumer drained and blocked on notEmptyFuture between our getAndAdd and queue.add.
        // notifyNotEmpty() is a no-op when no listener is registered, so unconditional fire is cheap.
        notifyNotEmpty();
        if (noMoreInputs) {
            // O(N) but acceptable because it only occurs with finish(), and the queue size should be very small.
            if (queue.removeIf(p -> p == page)) {
                page.releaseBlocks();
                queueSize.decrementAndGet();
                long afterRemove = bytesInBuffer.addAndGet(-pageBytes);
                if (afterRemove < maxBufferBytes) {
                    notifyNotFull();
                }
                if (queueSize.get() == 0) {
                    completionFuture.onResponse(null);
                }
            }
        }
        assert invariantsHold() : "buffer invariants violated after addPage";
    }

    /**
     * Poll a page from the buffer. Called by the operator (driver thread).
     *
     * @return the next page, or {@code null} if no pages available
     */
    public Page pollPage() {
        Page page = queue.poll();
        if (page == null) {
            signalCompletionIfDrained();
            assert invariantsHold() : "buffer invariants violated after pollPage (empty)";
            return null;
        }
        queueSize.decrementAndGet();
        long pageBytes = page.ramBytesUsedByBlocks();
        bytesInBuffer.addAndGet(-pageBytes);
        // Always notify: the previous threshold-crossing guard could miss a crossing because the
        // producer's waitForSpace snapshot of bytesInBuffer can race with concurrent addPage calls,
        // orphaning notFullFuture. notifyNotFull() is a no-op when no listener is registered.
        notifyNotFull();
        signalCompletionIfDrained();
        assert invariantsHold() : "buffer invariants violated after pollPage";
        return page;
    }

    /**
     * Completes {@link #completionFuture} once the queue is drained and no more input is expected.
     * Safe to call repeatedly; no-ops if completion was already signaled.
     */
    private void signalCompletionIfDrained() {
        if (noMoreInputs == false || queueSize.get() != 0 || completionFuture.isDone()) {
            return;
        }
        if (failure != null) {
            completionFuture.onFailure(new Exception(failure));
        } else {
            completionFuture.onResponse(null);
        }
    }

    private void notifyNotEmpty() {
        final SubscribableListener<Void> toNotify;
        synchronized (notEmptyLock) {
            toNotify = notEmptyFuture;
            notEmptyFuture = null;
        }
        if (toNotify != null) {
            toNotify.onResponse(null);
        }
    }

    private void notifyNotFull() {
        final SubscribableListener<Void> toNotify;
        synchronized (notFullLock) {
            toNotify = notFullFuture;
            notFullFuture = null;
        }
        if (toNotify != null) {
            toNotify.onResponse(null);
        }
    }

    /**
     * Returns a {@link SubscribableListener} that completes when the buffer has space for writing.
     * This is the method producers use for backpressure coordination: it integrates directly with
     * ES async patterns and the producer drain loops.
     *
     * @return a listener that completes when space is available, or an already-completed listener if space exists
     */
    public SubscribableListener<Void> waitForSpace() {
        if (bytesInBuffer.get() < maxBufferBytes || noMoreInputs) {
            return SubscribableListener.newSucceeded(null);
        }
        synchronized (notFullLock) {
            if (bytesInBuffer.get() < maxBufferBytes || noMoreInputs) {
                return SubscribableListener.newSucceeded(null);
            }
            if (notFullFuture == null) {
                notFullFuture = new SubscribableListener<>();
            }
            return notFullFuture;
        }
    }

    /**
     * Returns an {@link IsBlockedResult} that completes when the buffer has data for reading.
     * Used by operator to signal driver when waiting for data.
     */
    public IsBlockedResult waitForReading() {
        if (size() > 0 || noMoreInputs) {
            return Operator.NOT_BLOCKED;
        }
        synchronized (notEmptyLock) {
            if (size() > 0 || noMoreInputs) {
                return Operator.NOT_BLOCKED;
            }
            if (notEmptyFuture == null) {
                notEmptyFuture = new SubscribableListener<>();
            }
            return new IsBlockedResult(notEmptyFuture, "async external source buffer empty");
        }
    }

    // Drains and releases every queued page on teardown. Only call from finish/onFailure;
    // bytesInBuffer is reset wholesale, which is only safe when no further pollPage() is expected
    // to subtract from it.
    private void discardPages() {
        Page p;
        while ((p = queue.poll()) != null) {
            queueSize.decrementAndGet();
            p.releaseBlocks();
        }
        bytesInBuffer.set(0);
        assert invariantsHold() : "buffer invariants violated after discardPages";
    }

    /**
     * Mark the buffer as finished. Called when reading is done or an error occurs.
     */
    public void finish(boolean drainingPages) {
        noMoreInputs = true;
        if (drainingPages) {
            discardPages();
        }
        notifyNotEmpty();
        notifyNotFull(); // wake producers so they observe noMoreInputs and exit
        signalCompletionIfDrained();
        assert invariantsHold() : "buffer invariants violated after finish";
    }

    /**
     * Mark the buffer as failed. Called when the background reader encounters an error.
     * <p>
     * Queued pages are retained so the driver can drain them before {@link AsyncExternalSourceOperator}
     * surfaces the failure via {@link org.elasticsearch.compute.operator.SourceOperator#getOutput()}.
     */
    public void onFailure(Throwable t) {
        this.failure = t;
        noMoreInputs = true;
        notifyNotEmpty();
        notifyNotFull();
        signalCompletionIfDrained();
        assert invariantsHold() : "buffer invariants violated after onFailure";
    }

    public boolean isFinished() {
        return completionFuture.isDone();
    }

    public boolean noMoreInputs() {
        return noMoreInputs;
    }

    public int size() {
        return queueSize.get();
    }

    /**
     * Adds a listener that will be notified when this buffer is finished.
     */
    public void addCompletionListener(ActionListener<Void> listener) {
        completionFuture.addListener(listener);
    }

    public Throwable failure() {
        return failure;
    }

    /**
     * Returns the current number of bytes buffered, as measured by {@link Page#ramBytesUsedByBlocks()}.
     */
    public long bytesInBuffer() {
        return bytesInBuffer.get();
    }

    /**
     * Verifies internal invariants under {@code -ea}. Called from each buffer mutator so that
     * every existing test exercises the checks automatically without dedicated assertions.
     * <p>
     * Scope is intentionally narrow. The buffer is lock-free and counter updates are not atomic
     * across fields, so several legitimate transient states cannot be asserted on without
     * introducing flakiness:
     * <ul>
     * <li>{@link #addPage} updates {@code bytesInBuffer} before {@code queueSize}, so a
     *     concurrent reader may briefly observe {@code bytes > 0 && size == 0}.</li>
     * <li>{@link #pollPage} updates {@code queueSize} before {@code bytesInBuffer}, so a
     *     concurrent reader may briefly observe {@code size < N && bytes} still reflecting
     *     {@code N}.</li>
     * <li>A race between {@link #discardPages()} (which sets {@code bytesInBuffer} to {@code 0}
     *     wholesale) and the {@code noMoreInputs} cleanup branch of {@link #addPage} (which
     *     subtracts its own page bytes) can transiently push counters below zero by an
     *     unpredictable amount.</li>
     * </ul>
     * Hence the only invariant asserted here is the forward direction of completion
     * consistency: if {@code completionFuture} has signalled success, then the buffer must
     * already have observed {@code noMoreInputs}. This catches premature completion (signalling
     * done before {@code finish} / {@code onFailure} was called). Lost-wakeup regressions — the
     * bug class fixed in the unconditional {@code notifyNotEmpty}/{@code notifyNotFull} changes
     * — leave counters and the completion future internally consistent and are not detected
     * here; see {@code AsyncExternalSourceBufferTests#testNoLostWakeupUnderConcurrentAddAndPoll}
     * for that coverage.
     */
    private boolean invariantsHold() {
        if (completionFuture.isDone() && failure == null) {
            assert noMoreInputs : "completionFuture done with no failure but noMoreInputs is false";
        }
        return true;
    }
}
