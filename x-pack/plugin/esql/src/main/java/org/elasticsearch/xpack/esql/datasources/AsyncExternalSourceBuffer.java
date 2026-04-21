/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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

    private final ReentrantLock notFullLock = new ReentrantLock();
    private final Condition notFullCondition = notFullLock.newCondition();
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
            page.releaseBlocks();
            return;
        }
        long pageBytes = page.ramBytesUsedByBlocks();
        long prevBytes = bytesInBuffer.getAndAdd(pageBytes);
        queue.add(page);
        queueSize.incrementAndGet();
        if (prevBytes == 0) {
            notifyNotEmpty();
        }
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
    }

    /**
     * Poll a page from the buffer. Called by the operator (driver thread).
     * @return the next page, or null if no pages available
     */
    public Page pollPage() {
        final var page = queue.poll();
        if (page != null) {
            queueSize.decrementAndGet();
            long pageBytes = page.ramBytesUsedByBlocks();
            long prevBytes = bytesInBuffer.getAndAdd(-pageBytes);
            if (prevBytes >= maxBufferBytes && (prevBytes - pageBytes) < maxBufferBytes) {
                notifyNotFull();
            }
        }
        signalCompletionIfDrained();
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
        notFullLock.lock();
        try {
            toNotify = notFullFuture;
            notFullFuture = null;
            notFullCondition.signalAll();
        } finally {
            notFullLock.unlock();
        }
        if (toNotify != null) {
            toNotify.onResponse(null);
        }
    }

    /**
     * Returns an {@link IsBlockedResult} that completes when the buffer has space for writing.
     * Used by background reader for backpressure.
     */
    public IsBlockedResult waitForWriting() {
        if (bytesInBuffer.get() < maxBufferBytes || noMoreInputs) {
            return Operator.NOT_BLOCKED;
        }
        notFullLock.lock();
        try {
            if (bytesInBuffer.get() < maxBufferBytes || noMoreInputs) {
                return Operator.NOT_BLOCKED;
            }
            if (notFullFuture == null) {
                notFullFuture = new SubscribableListener<>();
            }
            return new IsBlockedResult(notFullFuture, "async external source buffer full");
        } finally {
            notFullLock.unlock();
        }
    }

    /**
     * Returns a {@link SubscribableListener} that completes when the buffer has space for writing.
     * This is the preferred method for producers to use for backpressure coordination.
     * <p>
     * Unlike {@link #waitForWriting()} which returns an {@link IsBlockedResult}, this method
     * returns a {@link SubscribableListener} that can be used directly with ES async patterns.
     *
     * @return a listener that completes when space is available, or an already-completed listener if space exists
     */
    public SubscribableListener<Void> waitForSpace() {
        if (bytesInBuffer.get() < maxBufferBytes || noMoreInputs) {
            return SubscribableListener.newSucceeded(null);
        }
        notFullLock.lock();
        try {
            if (bytesInBuffer.get() < maxBufferBytes || noMoreInputs) {
                return SubscribableListener.newSucceeded(null);
            }
            if (notFullFuture == null) {
                notFullFuture = new SubscribableListener<>();
            }
            return notFullFuture;
        } finally {
            notFullLock.unlock();
        }
    }

    /**
     * Blocks the producer (same condition as {@link #waitForSpace()}) on the same not-full lock
     * used to install {@code notFullFuture} until there is capacity, {@link #noMoreInputs} is set,
     * or the wait times out. Used by {@link ExternalSourceDrainUtils} instead of
     * {@code PlainActionFuture} or {@code SubscribableListener} + latches.
     */
    public void awaitSpaceForProducer(TimeValue timeout) {
        long remainingNanos = timeout.nanos();
        notFullLock.lock();
        try {
            while (bytesInBuffer.get() >= maxBufferBytes && noMoreInputs == false) {
                if (remainingNanos <= 0) {
                    throw new ElasticsearchTimeoutException("timeout waiting for async external source buffer space");
                }
                try {
                    remainingNanos = notFullCondition.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for buffer space", e);
                }
            }
        } finally {
            notFullLock.unlock();
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

    private void discardPages() {
        Page p;
        while ((p = queue.poll()) != null) {
            queueSize.decrementAndGet();
            p.releaseBlocks();
        }
        bytesInBuffer.set(0);
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
}
