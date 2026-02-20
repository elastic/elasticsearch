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

/**
 * Thread-safe buffer for async external source data.
 * Modeled after {@link org.elasticsearch.compute.operator.exchange.ExchangeBuffer}.
 *
 * This buffer provides:
 * - Thread-safe page queue for cross-thread communication
 * - Backpressure control via max buffer size
 * - Notification via {@link SubscribableListener} when data becomes available
 * - Lifecycle management (finished state tracking)
 */
public final class AsyncExternalSourceBuffer {

    private final Queue<Page> queue = new ConcurrentLinkedQueue<>();
    // uses a separate counter for size for CAS; and ConcurrentLinkedQueue#size is not a constant time operation.
    private final AtomicInteger queueSize = new AtomicInteger();
    private final int maxSize;

    private final Object notEmptyLock = new Object();
    private SubscribableListener<Void> notEmptyFuture = null;

    private final Object notFullLock = new Object();
    private SubscribableListener<Void> notFullFuture = null;

    private final SubscribableListener<Void> completionFuture = new SubscribableListener<>();

    private volatile boolean noMoreInputs = false;
    private volatile Throwable failure = null;

    public AsyncExternalSourceBuffer(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("max_buffer_size must be at least one; got=" + maxSize);
        }
        this.maxSize = maxSize;
    }

    /**
     * Add a page to the buffer. Called by the background reader thread.
     */
    public void addPage(Page page) {
        if (failure != null) {
            page.releaseBlocks();
            return;
        }
        queue.add(page);
        if (queueSize.incrementAndGet() == 1) {
            notifyNotEmpty();
        }
        if (noMoreInputs) {
            // O(N) but acceptable because it only occurs with finish(), and the queue size should be very small.
            if (queue.removeIf(p -> p == page)) {
                page.releaseBlocks();
                final int size = queueSize.decrementAndGet();
                if (size == maxSize - 1) {
                    notifyNotFull();
                }
                if (size == 0) {
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
        if (page != null && queueSize.decrementAndGet() == maxSize - 1) {
            notifyNotFull();
        }
        if (page == null && noMoreInputs && queueSize.get() == 0) {
            if (failure != null) {
                completionFuture.onFailure(new Exception(failure));
            } else {
                completionFuture.onResponse(null);
            }
        }
        return page;
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
     * Returns an {@link IsBlockedResult} that completes when the buffer has space for writing.
     * Used by background reader for backpressure.
     */
    public IsBlockedResult waitForWriting() {
        // maxBufferSize check is not water-tight as more than one sink can pass this check at the same time.
        if (queueSize.get() < maxSize || noMoreInputs) {
            return Operator.NOT_BLOCKED;
        }
        synchronized (notFullLock) {
            if (queueSize.get() < maxSize || noMoreInputs) {
                return Operator.NOT_BLOCKED;
            }
            if (notFullFuture == null) {
                notFullFuture = new SubscribableListener<>();
            }
            return new IsBlockedResult(notFullFuture, "async external source buffer full");
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
        if (queueSize.get() < maxSize || noMoreInputs) {
            return SubscribableListener.newSucceeded(null);
        }
        synchronized (notFullLock) {
            if (queueSize.get() < maxSize || noMoreInputs) {
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

    private void discardPages() {
        Page p;
        while ((p = pollPage()) != null) {
            p.releaseBlocks();
        }
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
        if (drainingPages || queueSize.get() == 0) {
            if (failure != null) {
                completionFuture.onFailure(new Exception(failure));
            } else {
                completionFuture.onResponse(null);
            }
        }
    }

    /**
     * Mark the buffer as failed. Called when the background reader encounters an error.
     */
    public void onFailure(Throwable t) {
        this.failure = t;
        finish(true);
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
}
