/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

final class ExchangeBuffer {

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

    ExchangeBuffer(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("max_buffer_size must be at least one; got=" + maxSize);
        }
        this.maxSize = maxSize;
    }

    void addPage(Page page) {
        queue.add(page);
        if (queueSize.incrementAndGet() == 1) {
            notifyNotEmpty();
        }
        if (noMoreInputs) {
            // O(N) but acceptable because it only occurs with the stop API, and the queue size should be very small.
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

    Page pollPage() {
        final var page = queue.poll();
        if (page != null && queueSize.decrementAndGet() == maxSize - 1) {
            notifyNotFull();
        }
        if (page == null && noMoreInputs && queueSize.get() == 0) {
            completionFuture.onResponse(null);
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

    IsBlockedResult waitForWriting() {
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
            return new IsBlockedResult(notFullFuture, "exchange full");
        }
    }

    IsBlockedResult waitForReading() {
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
            return new IsBlockedResult(notEmptyFuture, "exchange empty");
        }
    }

    private void discardPages() {
        Page p;
        while ((p = pollPage()) != null) {
            p.releaseBlocks();
        }
    }

    void finish(boolean drainingPages) {
        noMoreInputs = true;
        if (drainingPages) {
            discardPages();
        }
        notifyNotEmpty();
        if (drainingPages || queueSize.get() == 0) {
            completionFuture.onResponse(null);
        }
    }

    boolean isFinished() {
        return completionFuture.isDone();
    }

    boolean noMoreInputs() {
        return noMoreInputs;
    }

    int size() {
        return queueSize.get();
    }

    /**
     * Adds a listener that will be notified when this exchange buffer is finished.
     */
    void addCompletionListener(ActionListener<Void> listener) {
        completionFuture.addListener(listener);
    }
}
