/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An in memory exchanger between local sinks and sources
 */
public final class LocalExchanger {
    private final Queue<Page> buffer = new ConcurrentLinkedQueue<>();
    // uses a separate counter for size for CAS; and ConcurrentLinkedQueue#size is not a constant time operation.
    private final AtomicInteger bufferSize = new AtomicInteger();
    private final int maxBufferSize;

    private final Object notEmptyLock = new Object();
    private ListenableActionFuture<Void> notEmptyFuture = null;

    private final Object notFullLock = new Object();
    private ListenableActionFuture<Void> notFullFuture = null;

    private final AtomicInteger outstandingSinks = new AtomicInteger();

    public LocalExchanger(int maxBufferSize) {
        if (maxBufferSize < 1) {
            throw new IllegalArgumentException("max_buffer_size must be at least one; got=" + maxBufferSize);
        }
        this.maxBufferSize = maxBufferSize;
    }

    private void addPageToBuffer(Page page) {
        buffer.add(page);
        if (bufferSize.incrementAndGet() == 1) {
            notifyNotEmpty();
        }
    }

    private Page pollPageFromBuffer() {
        final var page = buffer.poll();
        if (page != null && bufferSize.decrementAndGet() == maxBufferSize - 1) {
            final ListenableActionFuture<Void> toNotify;
            synchronized (notFullLock) {
                toNotify = notFullFuture;
                notFullFuture = null;
            }
            if (toNotify != null) {
                toNotify.onResponse(null);
            }
        }
        return page;
    }

    private void notifyNotEmpty() {
        final ListenableActionFuture<Void> toNotify;
        synchronized (notEmptyLock) {
            toNotify = notEmptyFuture;
            notEmptyFuture = null;
        }
        if (toNotify != null) {
            toNotify.onResponse(null);
        }
    }

    private class LocalExchangeSource implements ExchangeSource {
        @Override
        public Page pollPage() {
            return pollPageFromBuffer();
        }

        @Override
        public boolean isFinished() {
            return outstandingSinks.get() == 0 && bufferSize.get() == 0;
        }

        @Override
        public ListenableActionFuture<Void> waitForReading() {
            if (isFinished()) {
                return Operator.NOT_BLOCKED;
            }
            synchronized (notEmptyLock) {
                if (isFinished() || bufferSize.get() > 0) {
                    return Operator.NOT_BLOCKED;
                }
                if (notEmptyFuture == null) {
                    notEmptyFuture = new ListenableActionFuture<>();
                }
                return notEmptyFuture;
            }
        }

        @Override
        public int bufferSize() {
            return bufferSize.get();
        }
    }

    private class LocalExchangeSink implements ExchangeSink {
        boolean finished;

        LocalExchangeSink() {
            outstandingSinks.incrementAndGet();
        }

        @Override
        public void addPage(Page page) {
            addPageToBuffer(page);
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                if (outstandingSinks.decrementAndGet() == 0) {
                    notifyNotEmpty();
                }
            }
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public ListenableActionFuture<Void> waitForWriting() {
            // maxBufferSize check is not water-tight as more than one sink can pass this check at the same time.
            if (bufferSize.get() < maxBufferSize) {
                return Operator.NOT_BLOCKED;
            }
            synchronized (notFullLock) {
                if (bufferSize.get() < maxBufferSize) {
                    return Operator.NOT_BLOCKED;
                }
                if (notFullFuture == null) {
                    notFullFuture = new ListenableActionFuture<>();
                }
                return notFullFuture;
            }
        }
    }

    public ExchangeSource createExchangeSource() {
        return new LocalExchangeSource();
    }

    public ExchangeSink createExchangeSink() {
        return new LocalExchangeSink();
    }
}
