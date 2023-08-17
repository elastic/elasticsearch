/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.compute.data.Page;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * An {@link ExchangeSinkHandler} receives pages and status from its {@link ExchangeSink}s, which are created using
 * {@link #createExchangeSink()}} method. Pages and status can then be retrieved asynchronously by {@link ExchangeSourceHandler}s
 * using the {@link #fetchPageAsync(boolean, ActionListener)} method.
 *
 * @see #createExchangeSink()
 * @see #fetchPageAsync(boolean, ActionListener)
 * @see ExchangeSourceHandler
 */
public final class ExchangeSinkHandler {

    private final ExchangeBuffer buffer;
    private final Queue<ActionListener<ExchangeResponse>> listeners = new ConcurrentLinkedQueue<>();
    private final AtomicInteger outstandingSinks = new AtomicInteger();
    // listeners are notified by only one thread.
    private final Semaphore promised = new Semaphore(1);

    private final ListenableActionFuture<Void> completionFuture;
    private final LongSupplier nowInMillis;
    private final AtomicLong lastUpdatedInMillis;

    public ExchangeSinkHandler(int maxBufferSize, LongSupplier nowInMillis) {
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.completionFuture = new ListenableActionFuture<>();
        this.buffer.addCompletionListener(completionFuture);
        this.nowInMillis = nowInMillis;
        this.lastUpdatedInMillis = new AtomicLong(nowInMillis.getAsLong());
    }

    private class LocalExchangeSink implements ExchangeSink {
        boolean finished;

        LocalExchangeSink() {
            onChanged();
            outstandingSinks.incrementAndGet();
        }

        @Override
        public void addPage(Page page) {
            buffer.addPage(page);
            notifyListeners();
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                onChanged();
                if (outstandingSinks.decrementAndGet() == 0) {
                    buffer.finish(false);
                    notifyListeners();
                }
            }
        }

        @Override
        public boolean isFinished() {
            return finished || buffer.isFinished();
        }

        @Override
        public ListenableActionFuture<Void> waitForWriting() {
            return buffer.waitForWriting();
        }
    }

    /**
     * Fetches pages and the sink status asynchronously.
     *
     * @param sourceFinished if true, then this handler can finish as sources have enough pages.
     * @param listener       the listener that will be notified when pages are ready or this handler is finished
     * @see RemoteSink
     * @see ExchangeSourceHandler#addRemoteSink(RemoteSink, int)
     */
    public void fetchPageAsync(boolean sourceFinished, ActionListener<ExchangeResponse> listener) {
        if (sourceFinished) {
            buffer.finish(true);
        }
        listeners.add(listener);
        onChanged();
        notifyListeners();
    }

    /**
     * Add a listener, which will be notified when this exchange sink handler is completed. An exchange sink
     * handler is consider completed when all associated sinks are completed and the output pages are fetched.
     */
    public void addCompletionListener(ActionListener<Void> listener) {
        completionFuture.addListener(listener);
    }

    boolean isFinished() {
        return completionFuture.isDone();
    }

    /**
     * Fails this sink exchange handler
     */
    void onFailure(Exception failure) {
        completionFuture.onFailure(failure);
        buffer.finish(true);
        notifyListeners();
    }

    private void notifyListeners() {
        while (listeners.isEmpty() == false && (buffer.size() > 0 || buffer.noMoreInputs())) {
            if (promised.tryAcquire() == false) {
                break;
            }
            final ActionListener<ExchangeResponse> listener;
            final ExchangeResponse response;
            try {
                // Use `poll` and recheck because `listeners.isEmpty()` might return true, while a listener is being added
                listener = listeners.poll();
                if (listener == null) {
                    continue;
                }
                response = new ExchangeResponse(buffer.pollPage(), buffer.isFinished());
            } finally {
                promised.release();
            }
            onChanged();
            listener.onResponse(response);
        }
    }

    /**
     * Create a new exchange sink for exchanging data
     *
     * @see ExchangeSinkOperator
     */
    public ExchangeSink createExchangeSink() {
        return new LocalExchangeSink();
    }

    /**
     * Whether this sink handler has sinks attached or available pages
     */
    boolean hasData() {
        return outstandingSinks.get() > 0 || buffer.size() > 0;
    }

    /**
     * Whether this sink handler has listeners waiting for data
     */
    boolean hasListeners() {
        return listeners.isEmpty() == false;
    }

    private void onChanged() {
        lastUpdatedInMillis.accumulateAndGet(nowInMillis.getAsLong(), Math::max);
    }

    /**
     * The time in millis when this sink handler was updated. This timestamp is used to prune idle sinks.
     *
     * @see ExchangeService#INACTIVE_SINKS_INTERVAL_SETTING
     */
    long lastUpdatedTimeInMillis() {
        return lastUpdatedInMillis.get();
    }
}
