/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * An {@link ExchangeSinkHandler} receives pages and status from its {@link ExchangeSink}s, which are created using
 * {@link #createExchangeSink(Runnable)}} method. Pages and status can then be retrieved asynchronously by {@link ExchangeSourceHandler}s
 * using the {@link #fetchPageAsync(boolean, ActionListener)} method.
 *
 * @see #createExchangeSink(Runnable)
 * @see #fetchPageAsync(boolean, ActionListener)
 * @see ExchangeSourceHandler
 */
public final class ExchangeSinkHandler {

    private final ExchangeBuffer buffer;
    private final Queue<ActionListener<ExchangeResponse>> listeners = new ConcurrentLinkedQueue<>();
    private final AtomicInteger outstandingSinks = new AtomicInteger();
    // listeners are notified by only one thread.
    private final Semaphore promised = new Semaphore(1);

    private final SubscribableListener<Void> completionFuture;
    private final LongSupplier nowInMillis;
    private final AtomicLong lastUpdatedInMillis;
    private final BlockFactory blockFactory;
    private volatile ProfileTracker profileTracker;

    public ExchangeSinkHandler(BlockFactory blockFactory, int maxBufferSize, LongSupplier nowInMillis) {
        this.blockFactory = blockFactory;
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.completionFuture = SubscribableListener.newForked(buffer::addCompletionListener);
        this.nowInMillis = nowInMillis;
        this.lastUpdatedInMillis = new AtomicLong(nowInMillis.getAsLong());
    }

    private class ExchangeSinkImpl implements ExchangeSink {
        boolean finished;
        private final Runnable onPageFetched;
        private final SubscribableListener<Void> onFinished = new SubscribableListener<>();

        ExchangeSinkImpl(Runnable onPageFetched) {
            this.onPageFetched = onPageFetched;
            onChanged();
            buffer.addCompletionListener(onFinished);
            outstandingSinks.incrementAndGet();
        }

        @Override
        public void addPage(Page page) {
            onPageFetched.run();
            buffer.addPage(page);
            notifyListeners();
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                onFinished.onResponse(null);
                onChanged();
                if (outstandingSinks.decrementAndGet() == 0) {
                    buffer.finish(false);
                    notifyListeners();
                }
            }
        }

        @Override
        public boolean isFinished() {
            return onFinished.isDone();
        }

        @Override
        public void addCompletionListener(ActionListener<Void> listener) {
            onFinished.addListener(listener);
        }

        @Override
        public IsBlockedResult waitForWriting() {
            return buffer.waitForWriting();
        }
    }

    /**
     * Fetches pages and the sink status asynchronously.
     *
     * @param sourceFinished if true, then this handler can finish as sources have enough pages.
     * @param listener       the listener that will be notified when pages are ready or this handler is finished
     * @see RemoteSink
     * @see ExchangeSourceHandler#addRemoteSink(RemoteSink, boolean, Runnable, int, ActionListener)
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

    /**
     * Returns true if an exchange is finished
     */
    public boolean isFinished() {
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
                Page page = buffer.pollPage();
                ProfileTracker tracker = profileTracker;
                if (page != null && tracker != null) {
                    tracker.pages.incrementAndGet();
                    tracker.rows.addAndGet(page.getPositionCount());
                    tracker.responseStarted();
                }
                response = tracker == null || page == null
                    ? new ExchangeResponse(blockFactory, page, buffer.isFinished())
                    : new ExchangeResponse(blockFactory, page, buffer.isFinished(), tracker);
            } finally {
                promised.release();
            }
            onChanged();
            ActionListener.respondAndRelease(listener, response);
        }
    }

    /**
     * Create a new exchange sink for exchanging data
     *
     * @param onPageFetched a {@link Runnable} that will be called when a page is fetched.
     * @see ExchangeSinkOperator
     */
    public ExchangeSink createExchangeSink(Runnable onPageFetched) {
        return new ExchangeSinkImpl(onPageFetched);
    }

    /**
     * Returns a snapshot of profiled data-page traffic.
     */
    public Profile profile() {
        ProfileTracker tracker = profileTracker;
        return tracker == null ? Profile.EMPTY : new Profile(tracker.pages.get(), tracker.rows.get(), tracker.serializedBytes.get());
    }

    /**
     * Enables traffic profiling. Must be called before exchanging pages.
     */
    public void enableProfiling() {
        if (profileTracker == null) {
            synchronized (this) {
                if (profileTracker == null) {
                    ProfileTracker tracker = new ProfileTracker();
                    profileTracker = tracker;
                    completionFuture.addListener(ActionListener.wrap(ignored -> tracker.bufferFinished(), tracker.completion::onFailure));
                }
            }
        }
    }

    /**
     * Notifies the listener after the handler and all profiled responses complete.
     */
    public void addProfileCompletionListener(ActionListener<Void> listener) {
        ProfileTracker tracker = profileTracker;
        if (tracker == null) {
            addCompletionListener(listener);
        } else {
            tracker.completion.addListener(listener);
        }
    }

    /**
     * Data-page traffic emitted by an exchange sink handler.
     *
     * @param pages number of data-page responses created
     * @param rows total positions in those pages
     * @param serializedBytes serialized data-page response bytes before transport framing or compression
     */
    public record Profile(long pages, long rows, long serializedBytes) implements Writeable {
        public static final Profile EMPTY = new Profile(0L, 0L, 0L);

        public Profile(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(pages);
            out.writeVLong(rows);
            out.writeVLong(serializedBytes);
        }
    }

    private static class ProfileTracker implements ExchangeResponse.ProfileListener {
        private final AtomicLong pages = new AtomicLong();
        private final AtomicLong rows = new AtomicLong();
        private final AtomicLong serializedBytes = new AtomicLong();
        private final AtomicInteger inFlightResponses = new AtomicInteger();
        private final SubscribableListener<Void> completion = new SubscribableListener<>();
        private volatile boolean bufferFinished;

        private void responseStarted() {
            inFlightResponses.incrementAndGet();
        }

        @Override
        public void onSerialized(long bytes) {
            serializedBytes.addAndGet(bytes);
        }

        @Override
        public void onReleased() {
            if (inFlightResponses.decrementAndGet() == 0) {
                completeIfFinished();
            }
        }

        private void bufferFinished() {
            bufferFinished = true;
            completeIfFinished();
        }

        private void completeIfFinished() {
            if (bufferFinished && inFlightResponses.get() == 0) {
                completion.onResponse(null);
            }
        }
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

    /**
     * Returns the number of pages available in the buffer.
     * This method should be used for testing only.
     */
    public int bufferSize() {
        return buffer.size();
    }
}
