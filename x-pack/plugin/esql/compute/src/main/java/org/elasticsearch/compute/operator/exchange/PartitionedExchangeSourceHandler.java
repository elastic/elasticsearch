/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An exchange source handler for a specific driver in a partitioned exchange. It fetches pages
 * from a {@link PartitionedExchangeSinkHandler} for a specific driver index and buffers them
 * locally for {@link ExchangeSource}s to consume.
 *
 * @see PartitionedExchangeSinkHandler
 * @see ExchangeSourceHandler for the non-partitioned variant
 */
public final class PartitionedExchangeSourceHandler {

    /** Local buffer that holds pages fetched from the sink handler, ready for the driver to poll. */
    private final ExchangeBuffer buffer;
    /** The partitioned sink handler that holds per-driver buffers we fetch from. */
    private final PartitionedExchangeSinkHandler sinkHandler;
    /** Which driver (and therefore which partition range) this source handler serves. */
    private final int driverIndex;
    /** Executor used to run the asynchronous fetch loop. */
    private final Executor fetchExecutor;

    /**
     * Tracks outstanding sink-side fetchers. When all fetchers complete (because the sink handler's
     * buffer for this driver is finished), the local buffer is marked as finished too.
     */
    private final PendingInstances outstandingSinks;
    /** Tracks outstanding source consumers (ExchangeSourceImpl instances). */
    private final PendingInstances outstandingSources;
    /** Set to true on failure; causes all subsequent polls to throw. */
    private volatile boolean aborted = false;

    /**
     * Creates a new partitioned exchange source handler for a specific driver.
     *
     * @param sinkHandler    the partitioned sink handler to fetch pages from
     * @param driverIndex    the driver index this source handler is assigned to
     * @param maxBufferSize  the maximum size of the local exchange buffer
     * @param fetchExecutor  the executor used to fetch pages
     */
    public PartitionedExchangeSourceHandler(
        PartitionedExchangeSinkHandler sinkHandler,
        int driverIndex,
        int maxBufferSize,
        Executor fetchExecutor
    ) {
        this.sinkHandler = sinkHandler;
        this.driverIndex = driverIndex;
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.fetchExecutor = fetchExecutor;
        this.outstandingSinks = new PendingInstances(() -> buffer.finish(false));
        this.outstandingSources = new PendingInstances(() -> {});
    }

    public boolean isFinished() {
        return buffer.isFinished();
    }

    private void checkFailure() {
        if (aborted) {
            throw new org.elasticsearch.tasks.TaskCancelledException("exchange source failed");
        }
    }

    private class ExchangeSourceImpl implements ExchangeSource {
        private boolean finished;

        ExchangeSourceImpl() {
            outstandingSources.trackNewInstance();
        }

        @Override
        public Page pollPage() {
            checkFailure();
            return buffer.pollPage();
        }

        @Override
        public boolean isFinished() {
            checkFailure();
            return finished || buffer.isFinished();
        }

        @Override
        public IsBlockedResult waitForReading() {
            return buffer.waitForReading();
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                outstandingSources.finishInstance();
            }
        }

        @Override
        public int bufferSize() {
            return buffer.size();
        }
    }

    /**
     * Create a new {@link ExchangeSource} for this driver's partitions.
     */
    public ExchangeSource createExchangeSource() {
        return new ExchangeSourceImpl();
    }

    /**
     * Starts fetching pages from the partitioned sink handler for this driver's buffer.
     * Call this once to begin the fetch loop.
     *
     * @param instances the number of concurrent fetch loops
     * @param listener  a listener notified when all fetching is complete or fails
     */
    public void startFetching(int instances, ActionListener<Void> listener) {
        for (int i = 0; i < instances; i++) {
            outstandingSinks.trackNewInstance();
            fetchExecutor.execute(new ActionRunnable<>(listener) {
                @Override
                protected void doRun() {
                    new SinkFetcher(ActionListener.assertAtLeastOnce(ActionListener.running(() -> { outstandingSinks.finishInstance(); })))
                        .fetchPage();
                }

                @Override
                public void onFailure(Exception e) {
                    aborted = true;
                    buffer.waitForReading().listener().onResponse(null);
                    outstandingSinks.finishInstance();
                    listener.onFailure(e);
                }
            });
        }
    }

    /**
     * Continuously pulls pages from the partitioned sink handler's buffer for this driver
     * and pushes them into the local buffer. The fetch loop runs asynchronously: it calls
     * {@link PartitionedExchangeSinkHandler#fetchPageAsync} and, on response, either adds
     * the page to the local buffer and fetches again, or waits if the local buffer is full.
     * <p>
     * Uses {@link LoopControl} to avoid unbounded stack growth when responses complete
     * synchronously on the same thread (trampolining pattern).
     */
    private class SinkFetcher {
        private volatile boolean finished = false;
        private final ActionListener<Void> completionListener;

        SinkFetcher(ActionListener<Void> completionListener) {
            this.completionListener = completionListener;
        }

        void fetchPage() {
            final LoopControl loopControl = new LoopControl();
            while (loopControl.isRunning()) {
                loopControl.exiting();
                // Tell the sink handler to finish this driver's buffer if we're already done
                boolean toFinish = buffer.noMoreInputs() || aborted;
                sinkHandler.fetchPageAsync(driverIndex, toFinish, ActionListener.wrap(resp -> {
                    Page page = resp.takePage();
                    if (page != null) {
                        buffer.addPage(page);
                    }
                    if (resp.finished()) {
                        onComplete();
                    } else {
                        // If the local buffer is full, wait for space before fetching more
                        IsBlockedResult future = buffer.waitForWriting();
                        if (future.listener().isDone()) {
                            if (loopControl.tryResume() == false) {
                                fetchPage();
                            }
                        } else {
                            future.listener().addListener(ActionListener.wrap(unused -> {
                                if (loopControl.tryResume() == false) {
                                    fetchPage();
                                }
                            }, this::onFailed));
                        }
                    }
                }, this::onFailed));
            }
            loopControl.exited();
        }

        void onFailed(Exception e) {
            aborted = true;
            buffer.waitForReading().listener().onResponse(null);
            if (finished == false) {
                finished = true;
                completionListener.onFailure(e);
            }
        }

        void onComplete() {
            if (finished == false) {
                finished = true;
                completionListener.onResponse(null);
            }
        }
    }

    /**
     * Trampolining helper: when an async callback completes synchronously on the same thread
     * (e.g. because the page was already available in the buffer), naive recursion would grow
     * the stack unboundedly. LoopControl converts that recursion into a loop by detecting
     * same-thread re-entry and resuming the outer while-loop instead.
     * <p>
     * Duplicated from ExchangeSourceHandler since it is private there.
     */
    private static class LoopControl {
        enum Status {
            RUNNING,
            EXITING,
            EXITED
        }

        private final Thread startedThread;
        private Status status = Status.RUNNING;

        LoopControl() {
            this.startedThread = Thread.currentThread();
        }

        boolean isRunning() {
            return status == Status.RUNNING;
        }

        boolean tryResume() {
            if (startedThread == Thread.currentThread() && status != Status.EXITED) {
                status = Status.RUNNING;
                return true;
            } else {
                return false;
            }
        }

        void exiting() {
            status = Status.EXITING;
        }

        void exited() {
            status = Status.EXITED;
        }
    }

    /**
     * Reference-counting helper: tracks a set of outstanding instances (fetchers or sources)
     * and fires a completion callback when the last one finishes. Used to know when all
     * fetch loops are done (so the buffer can be marked finished) and when all sources are
     * done (for lifecycle management).
     */
    private static class PendingInstances {
        private final AtomicInteger instances = new AtomicInteger();
        private final SubscribableListener<Void> completion = new SubscribableListener<>();

        PendingInstances(Runnable onComplete) {
            completion.addListener(ActionListener.running(onComplete));
        }

        void trackNewInstance() {
            int refs = instances.incrementAndGet();
            assert refs > 0;
        }

        void finishInstance() {
            int refs = instances.decrementAndGet();
            assert refs >= 0;
            if (refs == 0) {
                completion.onResponse(null);
            }
        }
    }
}
