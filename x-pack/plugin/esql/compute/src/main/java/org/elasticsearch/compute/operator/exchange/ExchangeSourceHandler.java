/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.Releasable;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link ExchangeSourceHandler} asynchronously fetches pages and status from multiple {@link RemoteSink}s
 * and feeds them to its {@link ExchangeSource}, which are created using the {@link #createExchangeSource()}) method.
 * {@link RemoteSink}s are added using the {@link #addRemoteSink(RemoteSink, boolean, int, ActionListener)}) method.
 *
 * @see #createExchangeSource()
 * @see #addRemoteSink(RemoteSink, boolean, int, ActionListener)
 */
public final class ExchangeSourceHandler {
    private final ExchangeBuffer buffer;
    private final Executor fetchExecutor;

    private final PendingInstances outstandingSinks;
    private final PendingInstances outstandingSources;
    // Collect failures that occur while fetching pages from the remote sink with `failFast=true`.
    // The exchange source will stop fetching and abort as soon as any failure is added to this failure collector.
    // The final failure collected will be notified to callers via the {@code completionListener}.
    private final FailureCollector failure = new FailureCollector();

    /**
     * Creates a new ExchangeSourceHandler.
     *
     * @param maxBufferSize      the maximum size of the exchange buffer. A larger buffer reduces ``pauses`` but uses more memory,
     *                           which could otherwise be allocated for other purposes.
     * @param fetchExecutor      the executor used to fetch pages.
     * @param completionListener a listener that will be notified when the exchange source handler fails or completes
     */
    public ExchangeSourceHandler(int maxBufferSize, Executor fetchExecutor, ActionListener<Void> completionListener) {
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.fetchExecutor = fetchExecutor;
        this.outstandingSinks = new PendingInstances(() -> buffer.finish(false));
        this.outstandingSources = new PendingInstances(() -> buffer.finish(true));
        buffer.addCompletionListener(ActionListener.running(() -> {
            final ActionListener<Void> listener = ActionListener.assertAtLeastOnce(completionListener).delegateFailure((l, unused) -> {
                final Exception e = failure.getFailure();
                if (e != null) {
                    l.onFailure(e);
                } else {
                    l.onResponse(null);
                }
            });
            try (RefCountingListener refs = new RefCountingListener(listener)) {
                for (PendingInstances pending : List.of(outstandingSinks, outstandingSources)) {
                    // Create an outstanding instance and then finish to complete the completionListener
                    // if we haven't registered any instances of exchange sinks or exchange sources before.
                    pending.trackNewInstance();
                    pending.completion.addListener(refs.acquire());
                    pending.finishInstance();
                }
            }
        }));
    }

    private class ExchangeSourceImpl implements ExchangeSource {
        private boolean finished;

        ExchangeSourceImpl() {
            outstandingSources.trackNewInstance();
        }

        private void checkFailure() {
            Exception e = failure.getFailure();
            if (e != null) {
                throw ExceptionsHelper.convertToRuntime(e);
            }
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
     * Create a new {@link ExchangeSource} for exchanging data
     *
     * @see ExchangeSinkOperator
     */
    public ExchangeSource createExchangeSource() {
        return new ExchangeSourceImpl();
    }

    /**
     * If we continue fetching pages using the same thread, we risk encountering a StackOverflow error.
     * On the other hand, if we fork when receiving a reply on the same thread, we add unnecessary overhead
     * from thread scheduling and context switching. LoopControl can be used to avoid these issues.
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
     * Wraps {@link RemoteSink} with a fetch loop and error handling
     */
    private final class RemoteSinkFetcher {
        private volatile boolean finished = false;
        private final RemoteSink remoteSink;
        private final boolean failFast;
        private final ActionListener<Void> completionListener;

        RemoteSinkFetcher(RemoteSink remoteSink, boolean failFast, ActionListener<Void> completionListener) {
            outstandingSinks.trackNewInstance();
            this.remoteSink = remoteSink;
            this.failFast = failFast;
            this.completionListener = completionListener;
        }

        void fetchPage() {
            final LoopControl loopControl = new LoopControl();
            while (loopControl.isRunning()) {
                loopControl.exiting();
                // finish other sinks if one of them failed or source no longer need pages.
                boolean toFinishSinks = buffer.noMoreInputs() || failure.hasFailure();
                remoteSink.fetchPageAsync(toFinishSinks, ActionListener.wrap(resp -> {
                    Page page = resp.takePage();
                    if (page != null) {
                        buffer.addPage(page);
                    }
                    if (resp.finished()) {
                        onSinkComplete();
                    } else {
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
                            }, this::onSinkFailed));
                        }
                    }
                }, this::onSinkFailed));
            }
            loopControl.exited();
        }

        void onSinkFailed(Exception e) {
            if (failFast) {
                failure.unwrapAndCollect(e);
            }
            buffer.waitForReading().listener().onResponse(null); // resume the Driver if it is being blocked on reading
            if (finished == false) {
                finished = true;
                remoteSink.close(ActionListener.running(() -> {
                    outstandingSinks.finishInstance();
                    completionListener.onFailure(e);
                }));
            }
        }

        void onSinkComplete() {
            if (finished == false) {
                finished = true;
                outstandingSinks.finishInstance();
                completionListener.onResponse(null);
            }
        }
    }

    /**
     * Add a remote sink as a new data source of this handler. The handler will start fetching data from this remote sink intermediately.
     *
     * @param remoteSink the remote sink
     * @param failFast   determines how failures in this remote sink are handled:
     *                   - If {@code false}, failures from this remote sink will not cause the exchange source to abort.
     *                   Callers must handle these failures notified via {@code listener}.
     *                   - If {@code true}, failures from this remote sink will cause the exchange source to abort.
     *                   Callers can safely ignore failures notified via this listener, as they are collected and
     *                   reported by the exchange source.
     * @param instances  the number of concurrent ``clients`` that this handler should use to fetch pages.
     *                   More clients reduce latency, but add overhead.
     * @param listener   a listener that will be notified when the sink fails or completes
     * @see ExchangeSinkHandler#fetchPageAsync(boolean, ActionListener)
     */
    public void addRemoteSink(RemoteSink remoteSink, boolean failFast, int instances, ActionListener<Void> listener) {
        final ActionListener<Void> sinkListener = ActionListener.assertAtLeastOnce(ActionListener.notifyOnce(listener));
        fetchExecutor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (failFast) {
                    failure.unwrapAndCollect(e);
                }
                buffer.waitForReading().listener().onResponse(null); // resume the Driver if it is being blocked on reading
                remoteSink.close(ActionListener.running(() -> sinkListener.onFailure(e)));
            }

            @Override
            protected void doRun() {
                try (RefCountingListener refs = new RefCountingListener(sinkListener)) {
                    for (int i = 0; i < instances; i++) {
                        var fetcher = new RemoteSinkFetcher(remoteSink, failFast, refs.acquire());
                        fetcher.fetchPage();
                    }
                }
            }
        });
    }

    /**
     * Links this exchange source with an empty/dummy remote sink. The purpose of this is to prevent this exchange source from finishing
     * until we have performed other async actions, such as linking actual remote sinks.
     *
     * @return a Releasable that should be called when the caller no longer needs to prevent the exchange source from completing.
     */
    public Releasable addEmptySink() {
        outstandingSinks.trackNewInstance();
        return outstandingSinks::finishInstance;
    }

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
