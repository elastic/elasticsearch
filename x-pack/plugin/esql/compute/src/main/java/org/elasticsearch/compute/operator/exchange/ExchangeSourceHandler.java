/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.EsqlRefCountingListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link ExchangeSourceHandler} asynchronously fetches pages and status from multiple {@link RemoteSink}s
 * and feeds them to its {@link ExchangeSource}, which are created using the {@link #createExchangeSource()}) method.
 * {@link RemoteSink}s are added using the {@link #addAndStartRemoteSink(RemoteSink, boolean, Runnable, int, ActionListener)}) method.
 *
 * @see #createExchangeSource()
 * @see #addAndStartRemoteSink(RemoteSink, boolean, Runnable, int, ActionListener)
 */
public final class ExchangeSourceHandler {

    private final ExchangeBuffer buffer;
    private final Executor fetchExecutor;

    private final PendingInstances outstandingSinks;
    private final PendingInstances outstandingSources;
    // Track if this exchange source should abort. There is no need to track the actual failure since the actual failure
    // should be notified via #addRemoteSink(RemoteSink, boolean, Runnable, int, ActionListener).
    private volatile boolean aborted = false;

    private final AtomicInteger nextSinkId = new AtomicInteger();
    private final Map<Integer, RemoteSink> remoteSinks = ConcurrentCollections.newConcurrentMap();

    /**
     * Creates a new ExchangeSourceHandler.
     *
     * @param maxBufferSize      the maximum size of the exchange buffer. A larger buffer reduces ``pauses`` but uses more memory,
     *                           which could otherwise be allocated for other purposes.
     * @param fetchExecutor      the executor used to fetch pages.
     */
    public ExchangeSourceHandler(int maxBufferSize, Executor fetchExecutor) {
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.fetchExecutor = fetchExecutor;
        this.outstandingSinks = new PendingInstances(() -> buffer.finish(false));
        this.outstandingSources = new PendingInstances(() -> finishEarly(true, ActionListener.noop()));
    }

    public boolean isFinished() {
        return buffer.isFinished();
    }

    private void checkFailure() {
        if (aborted) {
            throw new TaskCancelledException("remote sinks failed");
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
        private final Runnable onPageFetched;
        private final ActionListener<Void> completionListener;

        RemoteSinkFetcher(RemoteSink remoteSink, boolean failFast, Runnable onPageFetched, ActionListener<Void> completionListener) {
            outstandingSinks.trackNewInstance();
            this.remoteSink = remoteSink;
            this.onPageFetched = onPageFetched;
            this.failFast = failFast;
            this.completionListener = completionListener;
        }

        void fetchPage() {
            final LoopControl loopControl = new LoopControl();
            while (loopControl.isRunning()) {
                loopControl.exiting();
                // finish other sinks if one of them failed or source no longer need pages.
                boolean toFinishSinks = buffer.noMoreInputs() || aborted;
                remoteSink.fetchPageAsync(toFinishSinks, ActionListener.wrap(resp -> {
                    Page page = resp.takePage();
                    if (page != null) {
                        onPageFetched.run();
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
                aborted = true;
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
     * @param remoteSink    the remote sink
     * @param failFast      determines how failures in this remote sink are handled:
     *                      - If {@code false}, failures from this remote sink will not cause the exchange source to abort.
     *                      Callers must handle these failures notified via {@code listener}.
     *                      - If {@code true}, failures from this remote sink will cause the exchange source to abort.
     *
     * @param onPageFetched a callback that will be called when a page is fetched from the remote sink
     * @param instances     the number of concurrent ``clients`` that this handler should use to fetch pages.
     *                      More clients reduce latency, but add overhead.
     * @param listener      a listener that will be notified when the sink fails or completes. Callers must handle failures notified via
     *                      this listener.
     * @see ExchangeSinkHandler#fetchPageAsync(boolean, ActionListener)
     */
    public void addAndStartRemoteSink(
        RemoteSink remoteSink,
        boolean failFast,
        Runnable onPageFetched,
        int instances,
        ActionListener<Void> listener
    ) {
        addRemoteSink(remoteSink, failFast, onPageFetched, instances, listener).run();
    }

    public Runnable addRemoteSink(
        RemoteSink remoteSink,
        boolean failFast,
        Runnable onPageFetched,
        int instances,
        ActionListener<Void> listener
    ) {
        final int sinkId = nextSinkId.incrementAndGet();
        remoteSinks.put(sinkId, remoteSink);
        final ActionListener<Void> sinkListener = ActionListener.assertAtLeastOnce(
            ActionListener.notifyOnce(ActionListener.runBefore(listener, () -> remoteSinks.remove(sinkId)))
        );
        final Releasable emptySink = addEmptySink();
        return () -> fetchExecutor.execute(new AbstractRunnable() {
            @Override
            public void onAfter() {
                emptySink.close();
            }

            @Override
            public void onFailure(Exception e) {
                if (failFast) {
                    aborted = true;
                }
                buffer.waitForReading().listener().onResponse(null); // resume the Driver if it is being blocked on reading
                remoteSink.close(ActionListener.running(() -> sinkListener.onFailure(e)));
            }

            @Override
            protected void doRun() {
                try (EsqlRefCountingListener refs = new EsqlRefCountingListener(sinkListener)) {
                    for (int i = 0; i < instances; i++) {
                        new RemoteSinkFetcher(remoteSink, failFast, onPageFetched, refs.acquire()).fetchPage();
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

    /**
     * Gracefully terminates the exchange source early by instructing all remote exchange sinks to stop their computations.
     * This can happen when the exchange source has accumulated enough data (e.g., reaching the LIMIT) or when users want to
     * see the current result immediately.
     *
     * @param drainingPages whether to discard pages already fetched in the exchange
     */
    public void finishEarly(boolean drainingPages, ActionListener<Void> listener) {
        buffer.finish(drainingPages);
        try (EsqlRefCountingListener refs = new EsqlRefCountingListener(listener)) {
            for (RemoteSink remoteSink : remoteSinks.values()) {
                remoteSink.close(refs.acquire());
            }
        }
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
