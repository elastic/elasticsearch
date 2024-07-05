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
import org.elasticsearch.core.Releasable;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link ExchangeSourceHandler} asynchronously fetches pages and status from multiple {@link RemoteSink}s
 * and feeds them to its {@link ExchangeSource}, which are created using the {@link #createExchangeSource()}) method.
 * {@link RemoteSink}s are added using the {@link #addRemoteSink(RemoteSink, int)}) method.
 *
 * @see #createExchangeSource()
 * @see #addRemoteSink(RemoteSink, int)
 */
public final class ExchangeSourceHandler {
    private final ExchangeBuffer buffer;
    private final Executor fetchExecutor;

    private final PendingInstances outstandingSinks;
    private final PendingInstances outstandingSources;
    private final FailureCollector failure = new FailureCollector();

    public ExchangeSourceHandler(int maxBufferSize, Executor fetchExecutor) {
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.fetchExecutor = fetchExecutor;
        this.outstandingSinks = new PendingInstances(() -> buffer.finish(false));
        this.outstandingSources = new PendingInstances(() -> buffer.finish(true));
    }

    private class ExchangeSourceImpl implements ExchangeSource {
        private boolean finished;

        ExchangeSourceImpl() {
            outstandingSources.trackNewInstance();
        }

        private void checkFailure() {
            Exception e = failure.getFailure();
            if (e != null) {
                throw ExceptionsHelper.convertToElastic(e);
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
        public SubscribableListener<Void> waitForReading() {
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

    public void addCompletionListener(ActionListener<Void> listener) {
        buffer.addCompletionListener(ActionListener.running(() -> {
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

        RemoteSinkFetcher(RemoteSink remoteSink) {
            outstandingSinks.trackNewInstance();
            this.remoteSink = remoteSink;
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
                        SubscribableListener<Void> future = buffer.waitForWriting();
                        if (future.isDone()) {
                            if (loopControl.tryResume() == false) {
                                fetchPage();
                            }
                        } else {
                            future.addListener(ActionListener.wrap(unused -> {
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
            failure.unwrapAndCollect(e);
            buffer.waitForReading().onResponse(null); // resume the Driver if it is being blocked on reading
            onSinkComplete();
        }

        void onSinkComplete() {
            if (finished == false) {
                finished = true;
                outstandingSinks.finishInstance();
            }
        }
    }

    /**
     * Add a remote sink as a new data source of this handler. The handler will start fetching data from this remote sink intermediately.
     *
     * @param remoteSink the remote sink
     * @param instances  the number of concurrent ``clients`` that this handler should use to fetch pages. More clients reduce latency,
     *                   but add overhead.
     * @see ExchangeSinkHandler#fetchPageAsync(boolean, ActionListener)
     */
    public void addRemoteSink(RemoteSink remoteSink, int instances) {
        for (int i = 0; i < instances; i++) {
            var fetcher = new RemoteSinkFetcher(remoteSink);
            fetchExecutor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    fetcher.onSinkFailed(e);
                }

                @Override
                protected void doRun() {
                    fetcher.fetchPage();
                }
            });
        }
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
