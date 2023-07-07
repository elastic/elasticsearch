/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link ExchangeSourceHandler} asynchronously fetches pages and status from multiple {@link RemoteSink}s
 * and feeds them to its {@link ExchangeSource}, which are created using the {@link #createExchangeSource()}) method.
 * {@link RemoteSink}s are added using the {@link #addRemoteSink(RemoteSink, int)}) method.
 *
 * @see #createExchangeSource()
 * @see #addRemoteSink(RemoteSink, int)
 */
public final class ExchangeSourceHandler extends AbstractRefCounted {
    private final ExchangeBuffer buffer;
    private final Executor fetchExecutor;

    private final PendingInstances outstandingSinks = new PendingInstances();
    private final PendingInstances outstandingSources = new PendingInstances();
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final ListenableActionFuture<Void> completionFuture = new ListenableActionFuture<>();

    public ExchangeSourceHandler(int maxBufferSize, Executor fetchExecutor) {
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.fetchExecutor = fetchExecutor;
    }

    private class LocalExchangeSource implements ExchangeSource {
        private boolean finished;

        LocalExchangeSource() {
            outstandingSources.trackNewInstance();
        }

        private void checkFailure() {
            Exception e = failure.get();
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
        public ListenableActionFuture<Void> waitForReading() {
            return buffer.waitForReading();
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                if (outstandingSources.finishInstance()) {
                    buffer.finish(true);
                }
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
        return new LocalExchangeSource();
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
                // finish other sinks if one of them failed or sources no longer need pages.
                boolean toFinishSinks = buffer.noMoreInputs() || failure.get() != null;
                remoteSink.fetchPageAsync(toFinishSinks, ActionListener.wrap(resp -> {
                    Page page = resp.page();
                    if (page != null) {
                        buffer.addPage(page);
                    }
                    if (resp.finished()) {
                        onSinkComplete();
                    } else {
                        ListenableActionFuture<Void> future = buffer.waitForWriting();
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
            failure.getAndUpdate(first -> {
                if (first == null) {
                    return e;
                }
                // ignore subsequent TaskCancelledException exceptions as they don't provide useful info.
                if (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
                    return first;
                }
                if (ExceptionsHelper.unwrap(first, TaskCancelledException.class) != null) {
                    return e;
                }
                if (ExceptionsHelper.unwrapCause(first) != ExceptionsHelper.unwrapCause(e)) {
                    first.addSuppressed(e);
                }
                return first;
            });
            onSinkComplete();
        }

        void onSinkComplete() {
            if (finished == false) {
                finished = true;
                if (outstandingSinks.finishInstance()) {
                    buffer.finish(false);
                }
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

    @Override
    protected void closeInternal() {
        Exception error = failure.get();
        if (error != null) {
            completionFuture.onFailure(error);
        } else {
            completionFuture.onResponse(null);
        }
    }

    /**
     * Add a listener, which will be notified when this exchange source handler is completed. An exchange source
     * handler is consider completed when all exchange sources and sinks are completed and de-attached.
     */
    public void addCompletionListener(ActionListener<Void> listener) {
        completionFuture.addListener(listener);
    }

    private final class PendingInstances {
        private final AtomicInteger instances = new AtomicInteger();

        void trackNewInstance() {
            incRef();
            instances.incrementAndGet();
        }

        boolean finishInstance() {
            decRef();
            return instances.decrementAndGet() == 0;
        }
    }

}
