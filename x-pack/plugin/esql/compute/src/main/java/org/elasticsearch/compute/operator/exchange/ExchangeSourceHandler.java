/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.Page;

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

    private final PendingInstances allSinks = new PendingInstances();
    private final PendingInstances allSources = new PendingInstances();

    public ExchangeSourceHandler(int maxBufferSize, Executor fetchExecutor) {
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.fetchExecutor = fetchExecutor;
    }

    private class LocalExchangeSource implements ExchangeSource {
        private boolean finished;

        LocalExchangeSource() {
            allSources.trackNewInstance();
        }

        @Override
        public Page pollPage() {
            return buffer.pollPage();
        }

        @Override
        public boolean isFinished() {
            return buffer.isFinished();
        }

        @Override
        public ListenableActionFuture<Void> waitForReading() {
            return buffer.waitForReading();
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                if (allSources.finishInstance()) {
                    buffer.drainPages();
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

    private void onRemoteSinkFailed(Exception e) {
        // TODO: handle error
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

    private void fetchPage(RemoteSink remoteSink) {
        final LoopControl loopControl = new LoopControl();
        while (loopControl.isRunning()) {
            loopControl.exiting();
            remoteSink.fetchPageAsync(new ExchangeRequest(allSources.finished()), ActionListener.wrap(resp -> {
                Page page = resp.page();
                if (page != null) {
                    buffer.addPage(page);
                }
                if (resp.finished()) {
                    if (allSinks.finishInstance()) {
                        buffer.finish();
                    }
                } else {
                    ListenableActionFuture<Void> future = buffer.waitForWriting();
                    if (future.isDone()) {
                        if (loopControl.tryResume() == false) {
                            fetchPage(remoteSink);
                        }
                    } else {
                        future.addListener(new ActionListener<>() {
                            @Override
                            public void onResponse(Void unused) {
                                if (loopControl.tryResume() == false) {
                                    fetchPage(remoteSink);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                onRemoteSinkFailed(e);
                            }
                        });
                    }
                }
            }, this::onRemoteSinkFailed));
        }
        loopControl.exited();
    }

    /**
     * Add a remote sink as a new data source of this handler. The handler will start fetching data from this remote sink intermediately.
     *
     * @param remoteSink the remote sink
     * @param instances  the number of concurrent ``clients`` that this handler should use to fetch pages. More clients reduce latency,
     *                   but add overhead.
     * @see ExchangeSinkHandler#fetchPageAsync(ExchangeRequest, ActionListener)
     */
    public void addRemoteSink(RemoteSink remoteSink, int instances) {
        for (int i = 0; i < instances; i++) {
            allSinks.trackNewInstance();
            fetchExecutor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    onRemoteSinkFailed(e);
                }

                @Override
                protected void doRun() {
                    fetchPage(remoteSink);
                }
            });
        }
    }

    private static final class PendingInstances {
        private volatile boolean finished;
        private final AtomicInteger instances = new AtomicInteger();

        void trackNewInstance() {
            instances.incrementAndGet();
        }

        boolean finishInstance() {
            if (instances.decrementAndGet() == 0) {
                finished = true;
                return true;
            } else {
                return false;
            }
        }

        boolean finished() {
            return finished;
        }
    }

}
