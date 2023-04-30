/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure.executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;

/**
 * Wrapper around {@link ThreadPool} that provides the necessary scheduling methods for a {@link reactor.core.scheduler.Scheduler} to
 * function. This allows injecting a custom Executor to the reactor schedulers factory and get fine grained control over the
 * thread resources used.
 */
@SuppressForbidden(reason = "It wraps a ThreadPool and delegates all the work")
public class ReactorScheduledExecutorService extends AbstractExecutorService implements ScheduledExecutorService {
    private final ThreadPool threadPool;
    private final String executorName;
    private final ExecutorService delegate;
    private final Logger logger = LogManager.getLogger(ReactorScheduledExecutorService.class);

    public ReactorScheduledExecutorService(ThreadPool threadPool, String executorName) {
        this.threadPool = threadPool;
        this.executorName = executorName;
        this.delegate = threadPool.executor(executorName);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        Scheduler.ScheduledCancellable schedule = threadPool.schedule(() -> {
            try {
                decorateCallable(callable).call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, new TimeValue(delay, unit), executorName);

        return new ReactorFuture<>(schedule);
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        Runnable decoratedCommand = decorateRunnable(command);
        Scheduler.ScheduledCancellable schedule = threadPool.schedule(decoratedCommand, new TimeValue(delay, unit), executorName);
        return new ReactorFuture<>(schedule);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        Runnable decoratedCommand = decorateRunnable(command);

        return threadPool.scheduler().scheduleAtFixedRate(() -> {
            try {
                delegate.execute(decoratedCommand);
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug(
                        () -> format(
                            "could not schedule execution of [%s] on [%s] as executor is shut down",
                            decoratedCommand,
                            executorName
                        ),
                        e
                    );
                } else {
                    throw e;
                }
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        Runnable decorateRunnable = decorateRunnable(command);

        Scheduler.Cancellable cancellable = threadPool.scheduleWithFixedDelay(decorateRunnable, new TimeValue(delay, unit), executorName);

        return new ReactorFuture<>(cancellable);
    }

    @Override
    public void shutdown() {
        // No-op
    }

    @Override
    public List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(decorateRunnable(command));
    }

    protected Runnable decorateRunnable(Runnable command) {
        return command;
    }

    protected <V> Callable<V> decorateCallable(Callable<V> callable) {
        return callable;
    }

    private static final class ReactorFuture<V> implements ScheduledFuture<V> {
        private final Scheduler.Cancellable cancellable;

        private ReactorFuture(Scheduler.Cancellable cancellable) {
            this.cancellable = cancellable;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(Delayed o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return cancellable.cancel();
        }

        @Override
        public boolean isCancelled() {
            return cancellable.isCancelled();
        }

        @Override
        public boolean isDone() {
            return cancellable.isCancelled();
        }

        @Override
        public V get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public V get(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }
    }
}
