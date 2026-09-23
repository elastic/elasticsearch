/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure.executors;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A Reactor {@link Scheduler} running on an Elasticsearch thread pool whose {@link Worker}s run their tasks one at a time and in submission
 * order, as the workers of Reactor's own schedulers do.
 * <p>
 * The Azure SDK is given its Reactor schedulers through a {@link Schedulers.Factory} so that its work runs on an Elasticsearch thread pool.
 * {@link Schedulers#fromExecutorService} would be the obvious way to build them, but the workers of such a scheduler hand every task to the
 * executor independently, so two tasks of the same worker can run concurrently on two pool threads; Reactor documents these workers as
 * "not guaranteed to run in FIFO order and strictly non-concurrently". Reactor's operators and the SDK are written and tested against the
 * stock schedulers, whose workers are effectively single-threaded, and some operators rely on it. For instance, {@code subscribeOn} hands
 * every {@code request} that arrives from another thread to its worker: with concurrent workers a request issued by the Netty event loop
 * while the worker is still producing runs concurrently with that production, which reached a {@code concatMap} that was not safe against
 * it and duplicated and dropped upload buffers (see {@code AzureBlobStoreToFluxTests}).
 * <p>
 * Reactor's {@link Schedulers#fromExecutor(java.util.concurrent.Executor, boolean) fromExecutor(executor, true)} has workers with the
 * required guarantee (each worker queues its tasks and drains the queue from one executor thread at a time), but it is not time-capable,
 * and the SDK's timeouts and retry backoff need delayed scheduling. This scheduler delegates all immediate scheduling to such a
 * trampolining scheduler and adds the time-based methods on top of the {@link ThreadPool}'s scheduler: when a delay elapses or a period
 * ticks, the task is handed to the trampolining scheduler or worker, so timed worker tasks are serialized with the worker's other tasks.
 */
public final class TrampolineScheduler implements Scheduler {

    private static final Logger logger = LogManager.getLogger(TrampolineScheduler.class);

    private final ThreadPool threadPool;
    private final String executorName;
    private final Scheduler trampoline;

    public TrampolineScheduler(ThreadPool threadPool, String executorName) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.executorName = Objects.requireNonNull(executorName);
        this.trampoline = Schedulers.fromExecutor(threadPool.executor(executorName), true);
    }

    @Override
    public Disposable schedule(Runnable task) {
        return trampoline.schedule(task);
    }

    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        return scheduleDelayed(task, delay, unit, trampoline::schedule, null);
    }

    @Override
    public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        return schedulePeriodically(task, initialDelay, period, unit, trampoline::schedule, null);
    }

    @Override
    public Worker createWorker() {
        return new TrampolineWorker(trampoline.createWorker());
    }

    @Override
    public long now(TimeUnit unit) {
        return trampoline.now(unit);
    }

    @Override
    public void init() {
        trampoline.init();
    }

    @Override
    public void dispose() {
        trampoline.dispose();
    }

    @Override
    public Mono<Void> disposeGracefully() {
        return trampoline.disposeGracefully();
    }

    @Override
    public boolean isDisposed() {
        return trampoline.isDisposed();
    }

    @Override
    public String toString() {
        return "TrampolineScheduler(" + executorName + ")";
    }

    /**
     * Runs {@code task} through {@code scheduleNow} once {@code delay} has elapsed. The thread pool's scheduler thread does nothing but
     * that hand-over.
     */
    private Disposable scheduleDelayed(
        Runnable task,
        long delay,
        TimeUnit unit,
        Function<Runnable, Disposable> scheduleNow,
        @Nullable TrampolineWorker owner
    ) {
        if (delay <= 0L) {
            return scheduleNow.apply(task);
        }
        final TimedTask timed = new TimedTask(task, owner);
        try {
            timed.setTimer(
                threadPool.schedule(() -> timed.fire(scheduleNow::apply), new TimeValue(delay, unit), EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        } catch (RejectedExecutionException e) {
            timed.dispose();
            throw Exceptions.failWithRejected(e);
        }
        return timed;
    }

    /**
     * Runs {@code task} through {@code scheduleNow} every {@code period}, starting after {@code initialDelay}, until disposed. Ticks that
     * find the previous run still queued are not conflated; Reactor's own schedulers do not conflate them either.
     */
    private Disposable schedulePeriodically(
        Runnable task,
        long initialDelay,
        long period,
        TimeUnit unit,
        Consumer<Runnable> scheduleNow,
        @Nullable TrampolineWorker owner
    ) {
        final TimedTask periodic = new TimedTask(task, owner);
        try {
            periodic.setTimer(
                org.elasticsearch.threadpool.Scheduler.wrapAsCancellable(
                    threadPool.scheduler().scheduleAtFixedRate(() -> periodic.fire(scheduleNow), initialDelay, period, unit)
                )
            );
        } catch (RejectedExecutionException e) {
            periodic.dispose();
            throw Exceptions.failWithRejected(e);
        }
        return periodic;
    }

    /**
     * A worker of the trampolining scheduler plus time-based scheduling. Timed tasks are tracked so that disposing the worker cancels them.
     */
    final class TrampolineWorker implements Worker {

        private final Worker delegate;
        private final Set<TimedTask> timed = ConcurrentCollections.newConcurrentSet();
        private volatile boolean disposed;

        TrampolineWorker(Worker delegate) {
            this.delegate = delegate;
        }

        @Override
        public Disposable schedule(Runnable task) {
            return scheduleNow(task);
        }

        @Override
        public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            return scheduleDelayed(task, delay, unit, this::scheduleNow, this);
        }

        @Override
        public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            return TrampolineScheduler.this.schedulePeriodically(task, initialDelay, period, unit, this::scheduleNow, this);
        }

        /**
         * Queues {@code task} on the delegate worker. The returned handle never disposes the delegate's own handle: disposing a queued
         * task of Reactor's trampolining worker removes it from the queue without adjusting the worker's work-in-progress count, which
         * leaves the drain loop spinning on a pool thread (reactor-core 3.7.19), and disposing the worker skips the last queued task's
         * handle. A disposed task therefore stays queued and runs as a no-op.
         */
        private Disposable scheduleNow(Runnable task) {
            if (disposed) {
                throw Exceptions.failWithRejected();
            }
            final WorkerTask queued = new WorkerTask(task);
            delegate.schedule(queued);
            return queued;
        }

        private final class WorkerTask implements Runnable, Disposable {
            private final Runnable task;
            private volatile boolean disposed;

            WorkerTask(Runnable task) {
                this.task = Objects.requireNonNull(task);
            }

            @Override
            public void run() {
                if (isDisposed() == false) {
                    task.run();
                }
            }

            @Override
            public void dispose() {
                disposed = true;
            }

            @Override
            public boolean isDisposed() {
                return disposed || TrampolineWorker.this.disposed;
            }

            @Override
            public String toString() {
                return "WorkerTask(" + task + ")";
            }
        }

        @Override
        public void dispose() {
            if (disposed) {
                return;
            }
            disposed = true;
            for (TimedTask task : timed) {
                task.dispose();
            }
            timed.clear();
            delegate.dispose();
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }

        @Override
        public String toString() {
            return "TrampolineWorker(" + delegate + ")";
        }
    }

    /**
     * A delayed or periodic task and the timer that fires it. Disposing cancels the timer; runs that were already handed over to the
     * trampolining scheduler or worker become no-ops.
     */
    private static final class TimedTask implements Disposable {

        private final Runnable task;
        @Nullable
        private final TrampolineWorker owner;
        private volatile boolean disposed;
        private volatile org.elasticsearch.threadpool.Scheduler.Cancellable timer;

        TimedTask(Runnable task, @Nullable TrampolineWorker owner) {
            this.task = Objects.requireNonNull(task);
            this.owner = owner;
            if (owner != null) {
                if (owner.disposed) {
                    throw Exceptions.failWithRejected();
                }
                owner.timed.add(this);
                if (owner.disposed) {
                    owner.timed.remove(this);
                    throw Exceptions.failWithRejected();
                }
            }
        }

        void setTimer(org.elasticsearch.threadpool.Scheduler.Cancellable timer) {
            this.timer = timer;
            if (disposed) {
                timer.cancel();
            }
        }

        void fire(Consumer<Runnable> scheduleNow) {
            if (isDisposed()) {
                return;
            }
            try {
                scheduleNow.accept(() -> {
                    if (isDisposed() == false) {
                        task.run();
                    }
                });
            } catch (RejectedExecutionException e) {
                // the worker or the executor was disposed after the timer was started; there is nothing to run the task on anymore
                logger.debug(() -> "dropping timed task " + this, e);
            }
        }

        @Override
        public void dispose() {
            disposed = true;
            if (owner != null) {
                owner.timed.remove(this);
            }
            final org.elasticsearch.threadpool.Scheduler.Cancellable timer = this.timer;
            if (timer != null) {
                timer.cancel();
            }
        }

        @Override
        public boolean isDisposed() {
            return disposed || (owner != null && owner.disposed);
        }

        @Override
        public String toString() {
            return "TimedTask(" + task + ")";
        }
    }
}
