/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionHandler.RejectionMetrics;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

/**
 * Virtual thread executor mimicking the behavior of {@link EsThreadPoolExecutor} with virtual threads.
 *
 * <p>Each task is run in a new virtual thread. Concurrency is limited by the {@link #carrierThreads} {@link Semaphore} with
 * max {@link #threads} permits. Further tasks will be queued and, unless unbound ({@link #queueSize}{@code =-1}), tasks will eventually
 * be rejected if exceeding {@link #queueSize}.
 *
 * <p>{@link AbstractRunnable#isForceExecution()}{@code =true}) allows to bypass the queue limit and execute the task regardless.
 *
 * <p>Note: Sharing of expensive resources such as buffers by means of {@link ThreadLocal}s isn't applicable if using virtual threads as
 * there's no point pooling them. If using virtual threads, {@link ThreadLocal}s should be replaced by object pools or similar for sharing
 * resources in most of the cases.
 */
public class EsVirtualThreadExecutorService extends AbstractExecutorService implements EsExecutorService {
    private static final Logger logger = LogManager.getLogger(EsVirtualThreadExecutorService.class);
    private static final VarHandle PENDING_HANDLE;

    static {
        try {
            PENDING_HANDLE = MethodHandles.lookup().findVarHandle(EsVirtualThreadExecutorService.class, "pendingTasks", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    final ExecutorService virtualExecutor;

    private final String name;
    private final RejectionMetrics rejectionMetrics = new RejectionMetrics();
    private final Semaphore carrierThreads;
    private final int threads;
    private final int queueSize;
    private final ThreadContext contextHolder;

    private final LongAdder completed = new LongAdder();
    private volatile int largestPoolSize = 0;
    private volatile int pendingTasks = 0;

    public static EsVirtualThreadExecutorService create(
        String name,
        int threads,
        int queueSize,
        boolean rejectAfterShutdown,
        ThreadContext contextHolder,
        TaskTrackingConfig trackingConfig
    ) {
        if (rejectAfterShutdown) {
            return trackingConfig.trackExecutionTime()
                ? new TaskTrackingEsVirtualThreadExecutorService(name, threads, queueSize, contextHolder, trackingConfig)
                : new EsVirtualThreadExecutorService(name, threads, queueSize, contextHolder);
        } else {
            return trackingConfig.trackExecutionTime()
                ? new TaskTrackingDelayedShutdownEsVirtualThreadExecutorService(name, threads, queueSize, contextHolder, trackingConfig)
                : new DelayedShutdownEsVirtualThreadExecutorService(name, threads, queueSize, contextHolder);
        }
    }

    private EsVirtualThreadExecutorService(String name, int threads, int queueSize, ThreadContext contextHolder) {
        this.name = name;
        this.virtualExecutor = Executors.newThreadPerTaskExecutor(virtualThreadFactory(name));
        this.carrierThreads = new Semaphore(threads, true);
        this.threads = threads;
        this.queueSize = queueSize;
        this.contextHolder = contextHolder;
    }

    // FIXME remove hack, should be pushed up
    private static ThreadFactory virtualThreadFactory(String name) {
        String[] split = name.split("/", 2);
        name = split.length == 2 ? EsExecutors.threadName(split[0], split[1]) : EsExecutors.threadName("", split[0]);
        return Thread.ofVirtual().name(name, 0).factory();
    }

    @Override
    public Stream<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
        // See EsThreadPoolExecutor#setupMetrics
        rejectionMetrics.registerCounter(meterRegistry, threadPoolName);
        return Stream.empty();
    }

    @Override
    public int getActiveCount() {
        return threads - carrierThreads.availablePermits();
    }

    @Override
    public long getRejectedTaskCount() {
        return rejectionMetrics.getRejectedTaskCount();
    }

    @Override
    public long getCompletedTaskCount() {
        return completed.sum();
    }

    @Override
    public int getCurrentQueueSize() {
        return Math.max(0, pendingTasks - threads);
    }

    @Override
    public int getMaximumPoolSize() {
        return threads;
    }

    @Override
    public int getPoolSize() {
        return getActiveCount();
    }

    @Override
    public int getLargestPoolSize() {
        return largestPoolSize;
    }

    private class ThrottledRunnable implements Runnable {
        private final Runnable r;

        ThrottledRunnable(Runnable r) {
            this.r = r;
        }

        @Override
        public void run() {
            try {
                long creationTimeNanos = System.nanoTime();
                carrierThreads.acquire();
                largestPoolSize = Math.max(getActiveCount(), largestPoolSize); // decent estimate
                try {
                    doExecute(r, creationTimeNanos);
                } finally {
                    carrierThreads.release();
                }
            } catch (InterruptedException e) {
                assert virtualExecutor.isShutdown();
                Thread.currentThread().interrupt();
            } finally {
                decrementPendingTasks();
                completed.increment();
            }
        }
    }

    // corresponds tp beforeExecute / afterExecute
    protected void doExecute(Runnable r, long creationTimeNanos) {
        r.run();
        EsExecutors.rethrowErrors(unwrap(r));
        assert assertDefaultContext(r);
    }

    // FIXME is this needed with virtual threads?
    private boolean assertDefaultContext(Runnable r) {
        assert contextHolder.isDefaultContext()
            : "the thread context is not the default context and the thread ["
                + Thread.currentThread().getName()
                + "] is being returned to the pool after executing ["
                + r
                + "]";
        return true;
    }

    @Override
    public void execute(Runnable command) {
        final Runnable wrappedRunnable = wrapRunnable(command);
        try {
            boolean forceExecution = EsAbortPolicy.isForceExecution(wrappedRunnable);
            if (virtualExecutor.isShutdown() == false && incrementPendingTasks(forceExecution) > 0) {
                try {
                    virtualExecutor.execute(new ThrottledRunnable(wrappedRunnable));
                } catch (RejectedExecutionException re) {
                    decrementPendingTasks();
                    handleRejection(wrappedRunnable);
                } catch (Exception e) {
                    decrementPendingTasks();
                    throw e;
                }
            } else {
                handleRejection(wrappedRunnable);
            }
        } catch (Exception e) {
            if (wrappedRunnable instanceof AbstractRunnable abstractRunnable) {
                try {
                    // If we are an abstract runnable we can handle the exception
                    // directly and don't need to rethrow it, but we log and assert
                    // any unexpected exception first.
                    if (e instanceof EsRejectedExecutionException == false) {
                        logException(abstractRunnable, e);
                    }
                    abstractRunnable.onRejection(e);
                } finally {
                    abstractRunnable.onAfter();
                }
            } else {
                throw e;
            }
        }
    }

    void handleRejection(Runnable wrappedRunnable) {
        rejectionMetrics.incrementRejections();
        throw EsRejectedExecutionHandler.newRejectedException(wrappedRunnable, this, isShutdown());
    }

    boolean hasPendingTasks() {
        return pendingTasks > 0;
    }

    int incrementPendingTasks(boolean forceExecution) {
        while (true) {
            int pending = pendingTasks;
            if (forceExecution == false && queueSize >= 0 && pending >= queueSize + threads) {
                return -1;
            }
            if (PENDING_HANDLE.weakCompareAndSet(this, pending, pending + 1)) {
                return pending + 1;
            }
        }
    }

    int decrementPendingTasks() {
        while (true) {
            int pending = pendingTasks;
            if (PENDING_HANDLE.weakCompareAndSet(this, pending, pending - 1)) {
                return pending - 1;
            }
        }
    }

    // package-visible for testing
    void logException(AbstractRunnable r, Exception e) {
        logger.error(() -> format("[%s] unexpected exception when submitting task [%s] for execution", name, r), e);
        assert false : "executor throws an exception (not a rejected execution exception) before the task has been submitted " + e;
    }

    @Override
    public final String toString() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName()).append('[');
        b.append("name = ").append(name).append(", ");
        if (queueSize >= 0) {
            b.append("queue capacity = ").append(queueSize).append(", ");
        }
        appendExecutorDetails(b);
        // append details similar to ThreadPoolExecutor.toString()
        b.append('[').append(isShutdown() == false ? "Running" : isTerminated() ? "Terminated" : "Shutting down");
        b.append(", pool size = ").append(getPoolSize());
        b.append(", active threads = ").append(getActiveCount());
        b.append(", queued tasks = ").append(getCurrentQueueSize());
        b.append(", completed tasks = ").append(getCompletedTaskCount());
        b.append(']');
        return b.toString();
    }

    /**
     * Append details about this thread pool as key/value pairs in the form "%s = %s, ".
     */
    protected void appendExecutorDetails(final StringBuilder sb) {}

    public void shutdown() {
        virtualExecutor.shutdown();
    }

    public boolean isShutdown() {
        return virtualExecutor.isShutdown();
    }

    public List<Runnable> shutdownNow() {
        return virtualExecutor.shutdownNow();
    }

    public boolean isTerminated() {
        return virtualExecutor.isTerminated();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return virtualExecutor.awaitTermination(timeout, unit);
    }

    protected Runnable wrapRunnable(Runnable command) {
        return contextHolder.preserveContext(command);
    }

    protected Runnable unwrap(Runnable runnable) {
        return ThreadContext.unwrap(runnable);
    }

    /**
     * This ExecutorService delays shutdown of the underlying virtual thread executor until pending tasks have been completed
     * to mimic the behavior of the {@link EsThreadPoolExecutor} in combination with {@link EsExecutors.ForceQueuePolicy}
     * if {@code rejectAfterShutdown=false}:
     *
     * If capacity (queue size) permits, tasks are accepted and executed even after shutdown. Once all pending tasks have been completed,
     * the underlying virtual thread executor is shutdown. Any attempt to submit new tasks afterwards is silently ignored without
     * rejecting these tasks.
     */
    private static class DelayedShutdownEsVirtualThreadExecutorService extends EsVirtualThreadExecutorService {
        private volatile boolean shutdown = false;
        private final CountDownLatch terminated = new CountDownLatch(1);

        DelayedShutdownEsVirtualThreadExecutorService(String name, int maximumPoolSize, int maximumQueueSize, ThreadContext contextHolder) {
            super(name, maximumPoolSize, maximumQueueSize, contextHolder);
        }

        @Override
        public void shutdown() {
            shutdown = true;
            if (hasPendingTasks() == false) {
                // only forward shutdown to executor if no pending tasks
                shutdownExecutor();
            }
        }

        private void shutdownExecutor() {
            terminated.countDown();
            virtualExecutor.shutdown();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return terminated.getCount() == 0;
        }

        @Override
        public List<Runnable> shutdownNow() {
            terminated.countDown();
            return virtualExecutor.shutdownNow();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            if (shutdown && hasPendingTasks() == false) {
                shutdownExecutor();
                return true;
            }
            return terminated.await(timeout, unit);
        }

        @Override
        int decrementPendingTasks() {
            int tasks = super.decrementPendingTasks();
            if (tasks == 0 && shutdown) {
                shutdownExecutor();
            }
            return tasks;
        }

        @Override
        void handleRejection(Runnable wrappedRunnable) {
            // if executor not shutdown rejection is due to capacity limit, else ignore silently
            if (virtualExecutor.isShutdown() == false) {
                super.handleRejection(wrappedRunnable);
            }
        }
    }

    private interface TaskTrackerEsExecutorService extends TaskTrackingEsExecutorService {
        TaskTracker taskTracker();

        default long getMaxQueueLatencyMillisSinceLastPollAndReset() {
            return taskTracker().getMaxQueueLatencyMillisSinceLastPollAndReset();
        }

        default double getTaskExecutionEWMA() {
            return taskTracker().getTaskExecutionEWMA();
        }

        default double pollUtilization(UtilizationTrackingPurpose utilizationTrackingPurpose) {
            return taskTracker().pollUtilization(utilizationTrackingPurpose);
        }
    }

    private static class TaskTrackingEsVirtualThreadExecutorService extends EsVirtualThreadExecutorService
        implements
            TaskTrackerEsExecutorService {
        private final TaskTracker taskTracker;

        TaskTrackingEsVirtualThreadExecutorService(
            String name,
            int maximumPoolSize,
            int maximumQueueSize,
            ThreadContext contextHolder,
            TaskTrackingConfig trackingConfig
        ) {
            super(name, maximumPoolSize, maximumQueueSize, contextHolder);
            this.taskTracker = new TaskTracker(trackingConfig, maximumPoolSize);

        }

        public Stream<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
            return Stream.concat(
                super.setupMetrics(meterRegistry, threadPoolName),
                taskTracker.setupMetrics(meterRegistry, threadPoolName)
            );
        }

        @Override
        protected void doExecute(Runnable r, long creationTimeNanos) {
            taskTracker.trackTask(r);
            try {
                long startTimeNanos = System.nanoTime();
                taskTracker.taskQueueLatency(startTimeNanos - creationTimeNanos);
                super.doExecute(r, creationTimeNanos);
                taskTracker.taskExecutionTime(Math.max(System.nanoTime() - startTimeNanos, 1L));
            } finally {
                taskTracker.untrackTask(r);
            }
        }

        @Override
        protected void appendExecutorDetails(StringBuilder sb) {
            taskTracker.appendTaskExecutionDetails(sb);
        }

        @Override
        public TaskTracker taskTracker() {
            return taskTracker;
        }
    }

    private static class TaskTrackingDelayedShutdownEsVirtualThreadExecutorService extends DelayedShutdownEsVirtualThreadExecutorService
        implements
            TaskTrackerEsExecutorService {
        private final TaskTracker taskTracker;

        TaskTrackingDelayedShutdownEsVirtualThreadExecutorService(
            String name,
            int maximumPoolSize,
            int maximumQueueSize,
            ThreadContext contextHolder,
            TaskTrackingConfig trackingConfig
        ) {
            super(name, maximumPoolSize, maximumQueueSize, contextHolder);
            this.taskTracker = new TaskTracker(trackingConfig, maximumPoolSize);

        }

        public Stream<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
            return Stream.concat(
                super.setupMetrics(meterRegistry, threadPoolName),
                taskTracker.setupMetrics(meterRegistry, threadPoolName)
            );
        }

        @Override
        protected void doExecute(Runnable r, long creationTimeNanos) {
            taskTracker.trackTask(r);
            try {
                long startTimeNanos = System.nanoTime();
                taskTracker.taskQueueLatency(startTimeNanos - creationTimeNanos);
                super.doExecute(r, creationTimeNanos);
                taskTracker.taskExecutionTime(Math.max(System.nanoTime() - startTimeNanos, 1L));
            } finally {
                taskTracker.untrackTask(r);
            }
        }

        @Override
        protected void appendExecutorDetails(StringBuilder sb) {
            taskTracker.appendTaskExecutionDetails(sb);
        }

        @Override
        public TaskTracker taskTracker() {
            return taskTracker;
        }
    }
}
