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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.concurrent.EsAbortPolicy.isForceExecution;
import static org.elasticsearch.core.Strings.format;

/**
 * Virtual thread executor mimicking the behavior of {@link EsThreadPoolExecutor} with virtual threads.
 *
 * <p>Until the concurrency limit {@link #maxThreads} is reached, each new task is run on a new virtual thread. Otherwise, tasks will be
 * queued while capacity {@link #maxQueueSize} allows and rejected afterwards. If {@link #maxQueueSize} is negative, the queue is unbounded.
 *
 * <p>{@link AbstractRunnable#isForceExecution()}{@code =true} allows bypassing the queue limit and force queuing of a task regardless
 * of available capacity.
 *
 * <p>On completion of a task, further queued tasks might be executed on the same virtual thread.
 *
 * <p>This executor delays shutdown of the underlying virtual thread executor until pending tasks have been completed
 * to mimic the behavior of the {@link EsThreadPoolExecutor} in combination with {@link EsExecutors.ForceQueuePolicy}
 * if {@link #rejectAfterShutdown}{@code =false}.
 *
 * <p>In that case, if capacity permits, tasks are accepted and executed even after shutdown. Once all pending tasks have been completed,
 * the underlying virtual thread executor is shutdown. Any attempt to submit new tasks afterwards is silently ignored without
 * rejecting the tasks.
 *
 * <p>Note: Sharing of expensive resources such as buffers by means of {@link ThreadLocal}s isn't applicable if using virtual threads as
 * there's no point pooling them. If using virtual threads, {@link ThreadLocal}s should be replaced by object pools or similar for sharing
 * resources in most of the cases.
 */
public class EsVirtualThreadExecutorService extends AbstractExecutorService implements EsExecutorService {
    private static final Logger logger = LogManager.getLogger(EsVirtualThreadExecutorService.class);
    private static final VarHandle STATE_HANDLE;

    static {
        try {
            STATE_HANDLE = MethodHandles.lookup().findVarHandle(EsVirtualThreadExecutorService.class, "state", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final String name;
    private final int maxThreads;
    private final int maxQueueSize;
    private final boolean rejectAfterShutdown;
    private final ThreadContext contextHolder;
    private final RejectionMetrics rejectionMetrics = new RejectionMetrics();

    private final ExecutorService virtualExecutor;
    private final CountDownLatch terminated = new CountDownLatch(1);
    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final LongAdder completed = new LongAdder();

    private volatile boolean shutdown = false;
    private volatile int largestPoolSize = 0;
    // low: active tasks, high: queued tasks
    private volatile long state = 0;

    public static EsVirtualThreadExecutorService create(
        String name,
        int threads,
        int queueSize,
        boolean rejectAfterShutdown,
        ThreadContext contextHolder,
        TaskTrackingConfig trackingConfig
    ) {
        return trackingConfig.trackExecutionTime()
            ? new TaskTrackingEsVirtualThreadExecutorService(name, threads, queueSize, rejectAfterShutdown, contextHolder, trackingConfig)
            : new EsVirtualThreadExecutorService(name, threads, queueSize, rejectAfterShutdown, contextHolder);
    }

    @SuppressForbidden(reason = "internal implementation for EsExecutors")
    private EsVirtualThreadExecutorService(
        String name,
        int maxThreads,
        int maxQueueSize,
        boolean rejectAfterShutdown,
        ThreadContext contextHolder
    ) {
        this.name = name;
        this.virtualExecutor = Executors.newThreadPerTaskExecutor(virtualThreadFactory(name));
        this.maxThreads = maxThreads;
        this.maxQueueSize = 10;// maxQueueSize;
        this.rejectAfterShutdown = rejectAfterShutdown;
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
        return threads(state);
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
        return queueSize(state);
    }

    @Override
    public int getMaximumPoolSize() {
        return maxThreads;
    }

    @Override
    public int getPoolSize() {
        return getActiveCount();
    }

    @Override
    public int getLargestPoolSize() {
        return largestPoolSize;
    }

    private class TaskLoop implements Runnable {
        private Runnable wrapped;

        TaskLoop(Runnable wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void run() {
            largestPoolSize = Math.max(getActiveCount(), largestPoolSize);
            Throwable ex = null;
            while (wrapped != null) {
                try {
                    beforeExecute(wrapped);
                    try {
                        wrapped.run();
                    } catch (Throwable t) {
                        ex = t;
                        throw t;
                    } finally {
                        completed.increment();
                        // this is slightly different from EsThreadPoolExecutor, where afterExecute
                        // might be invoked twice if it throws an exception
                        afterExecute(wrapped, ex);
                    }
                } finally {
                    // if a next queued runnable is immediately available, either continue on the same virtual thread
                    // or fork a new one in case an exception was thrown.
                    wrapped = nextQueuedRunnable(true);
                    if (ex != null && wrapped != null) {
                        newVirtualThread(wrapped); // fork due to exception
                    }
                }
            }
        }

        /** Attempts to immediately execute the next queued task while still claiming an active thread.*/
        private Runnable nextQueuedRunnable(boolean recheckAfterRelease) {
            if (Thread.interrupted() || isTerminated()) {
                releaseThread();
                return null;
            }
            Runnable next = pollFromQueue();
            if (next != null) {
                return next;
            }
            releaseThread();
            // check if a task was queued since we released this thread and attempt to process in case reclaiming the thread succeeds.
            // this is necessary to avoid starvation if there are no active run loops.
            if (recheckAfterRelease && queueSize(state) > 0 && acquireThread()) {
                return nextQueuedRunnable(false);
            }
            return null;
        }
    }

    protected void beforeExecute(Runnable wrapped) {}

    protected void afterExecute(Runnable wrapped, Throwable t) {
        EsExecutors.rethrowErrors(unwrap(wrapped));
        assert assertDefaultContext(wrapped);
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

    private void newVirtualThread(Runnable wrapped) {
        try {
            virtualExecutor.execute(new TaskLoop(wrapped));
        } catch (RejectedExecutionException re) {
            releaseThread();
            handleRejection(wrapped);
        } catch (Throwable e) {
            releaseThread();
            throw e;
        }
    }

    @Override
    public void execute(Runnable command) {
        final Runnable wrapped = wrapRunnable(command);
        try {
            switch (acquireThreadOrQueueCapacity(wrapped)) {
                case THREAD -> newVirtualThread(wrapped);
                case QUEUE -> queue.add(wrapped);
                case REJECTED -> handleRejection(wrapped);
            }
        } catch (Exception e) {
            if (wrapped instanceof AbstractRunnable abstractRunnable) {
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

    void handleRejection(Runnable wrapped) {
        if (rejectAfterShutdown == false && shutdown) {
            incrementQueueUnbounded();
            queue.add(wrapped);
            return;
        }
        rejectionMetrics.incrementRejections();
        throw EsRejectedExecutionHandler.newRejectedException(wrapped, this, isShutdown());
    }

    private Runnable pollFromQueue() {
        if (decrementQueue()) {
            Runnable runnable;
            while ((runnable = queue.poll()) == null) {
            }
            return runnable;
        }
        return null;
    }

    // competes with other consumers for tasks in queue
    // possibly use ConcurrentLinkedDeque instead of ConcurrentLinkedQueue when drainQueue is necessary,
    // to use pollLast instead of poll in case order matters
    private List<Runnable> drainQueueAndCollectTasks() {
        int size = queueSize(state);
        if (size == 0) {
            return Collections.emptyList();
        }
        List<Runnable> drained = new ArrayList<>(size);
        Runnable runnable;
        while ((runnable = pollFromQueue()) != null) {
            drained.add(runnable);
        }
        return drained;
    }

    @Override
    public int drainQueue() {
        return drainQueueAndCollectTasks().size();
    }

    @Override
    public Stream<Runnable> getTasks() {
        return queue.stream().map(this::unwrap);
    }

    public void shutdown() {
        shutdown = true;
        maybeShutdownExecutor();
    }

    private boolean maybeShutdownExecutor() {
        if (shutdown && state == 0) {
            terminated.countDown();
            virtualExecutor.shutdown();
            return true;
        }
        return false;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public List<Runnable> shutdownNow() {
        shutdown = true;
        virtualExecutor.shutdownNow();
        terminated.countDown();
        return drainQueueAndCollectTasks();
    }

    public boolean isTerminated() {
        return terminated.getCount() == 0;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return maybeShutdownExecutor() ? true : terminated.await(timeout, unit);
    }

    protected Runnable wrapRunnable(Runnable runnable) {
        return contextHolder.preserveContext(runnable);
    }

    protected Runnable unwrap(Runnable runnable) {
        return ThreadContext.unwrap(runnable);
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
        if (maxQueueSize >= 0) {
            b.append("queue capacity = ").append(maxQueueSize).append(", ");
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

    private static int threads(long state) {
        return (int) state; // low bits of state
    }

    private static int queueSize(long state) {
        return (int) (state >>> 32); // high bits of state
    }

    private static final byte REJECTED = 0;
    private static final byte THREAD = 1;
    private static final byte QUEUE = 2;

    private boolean tryUpdateState(long current, long incr) {
        assert threads(current + incr) >= 0 : "negative thread count";
        assert queueSize(current + incr) >= 0 : "negative queue size";
        return STATE_HANDLE.weakCompareAndSet(this, current, current + incr);
    }

    private byte acquireThreadOrQueueCapacity(Runnable wrapped) {
        if (rejectAfterShutdown && shutdown) {
            return REJECTED;
        }
        boolean isTerminated = isTerminated();
        boolean queueUnbounded = maxQueueSize < 0 || isForceExecution(wrapped) || (rejectAfterShutdown == false && shutdown);
        while (true) {
            long current = state;
            if (isTerminated == false && threads(current) < maxThreads) {
                if (tryUpdateState(current, 1L)) { // increment thread count
                    return THREAD;
                }
            } else if (queueUnbounded || queueSize(current) < maxQueueSize) {
                if (tryUpdateState(current, 1L << 32)) { // increment queue size
                    return QUEUE;
                }
            } else {
                return REJECTED;
            }
        }
    }

    private boolean acquireThread() {
        while (true) {
            long current = state;
            if (threads(current) >= maxThreads) {
                return false;
            }
            if (tryUpdateState(current, 1L)) { // increment thread count
                return true;
            }
        }
    }

    private void releaseThread() {
        while (true) {
            long current = state;
            if (tryUpdateState(current, -1L)) { // decrement thread count
                maybeShutdownExecutor();
                return;
            }
        }
    }

    private boolean decrementQueue() {
        while (true) {
            long current = state;
            if (queueSize(current) <= 0) {
                return false;
            }
            if (tryUpdateState(current, -(1L << 32))) { // decrement queue size
                return true;
            }
        }
    }

    private void incrementQueueUnbounded() {
        while (true) {
            if (tryUpdateState(state, 1L << 32)) { // increment queue size
                return;
            }
        }
    }

    private static class TaskTrackingEsVirtualThreadExecutorService extends EsVirtualThreadExecutorService
        implements
            TaskTrackingEsExecutorService {
        private final TaskTracker taskTracker;

        TaskTrackingEsVirtualThreadExecutorService(
            String name,
            int maximumPoolSize,
            int maximumQueueSize,
            boolean rejectAfterShutdown,
            ThreadContext contextHolder,
            TaskTrackingConfig trackingConfig
        ) {
            super(name, maximumPoolSize, maximumQueueSize, rejectAfterShutdown, contextHolder);
            this.taskTracker = new TaskTracker(trackingConfig, maximumPoolSize);

        }

        @Override
        public Stream<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
            return Stream.concat(
                super.setupMetrics(meterRegistry, threadPoolName),
                taskTracker.setupMetrics(meterRegistry, threadPoolName)
            );
        }

        @Override
        public long getMaxQueueLatencyMillisSinceLastPollAndReset() {
            return taskTracker.getMaxQueueLatencyMillisSinceLastPollAndReset();
        }

        @Override
        public double getTaskExecutionEWMA() {
            return taskTracker.getTaskExecutionEWMA();
        }

        @Override
        public double pollUtilization(UtilizationTrackingPurpose utilizationTrackingPurpose) {
            return taskTracker.pollUtilization(utilizationTrackingPurpose);
        }

        @Override
        protected void beforeExecute(Runnable wrapped) {
            taskTracker.trackTask(wrapped);
            assert super.unwrap(wrapped) instanceof TimedRunnable : "expected only TimedRunnables in queue";
            taskTracker.beforeExecute((TimedRunnable) super.unwrap(wrapped));
        }

        @Override
        protected void afterExecute(Runnable wrapped, Throwable t) {
            try {
                super.afterExecute(wrapped, t);
                // A task has been completed, it has left the building. We should now be able to get the
                // total time as a combination of the time in the queue and time spent running the task. We
                // only want runnables that did not throw errors though, because they could be fast-failures
                // that throw off our timings, so only check when t is null.
                assert super.unwrap(wrapped) instanceof TimedRunnable : "expected only TimedRunnables in queue";
                taskTracker.afterExecute((TimedRunnable) super.unwrap(wrapped));
            } finally {
                taskTracker.untrackTask(wrapped);
            }
        }

        @Override
        protected void appendExecutorDetails(StringBuilder sb) {
            taskTracker.appendTaskExecutionDetails(sb);
        }

        @Override
        protected Runnable wrapRunnable(Runnable runnable) {
            return super.wrapRunnable(new TimedRunnable(runnable));
        }

        @Override
        protected Runnable unwrap(Runnable runnable) {
            final Runnable unwrapped = super.unwrap(runnable);
            if (unwrapped instanceof WrappedRunnable) {
                return ((WrappedRunnable) unwrapped).unwrap();
            } else {
                return unwrapped;
            }
        }
    }
}
