/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.node.Node;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A collection of static methods to help create different ES Executor types.
 */
public class EsExecutors {

    // although the available processors may technically change, for node sizing we use the number available at launch
    private static final int MAX_NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();

    /**
     * Setting to manually control the number of allocated processors. This setting is used to adjust thread pool sizes per node. The
     * default value is {@link Runtime#availableProcessors()} but should be manually controlled if not all processors on the machine are
     * available to Elasticsearch (e.g., because of CPU limits). Note that this setting accepts floating point processors.
     * If a rounded number is needed, always use {@link EsExecutors#allocatedProcessors(Settings)}.
     */
    public static final Setting<Processors> NODE_PROCESSORS_SETTING = new Setting<>(
        "node.processors",
        Double.toString(MAX_NUM_PROCESSORS),
        textValue -> {
            double numberOfProcessors = Double.parseDouble(textValue);
            if (Double.isNaN(numberOfProcessors) || Double.isInfinite(numberOfProcessors)) {
                String err = "Failed to parse value [" + textValue + "] for setting [node.processors]";
                throw new IllegalArgumentException(err);
            }

            if (numberOfProcessors <= 0.0) {
                String err = "Failed to parse value [" + textValue + "] for setting [node.processors] must be > 0";
                throw new IllegalArgumentException(err);
            }

            if (numberOfProcessors > MAX_NUM_PROCESSORS) {
                String err = "Failed to parse value [" + textValue + "] for setting [node.processors] must be <= " + MAX_NUM_PROCESSORS;
                throw new IllegalArgumentException(err);
            }
            return Processors.of(numberOfProcessors);
        },
        Property.NodeScope
    );

    /**
     * Returns the number of allocated processors. Defaults to {@link Runtime#availableProcessors()} but can be overridden by passing a
     * {@link Settings} instance with the key {@code node.processors} set to the desired value.
     *
     * @param settings a {@link Settings} instance from which to derive the allocated processors
     * @return the number of allocated processors
     */
    public static int allocatedProcessors(final Settings settings) {
        return NODE_PROCESSORS_SETTING.get(settings).roundUp();
    }

    public static Processors nodeProcessors(final Settings settings) {
        return NODE_PROCESSORS_SETTING.get(settings);
    }

    public static PrioritizedEsThreadPoolExecutor newSinglePrioritizing(
        String name,
        ThreadFactory threadFactory,
        ThreadContext contextHolder,
        ScheduledExecutorService timer
    ) {
        return new PrioritizedEsThreadPoolExecutor(name, 1, 1, 0L, TimeUnit.MILLISECONDS, threadFactory, contextHolder, timer);
    }

    /**
     * Creates a scaling {@link EsThreadPoolExecutor} using an unbounded work queue.
     * <p>
     * The {@link EsThreadPoolExecutor} scales the same way as a regular {@link ThreadPoolExecutor} until the core pool size
     * (and at least 1) is reached: each time a task is submitted a new worker is added regardless if an idle worker is available.
     * <p>
     * Once having reached the core pool size, a {@link ThreadPoolExecutor} will only add a new worker if the work queue rejects
     * a task offer. Typically, using a regular unbounded queue, task offers won't ever be rejected, meaning the worker pool would never
     * scale beyond the core pool size.
     * <p>
     * Scaling {@link EsThreadPoolExecutor}s use a customized unbounded {@link LinkedTransferQueue}, which rejects every task offer unless
     * it can be immediately transferred to an available idle worker. If no such worker is available, the executor will add
     * a new worker if capacity remains, otherwise the task is rejected and then appended to the work queue via the {@link ForceQueuePolicy}
     * rejection handler.
     */
    public static EsThreadPoolExecutor newScaling(
        String name,
        int min,
        int max,
        long keepAliveTime,
        TimeUnit unit,
        boolean rejectAfterShutdown,
        ThreadFactory threadFactory,
        ThreadContext contextHolder,
        TaskTrackingConfig config
    ) {
        LinkedTransferQueue<Runnable> queue = newUnboundedScalingLTQueue(min, max);
        // Force queued work via ForceQueuePolicy might starve if no worker is available (if core size is empty),
        // probing the worker pool prevents this.
        boolean probeWorkerPool = min == 0 && queue instanceof ExecutorScalingQueue;
        if (config.trackExecutionTime()) {
            return new TaskExecutionTimeTrackingEsThreadPoolExecutor(
                name,
                min,
                max,
                keepAliveTime,
                unit,
                queue,
                TimedRunnable::new,
                threadFactory,
                new ForceQueuePolicy(rejectAfterShutdown, probeWorkerPool),
                contextHolder,
                config
            );
        } else {
            return new EsThreadPoolExecutor(
                name,
                min,
                max,
                keepAliveTime,
                unit,
                queue,
                threadFactory,
                new ForceQueuePolicy(rejectAfterShutdown, probeWorkerPool),
                contextHolder
            );
        }
    }

    /**
     * Creates a scaling {@link EsThreadPoolExecutor} using an unbounded work queue.
     * <p>
     * The {@link EsThreadPoolExecutor} scales the same way as a regular {@link ThreadPoolExecutor} until the core pool size
     * (and at least 1) is reached: each time a task is submitted a new worker is added regardless if an idle worker is available.
     * <p>
     * Once having reached the core pool size, a {@link ThreadPoolExecutor} will only add a new worker if the work queue rejects
     * a task offer. Typically, using a regular unbounded queue, task offers won't ever be rejected, meaning the worker pool would never
     * scale beyond the core pool size.
     * <p>
     * Scaling {@link EsThreadPoolExecutor}s use a customized unbounded {@link LinkedTransferQueue}, which rejects every task offer unless
     * it can be immediately transferred to an available idle worker. If no such worker is available, the executor will add
     * a new worker if capacity remains, otherwise the task is rejected and then appended to the work queue via the {@link ForceQueuePolicy}
     * rejection handler.
     */
    public static EsThreadPoolExecutor newScaling(
        String name,
        int min,
        int max,
        long keepAliveTime,
        TimeUnit unit,
        boolean rejectAfterShutdown,
        ThreadFactory threadFactory,
        ThreadContext contextHolder
    ) {
        return newScaling(
            name,
            min,
            max,
            keepAliveTime,
            unit,
            rejectAfterShutdown,
            threadFactory,
            contextHolder,
            TaskTrackingConfig.DO_NOT_TRACK
        );
    }

    public static EsThreadPoolExecutor newFixed(
        String name,
        int size,
        int queueCapacity,
        ThreadFactory threadFactory,
        ThreadContext contextHolder,
        TaskTrackingConfig config
    ) {
        final BlockingQueue<Runnable> queue;
        final EsRejectedExecutionHandler rejectedExecutionHandler;
        if (queueCapacity < 0) {
            queue = ConcurrentCollections.newBlockingQueue();
            rejectedExecutionHandler = new RejectOnShutdownOnlyPolicy();
        } else {
            queue = new SizeBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), queueCapacity);
            rejectedExecutionHandler = new EsAbortPolicy();
        }
        if (config.trackExecutionTime()) {
            return new TaskExecutionTimeTrackingEsThreadPoolExecutor(
                name,
                size,
                size,
                0,
                TimeUnit.MILLISECONDS,
                queue,
                TimedRunnable::new,
                threadFactory,
                rejectedExecutionHandler,
                contextHolder,
                config
            );
        } else {
            return new EsThreadPoolExecutor(
                name,
                size,
                size,
                0,
                TimeUnit.MILLISECONDS,
                queue,
                threadFactory,
                rejectedExecutionHandler,
                contextHolder
            );
        }
    }

    /**
     * Checks if the runnable arose from asynchronous submission of a task to an executor. If an uncaught exception was thrown
     * during the execution of this task, we need to inspect this runnable and see if it is an error that should be propagated
     * to the uncaught exception handler.
     *
     * @param runnable the runnable to inspect, should be a RunnableFuture
     * @return non fatal exception or null if no exception.
     */
    public static Throwable rethrowErrors(Runnable runnable) {
        if (runnable instanceof RunnableFuture<?> runnableFuture) {
            assert runnableFuture.isDone();
            try {
                runnableFuture.get();
            } catch (final Exception e) {
                /*
                 * In theory, Future#get can only throw a cancellation exception, an interrupted exception, or an execution
                 * exception. We want to ignore cancellation exceptions, restore the interrupt status on interrupted exceptions, and
                 * inspect the cause of an execution. We are going to be extra paranoid here though and completely unwrap the
                 * exception to ensure that there is not a buried error anywhere. We assume that a general exception has been
                 * handled by the executed task or the task submitter.
                 */
                assert e instanceof CancellationException || e instanceof InterruptedException || e instanceof ExecutionException : e;
                final Optional<Error> maybeError = ExceptionsHelper.maybeError(e);
                if (maybeError.isPresent()) {
                    // throw this error where it will propagate to the uncaught exception handler
                    throw maybeError.get();
                }
                if (e instanceof InterruptedException) {
                    // restore the interrupt status
                    Thread.currentThread().interrupt();
                }
                if (e instanceof ExecutionException) {
                    return e.getCause();
                }
            }
        }

        return null;
    }

    private static final class DirectExecutorService extends AbstractExecutorService {

        @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
        DirectExecutorService() {
            super();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            command.run();
            rethrowErrors(command);
        }
    }

    /**
     * {@link ExecutorService} that executes submitted tasks on the current thread. This executor service does not support being
     * shutdown.
     */
    public static final ExecutorService DIRECT_EXECUTOR_SERVICE = new DirectExecutorService();

    public static String threadName(Settings settings, String namePrefix) {
        if (Node.NODE_NAME_SETTING.exists(settings)) {
            return threadName(Node.NODE_NAME_SETTING.get(settings), namePrefix);
        } else {
            // TODO this should only be allowed in tests
            return threadName("", namePrefix);
        }
    }

    public static String threadName(final String nodeName, final String namePrefix) {
        // TODO missing node names should only be allowed in tests
        return nodeName.isEmpty() == false ? "elasticsearch[" + nodeName + "][" + namePrefix + "]" : "elasticsearch[" + namePrefix + "]";
    }

    public static String executorName(String threadName) {
        // subtract 2 to avoid the `]` of the thread number part.
        int executorNameEnd = threadName.lastIndexOf(']', threadName.length() - 2);
        int executorNameStart = threadName.lastIndexOf('[', executorNameEnd);
        if (executorNameStart == -1
            || executorNameEnd - executorNameStart <= 1
            || threadName.startsWith("TEST-")
            || threadName.startsWith("LuceneTestCase")) {
            return null;
        }
        return threadName.substring(executorNameStart + 1, executorNameEnd);
    }

    public static String executorName(Thread thread) {
        return executorName(thread.getName());
    }

    public static ThreadFactory daemonThreadFactory(Settings settings, String namePrefix) {
        return createDaemonThreadFactory(threadName(settings, namePrefix), false);
    }

    public static ThreadFactory daemonThreadFactory(String nodeName, String namePrefix) {
        return daemonThreadFactory(nodeName, namePrefix, false);
    }

    public static ThreadFactory daemonThreadFactory(String nodeName, String namePrefix, boolean isSystemThread) {
        assert nodeName != null && false == nodeName.isEmpty();
        return createDaemonThreadFactory(threadName(nodeName, namePrefix), isSystemThread);
    }

    public static ThreadFactory daemonThreadFactory(String name) {
        assert name != null && name.isEmpty() == false;
        return createDaemonThreadFactory(name, false);
    }

    private static ThreadFactory createDaemonThreadFactory(String namePrefix, boolean isSystemThread) {
        return new EsThreadFactory(namePrefix, isSystemThread);
    }

    static class EsThreadFactory implements ThreadFactory {

        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;
        final boolean isSystem;

        EsThreadFactory(String namePrefix, boolean isSystem) {
            this.namePrefix = namePrefix;
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.isSystem = isSystem;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new EsThread(group, r, namePrefix + "[T#" + threadNumber.getAndIncrement() + "]", 0, isSystem);
            t.setDaemon(true);
            return t;
        }
    }

    public static class EsThread extends Thread {
        private final boolean isSystem;

        EsThread(ThreadGroup group, Runnable target, String name, long stackSize, boolean isSystem) {
            super(group, target, name, stackSize);
            this.isSystem = isSystem;
        }

        public boolean isSystem() {
            return isSystem;
        }
    }

    /**
     * Cannot instantiate.
     */
    private EsExecutors() {}

    private static <E> LinkedTransferQueue<E> newUnboundedScalingLTQueue(int corePoolSize, int maxPoolSize) {
        if (maxPoolSize == 1 || maxPoolSize == corePoolSize) {
            // scaling beyond core pool size (or 1) not required, use a regular unbounded LinkedTransferQueue
            return new LinkedTransferQueue<>();
        }
        // scaling beyond core pool size with an unbounded queue requires ExecutorScalingQueue
        // note, reconfiguration of core / max pool size not supported in EsThreadPoolExecutor
        return new ExecutorScalingQueue<>();
    }

    /**
     * Customized {@link LinkedTransferQueue} to allow a {@link ThreadPoolExecutor} to scale beyond its core pool size despite having an
     * unbounded queue.
     * <p>
     * Note, usage of unbounded work queues is a problem by itself. For once, it makes error-prone customizations necessary so that
     * thread pools can scale up adequately. But worse, infinite queues prevent backpressure and impose a high risk of causing OOM errors.
     * <a href="https://github.com/elastic/elasticsearch/issues/18613">Github #18613</a> captures various long outstanding, but important
     * improvements to thread pools.
     * <p>
     * Once having reached its core pool size, a {@link ThreadPoolExecutor} will only add more workers if capacity remains and
     * the task offer is rejected by the work queue. Typically that's never the case using a regular unbounded queue.
     * <p>
     * This customized implementation rejects every task offer unless it can be immediately transferred to an available idle worker.
     * It relies on {@link ForceQueuePolicy} rejection handler to append the task to the work queue if no additional worker can be added
     * and the task is rejected by the executor.
     * <p>
     * Note, {@link ForceQueuePolicy} cannot guarantee there will be available workers when appending tasks directly to the queue.
     * For that reason {@link ExecutorScalingQueue} cannot be used with executors with empty core and max pool size of 1:
     * the only available worker could time out just about at the same time as the task is appended, see
     * <a href="https://github.com/elastic/elasticsearch/issues/124667">Github #124667</a> for more details.
     * <p>
     * Note, configuring executors using core = max size in combination with {@code allowCoreThreadTimeOut} could be an alternative to
     * {@link ExecutorScalingQueue}. However, the scaling behavior would be very different: Using {@link ExecutorScalingQueue}
     * we are able to reuse idle workers if available by means of {@link ExecutorScalingQueue#tryTransfer(Object)}.
     * If setting core = max size, the executor will add a new worker for every task submitted until reaching the core/max pool size
     * even if there's idle workers available.
     */
    static class ExecutorScalingQueue<E> extends LinkedTransferQueue<E> {

        ExecutorScalingQueue() {}

        @Override
        public boolean offer(E e) {
            if (e == EsThreadPoolExecutor.WORKER_PROBE) { // referential equality
                // this probe ensures a worker is available after force queueing a task via ForceQueuePolicy
                return super.offer(e);
            }
            // try to transfer to a waiting worker thread
            // otherwise reject queuing the task to force the thread pool executor to add a worker if it can;
            // combined with ForceQueuePolicy, this causes the thread pool to always scale up to max pool size
            // so that we only queue when there is no spare capacity
            return tryTransfer(e);
        }

        // Overridden to workaround a JDK bug introduced in JDK 21.0.2
        // https://bugs.openjdk.org/browse/JDK-8323659
        @Override
        public void put(E e) {
            // As the queue is unbounded, this method will always add to the queue.
            super.offer(e);
        }

        // Overridden to workaround a JDK bug introduced in JDK 21.0.2
        // https://bugs.openjdk.org/browse/JDK-8323659
        @Override
        public boolean add(E e) {
            // As the queue is unbounded, this method will never return false.
            return super.offer(e);
        }

        // Overridden to workaround a JDK bug introduced in JDK 21.0.2
        // https://bugs.openjdk.org/browse/JDK-8323659
        @Override
        public boolean offer(E e, long timeout, TimeUnit unit) {
            // As the queue is unbounded, this method will never return false.
            return super.offer(e);
        }
    }

    /**
     * A handler for rejected tasks that adds the specified element to this queue,
     * waiting if necessary for space to become available.
     */
    static class ForceQueuePolicy extends EsRejectedExecutionHandler {

        /**
         * This flag is used to indicate if {@link Runnable} should be rejected once the thread pool is shutting down, ie once
         * {@link ThreadPoolExecutor#shutdown()} has been called. Scaling thread pools are expected to always handle tasks rejections, even
         * after shutdown or termination, but it's not the case of all existing thread pools so this flag allows to keep the previous
         * behavior.
         */
        private final boolean rejectAfterShutdown;

        /**
         * Flag to indicate if the worker pool needs to be probed after force queuing a task to guarantee a worker is available.
         */
        private final boolean probeWorkerPool;

        /**
         * @param rejectAfterShutdown indicates if {@link Runnable} should be rejected once the thread pool is shutting down
         */
        ForceQueuePolicy(boolean rejectAfterShutdown, boolean probeWorkerPool) {
            this.rejectAfterShutdown = rejectAfterShutdown;
            this.probeWorkerPool = probeWorkerPool;
        }

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            if (task == EsThreadPoolExecutor.WORKER_PROBE) { // referential equality
                return;
            }
            if (rejectAfterShutdown) {
                if (executor.isShutdown()) {
                    reject(executor, task);
                } else {
                    put(executor, task);
                    // we need to check again the executor state as it might have been concurrently shut down; in this case
                    // the executor's workers are shutting down and might have already picked up the task for execution.
                    if (executor.isShutdown() && executor.remove(task)) {
                        reject(executor, task);
                    }
                }
            } else {
                put(executor, task);
            }
        }

        private void put(ThreadPoolExecutor executor, Runnable task) {
            final BlockingQueue<Runnable> queue = executor.getQueue();
            // force queue policy should only be used with a scaling queue (ExecutorScalingQueue / LinkedTransferQueue)
            assert queue instanceof LinkedTransferQueue;
            try {
                queue.put(task);
                if (probeWorkerPool && task == queue.peek()) { // referential equality
                    // If the task is at the head of the queue, we can assume the queue was previously empty. In this case available workers
                    // might have timed out in the meanwhile. To prevent the task from starving, we submit a noop probe to the executor.
                    // Note, this deliberately doesn't check getPoolSize()==0 to avoid potential race conditions,
                    // as the count in the atomic state (used by workerCountOf) is decremented first.
                    executor.execute(EsThreadPoolExecutor.WORKER_PROBE);
                }
            } catch (final InterruptedException e) {
                assert false : "a scaling queue never blocks so a put to it can never be interrupted";
                throw new AssertionError(e);
            }
        }

        private void reject(ThreadPoolExecutor executor, Runnable task) {
            incrementRejections();
            throw newRejectedException(task, executor, true);
        }
    }

    static class RejectOnShutdownOnlyPolicy extends EsRejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            assert executor.isShutdown() : executor;
            incrementRejections();
            throw newRejectedException(task, executor, true);
        }
    }

    public static class TaskTrackingConfig {
        // This is a random starting point alpha. TODO: revisit this with actual testing and/or make it configurable
        public static final double DEFAULT_EWMA_ALPHA = 0.3;

        private final boolean trackExecutionTime;
        private final boolean trackOngoingTasks;
        private final double ewmaAlpha;

        public static final TaskTrackingConfig DO_NOT_TRACK = new TaskTrackingConfig(false, false, DEFAULT_EWMA_ALPHA);
        public static final TaskTrackingConfig DEFAULT = new TaskTrackingConfig(true, false, DEFAULT_EWMA_ALPHA);

        public TaskTrackingConfig(boolean trackOngoingTasks, double ewmaAlpha) {
            this(true, trackOngoingTasks, ewmaAlpha);
        }

        private TaskTrackingConfig(boolean trackExecutionTime, boolean trackOngoingTasks, double EWMAAlpha) {
            this.trackExecutionTime = trackExecutionTime;
            this.trackOngoingTasks = trackOngoingTasks;
            this.ewmaAlpha = EWMAAlpha;
        }

        public boolean trackExecutionTime() {
            return trackExecutionTime;
        }

        public boolean trackOngoingTasks() {
            return trackOngoingTasks;
        }

        public double getEwmaAlpha() {
            return ewmaAlpha;
        }
    }

}
