/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Permits the testing of async processes by interleaving all the tasks on a single thread in a pseudo-random (deterministic) fashion,
 * letting each task spawn future tasks, and simulating the passage of time. Tasks can be scheduled directly via {@link #scheduleNow} and
 * {@link #scheduleAt}, or can be executed using the thread pool returned from {@link #getThreadPool}. The scheduling of tasks can be
 * made more variable with {@link #setExecutionDelayVariabilityMillis} to simulate a system that is not running tasks in a timely fashion
 * e.g. due to overload or network delays.
 */
public class DeterministicTaskQueue {

    protected static final Logger logger = LogManager.getLogger(DeterministicTaskQueue.class);

    public static final String NODE_ID_LOG_CONTEXT_KEY = "nodeId";

    /**
     * A runnable task with what {@link SelectionPolicy#RANDOM_PRIORITY} needs to know about it: its priority, and the value of
     * {@link #picks} when it became runnable.
     */
    private record RunnableTask(Runnable task, double priority, long runnableSincePick) {}

    private final List<RunnableTask> runnableTasks = new ArrayList<>();

    /**
     * How {@link #runRandomTask()} chooses among the runnable tasks.
     * <p>
     * Example: task {@code L} must stay pending while the chain {@code C1 -> C2 -> ... -> Cm} runs, each link submitting the next,
     * and the runnable tasks are {@code [L, C1, X, Y]}.
     * <ul>
     *     <li>{@link #UNIFORM}: picking {@code X} or {@code Y} decides nothing, so what matters is which of {@code L} and {@code C1}
     *     is picked first, with probability 1/2 each. If {@code C1} wins it submits {@code C2}, and {@code L} faces a fresh coin
     *     flip. {@code L} has to lose {@code m} flips in a row: {@code 2^-m}.</li>
     *     <li>{@link #RANDOM_PRIORITY}: {@code L} draws its priority once, say 0.02, and keeps it. Each {@code Ci} draws its own
     *     and runs before {@code L} if it drew higher. If {@code L} drew low, it keeps losing, so {@code L} outlasts the chain if
     *     its draw is the lowest of the {@code m+1}: {@code 1/(m+1)}.</li>
     * </ul>
     * <pre>
     *    m   UNIFORM   RANDOM_PRIORITY
     *    1   1/2       1/2
     *    5   1/32      1/6
     *   10   1/1024    1/11
     *   35   3e-11     1/36
     * </pre>
     */
    public enum SelectionPolicy {
        /**
         * Pick a runnable task uniformly at random. A task stays pending while a causal chain of {@code m} other tasks runs only
         * if it loses {@code m} fair coin flips, with probability {@code 2^-m}, so orders in which one task lags far behind are
         * practically never explored.
         */
        UNIFORM,
        /**
         * Each task draws a random priority when it becomes runnable, and the highest-priority runnable task runs next. A task
         * stays pending while a causal chain of {@code m} other tasks runs if its priority is the lowest of {@code m+1} draws,
         * with probability {@code 1/(m+1)}. This is BasicPOS from Yuan, Yang and Gu, "Partial Order Aware Concurrency Sampling"
         * (CAV 2018, Sect. 4.1), without the priority reassignment of full POS. It reaches only orders that {@link #UNIFORM} can
         * also reach.
         */
        RANDOM_PRIORITY
    }

    /**
     * Under {@link SelectionPolicy#RANDOM_PRIORITY}, a task passed over more than this many times fails the test rather than
     * being released quietly: reaching it means a zero-delay livelock or starvation by the policy, and either needs a look.
     */
    static final long DEFAULT_PRIORITY_AGE_CAP = 10_000L;

    private SelectionPolicy selectionPolicy = SelectionPolicy.UNIFORM;
    // Separate generator so that priority draws neither shift the uniform policy's draws nor trip the test Random's thread check
    // when a task is submitted from outside the test thread.
    private Random priorityRandom;
    private final long priorityAgeCap;
    private long picks;

    private final Random random;
    private List<DeferredTask> deferredTasks = new ArrayList<>();
    private long currentTimeMillis;
    private long nextDeferredTaskExecutionTimeMillis = Long.MAX_VALUE;
    private long executionDelayVariabilityMillis;
    private long latestDeferredExecutionTime;

    public DeterministicTaskQueue(Random random) {
        this(random, DEFAULT_PRIORITY_AGE_CAP);
    }

    // visible for testing
    DeterministicTaskQueue(Random random, long priorityAgeCap) {
        this.random = random;
        this.priorityAgeCap = priorityAgeCap;
    }

    public DeterministicTaskQueue() {
        this(ESTestCase.random());
    }

    public long getExecutionDelayVariabilityMillis() {
        return executionDelayVariabilityMillis;
    }

    public void setExecutionDelayVariabilityMillis(long executionDelayVariabilityMillis) {
        assert executionDelayVariabilityMillis >= 0 : executionDelayVariabilityMillis;
        this.executionDelayVariabilityMillis = executionDelayVariabilityMillis;
    }

    public void runAllRunnableTasks() {
        while (hasRunnableTasks()) {
            runRandomTask();
        }
    }

    public void runAllTasks() {
        while (hasAnyTasks()) {
            if (hasDeferredTasks() && random.nextBoolean()) {
                advanceTime();
            } else if (hasRunnableTasks()) {
                runRandomTask();
            }
        }
    }

    public void runAllTasksInTimeOrder() {
        while (hasAnyTasks()) {
            if (hasRunnableTasks()) {
                runRandomTask();
            } else {
                advanceTime();
            }
        }
    }

    /**
     * Run all {@code runnableTasks} and {@code deferredTasks} that are scheduled to run before or on the given {@code timeInMillis}.
     * The current time will be set to {@code timeInMillis} once the method returns.
     */
    public void runTasksUpToTimeInOrder(long timeInMillis) {
        runAllRunnableTasks();
        while (nextDeferredTaskExecutionTimeMillis <= timeInMillis) {
            advanceTime();
            runAllRunnableTasks();
        }
        currentTimeMillis = timeInMillis;
    }

    /**
     * @return whether there are any runnable tasks.
     */
    public boolean hasRunnableTasks() {
        return runnableTasks.isEmpty() == false;
    }

    /**
     * @return whether there are any deferred tasks, i.e. tasks that are scheduled for the future.
     */
    public boolean hasDeferredTasks() {
        return deferredTasks.isEmpty() == false;
    }

    /**
     * @return whether there are any runnable or deferred tasks
     */
    public boolean hasAnyTasks() {
        return hasDeferredTasks() || hasRunnableTasks();
    }

    /**
     * @return the current (simulated) time, in milliseconds.
     */
    public long getCurrentTimeMillis() {
        return currentTimeMillis;
    }

    /**
     * Runs an arbitrary runnable task.
     */
    public void runRandomTask() {
        assert hasRunnableTasks();
        switch (selectionPolicy) {
            case UNIFORM -> runTask(RandomNumbers.randomIntBetween(random, 0, runnableTasks.size() - 1));
            case RANDOM_PRIORITY -> runTask(pickByPriority());
        }
    }

    /**
     * Opt in to a different {@link SelectionPolicy}. Call it before any task is runnable, typically at the start of a test that
     * exercises fail-overs; tests that never call it keep {@link SelectionPolicy#UNIFORM} and their existing random sequences.
     */
    public void setSelectionPolicy(SelectionPolicy selectionPolicy) {
        assert runnableTasks.isEmpty() : "selection policy must be chosen before any task is runnable";
        this.selectionPolicy = selectionPolicy;
        this.priorityRandom = switch (selectionPolicy) {
            case UNIFORM -> null;
            case RANDOM_PRIORITY -> createPriorityRandom();
        };
    }

    // visible for testing, so that a test can choose each task's priority
    Random createPriorityRandom() {
        return new Random(random.nextLong());
    }

    private int pickByPriority() {
        int best = 0;
        for (int i = 1; i < runnableTasks.size(); i++) {
            if (runnableTasks.get(i).priority() > runnableTasks.get(best).priority()) {
                best = i;
            }
        }
        // every other runnable task is passed over once more by this pick
        int oldest = -1;
        for (int i = 0; i < runnableTasks.size(); i++) {
            if (i != best && (oldest == -1 || runnableTasks.get(i).runnableSincePick() < runnableTasks.get(oldest).runnableSincePick())) {
                oldest = i;
            }
        }
        if (oldest != -1) {
            final long passedOver = picks + 1 - runnableTasks.get(oldest).runnableSincePick();
            if (passedOver > priorityAgeCap) {
                throw new AssertionError(
                    "priority policy: task passed over ["
                        + passedOver
                        + "] times, more than the cap ["
                        + priorityAgeCap
                        + "]: "
                        + runnableTasks.get(oldest).task()
                );
            }
        }
        return best;
    }

    private void addRunnable(final Runnable task) {
        final double priority = switch (selectionPolicy) {
            case UNIFORM -> 0.0;
            case RANDOM_PRIORITY -> priorityRandom.nextDouble();
        };
        runnableTasks.add(new RunnableTask(task, priority, picks));
    }

    private void runTask(final int index) {
        final Runnable task = runnableTasks.remove(index).task();
        picks++;
        logger.trace("running task {} of {}: {}", index, runnableTasks.size() + 1, task);
        task.run();
    }

    /**
     * Schedule a task for immediate execution.
     */
    public void scheduleNow(final Runnable task) {
        if (executionDelayVariabilityMillis > 0 && random.nextBoolean()) {
            final long executionDelay = RandomNumbers.randomLongBetween(random, 1, executionDelayVariabilityMillis);
            final DeferredTask deferredTask = new DeferredTask(currentTimeMillis + executionDelay, task);
            logger.trace("scheduleNow: delaying [{}ms], scheduling {}", executionDelay, deferredTask);
            scheduleDeferredTask(deferredTask);
        } else {
            logger.trace("scheduleNow: adding runnable {}", task);
            addRunnable(task);
        }
    }

    /**
     * Schedule a task for future execution.
     */
    public void scheduleAt(final long executionTimeMillis, final Runnable task) {
        final long extraDelayMillis = RandomNumbers.randomLongBetween(random, 0, executionDelayVariabilityMillis);
        final long actualExecutionTimeMillis = executionTimeMillis + extraDelayMillis;
        if (actualExecutionTimeMillis <= currentTimeMillis) {
            logger.trace("scheduleAt: [{}ms] is not in the future, adding runnable {}", executionTimeMillis, task);
            addRunnable(task);
        } else {
            final DeferredTask deferredTask = new DeferredTask(actualExecutionTimeMillis, task);
            logger.trace("scheduleAt: adding {} with extra delay of [{}ms]", deferredTask, extraDelayMillis);
            scheduleDeferredTask(deferredTask);
        }
    }

    /**
     * Similar to {@link #scheduleAt} but also advance time to {@code executionTimeMillis} and run all eligible tasks.
     */
    public void scheduleAtAndRunUpTo(final long executionTimeMillis, final Runnable task) {
        scheduleAt(executionTimeMillis, task);
        runTasksUpToTimeInOrder(executionTimeMillis);
    }

    private void scheduleDeferredTask(DeferredTask deferredTask) {
        nextDeferredTaskExecutionTimeMillis = Math.min(nextDeferredTaskExecutionTimeMillis, deferredTask.executionTimeMillis());
        latestDeferredExecutionTime = Math.max(latestDeferredExecutionTime, deferredTask.executionTimeMillis());
        deferredTasks.add(deferredTask);
    }

    /**
     * Advance the current time to the time of the next deferred task, and update the sets of deferred and runnable tasks accordingly.
     */
    public void advanceTime() {
        assert hasDeferredTasks();
        assert currentTimeMillis < nextDeferredTaskExecutionTimeMillis;

        logger.trace("advanceTime: from [{}ms] to [{}ms]", currentTimeMillis, nextDeferredTaskExecutionTimeMillis);
        currentTimeMillis = nextDeferredTaskExecutionTimeMillis;
        assert currentTimeMillis <= latestDeferredExecutionTime : latestDeferredExecutionTime + " < " + currentTimeMillis;

        nextDeferredTaskExecutionTimeMillis = Long.MAX_VALUE;
        List<DeferredTask> remainingDeferredTasks = new ArrayList<>();
        for (final DeferredTask deferredTask : deferredTasks) {
            assert currentTimeMillis <= deferredTask.executionTimeMillis();
            if (deferredTask.executionTimeMillis() == currentTimeMillis) {
                logger.trace("advanceTime: no longer deferred: {}", deferredTask);
                addRunnable(deferredTask.task());
            } else {
                remainingDeferredTasks.add(deferredTask);
                nextDeferredTaskExecutionTimeMillis = Math.min(nextDeferredTaskExecutionTimeMillis, deferredTask.executionTimeMillis());
            }
        }
        deferredTasks = remainingDeferredTasks;

        assert deferredTasks.isEmpty() == (nextDeferredTaskExecutionTimeMillis == Long.MAX_VALUE);
    }

    public PrioritizedEsThreadPoolExecutor getPrioritizedEsThreadPoolExecutor() {
        return getPrioritizedEsThreadPoolExecutor(Function.identity());
    }

    public PrioritizedEsThreadPoolExecutor getPrioritizedEsThreadPoolExecutor(Function<Runnable, Runnable> runnableWrapper) {
        return new PrioritizedEsThreadPoolExecutor("DeterministicTaskQueue", 1, 1, 1, TimeUnit.SECONDS, r -> {
            throw new AssertionError("should not create new threads");
        }, null, null) {
            @Override
            public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                throw new AssertionError("not implemented");
            }

            @Override
            public void execute(Runnable command) {
                final var wrappedCommand = runnableWrapper.apply(command);
                runnableWrapper.apply(() -> scheduleNow(wrappedCommand)).run();
            }
        };
    }

    /**
     * @return A <code>ThreadPool</code> that uses this task queue.
     */
    public ThreadPool getThreadPool() {
        return getThreadPool(Function.identity());
    }

    /**
     * @return A <code>ThreadPool</code> that uses this task queue and wraps <code>Runnable</code>s in the given wrapper.
     */
    public ThreadPool getThreadPool(Function<Runnable, Runnable> runnableWrapper) {
        return new DeterministicThreadPool(runnableWrapper);
    }

    protected class DeterministicThreadPool extends ThreadPool {
        private final Map<String, Info> infos = new HashMap<>();

        protected final ExecutorService forkingExecutor = new ExecutorService() {

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
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> Future<T> submit(Callable<T> task) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> Future<T> submit(Runnable task, T result1) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<?> submit(Runnable task) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void execute(Runnable command) {
                scheduleNow(runnableWrapper.apply(command));
            }

            @Override
            public String toString() {
                return "DeterministicTaskQueue/forkingExecutor";
            }
        };

        protected final Function<Runnable, Runnable> runnableWrapper;

        protected DeterministicThreadPool(Function<Runnable, Runnable> runnableWrapper) {
            this.runnableWrapper = runnable -> getThreadContext().preserveContext(runnableWrapper.apply(runnable));
        }

        @Override
        public long relativeTimeInNanos() {
            return TimeValue.timeValueMillis(currentTimeMillis).nanos();
        }

        @Override
        public long relativeTimeInMillis() {
            return currentTimeMillis;
        }

        @Override
        public long rawRelativeTimeInMillis() {
            return currentTimeMillis;
        }

        @Override
        public long absoluteTimeInMillis() {
            return currentTimeMillis;
        }

        @Override
        public ThreadPoolInfo info() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Info info(String name) {
            return infos.computeIfAbsent(name, n -> new Info(n, ThreadPoolType.FIXED, random.nextInt(10) + 1));
        }

        @Override
        public ThreadPoolStats stats() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutorService generic() {
            return executor(Names.GENERIC);
        }

        @Override
        public ExecutorService executor(String name) {
            return forkingExecutor;
        }

        @Override
        public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
            final int NOT_STARTED = 0;
            final int STARTED = 1;
            final int CANCELLED = 2;
            final AtomicInteger taskState = new AtomicInteger(NOT_STARTED);

            scheduleAt(currentTimeMillis + delay.millis(), runnableWrapper.apply(new Runnable() {
                @Override
                public void run() {
                    if (taskState.compareAndSet(NOT_STARTED, STARTED)) {
                        command.run();
                    }
                }

                @Override
                public String toString() {
                    return command.toString();
                }
            }));

            return new ScheduledCancellable() {
                @Override
                public long getDelay(TimeUnit unit) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int compareTo(Delayed o) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean cancel() {
                    return taskState.compareAndSet(NOT_STARTED, CANCELLED);
                }

                @Override
                public boolean isCancelled() {
                    return taskState.get() == CANCELLED;
                }

            };
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdownNow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledExecutorService scheduler() {
            return new ScheduledExecutorService() {
                @Override
                public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                    // For GlobalCheckpointListeners#add
                    final int NOT_STARTED = 0;
                    final int STARTED = 1;
                    final int CANCELLED = 2;
                    final int DONE = 3;
                    final AtomicInteger taskState = new AtomicInteger(NOT_STARTED);

                    scheduleAt(currentTimeMillis + new TimeValue(delay, unit).millis(), runnableWrapper.apply(new Runnable() {
                        @Override
                        public void run() {
                            if (taskState.compareAndSet(NOT_STARTED, STARTED)) {
                                try {
                                    command.run();
                                } finally {
                                    taskState.compareAndSet(STARTED, DONE);
                                }
                            }
                        }

                        @Override
                        public String toString() {
                            return command.toString();
                        }
                    }));

                    return new ScheduledFuture<>() {
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
                            return taskState.compareAndSet(NOT_STARTED, CANCELLED);
                        }

                        @Override
                        public boolean isCancelled() {
                            return taskState.get() == CANCELLED;
                        }

                        @Override
                        public boolean isDone() {
                            return taskState.get() == DONE;
                        }

                        @Override
                        public Object get() throws InterruptedException, ExecutionException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override
                public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
                    throw new UnsupportedOperationException();
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
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isTerminated() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean awaitTermination(long timeout, TimeUnit unit) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <T> Future<T> submit(Callable<T> task) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <T> Future<T> submit(Runnable task, T result) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Future<?> submit(Runnable task) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void execute(Runnable command) {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public long getLatestDeferredExecutionTime() {
        return latestDeferredExecutionTime;
    }

    private record DeferredTask(long executionTimeMillis, Runnable task) {
        private DeferredTask {
            assert executionTimeMillis < Long.MAX_VALUE : "Long.MAX_VALUE is special, cannot be an execution time";
        }
    }

    public static String getNodeIdForLogContext(DiscoveryNode node) {
        return "{" + node.getId() + "}{" + node.getEphemeralId() + "}";
    }

    public static Runnable onNodeLog(DiscoveryNode node, Runnable runnable) {
        final String nodeId = getNodeIdForLogContext(node);
        return new Runnable() {
            @Override
            public void run() {
                try (var ignored = getLogContext(nodeId)) {
                    runnable.run();
                }
            }

            @Override
            public String toString() {
                return nodeId + ": " + runnable.toString();
            }
        };
    }

    public static CloseableThreadContext.Instance getLogContext(String value) {
        return CloseableThreadContext.put(NODE_ID_LOG_CONTEXT_KEY, value);
    }

}
