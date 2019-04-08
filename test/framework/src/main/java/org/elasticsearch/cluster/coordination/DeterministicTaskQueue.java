/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class DeterministicTaskQueue {

    private static final Logger logger = LogManager.getLogger(DeterministicTaskQueue.class);

    private final Settings settings;
    private final List<Runnable> runnableTasks = new ArrayList<>();
    private final Random random;
    private List<DeferredTask> deferredTasks = new ArrayList<>();
    private long currentTimeMillis;
    private long nextDeferredTaskExecutionTimeMillis = Long.MAX_VALUE;
    private long executionDelayVariabilityMillis;
    private long latestDeferredExecutionTime;

    public DeterministicTaskQueue(Settings settings, Random random) {
        this.settings = settings;
        this.random = random;
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
        while (hasDeferredTasks() || hasRunnableTasks()) {
            if (hasDeferredTasks() && random.nextBoolean()) {
                advanceTime();
            } else if (hasRunnableTasks()) {
                runRandomTask();
            }
        }
    }

    public void runAllTasksInTimeOrder() {
        while (hasDeferredTasks() || hasRunnableTasks()) {
            if (hasRunnableTasks()) {
                runRandomTask();
            } else {
                advanceTime();
            }
        }
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
        runTask(RandomNumbers.randomIntBetween(random, 0, runnableTasks.size() - 1));
    }

    private void runTask(final int index) {
        final Runnable task = runnableTasks.remove(index);
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
            runnableTasks.add(task);
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
            runnableTasks.add(task);
        } else {
            final DeferredTask deferredTask = new DeferredTask(actualExecutionTimeMillis, task);
            logger.trace("scheduleAt: adding {} with extra delay of [{}ms]", deferredTask, extraDelayMillis);
            scheduleDeferredTask(deferredTask);
        }
    }

    private void scheduleDeferredTask(DeferredTask deferredTask) {
        nextDeferredTaskExecutionTimeMillis = Math.min(nextDeferredTaskExecutionTimeMillis, deferredTask.getExecutionTimeMillis());
        latestDeferredExecutionTime = Math.max(latestDeferredExecutionTime, deferredTask.getExecutionTimeMillis());
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
            assert currentTimeMillis <= deferredTask.getExecutionTimeMillis();
            if (deferredTask.getExecutionTimeMillis() == currentTimeMillis) {
                logger.trace("advanceTime: no longer deferred: {}", deferredTask);
                runnableTasks.add(deferredTask.getTask());
            } else {
                remainingDeferredTasks.add(deferredTask);
                nextDeferredTaskExecutionTimeMillis = Math.min(nextDeferredTaskExecutionTimeMillis, deferredTask.getExecutionTimeMillis());
            }
        }
        deferredTasks = remainingDeferredTasks;

        assert deferredTasks.isEmpty() == (nextDeferredTaskExecutionTimeMillis == Long.MAX_VALUE);
    }

    /**
     * @return A <code>ExecutorService</code> that uses this task queue.
     */
    public ExecutorService getExecutorService() {
        return getExecutorService(Function.identity());
    }

    /**
     * @return A <code>ExecutorService</code> that uses this task queue and wraps <code>Runnable</code>s in the given wrapper.
     */
    public ExecutorService getExecutorService(Function<Runnable, Runnable> runnableWrapper) {
        return new ExecutorService() {

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
                scheduleNow(runnableWrapper.apply(command));
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
        return new ThreadPool(settings) {

            {
                stopCachedTimeThread();
            }

            @Override
            public long relativeTimeInMillis() {
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
                throw new UnsupportedOperationException();
            }

            @Override
            public ThreadPoolStats stats() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ExecutorService generic() {
                return getExecutorService(runnableWrapper);
            }

            @Override
            public ExecutorService executor(String name) {
                return getExecutorService(runnableWrapper);
            }

            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, String executor) {
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
            public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor) {
                return super.scheduleWithFixedDelay(command, interval, executor);
            }

            @Override
            public Runnable preserveContext(Runnable command) {
                throw new UnsupportedOperationException();
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
                        throw new UnsupportedOperationException();
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
        };
    }

    public long getLatestDeferredExecutionTime() {
        return latestDeferredExecutionTime;
    }

    private static class DeferredTask {
        private final long executionTimeMillis;
        private final Runnable task;

        DeferredTask(long executionTimeMillis, Runnable task) {
            this.executionTimeMillis = executionTimeMillis;
            this.task = task;
            assert executionTimeMillis < Long.MAX_VALUE : "Long.MAX_VALUE is special, cannot be an execution time";
        }

        long getExecutionTimeMillis() {
            return executionTimeMillis;
        }

        Runnable getTask() {
            return task;
        }

        @Override
        public String toString() {
            return "DeferredTask{" +
                "executionTimeMillis=" + executionTimeMillis +
                ", task=" + task +
                '}';
        }
    }
}
