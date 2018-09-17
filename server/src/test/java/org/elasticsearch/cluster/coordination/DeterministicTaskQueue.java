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
import org.apache.lucene.util.Counter;
import org.elasticsearch.common.component.AbstractComponent;
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

public class DeterministicTaskQueue extends AbstractComponent {

    private final List<Runnable> runnableTasks = new ArrayList<>();
    private List<DeferredTask> deferredTasks = new ArrayList<>();
    private long currentTimeMillis;
    private long nextDeferredTaskExecutionTimeMillis = Long.MAX_VALUE;

    public DeterministicTaskQueue(Settings settings) {
        super(settings);
    }

    public void runAllTasks() {
        while (true) {
            runAllRunnableTasks();
            if (hasDeferredTasks()) {
                advanceTime();
            } else {
                break;
            }
        }
    }

    public void runAllRunnableTasks() {
        while (hasRunnableTasks()) {
            runNextTask();
        }
    }

    public void runAllRunnableTasks(Random random) {
        while (hasRunnableTasks()) {
            runRandomTask(random);
        }
    }

    public void runAllTasks(Random random) {
        while (hasDeferredTasks() || hasRunnableTasks()) {
            if (hasDeferredTasks() && random.nextBoolean()) {
                advanceTime();
            } else if (hasRunnableTasks()) {
                runRandomTask(random);
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
     * Runs the first runnable task.
     */
    public void runNextTask() {
        assert hasRunnableTasks();
        runTask(0);
    }

    /**
     * Runs an arbitrary runnable task.
     */
    public void runRandomTask(final Random random) {
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
        logger.trace("scheduleNow: adding runnable {}", task);
        runnableTasks.add(task);
    }

    /**
     * Schedule a task for future execution.
     */
    public void scheduleAt(final long executionTimeMillis, final Runnable task) {
        if (executionTimeMillis <= currentTimeMillis) {
            logger.trace("scheduleAt: [{}ms] is not in the future, adding runnable {}", executionTimeMillis, task);
            runnableTasks.add(task);
        } else {
            final DeferredTask deferredTask = new DeferredTask(executionTimeMillis, task);
            logger.trace("scheduleAt: adding {}", deferredTask);
            nextDeferredTaskExecutionTimeMillis = Math.min(nextDeferredTaskExecutionTimeMillis, executionTimeMillis);
            deferredTasks.add(deferredTask);
        }
    }

    /**
     * Advance the current time to the time of the next deferred task, and update the sets of deferred and runnable tasks accordingly.
     */
    public void advanceTime() {
        assert hasDeferredTasks();
        assert currentTimeMillis < nextDeferredTaskExecutionTimeMillis;

        logger.trace("advanceTime: from [{}ms] to [{}ms]", currentTimeMillis, nextDeferredTaskExecutionTimeMillis);
        currentTimeMillis = nextDeferredTaskExecutionTimeMillis;

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
                scheduleNow(command);
            }
        };
    }

    /**
     * @return A <code>ThreadPool</code> that uses this task queue.
     */
    public ThreadPool getThreadPool() {
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
            public Counter estimatedTimeInMillisCounter() {
                return new Counter() {
                    @Override
                    public long addAndGet(long delta) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long get() {
                        return currentTimeMillis;
                    }
                };
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
                return getExecutorService();
            }

            @Override
            public ExecutorService executor(String name) {
                return getExecutorService();
            }

            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String executor, Runnable command) {
                scheduleAt(currentTimeMillis + delay.millis(), command);
                return new ScheduledFuture<Object>() {
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
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean isDone() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Object get() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Object get(long timeout, TimeUnit unit) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor) {
                throw new UnsupportedOperationException();
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
                throw new UnsupportedOperationException();
            }
        };
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
