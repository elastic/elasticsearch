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

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests for the automatic queue resizing of the {@code QueueResizingEsThreadPoolExecutorTests}
 * based on the time taken for each event.
 */
public class QueueResizingEsThreadPoolExecutorTests extends ESTestCase {

    public void testExactWindowSizeAdjustment() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue =
                new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 100);

        int threads = randomIntBetween(1, 3);
        int measureWindow = 3;
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizingEsThreadPoolExecutor executor =
                new QueueResizingEsThreadPoolExecutor(
                        "test-threadpool", threads, threads, 1000,
                        TimeUnit.MILLISECONDS, queue, 10, 1000, fastWrapper(),
                        measureWindow, TimeValue.timeValueMillis(1), EsExecutors.daemonThreadFactory("queuetest"),
                        new EsAbortPolicy(), context);
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute exactly 3 (measureWindow) times
        executor.execute(() -> {});
        executor.execute(() -> {});
        executor.execute(() -> {});

        // The queue capacity should have increased by 50 since they were very fast tasks
        assertBusy(() -> {
            assertThat(queue.capacity(), equalTo(150));
        });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        context.close();
    }

    public void testAutoQueueSizingUp() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue =
                new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(),
                        2000);

        int threads = randomIntBetween(1, 10);
        int measureWindow = randomIntBetween(100, 200);
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizingEsThreadPoolExecutor executor =
                new QueueResizingEsThreadPoolExecutor(
                        "test-threadpool", threads, threads, 1000,
                        TimeUnit.MILLISECONDS, queue, 10, 3000, fastWrapper(),
                        measureWindow, TimeValue.timeValueMillis(1), EsExecutors.daemonThreadFactory("queuetest"),
                        new EsAbortPolicy(), context);
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute a task multiple times that takes 1ms
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> {
            assertThat(queue.capacity(), greaterThan(2000));
        });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        context.close();
    }

    public void testAutoQueueSizingDown() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue =
                new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(),
                        2000);

        int threads = randomIntBetween(1, 10);
        int measureWindow = randomIntBetween(100, 200);
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizingEsThreadPoolExecutor executor =
                new QueueResizingEsThreadPoolExecutor(
                        "test-threadpool", threads, threads, 1000,
                        TimeUnit.MILLISECONDS, queue, 10, 3000, slowWrapper(), measureWindow, TimeValue.timeValueMillis(1),
                        EsExecutors.daemonThreadFactory("queuetest"), new EsAbortPolicy(), context);
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute a task multiple times that takes 1m
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> {
            assertThat(queue.capacity(), lessThan(2000));
        });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        context.close();
    }

    public void testAutoQueueSizingWithMin() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue =
                new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(),
                        5000);

        int threads = randomIntBetween(1, 5);
        int measureWindow = randomIntBetween(10, 100);;
        int min = randomIntBetween(4981, 4999);
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizingEsThreadPoolExecutor executor =
                new QueueResizingEsThreadPoolExecutor(
                        "test-threadpool", threads, threads, 1000,
                        TimeUnit.MILLISECONDS, queue, min, 100000, slowWrapper(), measureWindow, TimeValue.timeValueMillis(1),
                        EsExecutors.daemonThreadFactory("queuetest"), new EsAbortPolicy(), context);
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute a task multiple times that takes 1m
        executeTask(executor, (measureWindow * 5));

        // The queue capacity should decrease, but no lower than the minimum
        assertBusy(() -> {
            assertThat(queue.capacity(), equalTo(min));
        });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        context.close();
    }

    public void testAutoQueueSizingWithMax() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue =
                new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(),
                        5000);

        int threads = randomIntBetween(1, 5);
        int measureWindow = randomIntBetween(10, 100);
        int max = randomIntBetween(5010, 5024);
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizingEsThreadPoolExecutor executor =
                new QueueResizingEsThreadPoolExecutor(
                        "test-threadpool", threads, threads, 1000,
                        TimeUnit.MILLISECONDS, queue, 10, max, fastWrapper(), measureWindow, TimeValue.timeValueMillis(1),
                        EsExecutors.daemonThreadFactory("queuetest"), new EsAbortPolicy(), context);
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute a task multiple times that takes 1ms
        executeTask(executor, measureWindow * 3);

        // The queue capacity should increase, but no higher than the maximum
        assertBusy(() -> {
            assertThat(queue.capacity(), equalTo(max));
        });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        context.close();
    }

    public void testExecutionEWMACalculation() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue =
                new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(),
                        100);

        QueueResizingEsThreadPoolExecutor executor =
                new QueueResizingEsThreadPoolExecutor(
                        "test-threadpool", 1, 1, 1000,
                        TimeUnit.MILLISECONDS, queue, 10, 200, fastWrapper(), 10, TimeValue.timeValueMillis(1),
                        EsExecutors.daemonThreadFactory("queuetest"), new EsAbortPolicy(), context);
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        assertThat((long)executor.getTaskExecutionEWMA(), equalTo(0L));
        executeTask(executor,  1);
        assertBusy(() -> {
            assertThat((long)executor.getTaskExecutionEWMA(), equalTo(30L));
        });
        executeTask(executor,  1);
        assertBusy(() -> {
            assertThat((long)executor.getTaskExecutionEWMA(), equalTo(51L));
        });
        executeTask(executor,  1);
        assertBusy(() -> {
            assertThat((long)executor.getTaskExecutionEWMA(), equalTo(65L));
        });
        executeTask(executor,  1);
        assertBusy(() -> {
            assertThat((long)executor.getTaskExecutionEWMA(), equalTo(75L));
        });
        executeTask(executor,  1);
        assertBusy(() -> {
            assertThat((long)executor.getTaskExecutionEWMA(), equalTo(83L));
        });

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        context.close();
    }

    private Function<Runnable, Runnable> randomBetweenLimitsWrapper(final int minNs, final int maxNs) {
        return (runnable) -> {
            return new SettableTimedRunnable(randomIntBetween(minNs, maxNs));
        };
    }

    private Function<Runnable, Runnable> fastWrapper() {
        return (runnable) -> {
            return new SettableTimedRunnable(TimeUnit.NANOSECONDS.toNanos(100));
        };
    }

    private Function<Runnable, Runnable> slowWrapper() {
        return (runnable) -> {
            return new SettableTimedRunnable(TimeUnit.MINUTES.toNanos(2));
        };
    }

    /** Execute a blank task {@code times} times for the executor */
    private void executeTask(QueueResizingEsThreadPoolExecutor executor, int times) {
        logger.info("--> executing a task [{}] times", times);
        for (int i = 0; i < times; i++) {
            executor.execute(() -> {});
        }
    }

    public class SettableTimedRunnable extends TimedRunnable {
        private final long timeTaken;

        public SettableTimedRunnable(long timeTaken) {
            super(() -> {});
            this.timeTaken = timeTaken;
        }

        @Override
        public long getTotalNanos() {
            return timeTaken;
        }

        @Override
        public long getTotalExecutionNanos() {
            return timeTaken;
        }
    }
}
