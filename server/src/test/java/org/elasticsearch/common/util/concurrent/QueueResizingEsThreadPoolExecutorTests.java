/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 100);

        int threads = randomIntBetween(1, 3);
        int measureWindow = 3;
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizingEsThreadPoolExecutor executor = new QueueResizingEsThreadPoolExecutor(
            "test-threadpool",
            threads,
            threads,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            10,
            1000,
            fastWrapper(),
            measureWindow,
            TimeValue.timeValueMillis(1),
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute exactly 3 (measureWindow) times
        executor.execute(() -> {});
        executor.execute(() -> {});
        executor.execute(() -> {});

        // The queue capacity should have increased by 50 since they were very fast tasks
        assertBusy(() -> { assertThat(queue.capacity(), equalTo(150)); });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void testAutoQueueSizingUp() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 2000);

        int threads = randomIntBetween(1, 10);
        int measureWindow = randomIntBetween(100, 200);
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizingEsThreadPoolExecutor executor = new QueueResizingEsThreadPoolExecutor(
            "test-threadpool",
            threads,
            threads,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            10,
            3000,
            fastWrapper(),
            measureWindow,
            TimeValue.timeValueMillis(1),
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute a task multiple times that takes 1ms
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> { assertThat(queue.capacity(), greaterThan(2000)); });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void testAutoQueueSizingDown() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 2000);

        int threads = randomIntBetween(1, 10);
        int measureWindow = randomIntBetween(100, 200);
        logger.info("--> auto-queue with a measurement window of {} tasks", measureWindow);
        QueueResizingEsThreadPoolExecutor executor = new QueueResizingEsThreadPoolExecutor(
            "test-threadpool",
            threads,
            threads,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            10,
            3000,
            slowWrapper(),
            measureWindow,
            TimeValue.timeValueMillis(1),
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Execute a task multiple times that takes 1m
        executeTask(executor, (measureWindow * 5) + 2);

        assertBusy(() -> { assertThat(queue.capacity(), lessThan(2000)); });
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void testExecutionEWMACalculation() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 100);

        QueueResizingEsThreadPoolExecutor executor = new QueueResizingEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            10,
            200,
            fastWrapper(),
            10,
            TimeValue.timeValueMillis(1),
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(30L)); });
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(51L)); });
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(65L)); });
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(75L)); });
        executeTask(executor, 1);
        assertBusy(() -> { assertThat((long) executor.getTaskExecutionEWMA(), equalTo(83L)); });

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    /** Use a runnable wrapper that simulates a task with unknown failures. */
    public void testExceptionThrowingTask() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(ConcurrentCollections.<Runnable>newBlockingQueue(), 100);

        QueueResizingEsThreadPoolExecutor executor = new QueueResizingEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            queue,
            10,
            200,
            exceptionalWrapper(),
            10,
            TimeValue.timeValueMillis(1),
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        executeTask(executor, 1);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    private Function<Runnable, WrappedRunnable> fastWrapper() {
        return (runnable) -> new SettableTimedRunnable(TimeUnit.NANOSECONDS.toNanos(100), false);
    }

    private Function<Runnable, WrappedRunnable> slowWrapper() {
        return (runnable) -> new SettableTimedRunnable(TimeUnit.MINUTES.toNanos(2), false);
    }

    /**
     * The returned function outputs a WrappedRunnabled that simulates the case
     * where {@link TimedRunnable#getTotalExecutionNanos()} returns -1 because
     * the job failed or was rejected before it finished.
     */
    private Function<Runnable, WrappedRunnable> exceptionalWrapper() {
        return (runnable) -> new SettableTimedRunnable(TimeUnit.NANOSECONDS.toNanos(-1), true);
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
        private final boolean testFailedOrRejected;

        public SettableTimedRunnable(long timeTaken, boolean failedOrRejected) {
            super(() -> {});
            this.timeTaken = timeTaken;
            this.testFailedOrRejected = failedOrRejected;
        }

        @Override
        public long getTotalNanos() {
            return timeTaken;
        }

        @Override
        public long getTotalExecutionNanos() {
            return timeTaken;
        }

        @Override
        public boolean getFailedOrRejected() {
            return testFailedOrRejected;
        }
    }
}
