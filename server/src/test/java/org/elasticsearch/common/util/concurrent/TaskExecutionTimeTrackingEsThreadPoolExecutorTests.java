/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the automatic queue resizing of the {@code QueueResizingEsThreadPoolExecutorTests}
 * based on the time taken for each event.
 */
public class TaskExecutionTimeTrackingEsThreadPoolExecutorTests extends ESTestCase {

    public void testExecutionEWMACalculation() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);

        TaskExecutionTimeTrackingEsThreadPoolExecutor executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            settableWrapper(TimeUnit.NANOSECONDS.toNanos(100)),
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context,
            TaskExecutionTimeTrackingEsThreadPoolExecutor.EWMA_ALPHA
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        assertThat(executor.getTotalTaskExecutionTime(), equalTo(0L));
        // Using the settableWrapper each task would take 100ns
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(30L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(100L));
        });
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(51L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(200L));
        });
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(65L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(300L));
        });
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(75L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(400L));
        });
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(83L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(500L));
        });

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    /** Use a runnable wrapper that simulates a task with unknown failures. */
    public void testExceptionThrowingTask() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        TaskExecutionTimeTrackingEsThreadPoolExecutor executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            exceptionalWrapper(),
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context,
            TaskExecutionTimeTrackingEsThreadPoolExecutor.EWMA_ALPHA
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Using the exceptionalWrapper each task's execution time is -1 to simulate unknown failures/rejections.
        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        int taskCount = randomIntBetween(1, 100);
        executeTask(executor, taskCount);
        assertBusy(() -> assertThat(executor.getCompletedTaskCount(), equalTo((long) taskCount)));
        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        assertThat(executor.getTotalTaskExecutionTime(), equalTo(0L));
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    /**
     * The returned function outputs a WrappedRunnabled that simulates the case
     * where {@link TimedRunnable#getTotalExecutionNanos()} always returns {@code timeTakenNanos}.
     */
    private Function<Runnable, WrappedRunnable> settableWrapper(long timeTakenNanos) {
        return (runnable) -> new SettableTimedRunnable(timeTakenNanos, false);
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
    private void executeTask(TaskExecutionTimeTrackingEsThreadPoolExecutor executor, int times) {
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
        public long getTotalExecutionNanos() {
            return timeTaken;
        }

        @Override
        public boolean getFailedOrRejected() {
            return testFailedOrRejected;
        }
    }
}
