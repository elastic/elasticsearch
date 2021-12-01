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
public class EWMATrackingEsThreadPoolExecutorTests extends ESTestCase {

    public void testExecutionEWMACalculation() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);

        EWMATrackingEsThreadPoolExecutor executor = new EWMATrackingEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            fastWrapper(),
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
        EWMATrackingEsThreadPoolExecutor executor = new EWMATrackingEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            exceptionalWrapper(),
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

    /**
     * The returned function outputs a WrappedRunnabled that simulates the case
     * where {@link TimedRunnable#getTotalExecutionNanos()} returns -1 because
     * the job failed or was rejected before it finished.
     */
    private Function<Runnable, WrappedRunnable> exceptionalWrapper() {
        return (runnable) -> new SettableTimedRunnable(TimeUnit.NANOSECONDS.toNanos(-1), true);
    }

    /** Execute a blank task {@code times} times for the executor */
    private void executeTask(EWMATrackingEsThreadPoolExecutor executor, int times) {
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
