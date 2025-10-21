/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.metrics.ExponentialBucketHistogram;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestEsExecutors;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig.DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

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
            TestEsExecutors.testOnlyDaemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context,
            randomBoolean()
                ? EsExecutors.TaskTrackingConfig.builder()
                    .trackOngoingTasks()
                    .trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST)
                    .build()
                : EsExecutors.TaskTrackingConfig.builder().trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST).build()
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
        assertThat(executor.getOngoingTasks().toString(), executor.getOngoingTasks().size(), equalTo(0));
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }

    /**
     * Verifies that we can peek at the task in front of the task queue to fetch the duration that the oldest task has been queued.
     * Tests {@link TaskExecutionTimeTrackingEsThreadPoolExecutor#peekMaxQueueLatencyInQueueMillis}.
     */
    public void testFrontOfQueueLatency() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        final var barrier = new CyclicBarrier(2);
        // Replace all tasks submitted to the thread pool with a configurable task that supports configuring queue latency durations and
        // waiting for task execution to begin via the supplied barrier.
        var adjustableTimedRunnable = new AdjustableQueueTimeWithExecutionBarrierTimedRunnable(
            barrier,
            // This won't actually be used, because it is reported after a task is taken off the queue. This test peeks at the still queued
            // tasks.
            TimeUnit.MILLISECONDS.toNanos(1)
        );
        TaskExecutionTimeTrackingEsThreadPoolExecutor executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1_000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            (runnable) -> adjustableTimedRunnable,
            TestEsExecutors.testOnlyDaemonThreadFactory("queue-latency-test"),
            new EsAbortPolicy(),
            context,
            randomBoolean()
                ? EsExecutors.TaskTrackingConfig.builder()
                    .trackOngoingTasks()
                    .trackMaxQueueLatency()
                    .trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST)
                    .build()
                : EsExecutors.TaskTrackingConfig.builder()
                    .trackMaxQueueLatency()
                    .trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST)
                    .build()
        );
        try {
            executor.prestartAllCoreThreads();
            logger.info("--> executor: {}", executor);

            // Check that the peeking at a non-existence queue returns zero.
            assertEquals("Zero should be returned when there is no queue", 0, executor.peekMaxQueueLatencyInQueueMillis());

            // Submit two tasks, into the thread pool with a single worker thread. The second one will be queued (because the pool only has
            // one thread) and can be peeked at.
            executor.execute(() -> {});
            executor.execute(() -> {});

            waitForTimeToElapse();
            var frontOfQueueDuration = executor.peekMaxQueueLatencyInQueueMillis();
            assertThat("Expected a task to be queued", frontOfQueueDuration, greaterThan(0L));
            waitForTimeToElapse();
            var updatedFrontOfQueueDuration = executor.peekMaxQueueLatencyInQueueMillis();
            assertThat(
                "Expected a second peek to report a longer duration",
                updatedFrontOfQueueDuration,
                greaterThan(frontOfQueueDuration)
            );

            // Release the first task that's running, and wait for the second to start -- then it is ensured that the queue will be empty.
            safeAwait(barrier);
            safeAwait(barrier);
            assertEquals("Queue should be emptied", 0, executor.peekMaxQueueLatencyInQueueMillis());
        } finally {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Verifies that tracking of the max queue latency (captured on task dequeue) is maintained.
     * Tests {@link TaskExecutionTimeTrackingEsThreadPoolExecutor#getMaxQueueLatencyMillisSinceLastPollAndReset()}.
     */
    public void testMaxDequeuedQueueLatency() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        final var barrier = new CyclicBarrier(2);
        // Replace all tasks submitted to the thread pool with a configurable task that supports configuring queue latency durations and
        // waiting for task execution to begin via the supplied barrier.
        var adjustableTimedRunnable = new AdjustableQueueTimeWithExecutionBarrierTimedRunnable(
            barrier,
            TimeUnit.NANOSECONDS.toNanos(1000000) // Until changed, queue latencies will always be 1 millisecond.
        );
        TaskExecutionTimeTrackingEsThreadPoolExecutor executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            (runnable) -> adjustableTimedRunnable,
            TestEsExecutors.testOnlyDaemonThreadFactory("queue-latency-test"),
            new EsAbortPolicy(),
            context,
            randomBoolean()
                ? EsExecutors.TaskTrackingConfig.builder()
                    .trackOngoingTasks()
                    .trackMaxQueueLatency()
                    .trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST)
                    .build()
                : EsExecutors.TaskTrackingConfig.builder()
                    .trackMaxQueueLatency()
                    .trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST)
                    .build()
        );
        try {
            executor.prestartAllCoreThreads();
            logger.info("--> executor: {}", executor);

            // Check that the max is zero initially and after a reset.
            assertEquals("The queue latency should be initialized zero", 0, executor.getMaxQueueLatencyMillisSinceLastPollAndReset());
            executor.execute(() -> {});
            safeAwait(barrier); // Wait for the task to start, which means implies has finished the queuing stage.
            assertEquals("Ran one task of 1ms, should be the max", 1, executor.getMaxQueueLatencyMillisSinceLastPollAndReset());
            assertEquals("The max was just reset, should be zero", 0, executor.getMaxQueueLatencyMillisSinceLastPollAndReset());

            // Check that the max is kept across multiple calls, where the last is not the max.
            adjustableTimedRunnable.setQueuedTimeTakenNanos(5000000);
            executeTask(executor, 1);
            safeAwait(barrier); // Wait for the task to start, which means implies has finished the queuing stage.
            adjustableTimedRunnable.setQueuedTimeTakenNanos(1000000);
            executeTask(executor, 1);
            safeAwait(barrier);
            assertEquals("Max should not be the last task", 5, executor.getMaxQueueLatencyMillisSinceLastPollAndReset());
            assertEquals("The max was just reset, should be zero", 0, executor.getMaxQueueLatencyMillisSinceLastPollAndReset());
        } finally {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
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
            TestEsExecutors.testOnlyDaemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context,
            randomBoolean()
                ? EsExecutors.TaskTrackingConfig.builder()
                    .trackOngoingTasks()
                    .trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST)
                    .build()
                : EsExecutors.TaskTrackingConfig.builder().trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST).build()
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
        assertThat(executor.getActiveCount(), equalTo(0));
        assertThat(executor.getOngoingTasks().toString(), executor.getOngoingTasks().size(), equalTo(0));
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }

    public void testGetOngoingTasks() throws Exception {
        var testStartTimeNanos = System.nanoTime();
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        var executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            TimedRunnable::new,
            TestEsExecutors.testOnlyDaemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context,
            EsExecutors.TaskTrackingConfig.builder()
                .trackOngoingTasks()
                .trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST)
                .build()
        );
        var taskRunningLatch = new CountDownLatch(1);
        var exitTaskLatch = new CountDownLatch(1);
        assertThat(executor.getOngoingTasks().toString(), executor.getOngoingTasks().size(), equalTo(0));
        Runnable runnable = () -> {
            taskRunningLatch.countDown();
            safeAwait(exitTaskLatch);
        };
        executor.execute(runnable);
        safeAwait(taskRunningLatch);
        var ongoingTasks = executor.getOngoingTasks();
        assertThat(ongoingTasks.toString(), ongoingTasks.size(), equalTo(1));
        assertThat(ongoingTasks.values().iterator().next(), greaterThanOrEqualTo(testStartTimeNanos));
        exitTaskLatch.countDown();
        assertBusy(() -> assertThat(executor.getOngoingTasks().toString(), executor.getOngoingTasks().size(), equalTo(0)));
        assertThat(executor.getTotalTaskExecutionTime(), greaterThan(0L));
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }

    public void testQueueLatencyHistogramMetrics() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final var threadPoolName = randomIdentifier();
        var executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            threadPoolName,
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            TimedRunnable::new,
            TestEsExecutors.testOnlyDaemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.builder()
                .trackOngoingTasks()
                .trackExecutionTime(DEFAULT_EXECUTION_TIME_EWMA_ALPHA_FOR_TEST)
                .build()
        );
        executor.setupMetrics(meterRegistry, threadPoolName);

        try {
            final var barrier = new CyclicBarrier(2);
            final ExponentialBucketHistogram expectedHistogram = new ExponentialBucketHistogram(
                TaskExecutionTimeTrackingEsThreadPoolExecutor.QUEUE_LATENCY_HISTOGRAM_BUCKETS
            );

            /*
             * The thread pool has a single thread, so we submit a task that will occupy that thread
             * and cause subsequent tasks to be queued
             */
            Future<?> runningTask = executor.submit(() -> {
                safeAwait(barrier);
                safeAwait(barrier);
            });
            safeAwait(barrier); // wait till the first task starts
            expectedHistogram.addObservation(0L); // the first task should not be delayed

            /*
             *  On each iteration we submit a task - which will be queued because of the
             *  currently running task, pause for some random interval, then unblock the
             *  new task by releasing the currently running task. This gives us a lower
             *  bound for the real delays (the real delays will be greater than or equal
             *  to the synthetic delays we add, i.e. each percentile should be >= our
             *  expected values)
             */
            for (int i = 0; i < 10; i++) {
                Future<?> waitingTask = executor.submit(() -> {
                    safeAwait(barrier);
                    safeAwait(barrier);
                });
                final long delayTimeMs = randomLongBetween(1, 50);
                safeSleep(delayTimeMs);
                safeAwait(barrier); // let the running task complete
                safeAwait(barrier); // wait for the next task to start
                safeGet(runningTask); // ensure previous task is complete
                expectedHistogram.addObservation(delayTimeMs);
                runningTask = waitingTask;
            }
            safeAwait(barrier); // let the last task finish
            safeGet(runningTask);
            meterRegistry.getRecorder().collect();

            List<Measurement> measurements = meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.LONG_GAUGE,
                    ThreadPool.THREAD_POOL_METRIC_PREFIX + threadPoolName + ThreadPool.THREAD_POOL_METRIC_NAME_QUEUE_TIME
                );
            assertThat(measurements, hasSize(3));
            // we have to use greater than or equal to because the actual delay might be higher than what we imposed
            assertThat(getPercentile(measurements, "99"), greaterThanOrEqualTo(expectedHistogram.getPercentile(0.99f)));
            assertThat(getPercentile(measurements, "90"), greaterThanOrEqualTo(expectedHistogram.getPercentile(0.9f)));
            assertThat(getPercentile(measurements, "50"), greaterThanOrEqualTo(expectedHistogram.getPercentile(0.5f)));
        } finally {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }

    private long getPercentile(List<Measurement> measurements, String percentile) {
        return measurements.stream().filter(m -> m.attributes().get("percentile").equals(percentile)).findFirst().orElseThrow().getLong();
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
        private final long executionTimeTakenNanos;
        private final boolean testFailedOrRejected;

        public SettableTimedRunnable(long executionTimeTakenNanos, boolean failedOrRejected) {
            super(() -> {});
            this.executionTimeTakenNanos = executionTimeTakenNanos;
            this.testFailedOrRejected = failedOrRejected;
        }

        @Override
        public long getTotalExecutionNanos() {
            return executionTimeTakenNanos;
        }

        @Override
        public boolean getFailedOrRejected() {
            return testFailedOrRejected;
        }
    }

    /**
     * This TimedRunnable override provides the following:
     * <ul>
     * <li> Overrides {@link TimedRunnable#getQueueTimeNanos()} so that arbitrary queue latencies can be set for the thread pool.</li>
     * <li> Replaces any submitted Runnable task to the thread pool with a Runnable that only waits on a {@link CyclicBarrier}.</li>
     * </ul>
     * This allows dynamically manipulating the queue time with {@link #setQueuedTimeTakenNanos}, and provides a means of waiting for a task
     * to start by calling {@code safeAwait(barrier)} after submitting a task.
     * <p>
     * Look at {@link TaskExecutionTimeTrackingEsThreadPoolExecutor#wrapRunnable} for how the ThreadPool uses this as a wrapper around all
     * submitted tasks.
     */
    public class AdjustableQueueTimeWithExecutionBarrierTimedRunnable extends TimedRunnable {
        private long queuedTimeTakenNanos;

        /**
         * @param barrier A barrier that the caller can wait upon to ensure a task starts.
         * @param queuedTimeTakenNanos The default queue time reported for all tasks.
         */
        public AdjustableQueueTimeWithExecutionBarrierTimedRunnable(CyclicBarrier barrier, long queuedTimeTakenNanos) {
            super(() -> { safeAwait(barrier); });
            this.queuedTimeTakenNanos = queuedTimeTakenNanos;
        }

        public void setQueuedTimeTakenNanos(long timeTakenNanos) {
            this.queuedTimeTakenNanos = timeTakenNanos;
        }

        @Override
        long getQueueTimeNanos() {
            return queuedTimeTakenNanos;
        }
    }

    /**
     * Ensures that the time reported by {@code System.nanoTime()} has advanced. It is otherwise feasible for the clock to report no time
     * passing between operations. Call this method if time passing must be guaranteed.
     */
    private static void waitForTimeToElapse() throws InterruptedException {
        final var startNanoTime = System.nanoTime();
        while (TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS) < 1) {
            Thread.sleep(Duration.ofMillis(1));
        }
    }
}
