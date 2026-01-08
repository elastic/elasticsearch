/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.threadpool.ScalingExecutorBuilder.HOT_THREADS_ON_LARGE_QUEUE_DURATION_THRESHOLD_SETTING;
import static org.elasticsearch.threadpool.ScalingExecutorBuilder.HOT_THREADS_ON_LARGE_QUEUE_SIZE_THRESHOLD_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ThreadPoolHotThreadsIT extends ESIntegTestCase {

    public void testNotHotThreadsForPersistedManagementThreadPoolQueueSizeWhenDisabled() throws Exception {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(HOT_THREADS_ON_LARGE_QUEUE_DURATION_THRESHOLD_SETTING.getKey(), TimeValue.timeValueSeconds(1));
        if (randomBoolean()) {
            settingsBuilder.put(HOT_THREADS_ON_LARGE_QUEUE_SIZE_THRESHOLD_SETTING.getKey(), 0);
        }
        doTestHotThreadsForPersistedManagementThreadPoolQueueSize(settingsBuilder.build());
    }

    public void testHotThreadsForPersistedManagementThreadPoolQueueSizeWhenEnabled() throws Exception {
        doTestHotThreadsForPersistedManagementThreadPoolQueueSize(
            Settings.builder()
                .put(HOT_THREADS_ON_LARGE_QUEUE_SIZE_THRESHOLD_SETTING.getKey(), between(8, 16))
                .put(HOT_THREADS_ON_LARGE_QUEUE_DURATION_THRESHOLD_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
    }

    private void doTestHotThreadsForPersistedManagementThreadPoolQueueSize(Settings nodeSettings) throws Exception {
        final int sizeThreshold = HOT_THREADS_ON_LARGE_QUEUE_SIZE_THRESHOLD_SETTING.get(nodeSettings);
        final TimeValue durationThreshold = HOT_THREADS_ON_LARGE_QUEUE_DURATION_THRESHOLD_SETTING.get(nodeSettings);
        final boolean enabled = sizeThreshold > 0;

        final String node = internalCluster().startNode(nodeSettings);
        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, node);
        final var managementExecutor = (EsThreadPoolExecutor) threadPool.executor(ThreadPool.Names.MANAGEMENT);
        final int maxManagementThreads = threadPool.info(ThreadPool.Names.MANAGEMENT).getMax();

        final var executingLatch = new CountDownLatch(maxManagementThreads);
        final var continueLatch = new CountDownLatch(1);

        final Runnable blockingTask = () -> {
            executingLatch.countDown();
            safeAwait(continueLatch);
        };

        // Activate all available threads
        for (int i = 0; i < maxManagementThreads; i++) {
            runTask(managementExecutor, blockingTask);
        }

        safeAwait(executingLatch);
        assertThat(managementExecutor.getActiveCount(), equalTo(maxManagementThreads));
        logger.info("--> all blocking tasks are running");

        // Fill the queue up to the threshold
        for (int i = 0; i < sizeThreshold; i++) {
            runTask(managementExecutor, () -> {});
        }
        assertThat(managementExecutor.getQueue().size(), equalTo(sizeThreshold));
        assertThat(managementExecutor.getStartTimeOfLargeQueue(), enabled ? greaterThan(0L) : equalTo(-1L));
        waitForTimeElapse(durationThreshold);

        try {
            if (enabled) {
                assertThatLogger(
                    () -> runTask(managementExecutor, () -> {}),
                    EsThreadPoolExecutor.class,
                    new MockLog.SeenEventExpectation(
                        "should log hot threads",
                        EsThreadPoolExecutor.class.getCanonicalName(),
                        Level.INFO,
                        "ThreadPoolExecutor [*/management] queue size [" + sizeThreshold + "] has been over threshold for *"
                    )
                );
                assertThat(managementExecutor.getStartTimeOfLargeQueue(), greaterThan(0L));
            }

            assertThatLogger(
                () -> runTask(managementExecutor, () -> {}),
                EsThreadPoolExecutor.class,
                new MockLog.UnseenEventExpectation("should not log", EsThreadPoolExecutor.class.getCanonicalName(), Level.INFO, "*")
            );
            assertThat(managementExecutor.getStartTimeOfLargeQueue(), enabled ? greaterThan(0L) : equalTo(-1L));
        } finally {
            continueLatch.countDown();
        }

        if (enabled) {
            assertBusy(() -> assertThat(managementExecutor.getQueue().size(), lessThan(sizeThreshold - 1)));
            // Run a final task and the tracking should be cleared since the large queue size is gone
            runTask(managementExecutor, () -> {});
            assertThat(managementExecutor.getStartTimeOfLargeQueue(), equalTo(-1L));
        }
    }

    public void testEsThreadPoolExecutor() throws Exception {
        final long startTime = randomLongBetween(0, 1000);
        final var timer = new AtomicLong(startTime);
        final String nodeName = randomIdentifier();
        final String threadName = randomIdentifier();
        final int maxThreads = between(1, 5);
        final var sizeThreshold = between(10, 100);
        final TimeValue durationThreshold = randomTimeValue(1, 5, TimeUnit.MINUTES);
        final TimeValue interval = randomTimeValue(30, 60, TimeUnit.MINUTES);

        final var executor = new EsThreadPoolExecutor(
            threadName,
            1,
            maxThreads,
            5,
            TimeUnit.MINUTES,
            new EsExecutors.ExecutorScalingQueue<>(),
            EsExecutors.daemonThreadFactory(nodeName, threadName),
            new EsExecutors.ForceQueuePolicy(false, false),
            new ThreadContext(Settings.EMPTY),
            new EsExecutors.HotThreadsOnLargeQueueConfig(sizeThreshold, durationThreshold.millis(), interval),
            timer::get
        );
        assertThat(executor.getStartTimeOfLargeQueue(), equalTo(-1L));

        final var executingLatch = new CountDownLatch(maxThreads);
        final var continueLatch = new CountDownLatch(1);

        final Runnable blockingTask = () -> {
            executingLatch.countDown();
            safeAwait(continueLatch);
        };

        // Activate all available threads
        for (int i = 0; i < maxThreads; i++) {
            runTask(executor, blockingTask);
        }
        safeAwait(executingLatch);
        assertThat(executor.getActiveCount(), equalTo(maxThreads));

        try {
            // 1. Fill the queue up to the threshold - No logging but tracking should start due to queue size reaching the threshold
            assertThatLogger(() -> {
                for (int i = 0; i < sizeThreshold; i++) {
                    runTask(executor, () -> {});
                }
                assertThat(executor.getQueue().size(), equalTo(sizeThreshold));
                assertThat(executor.getStartTimeOfLargeQueue(), equalTo(timer.get()));
            },
                EsThreadPoolExecutor.class,
                new MockLog.UnseenEventExpectation("should not log", EsThreadPoolExecutor.class.getCanonicalName(), Level.INFO, "*")
            );

            // 2. Queue more tasks but should not see logging yet since duration threshold not met
            assertThatLogger(
                () -> IntStream.range(0, between(1, 5)).forEach(ignore -> runTask(executor, () -> {})),
                EsThreadPoolExecutor.class,
                new MockLog.UnseenEventExpectation("should not log", EsThreadPoolExecutor.class.getCanonicalName(), Level.INFO, "*")
            );

            // 3. Advance time and we should observe logging when adding more task
            final long elapsedMillis1 = durationThreshold.millis() + randomLongBetween(0, 1000);
            timer.addAndGet(elapsedMillis1);
            assertThatLogger(
                () -> runTask(executor, () -> {}),
                EsThreadPoolExecutor.class,
                new MockLog.SeenEventExpectation(
                    "should log hot threads",
                    EsThreadPoolExecutor.class.getCanonicalName(),
                    Level.INFO,
                    "ThreadPoolExecutor ["
                        + threadName
                        + "] queue size ["
                        + executor.getQueue().size()
                        + "] has been over threshold for ["
                        + TimeValue.timeValueMillis(elapsedMillis1)
                        + "]*"
                )
            );

            // 4. Add more task and there should be no more logging since logging interval has not passed yet
            assertThatLogger(
                () -> IntStream.range(0, between(1, 5)).forEach(ignore -> runTask(executor, () -> {})),
                EsThreadPoolExecutor.class,
                new MockLog.UnseenEventExpectation("should not log", EsThreadPoolExecutor.class.getCanonicalName(), Level.INFO, "*")
            );

            // 5. Advance time to pass the logging interval and we should observe logging again when adding more task
            final long elapsedMillis2 = interval.millis() + randomLongBetween(0, 1000);
            timer.addAndGet(elapsedMillis2);
            assertThatLogger(
                () -> runTask(executor, () -> {}),
                EsThreadPoolExecutor.class,
                new MockLog.SeenEventExpectation(
                    "should log hot threads",
                    EsThreadPoolExecutor.class.getCanonicalName(),
                    Level.INFO,
                    "ThreadPoolExecutor ["
                        + threadName
                        + "] queue size ["
                        + executor.getQueue().size()
                        + "] has been over threshold for ["
                        + TimeValue.timeValueMillis(elapsedMillis1 + elapsedMillis2)
                        + "]*"
                )
            );
        } finally {
            continueLatch.countDown();
        }

        // 6. Wait for the queue to drain and add one more task and the tracking should be reset
        assertBusy(() -> assertThat(executor.getQueue().size(), lessThan(sizeThreshold - 1)));
        assertThat(executor.getStartTimeOfLargeQueue(), equalTo(startTime));
        runTask(executor, () -> {});
        assertThat(executor.getStartTimeOfLargeQueue(), equalTo(-1L));

        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }

    private void runTask(EsThreadPoolExecutor executor, Runnable task) {
        if (randomBoolean()) {
            executor.execute(task);
        } else {
            executor.submit(task);
        }
    }

    private static void waitForTimeElapse(TimeValue duration) {
        final var startTime = System.currentTimeMillis();
        // Ensure we wait for at least the specified duration by loop and explicit check the elapsed time to avoid
        // potential inconsistency between slept time and System.currentTimeMillis() difference.
        while (System.currentTimeMillis() - startTime < duration.millis()) {
            safeSleep(duration);
        }
    }
}
