/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.util.concurrent.EsThreadPoolExecutorTestHelper.getHotThreadsOnLargeQueueConfig;
import static org.elasticsearch.common.util.concurrent.EsThreadPoolExecutorTestHelper.getStartTimeMillisOfLargeQueue;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.threadpool.ScalingExecutorBuilder.HOT_THREADS_ON_LARGE_QUEUE_DURATION_THRESHOLD_SETTING;
import static org.elasticsearch.threadpool.ScalingExecutorBuilder.HOT_THREADS_ON_LARGE_QUEUE_SIZE_THRESHOLD_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class ThreadPoolHotThreadsIT extends AbstractServerlessStatelessPluginIntegTestCase {

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

        final String node = startMasterAndIndexNode(nodeSettings);
        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, node);
        for (var info : threadPool.info()) {
            final var executor = threadPool.executor(info.getName());
            if (executor instanceof EsThreadPoolExecutor esThreadPoolExecutor) {
                if (enabled && ThreadPool.Names.MANAGEMENT.equals(info.getName())) {
                    assertTrue(getHotThreadsOnLargeQueueConfig(esThreadPoolExecutor).isEnabled());
                } else {
                    assertFalse(getHotThreadsOnLargeQueueConfig(esThreadPoolExecutor).isEnabled());
                }
            }
        }

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
        assertThat(getStartTimeMillisOfLargeQueue(managementExecutor), enabled ? greaterThan(0L) : equalTo(-1L));
        waitForTimeElapse(durationThreshold);

        try {
            if (enabled) {
                assertThatLogger(
                    () -> runTask(managementExecutor, () -> {}),
                    EsThreadPoolExecutor.class,
                    new MockLog.SeenEventExpectation(
                        "should log starting hot threads",
                        EsThreadPoolExecutor.class.getCanonicalName(),
                        Level.INFO,
                        "start logging hot-threads for large queue size [" + sizeThreshold + "] on [*/management] executor"
                    ),
                    new MockLog.SeenEventExpectation(
                        "should log hot threads",
                        EsThreadPoolExecutor.class.getCanonicalName(),
                        Level.INFO,
                        "ThreadPoolExecutor [*/management] queue size [" + sizeThreshold + "] has been over threshold for *"
                    )
                );
                assertThat(getStartTimeMillisOfLargeQueue(managementExecutor), greaterThan(0L));
            }

            assertThatLogger(
                () -> runTask(managementExecutor, () -> {}),
                EsThreadPoolExecutor.class,
                new MockLog.UnseenEventExpectation("should not log", EsThreadPoolExecutor.class.getCanonicalName(), Level.INFO, "*")
            );
            assertThat(getStartTimeMillisOfLargeQueue(managementExecutor), enabled ? greaterThan(0L) : equalTo(-1L));
        } finally {
            continueLatch.countDown();
        }

        if (enabled) {
            assertBusy(() -> assertThat(managementExecutor.getQueue().size(), lessThan(sizeThreshold - 1)));
            // Run a final task and the tracking should be cleared since the large queue size is gone
            runTask(managementExecutor, () -> {});
            assertThat(getStartTimeMillisOfLargeQueue(managementExecutor), equalTo(-1L));
        }
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
