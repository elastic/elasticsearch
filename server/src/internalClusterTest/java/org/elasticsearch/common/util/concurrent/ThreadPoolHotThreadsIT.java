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

import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.threadpool.ScalingExecutorBuilder.HOT_THREADS_ON_LARGE_QUEUE_DURATION_THRESHOLD_SETTING;
import static org.elasticsearch.threadpool.ScalingExecutorBuilder.HOT_THREADS_ON_LARGE_QUEUE_SIZE_THRESHOLD_SETTING;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ThreadPoolHotThreadsIT extends ESIntegTestCase {

    public void testNotHotThreadsForPersistedManagementThreadPoolQueueSizeWhenDisabled() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(HOT_THREADS_ON_LARGE_QUEUE_DURATION_THRESHOLD_SETTING.getKey(), TimeValue.timeValueSeconds(1));
        if (randomBoolean()) {
            settingsBuilder.put(HOT_THREADS_ON_LARGE_QUEUE_SIZE_THRESHOLD_SETTING.getKey(), 0);
        }
        doTestHotThreadsForPersistedManagementThreadPoolQueueSize(settingsBuilder.build());
    }

    public void testHotThreadsForPersistedManagementThreadPoolQueueSizeWhenEnabled() {
        doTestHotThreadsForPersistedManagementThreadPoolQueueSize(
            Settings.builder()
                .put(HOT_THREADS_ON_LARGE_QUEUE_SIZE_THRESHOLD_SETTING.getKey(), between(8, 16))
                .put(HOT_THREADS_ON_LARGE_QUEUE_DURATION_THRESHOLD_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
    }

    private void doTestHotThreadsForPersistedManagementThreadPoolQueueSize(Settings nodeSettings) {
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
            if (randomBoolean()) {
                managementExecutor.execute(blockingTask);
            } else {
                managementExecutor.submit(blockingTask);
            }
        }

        safeAwait(executingLatch);
        assertThat(managementExecutor.getActiveCount(), equalTo(maxManagementThreads));
        logger.info("--> all blocking tasks are running");

        // Fill the queue up to the threshold
        for (int i = 0; i < sizeThreshold; i++) {
            if (randomBoolean()) {
                managementExecutor.execute(() -> {});
            } else {
                managementExecutor.submit(() -> {});
            }
        }
        assertThat(managementExecutor.getQueue().size(), equalTo(sizeThreshold));
        waitForTimeElapse(durationThreshold);

        try {
            if (enabled) {
                assertThatLogger(() -> {
                    if (randomBoolean()) {
                        managementExecutor.execute(() -> {});
                    } else {
                        managementExecutor.submit(() -> {});
                    }
                },
                    EsThreadPoolExecutor.class,
                    new MockLog.SeenEventExpectation(
                        "should log hot threads",
                        EsThreadPoolExecutor.class.getCanonicalName(),
                        Level.INFO,
                        "ThreadPoolExecutor [*/management] queue size [" + sizeThreshold + "] has been over threshold for *"
                    )
                );
            }

            assertThatLogger(() -> {
                if (randomBoolean()) {
                    managementExecutor.execute(() -> {});
                } else {
                    managementExecutor.submit(() -> {});
                }
            },
                EsThreadPoolExecutor.class,
                new MockLog.UnseenEventExpectation(
                    "should not log again since it's frequency capped",
                    EsThreadPoolExecutor.class.getCanonicalName(),
                    Level.INFO,
                    "*"
                )
            );
        } finally {
            continueLatch.countDown();
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
