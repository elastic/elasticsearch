/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig.DEFAULT_EWMA_ALPHA;
import static org.hamcrest.Matchers.equalTo;

public class TaskExecutionTimeTrackingPerIndexEsThreadPoolExecutorTests extends ESTestCase {

    String INDEX_NAME = "index";

    public void testExecutionPerIndexStatistics() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);

        TaskExecutionTimeTrackingPerIndexEsThreadPoolExecutor executor = new TaskExecutionTimeTrackingPerIndexEsThreadPoolExecutor(
            "test-threadpool",
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            TimedRunnable::new,
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context,
            new EsExecutors.TaskTrackingConfig(randomBoolean(), DEFAULT_EWMA_ALPHA)
        );
        executor.prestartAllCoreThreads();

        assertThat((long) executor.getLoadEMWAPerIndex(INDEX_NAME), equalTo(0L));
        assertThat(executor.getSearchLoadPerIndex(INDEX_NAME), equalTo(0L));

        executeTask(executor, 1);
        assertBusy(() -> {
            assertTrue((long) executor.getLoadEMWAPerIndex(INDEX_NAME) > 0);
            assertTrue(executor.getSearchLoadPerIndex(INDEX_NAME) > 0);
        });

        shutdownExecutor(executor);
    }

    /** Execute a blank task {@code times} times for the executor */
    private void executeTask(TaskExecutionTimeTrackingPerIndexEsThreadPoolExecutor executor, int times) {
        for (int i = 0; i < times; i++) {
            Runnable runnable = () -> {};
            executor.registerIndexNameForRunnable(INDEX_NAME, runnable);
            executor.execute(runnable);
        }
    }

    private void shutdownExecutor(EsThreadPoolExecutor executor) {
        executor.shutdown();
        try {
            if (executor.awaitTermination(5, TimeUnit.SECONDS) == false) executor.shutdownNow();
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}
