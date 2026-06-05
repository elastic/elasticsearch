/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_QUEUE_BACKPRESSURE_THRESHOLD;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class EsqlWorkerThreadPoolTests extends ESTestCase {

    public void testDefaultThreadPoolSize() {
        EsqlPlugin plugin = new EsqlPlugin();
        Settings settings = Settings.EMPTY;
        List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(settings);
        assertEquals(1, builders.size());
    }

    public void testCustomThreadPoolSize() {
        Settings settings = Settings.builder().put(ESQL_WORKER_THREAD_POOL_SIZE.getKey(), 42).build();
        int configured = ESQL_WORKER_THREAD_POOL_SIZE.get(settings);
        assertEquals(42, configured);
    }

    public void testDefaultSettingValue() {
        Settings settings = Settings.EMPTY;
        int configured = ESQL_WORKER_THREAD_POOL_SIZE.get(settings);
        assertEquals(-1, configured);
    }

    public void testSettingRegistered() {
        EsqlPlugin plugin = new EsqlPlugin();
        boolean found = false;
        for (var setting : plugin.getSettings()) {
            if (setting.getKey().equals(ESQL_WORKER_THREAD_POOL_SIZE.getKey())) {
                found = true;
                break;
            }
        }
        assertTrue("ESQL_WORKER_THREAD_POOL_SIZE should be registered", found);
    }

    public void testThreadPoolNameConstant() {
        assertEquals("esql_worker", ESQL_WORKER_THREAD_POOL_NAME);
    }

    public void testBackpressureThresholdDefaultValue() {
        Settings settings = Settings.EMPTY;
        double threshold = ESQL_WORKER_QUEUE_BACKPRESSURE_THRESHOLD.get(settings);
        assertEquals(ESQL_WORKER_QUEUE_BACKPRESSURE_THRESHOLD.getDefault(Settings.EMPTY), threshold, 1e-9);
    }

    public void testBackpressureThresholdCustomValue() {
        Settings settings = Settings.builder().put(ESQL_WORKER_QUEUE_BACKPRESSURE_THRESHOLD.getKey(), 0.1).build();
        double threshold = ESQL_WORKER_QUEUE_BACKPRESSURE_THRESHOLD.get(settings);
        assertEquals(0.1, threshold, 1e-9);
    }

    public void testBackpressureThresholdRegistered() {
        EsqlPlugin plugin = new EsqlPlugin();
        boolean found = plugin.getSettings().stream().anyMatch(s -> s.getKey().equals(ESQL_WORKER_QUEUE_BACKPRESSURE_THRESHOLD.getKey()));
        assertTrue("ESQL_WORKER_QUEUE_BACKPRESSURE_THRESHOLD should be registered", found);
    }

    public void testBackpressureThresholdIsDynamic() {
        assertTrue(ESQL_WORKER_QUEUE_BACKPRESSURE_THRESHOLD.isDynamic());
    }

    /** Disabled threshold (≤ 0) → no limit. */
    public void testMaxDataParallelismDisabled() {
        assertThat(ComputeService.maxDataParallelism(EsExecutors.DIRECT_EXECUTOR_SERVICE, 0.0), equalTo(Integer.MAX_VALUE));
        assertThat(ComputeService.maxDataParallelism(EsExecutors.DIRECT_EXECUTOR_SERVICE, -1.0), equalTo(Integer.MAX_VALUE));
    }

    /** Non-ThreadPoolExecutor (e.g. direct executor in tests) → no limit. */
    public void testMaxDataParallelismNonThreadPoolExecutor() {
        assertThat(ComputeService.maxDataParallelism(EsExecutors.DIRECT_EXECUTOR_SERVICE, 0.05), equalTo(Integer.MAX_VALUE));
    }

    /** With a known queue capacity and threshold, the limit equals ceil(remaining * threshold). */
    public void testMaxDataParallelismFivePercent() {
        int queueCapacity = 1000;
        var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "bp-test", 1, queueCapacity, "bp-test", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        try {
            Executor executor = threadPool.executor("bp-test");
            // Queue empty → remaining = 1000, limit = ceil(1000 * 0.05) = 50
            assertThat(ComputeService.maxDataParallelism(executor, 0.05), equalTo(50));
        } finally {
            terminate(threadPool);
        }
    }

    /** Limit is at least 1 even when the queue is completely full. */
    public void testMaxDataParallelismAtLeastOne() {
        int queueCapacity = 1000;
        var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "bp-test", 1, queueCapacity, "bp-test", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        try {
            Executor executor = threadPool.executor("bp-test");
            CountDownLatch latch = new CountDownLatch(1);
            try {
                executor.execute(() -> safeAwait(latch)); // pin the single thread
                for (int i = 0; i < queueCapacity; i++) {
                    try {
                        executor.execute(() -> {});
                    } catch (Exception ignored) {
                        break;
                    }
                }
            } finally {
                latch.countDown();
            }
            assertThat(ComputeService.maxDataParallelism(executor, 0.05), greaterThanOrEqualTo(1));
        } finally {
            terminate(threadPool);
        }
    }
}
