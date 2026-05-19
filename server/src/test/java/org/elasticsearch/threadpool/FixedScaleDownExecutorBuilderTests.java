/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

/**
 * Tests for {@link FixedScaleDownExecutorBuilder}.
 */
public class FixedScaleDownExecutorBuilderTests extends ESTestCase {

    public void testBuildProducesExecutorThatRunsTasks() {
        final String poolName = "test_fixed_scale_down";
        final String prefix = "thread_pool." + poolName;
        final int poolSize = randomIntBetween(1, 4);
        final int queueSize = randomIntBetween(10, 32);
        final Settings settings = Settings.builder()
            .put("node.name", "fixed-scale-down-builder-test")
            .put(prefix + ".size", poolSize)
            .put(prefix + ".queue_size", queueSize)
            .build();

        final FixedScaleDownExecutorBuilder builder = new FixedScaleDownExecutorBuilder(
            settings,
            poolName,
            poolSize,
            queueSize,
            TimeValue.timeValueMinutes(10),
            prefix,
            EsExecutors.TaskTrackingConfig.DEFAULT,
            false
        );

        final FixedExecutorBuilder.FixedExecutorSettings executorSettings = builder.getSettings(settings);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final ThreadPool.ExecutorHolder holder = builder.build(executorSettings, threadContext);
        final ExecutorService executor = holder.executor();
        try {
            assertThat(executor, instanceOf(EsThreadPoolExecutor.class));

            final int capacity = poolSize + queueSize;
            final int taskCount = randomIntBetween(1, Math.min(32, capacity));
            final CountDownLatch latch = new CountDownLatch(taskCount);
            final AtomicInteger executed = new AtomicInteger();
            for (int i = 0; i < taskCount; i++) {
                executor.execute(() -> {
                    executed.incrementAndGet();
                    latch.countDown();
                });
            }
            safeAwait(latch);
            assertThat(executed.get(), equalTo(taskCount));
        } finally {
            terminate(executor);
        }
    }

    public void testPoolScalesDownToZeroThreadsWhenIdle() throws Exception {
        final String poolName = "test_scale_down_to_zero";
        final String prefix = "thread_pool." + poolName;
        final int poolSize = randomIntBetween(1, 4);
        final int queueSize = randomIntBetween(1, 16);
        final TimeValue scaleDownDelay = TimeValue.timeValueMillis(100);
        final Settings settings = Settings.builder()
            .put("node.name", "scale-down-to-zero-test")
            .put(prefix + ".size", poolSize)
            .put(prefix + ".queue_size", queueSize)
            .build();

        final FixedScaleDownExecutorBuilder builder = new FixedScaleDownExecutorBuilder(
            settings,
            poolName,
            poolSize,
            queueSize,
            scaleDownDelay,
            prefix,
            EsExecutors.TaskTrackingConfig.DEFAULT,
            false
        );

        final FixedExecutorBuilder.FixedExecutorSettings executorSettings = builder.getSettings(settings);
        final EsThreadPoolExecutor executor = (EsThreadPoolExecutor) builder.build(executorSettings, new ThreadContext(Settings.EMPTY))
            .executor();
        try {
            assertThat(executor.getPoolSize(), equalTo(0));

            final CountDownLatch taskStarted = new CountDownLatch(1);
            final CountDownLatch taskCanFinish = new CountDownLatch(1);
            executor.execute(() -> {
                taskStarted.countDown();
                safeAwait(taskCanFinish);
            });
            safeAwait(taskStarted);
            assertThat(executor.getPoolSize(), equalTo(1));

            taskCanFinish.countDown();
            assertBusy(() -> assertThat(executor.getPoolSize(), equalTo(0)));
        } finally {
            terminate(executor);
        }
    }

    public void testBuildConfiguresScaleDownAndThreadPoolInfo() {
        final String poolName = "test_fixed_scale_down_info";
        final String prefix = "thread_pool." + poolName;
        final int poolSize = randomIntBetween(2, 4);
        final int queueSize = randomIntBetween(8, 16);
        final TimeValue scaleDownDelay = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        final Settings settings = Settings.builder()
            .put("node.name", "fixed-scale-down-info-test")
            .put(prefix + ".size", poolSize)
            .put(prefix + ".queue_size", queueSize)
            .build();

        final FixedScaleDownExecutorBuilder builder = new FixedScaleDownExecutorBuilder(
            settings,
            poolName,
            poolSize,
            queueSize,
            scaleDownDelay,
            prefix,
            EsExecutors.TaskTrackingConfig.DEFAULT,
            false
        );

        final FixedExecutorBuilder.FixedExecutorSettings executorSettings = builder.getSettings(settings);
        final ThreadPool.ExecutorHolder holder = builder.build(executorSettings, new ThreadContext(Settings.EMPTY));
        final ExecutorService executor = holder.executor();
        try {
            final EsThreadPoolExecutor esExecutor = (EsThreadPoolExecutor) executor;
            assertTrue(esExecutor.allowsCoreThreadTimeOut());
            assertThat(esExecutor.getKeepAliveTime(TimeUnit.MILLISECONDS), equalTo(scaleDownDelay.millis()));

            final ThreadPool.Info info = holder.info;
            assertThat(info.getName(), equalTo(poolName));
            assertThat(info.getThreadPoolType(), equalTo(ThreadPool.ThreadPoolType.FIXED));
            assertThat(info.getMin(), equalTo(0));
            assertThat(info.getMax(), equalTo(poolSize));
            assertThat(info.getQueueSize(), equalTo((long) queueSize));
        } finally {
            terminate(executor);
        }
    }
}
