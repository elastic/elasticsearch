/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.telemetry.RecordingMeterRegistry.measures;
import static org.elasticsearch.xpack.stateless.StatelessPlugin.PREWARM_THREAD_POOL;
import static org.elasticsearch.xpack.stateless.StatelessPlugin.SHARD_READ_THREAD_POOL;
import static org.elasticsearch.xpack.stateless.commits.BCCHeaderReadExecutor.MAX_CONCURRENCY_SETTING;
import static org.elasticsearch.xpack.stateless.commits.BCCHeaderReadExecutor.maxConcurrency;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class BCCHeaderReadExecutorTests extends ESTestCase {

    public void testDerivesLimitFromTheLargerPool() {
        // enough processors for the prewarm pool to exceed the shard read pool
        assertThat(maxConcurrency(-1, 32, 10), equalTo(32));
        // small enough that the shard read pool is the larger of the two
        assertThat(maxConcurrency(-1, 2, 4), equalTo(4));
        assertThat(maxConcurrency(-1, 10, 10), equalTo(10));
    }

    public void testNeverDropsBelowTheShardReadPoolSize() {
        int shardReadMaxThreads = randomIntBetween(1, 28);
        int prewarmMaxThreads = randomIntBetween(1, 32);
        assertThat(maxConcurrency(-1, prewarmMaxThreads, shardReadMaxThreads), greaterThanOrEqualTo(shardReadMaxThreads));
    }

    public void testOverrideWins() {
        assertThat(maxConcurrency(1, 32, 10), equalTo(1));
        assertThat(maxConcurrency(64, 32, 10), equalTo(64));
    }

    public void testNonPositiveOverrideMeansDerive() {
        // -1 is the documented "unset" value
        assertThat(maxConcurrency(-1, 32, 10), equalTo(32));
        // 0 is an invalid value (only checked through assertions), and thus we should clamp to the
        // max(prewarmMaxThreads, shardReadMaxThreads)
        assertThat(maxConcurrency(0, 32, 10), equalTo(32));
    }

    public void testReadsTheOverrideFromNodeSettings() {
        final var nodeSettings = Settings.builder()
            .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of(DiscoveryNodeRole.INDEX_ROLE.roleName()))
            .build();
        final var executorBuilders = new StatelessPlugin(nodeSettings).getExecutorBuilders(nodeSettings).toArray(new ExecutorBuilder<?>[0]);

        final var threadPool = new TestThreadPool(getTestName(), nodeSettings, executorBuilders);
        try {
            assertThat(MAX_CONCURRENCY_SETTING.get(nodeSettings), equalTo(-1));
            assertThat(
                maxConcurrency(nodeSettings, threadPool),
                equalTo(Math.max(threadPool.info(PREWARM_THREAD_POOL).getMax(), threadPool.info(SHARD_READ_THREAD_POOL).getMax()))
            );

            final int override = randomIntBetween(1, 64);
            final var overridden = Settings.builder().put(nodeSettings).put(MAX_CONCURRENCY_SETTING.getKey(), override).build();
            assertThat(maxConcurrency(overridden, threadPool), equalTo(override));
        } finally {
            terminate(threadPool);
        }
    }

    public void testRecordsMetrics() throws Exception {
        final var nodeSettings = Settings.builder()
            .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of(DiscoveryNodeRole.INDEX_ROLE.roleName()))
            // set to 1 so second task gets queued up
            .put(MAX_CONCURRENCY_SETTING.getKey(), 1)
            .build();
        final var executorBuilders = new StatelessPlugin(nodeSettings).getExecutorBuilders(nodeSettings).toArray(new ExecutorBuilder<?>[0]);
        final var threadPool = new TestThreadPool(getTestName(), nodeSettings, executorBuilders);
        try {
            final var meterRegistry = new RecordingMeterRegistry();
            final var recorder = meterRegistry.getRecorder();
            final var bccHeader = new BCCHeaderReadExecutor(nodeSettings, threadPool, meterRegistry);

            final var taskCanFinish = new CountDownLatch(1);
            final var allTasksDone = new CountDownLatch(2);

            bccHeader.execute(() -> {
                safeAwait(taskCanFinish);
                allTasksDone.countDown();
            });
            bccHeader.execute(allTasksDone::countDown);

            recorder.collect();

            final var prefix = "es.throttled_task_runner.bcc_header_read.tasks.";

            // a task is queued due to MAX_CONCURRENCY_SETTING
            assertThat(
                recorder.getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, prefix + "queue.size"),
                measures(1L)
            );

            // a task is still running due to `taskCanFinish`
            assertThat(
                recorder.getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, prefix + "running.current"),
                measures(1L)
            );

            taskCanFinish.countDown();
            safeAwait(allTasksDone);

            // had 2 enqueued tasks and hence 2 polled tasks and hence queue latency was measured twice
            assertThat(recorder.getMeasurements(InstrumentType.LONG_HISTOGRAM, prefix + "queue.latency.histogram"), hasSize(2));

            // no task is running now
            assertBusy(() -> {
                recorder.resetCalls();
                recorder.collect();
                assertThat(
                    recorder.getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, prefix + "running.current"),
                    measures(0L)
                );
            });
        } finally {
            terminate(threadPool);
        }
    }
}
