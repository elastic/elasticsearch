/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class IndicesAllocationMetricsTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private MeterRegistry meterRegistry;

    @Before
    public void setup() throws Exception {
        threadPool = new TestThreadPool("indices-allocation-metrics");
        clusterService = new ClusterService(Settings.EMPTY, ClusterSettings.createBuiltInClusterSettings(), threadPool, null);
        meterRegistry = MeterRegistry.NOOP;
    }

    /**
     * This test ensures that persistent task initiates scheduled compute and publish runnable task
     * and stops when persistent task is canceled.
     */
    public void testScheduleAndCancelComputeOnPersistentTask() {
        final var computeRunLatch = new CountDownLatch(between(1, 10));
        final var cancelLatch = new CountDownLatch(1);
        final var scheduledTaskRef = new AtomicReference<IndicesAllocationMetrics.ComputeAndPublishScheduledTask>();

        final var noopMetricsCompute = new IndicesAllocationMetrics(
            threadPool,
            clusterService,
            meterRegistry,
            TimeValue.timeValueMillis(10)
        ) {
            @Override
            void scheduleComputeAndPublish(
                AllocatedPersistentTask persistentTask,
                ThreadPool threadPool,
                TimeValue computeInterval,
                ClusterService clusterService
            ) {
                final var scheduledTask = new IndicesAllocationMetrics.ComputeAndPublishScheduledTask(
                    persistentTask,
                    threadPool,
                    computeInterval,
                    clusterService
                ) {
                    @Override
                    Runnable computeAndPublishOnce() {
                        final var computeRunnable = super.computeAndPublishOnce();
                        return () -> {
                            computeRunnable.run();
                            computeRunLatch.countDown();
                            if (persistentTask.isCancelled()) {
                                cancelLatch.countDown();
                            }
                        };
                    }
                };
                scheduledTaskRef.set(scheduledTask);
            }
        };

        final var persistedTask = new AllocatedPersistentTask(randomLong(), "", "", "", null, null);
        noopMetricsCompute.nodeOperation(persistedTask, null, null);
        safeAwait(computeRunLatch);
        TaskCancelHelper.cancel(persistedTask, "kaput");
        safeAwait(cancelLatch);
        assertTrue(scheduledTaskRef.get().isCancelled());
    }

    @After
    public void cleanup() throws Exception {
        threadPool.shutdownNow();
    }
}
