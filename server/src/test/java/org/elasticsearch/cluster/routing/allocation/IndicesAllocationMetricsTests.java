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
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;

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

    public void testScheduleOnPersistentTask() {
        final var taskRuns = new CountDownLatch(between(1, 10));
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
                new IndicesAllocationMetrics.ComputeAndPublishScheduledTask(persistentTask, threadPool, computeInterval, clusterService) {
                    @Override
                    Runnable computeAndPublishOnce() {
                        return taskRuns::countDown;
                    }
                };
            }
        };

        final var persistedTask = new AllocatedPersistentTask(randomLong(), "", "", "", null, null);
        noopMetricsCompute.nodeOperation(persistedTask, null, null);
        safeAwait(taskRuns);
    }

    @After
    public void cleanup() throws Exception {
        threadPool.shutdownNow();
    }
}
