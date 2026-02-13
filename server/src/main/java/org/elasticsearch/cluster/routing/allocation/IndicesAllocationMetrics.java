/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * IndicesAllocationMetrics computes and publish index allocation metrics, in particular how balanced shards distributed across nodes in
 * the cluster for indices. There are potentially thousands of indices and shards, hence balance computation can be slow. For this
 * reason the computation runs in a management thread-pool. These metrics are cluster-wide and only one node publishes those using
 * PersistentTasks machinery.<br>
 * This service has 3 parts:<br>
 * 1. A persistent task that runs on a single node in a cluster<br>
 * 2. A scheduled runnable to compute and publish metrics when node receives persistent task<br>
 * 3. TODO: Compute allocation balance metrics and update metering registry when routing table changes
 */
public class IndicesAllocationMetrics extends PersistentTasksExecutor<IndicesAllocationMetrics.IndicesAllocationMetricsTaskParams> {

    private static final String PERSISTENT_TASK_NAME = "indices-allocation-metrics";
    private static final TransportVersion INDICES_ALLOCATION_METRICS_TASK_VERSION = TransportVersion.fromName(
        "indices_allocation_metrics_task"
    );

    private final ClusterService clusterService;
    private final MeterRegistry meterRegistry;
    private final ThreadPool threadPool;
    private final TimeValue computeInterval;

    protected IndicesAllocationMetrics(
        ThreadPool threadPool,
        ClusterService clusterService,
        MeterRegistry meterRegistry,
        TimeValue computeInterval
    ) {
        super(PERSISTENT_TASK_NAME, threadPool.executor(ThreadPool.Names.MANAGEMENT));
        this.clusterService = clusterService;
        this.meterRegistry = meterRegistry;
        this.threadPool = threadPool;
        this.computeInterval = computeInterval;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, IndicesAllocationMetricsTaskParams params, PersistentTaskState state) {
        scheduleComputeAndPublish(task, threadPool, computeInterval, clusterService);
    }

    // visible for testing
    void scheduleComputeAndPublish(
        AllocatedPersistentTask persistentTask,
        ThreadPool threadPool,
        TimeValue computeInterval,
        ClusterService clusterService
    ) {
        new ComputeAndPublishScheduledTask(persistentTask, threadPool, computeInterval, clusterService);
    }

    public record IndicesAllocationMetricsTaskParams() implements PersistentTaskParams {
        @Override
        public String getWriteableName() {
            return PERSISTENT_TASK_NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return INDICES_ALLOCATION_METRICS_TASK_VERSION;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    }

    static class ComputeAndPublishScheduledTask {
        private final AllocatedPersistentTask persistentTask;
        private final Scheduler.Cancellable scheduledTask;
        private final ClusterService clusterService;
        private volatile boolean refresh;
        private final ClusterStateListener routingTableChangeListener = e -> refresh = e.routingTableChanged();

        ComputeAndPublishScheduledTask(
            AllocatedPersistentTask persistentTask,
            ThreadPool threadPool,
            TimeValue computeInterval,
            ClusterService clusterService
        ) {
            ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);
            this.persistentTask = persistentTask;
            this.clusterService = clusterService;
            clusterService.addListener(routingTableChangeListener);
            refresh = true;
            this.scheduledTask = threadPool.scheduleWithFixedDelay(
                computeAndPublishOnce(),
                computeInterval,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
        }

        Runnable computeAndPublishOnce() {
            return () -> {
                if (persistentTask.isCancelled()) {
                    clusterService.removeListener(routingTableChangeListener);
                    scheduledTask.cancel();
                }
                // TODO compute and publish metrics
                refresh = false;
            };
        }

        boolean isCancelled() {
            return scheduledTask.isCancelled();
        }
    }

}
