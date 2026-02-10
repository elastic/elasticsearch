/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * IndicesAllocationMetrics computes and publish index allocation metrics, in particular how balanced shards distributed across nodes in
 * the cluster for indices. There are potentially thousands of indices and shards, hence balance computation can be slow. For this
 * reason the computation runs in a management thread-pool. These metrics are cluster-wide and only one node publishes those, currently
 * it's a master node. When a node become a master it starts scheduled task to compute and publish metrics, and stops when become
 * non-master.
 */
public class IndicesAllocationMetrics implements ClusterStateListener {

    private final ClusterService clusterService;
    private final MeterRegistry meterRegistry;
    private final ThreadPool computeThreadPool;
    private final TimeValue computeInterval;

    private volatile Scheduler.Cancellable scheduledUpdate;
    private volatile boolean needRefresh;

    public IndicesAllocationMetrics(
        ClusterService clusterService,
        MeterRegistry meterRegistry,
        ThreadPool computeThreadPool,
        TimeValue computeInterval
    ) {
        ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);
        this.meterRegistry = meterRegistry;
        this.clusterService = clusterService;
        this.computeThreadPool = computeThreadPool;
        this.computeInterval = computeInterval;
    }

    // overwritable in tests
    Runnable computeAndPublishTask() {
        // TODO compute and publish metrics
        return () -> needRefresh = false;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            if (scheduledUpdate == null) {
                scheduledUpdate = computeThreadPool.scheduleWithFixedDelay(
                    computeAndPublishTask(),
                    computeInterval,
                    EsExecutors.DIRECT_EXECUTOR_SERVICE
                );
            }
            if (event.routingTableChanged()) {
                needRefresh = true;
            }
        } else {
            if (scheduledUpdate != null) {
                scheduledUpdate.cancel();
                scheduledUpdate = null;
                needRefresh = false;
            }
        }
    }

    boolean isRunning() {
        return scheduledUpdate != null;
    }
}
