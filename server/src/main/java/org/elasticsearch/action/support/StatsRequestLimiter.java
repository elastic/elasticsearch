/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AdjustableSemaphore;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.tasks.Task;

/**
 * This class guards the amount of stats requests a node can concurrently coordinate.
 */
public class StatsRequestLimiter {

    public static final Setting<Integer> MAX_CONCURRENT_STATS_REQUESTS_PER_NODE = Setting.intSetting(
        "node.stats.max_concurrent_requests",
        100,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final AdjustableSemaphore maxConcurrentStatsRequestsPerNodeSemaphore;

    public StatsRequestLimiter(Settings settings, ClusterSettings clusterSettings) {
        this.maxConcurrentStatsRequestsPerNodeSemaphore = new AdjustableSemaphore(
            MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.get(settings),
            false
        );
        clusterSettings.addSettingsUpdateConsumer(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE, this::setMaxConcurrentStatsRequestsPerNode);
    }

    private void setMaxConcurrentStatsRequestsPerNode(int maxConcurrentBoundedDiagnosticRequestsPerNode) {
        this.maxConcurrentStatsRequestsPerNodeSemaphore.setMaxPermits(maxConcurrentBoundedDiagnosticRequestsPerNode);
    }

    public <Request, Response> void maybeDoExecute(
        Task task,
        Request request,
        ActionListener<Response> listener,
        TriConsumer<Task, Request, ActionListener<Response>> execute
    ) {
        if (maxConcurrentStatsRequestsPerNodeSemaphore.tryAcquire()) {
            final Runnable release = new RunOnce(maxConcurrentStatsRequestsPerNodeSemaphore::release);
            boolean success = false;
            try {
                execute.apply(task, request, ActionListener.runBefore(listener, release::run));
                success = true;
            } finally {
                if (success == false) {
                    release.run();
                }
            }
        } else {
            listener.onFailure(new EsRejectedExecutionException("too many bounded diagnostic requests"));
        }
    }

    // visible for testing
    boolean tryAcquire() {
        return maxConcurrentStatsRequestsPerNodeSemaphore.tryAcquire();
    }

    // visible for testting
    void release() {
        maxConcurrentStatsRequestsPerNodeSemaphore.release();
    }
}
