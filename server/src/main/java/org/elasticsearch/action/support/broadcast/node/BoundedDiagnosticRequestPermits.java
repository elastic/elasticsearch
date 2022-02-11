/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast.node;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AdjustableSemaphore;

/**
 * This class guards the amount of bounded diagnostic requests a node can concurrently coordinate.
 */
public class BoundedDiagnosticRequestPermits {

    public static final Setting<Integer> MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE = Setting.intSetting(
        "cluster.max_concurrent_bounded_diagnostic_requests_per_node",
        50,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final AdjustableSemaphore maxConcurrentBoundedDiagnosticRequestsPerNodeSemaphore;
    private volatile Integer maxConcurrentBoundedDiagnosticRequestsPerNode;

    public BoundedDiagnosticRequestPermits(Settings settings, ClusterSettings clusterSettings) {
        this.maxConcurrentBoundedDiagnosticRequestsPerNode = MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE.get(settings);
        this.maxConcurrentBoundedDiagnosticRequestsPerNodeSemaphore = new AdjustableSemaphore(
            this.maxConcurrentBoundedDiagnosticRequestsPerNode,
            false
        );
        clusterSettings.addSettingsUpdateConsumer(
            MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE,
            this::setMaxConcurrentBoundedDiagnosticRequestsPerNode
        );
    }

    private void setMaxConcurrentBoundedDiagnosticRequestsPerNode(int maxConcurrentBoundedDiagnosticRequestsPerNode) {
        this.maxConcurrentBoundedDiagnosticRequestsPerNode = maxConcurrentBoundedDiagnosticRequestsPerNode;
        this.maxConcurrentBoundedDiagnosticRequestsPerNodeSemaphore.setMaxPermits(maxConcurrentBoundedDiagnosticRequestsPerNode);
    }

    public boolean tryAcquire() {
        return maxConcurrentBoundedDiagnosticRequestsPerNodeSemaphore.tryAcquire();
    }

    public void release() {
        maxConcurrentBoundedDiagnosticRequestsPerNodeSemaphore.release();
    }
}
