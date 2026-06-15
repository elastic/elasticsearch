/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.EstimatedHeapUsageCollector;
import org.elasticsearch.cluster.ShardHeapUsageEstimates;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.util.Map;

public class StatelessHeapUsageCollector implements EstimatedHeapUsageCollector {

    private final StatelessPlugin plugin;

    public StatelessHeapUsageCollector() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessHeapUsageCollector(StatelessPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public void collectClusterHeapUsage(ActionListener<Map<String, Long>> listener) {
        StatelessMemoryMetricsService memoryMetricsService = plugin.getStatelessMemoryMetricsService();
        ClusterService clusterService = plugin.getClusterService();
        ActionListener.completeWith(listener, () -> memoryMetricsService.getPerNodeMemoryMetrics(clusterService.state()));
    }

    @Override
    public void collectShardHeapUsage(ActionListener<ShardHeapUsageEstimates> listener) {
        ActionListener.completeWith(listener, () -> plugin.getStatelessMemoryMetricsService().getShardHeapUsageEstimates());
    }
}
