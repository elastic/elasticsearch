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

/**
 * {@link EstimatedHeapUsageCollector} SPI implementation that reads heap usage estimates already computed by
 * {@link StatelessMemoryMetricsService} and hands them to {@link org.elasticsearch.cluster.InternalClusterInfoService}. It performs no
 * collection of its own: the actual metrics gathering happens in {@link ShardsMappingSizeCollector}, which runs on index nodes and
 * publishes {@link ShardMappingSize} to the master.
 * <p>
 * This class exists only because {@code server} cannot depend on the {@code stateless} plugin: {@link EstimatedHeapUsageCollector} is
 * the interface server-side code defines so it can ask "what's the estimated heap usage" without knowing stateless exists, and this is
 * the plugin-side implementation registered for it via SPI (see {@code module-info.java} and {@code META-INF/services}). Both of its
 * methods are one-line forwards to an already-computed value; there is no logic here to simplify away.
 */
public class StatelessHeapUsageReader implements EstimatedHeapUsageCollector {

    private final StatelessPlugin plugin;

    public StatelessHeapUsageReader() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessHeapUsageReader(StatelessPlugin plugin) {
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
