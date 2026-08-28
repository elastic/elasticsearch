/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EstimatedHeapUsageStats;
import org.elasticsearch.cluster.NodeHeapEstimates;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.ShardHeapUsageEstimates;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.util.Map;

import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatelessHeapUsageReaderTests extends ESTestCase {

    public void testCollectEstimatedHeapUsageDelegatesToServiceWithPluginClusterState() {
        ClusterState clusterState = mock(ClusterState.class);
        EstimatedHeapUsageStats estimatedHeapUsageStats = new EstimatedHeapUsageStats(
            Map.of("node-id", new NodeHeapEstimates(100L, 20L)),
            new ShardHeapUsageEstimates(
                Map.of(new ShardId("index", "uuid", 0), new ShardAndIndexHeapUsage(10L, 5L)),
                new ShardAndIndexHeapUsage(1L, 2L)
            )
        );
        StatelessMemoryMetricsService memoryMetricsService = mock(StatelessMemoryMetricsService.class);
        when(memoryMetricsService.getEstimatedHeapUsageStats(clusterState)).thenReturn(estimatedHeapUsageStats);
        StatelessHeapUsageReader reader = new StatelessHeapUsageReader(createPlugin(memoryMetricsService, clusterState));

        assertThat(invokeCollect(reader), sameInstance(estimatedHeapUsageStats));
        verify(memoryMetricsService).getEstimatedHeapUsageStats(clusterState);
    }

    public void testCollectEstimatedHeapUsagePropagatesFailures() throws Exception {
        ClusterState clusterState = mock(ClusterState.class);
        IllegalStateException failure = new IllegalStateException("simulated estimated heap usage failure");
        StatelessMemoryMetricsService memoryMetricsService = mock(StatelessMemoryMetricsService.class);
        when(memoryMetricsService.getEstimatedHeapUsageStats(clusterState)).thenThrow(failure);
        StatelessHeapUsageReader reader = new StatelessHeapUsageReader(createPlugin(memoryMetricsService, clusterState));

        PlainActionFuture<EstimatedHeapUsageStats> future = new PlainActionFuture<>();
        reader.collectEstimatedHeapUsage(future);
        assertThat(expectThrows(Exception.class, future::get).getCause(), sameInstance(failure));
        verify(memoryMetricsService).getEstimatedHeapUsageStats(clusterState);
    }

    private static EstimatedHeapUsageStats invokeCollect(StatelessHeapUsageReader reader) {
        PlainActionFuture<EstimatedHeapUsageStats> future = new PlainActionFuture<>();
        reader.collectEstimatedHeapUsage(future);
        return safeGet(future);
    }

    private static StatelessPlugin createPlugin(StatelessMemoryMetricsService memoryMetricsService, ClusterState clusterState) {
        StatelessPlugin plugin = mock(StatelessPlugin.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(plugin.getClusterService()).thenReturn(clusterService);
        when(plugin.getStatelessMemoryMetricsService()).thenReturn(memoryMetricsService);
        return plugin;
    }
}
