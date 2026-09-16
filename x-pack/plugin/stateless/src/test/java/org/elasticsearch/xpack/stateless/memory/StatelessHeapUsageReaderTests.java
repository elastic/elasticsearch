/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EstimatedHeapUsageStats;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatelessHeapUsageReaderTests extends ESTestCase {

    public void testCollectEstimatedHeapUsageReadsRealServiceUsingPluginClusterState() {
        ClusterState clusterState = ClusterStateCreationUtils.state(randomIdentifier(), 2, 1);
        StatelessMemoryMetricsService memoryMetricsService = new StatelessMemoryMetricsService(
            () -> 1L,
            new ClusterSettings(Settings.EMPTY, allSettings())
        );
        memoryMetricsService.clusterChanged(new ClusterChangedEvent("init", clusterState, ClusterState.EMPTY_STATE));
        EstimatedHeapUsageStats expectedStats = memoryMetricsService.getEstimatedHeapUsageStats(clusterState);
        StatelessHeapUsageReader reader = new StatelessHeapUsageReader(createPlugin(memoryMetricsService, clusterState));

        assertThat(invokeCollect(reader), equalTo(expectedStats));
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

    private static Set<Setting<?>> allSettings() {
        return Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(
                StatelessMemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING,
                StatelessMemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING,
                StatelessMemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING,
                StatelessMemoryMetricsService.MERGE_MEMORY_ESTIMATE_ENABLED_SETTING,
                StatelessMemoryMetricsService.ADAPTIVE_EXTRA_OVERHEAD_SETTING,
                StatelessMemoryMetricsService.SELF_REPORTED_SHARD_MEMORY_OVERHEAD_ENABLED_SETTING,
                StatelessMemoryMetricsService.ADAPTIVE_SHARD_MEMORY_ESTIMATION_MIN_THRESHOLD_ENABLED_SETTING,
                SETTING_CLUSTER_MAX_SHARDS_PER_NODE
            )
        ).collect(Collectors.toSet());
    }
}
