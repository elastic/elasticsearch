/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EstimatedHeapUsageStats;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.junit.After;
import org.junit.Before;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatelessHeapUsageReaderTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void startClusterService() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool, new ClusterSettings(Settings.EMPTY, allSettings()));
    }

    @After
    public void stopClusterService() {
        clusterService.close();
        terminate(threadPool);
    }

    public void testCollectEstimatedHeapUsageReadsRealServiceUsingPluginClusterState() {
        StatelessMemoryMetricsService memoryMetricsService = new StatelessMemoryMetricsService(() -> 1L, clusterService);
        clusterService.addListener(memoryMetricsService);
        ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.state(randomIdentifier(), 2, 1));

        EstimatedHeapUsageStats expectedStats = memoryMetricsService.getEstimatedHeapUsageStats(clusterService.state());
        StatelessHeapUsageReader reader = new StatelessHeapUsageReader(createPlugin(memoryMetricsService));

        assertThat(invokeCollect(reader), equalTo(expectedStats));
    }

    public void testCollectEstimatedHeapUsagePropagatesFailures() {
        IllegalStateException failure = new IllegalStateException("simulated estimated heap usage failure");
        StatelessMemoryMetricsService memoryMetricsService = new StatelessMemoryMetricsService(() -> 1L, clusterService) {
            @Override
            public EstimatedHeapUsageStats getEstimatedHeapUsageStats(ClusterState clusterState) {
                throw failure;
            }
        };
        StatelessHeapUsageReader reader = new StatelessHeapUsageReader(createPlugin(memoryMetricsService));

        PlainActionFuture<EstimatedHeapUsageStats> future = new PlainActionFuture<>();
        reader.collectEstimatedHeapUsage(future);
        assertThat(expectThrows(Exception.class, future::get).getCause(), sameInstance(failure));
    }

    private static EstimatedHeapUsageStats invokeCollect(StatelessHeapUsageReader reader) {
        PlainActionFuture<EstimatedHeapUsageStats> future = new PlainActionFuture<>();
        reader.collectEstimatedHeapUsage(future);
        return safeGet(future);
    }

    private StatelessPlugin createPlugin(StatelessMemoryMetricsService memoryMetricsService) {
        StatelessPlugin plugin = mock(StatelessPlugin.class);
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
