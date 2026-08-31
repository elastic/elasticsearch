/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.ClusterInfoServiceUtils.refresh;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class InternalClusterInfoServiceRefreshTests extends ESTestCase {

    public void testEstimatedHeapUsageCollectorSuccessAndFailure() {
        final Settings settings = baseSettingsBuilder().put(
            InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(),
            true
        ).build();

        try (RefreshTestContext context = RefreshTestContext.create(settings)) {
            final Map<String, NodeHeapEstimates> nodeHeapEstimates = Map.of("node-id", new NodeHeapEstimates(100L, 20L));
            final ShardId shardId = new ShardId("index", "uuid", 0);
            final ShardHeapUsageEstimates shardHeapUsageEstimates = new ShardHeapUsageEstimates(
                Map.of(shardId, new ShardAndIndexHeapUsage(10L, 5L)),
                new ShardAndIndexHeapUsage(1L, 2L)
            );
            final EstimatedHeapUsageStats estimatedHeapUsageStats = new EstimatedHeapUsageStats(nodeHeapEstimates, shardHeapUsageEstimates);
            final AtomicBoolean failEstimatedHeapUsage = new AtomicBoolean();
            final EstimatedHeapUsageCollector estimatedHeapUsageCollector = mock(EstimatedHeapUsageCollector.class);
            doAnswer(invocation -> {
                final ActionListener<EstimatedHeapUsageStats> listener = invocation.getArgument(0);
                if (failEstimatedHeapUsage.get()) {
                    listener.onFailure(new IllegalStateException("simulated estimated heap usage failure"));
                } else {
                    listener.onResponse(estimatedHeapUsageStats);
                }
                return null;
            }).when(estimatedHeapUsageCollector).collectEstimatedHeapUsage(any());

            final InternalClusterInfoService clusterInfoService = context.createClusterInfoService(
                nodeStatsClient(context.threadPool()),
                estimatedHeapUsageCollector,
                CacheSizesAndCommitmentCollector.EMPTY,
                PartitionSizeCollector.EMPTY,
                SearchLaneRequirementsCollector.EMPTY
            );

            ClusterInfo clusterInfo = refresh(clusterInfoService);
            verify(estimatedHeapUsageCollector).collectEstimatedHeapUsage(any());
            assertThat(clusterInfo.getNodeHeapMetrics().get("node-id").nodeHeapEstimates(), equalTo(nodeHeapEstimates.get("node-id")));
            assertThat(clusterInfo.getEstimatedShardHeapUsages(), equalTo(shardHeapUsageEstimates.perShard()));
            assertThat(
                clusterInfo.getDefaultShardHeapUsageForShardsWithoutMetrics(),
                equalTo(shardHeapUsageEstimates.defaultForShardsWithoutMetrics())
            );

            Mockito.clearInvocations(estimatedHeapUsageCollector);
            failEstimatedHeapUsage.set(true);
            clusterInfo = refresh(clusterInfoService);
            verify(estimatedHeapUsageCollector).collectEstimatedHeapUsage(any());
            assertThat(clusterInfo.getNodeHeapMetrics(), equalTo(Map.of()));
            assertThat(clusterInfo.getEstimatedShardHeapUsages(), equalTo(Map.of()));
            assertThat(clusterInfo.getDefaultShardHeapUsageForShardsWithoutMetrics(), equalTo(ShardAndIndexHeapUsage.ZERO));
        }
    }

    public void testPartitionSizeCollectorSuccessAndFailure() {
        try (RefreshTestContext context = RefreshTestContext.create(baseSettings())) {
            final Map<String, Long> partitionSizes = Map.of("node-id", 1234L);
            final AtomicBoolean failPartitionSizes = new AtomicBoolean();
            final PartitionSizeCollector partitionSizeCollector = mock(PartitionSizeCollector.class);
            doAnswer(invocation -> {
                final ActionListener<Map<String, Long>> listener = invocation.getArgument(1);
                if (failPartitionSizes.get()) {
                    listener.onFailure(new IllegalStateException("simulated partition size failure"));
                } else {
                    listener.onResponse(partitionSizes);
                }
                return null;
            }).when(partitionSizeCollector).collectHostedShardsPartitionSizes(any(), any());

            final InternalClusterInfoService clusterInfoService = context.createClusterInfoService(
                EstimatedHeapUsageCollector.EMPTY,
                CacheSizesAndCommitmentCollector.EMPTY,
                partitionSizeCollector,
                SearchLaneRequirementsCollector.EMPTY
            );

            // Success populates the ClusterInfo
            ClusterInfo clusterInfo = refresh(clusterInfoService);
            verify(partitionSizeCollector).collectHostedShardsPartitionSizes(any(), any());
            assertThat(clusterInfo.getHostedShardsPartitionSizeByNodeId(), equalTo(partitionSizes));

            // Failure returns an empty map
            Mockito.clearInvocations(partitionSizeCollector);
            failPartitionSizes.set(true);
            clusterInfo = refresh(clusterInfoService);
            verify(partitionSizeCollector).collectHostedShardsPartitionSizes(any(), any());
            assertThat(clusterInfo.getHostedShardsPartitionSizeByNodeId(), equalTo(Map.of()));
        }
    }

    public void testCacheSizesAndCommitmentCollectorSuccessAndFailure() {
        try (RefreshTestContext context = RefreshTestContext.create(baseSettings())) {
            final Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements = Map.of(
                new ShardId("index", "uuid", 0),
                new BoostedAndUnboostedCacheRequirements(10L, 20L)
            );
            final Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments = Map.of(
                "node-id",
                new NodeCacheSizeAndCommitments(100L, 10L, 30L)
            );
            final CacheSizesAndCommitmentStats cacheSizesAndCommitmentStats = new CacheSizesAndCommitmentStats(
                shardCacheRequirements,
                nodeCacheSizeAndCommitments
            );
            final AtomicBoolean failCacheSizesAndCommitmentStats = new AtomicBoolean();
            final CacheSizesAndCommitmentCollector cacheSizesAndCommitmentCollector = mock(CacheSizesAndCommitmentCollector.class);
            doAnswer(invocation -> {
                final ActionListener<CacheSizesAndCommitmentStats> listener = invocation.getArgument(1);
                if (failCacheSizesAndCommitmentStats.get()) {
                    listener.onFailure(new IllegalStateException("simulated cache sizes and commitment stats failure"));
                } else {
                    listener.onResponse(cacheSizesAndCommitmentStats);
                }
                return null;
            }).when(cacheSizesAndCommitmentCollector).collectCacheSizesAndCommitmentStats(any(), any());

            final InternalClusterInfoService clusterInfoService = context.createClusterInfoService(
                EstimatedHeapUsageCollector.EMPTY,
                cacheSizesAndCommitmentCollector,
                PartitionSizeCollector.EMPTY,
                SearchLaneRequirementsCollector.EMPTY
            );

            ClusterInfo clusterInfo = refresh(clusterInfoService);
            verify(cacheSizesAndCommitmentCollector).collectCacheSizesAndCommitmentStats(any(), any());
            assertThat(clusterInfo.getShardCacheRequirements(), equalTo(shardCacheRequirements));
            assertThat(clusterInfo.getNodeCacheSizeAndCommitments(), equalTo(nodeCacheSizeAndCommitments));

            Mockito.clearInvocations(cacheSizesAndCommitmentCollector);
            failCacheSizesAndCommitmentStats.set(true);
            clusterInfo = refresh(clusterInfoService);
            verify(cacheSizesAndCommitmentCollector).collectCacheSizesAndCommitmentStats(any(), any());
            assertThat(clusterInfo.getShardCacheRequirements(), equalTo(Map.of()));
            assertThat(clusterInfo.getNodeCacheSizeAndCommitments(), equalTo(Map.of()));
        }
    }

    public void testSearchLaneRequirementsCollectorSuccessAndFailure() {
        try (RefreshTestContext context = RefreshTestContext.create(baseSettings())) {
            final Map<ShardId, Double> laneRequirements = Map.of(new ShardId("index", "uuid", 0), 3.2);
            final AtomicBoolean failLaneRequirements = new AtomicBoolean();
            final SearchLaneRequirementsCollector searchLaneRequirementsCollector = mock(SearchLaneRequirementsCollector.class);
            doAnswer(invocation -> {
                final ActionListener<Map<ShardId, Double>> listener = invocation.getArgument(1);
                if (failLaneRequirements.get()) {
                    listener.onFailure(new IllegalStateException("simulated search lane requirements failure"));
                } else {
                    listener.onResponse(laneRequirements);
                }
                return null;
            }).when(searchLaneRequirementsCollector).collectSearchLaneRequirements(any(), any());

            final InternalClusterInfoService clusterInfoService = context.createClusterInfoService(
                EstimatedHeapUsageCollector.EMPTY,
                CacheSizesAndCommitmentCollector.EMPTY,
                PartitionSizeCollector.EMPTY,
                searchLaneRequirementsCollector
            );

            // Success populates the ClusterInfo
            ClusterInfo clusterInfo = refresh(clusterInfoService);
            verify(searchLaneRequirementsCollector).collectSearchLaneRequirements(any(), any());
            assertThat(clusterInfo.getShardSearchLaneRequirements(), equalTo(laneRequirements));

            // Failure returns an empty map
            Mockito.clearInvocations(searchLaneRequirementsCollector);
            failLaneRequirements.set(true);
            clusterInfo = refresh(clusterInfoService);
            verify(searchLaneRequirementsCollector).collectSearchLaneRequirements(any(), any());
            assertThat(clusterInfo.getShardSearchLaneRequirements(), equalTo(Map.of()));
        }
    }

    private static Settings baseSettings() {
        return baseSettingsBuilder().build();
    }

    private static Settings.Builder baseSettingsBuilder() {
        return Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED
            );
    }

    private static NoOpClient nodeStatsClient(ThreadPool threadPool) {
        return new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof NodesStatsRequest) {
                    NodeStats nodeStats = mock(NodeStats.class);
                    JvmStats jvmStats = mock(JvmStats.class);
                    JvmStats.Mem mem = mock(JvmStats.Mem.class);
                    Mockito.when(nodeStats.getNode()).thenReturn(DiscoveryNodeUtils.create("node-id"));
                    Mockito.when(nodeStats.getJvm()).thenReturn(jvmStats);
                    Mockito.when(jvmStats.getMem()).thenReturn(mem);
                    Mockito.when(mem.getHeapMax()).thenReturn(ByteSizeValue.ofBytes(1_000L));
                    listener.onResponse((Response) new NodesStatsResponse(new ClusterName("cluster"), List.of(nodeStats), List.of()));
                } else {
                    fail("unexpected action: " + action.name());
                }
            }
        };
    }

    private record RefreshTestContext(Settings settings, ThreadPool threadPool, ClusterService clusterService) implements AutoCloseable {

        static RefreshTestContext create(Settings settings) {
            final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
            final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
            return new RefreshTestContext(settings, threadPool, ClusterServiceUtils.createClusterService(threadPool, clusterSettings));
        }

        InternalClusterInfoService createClusterInfoService(
            EstimatedHeapUsageCollector estimatedHeapUsageCollector,
            CacheSizesAndCommitmentCollector cacheSizesAndCommitmentCollector,
            PartitionSizeCollector partitionSizeCollector,
            SearchLaneRequirementsCollector searchLaneRequirementsCollector
        ) {
            return createClusterInfoService(
                new NoOpClient(threadPool),
                estimatedHeapUsageCollector,
                cacheSizesAndCommitmentCollector,
                partitionSizeCollector,
                searchLaneRequirementsCollector
            );
        }

        InternalClusterInfoService createClusterInfoService(
            NoOpClient client,
            EstimatedHeapUsageCollector estimatedHeapUsageCollector,
            CacheSizesAndCommitmentCollector cacheSizesAndCommitmentCollector,
            PartitionSizeCollector partitionSizeCollector,
            SearchLaneRequirementsCollector searchLaneRequirementsCollector
        ) {
            final InternalClusterInfoService clusterInfoService = new InternalClusterInfoService(
                settings,
                new WriteLoadConstraintSettings(clusterService.getClusterSettings()),
                clusterService,
                threadPool,
                client,
                estimatedHeapUsageCollector,
                cacheSizesAndCommitmentCollector,
                partitionSizeCollector,
                NodeUsageStatsForThreadPoolsCollector.EMPTY,
                searchLaneRequirementsCollector
            );
            // Refresh is a no-op if there are no listeners, and AsyncRefresh asserts that at least one listener is notified.
            clusterInfoService.addListener(ignored -> {});
            return clusterInfoService;
        }

        @Override
        public void close() {
            clusterService.close();
        }
    }
}
