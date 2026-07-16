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
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.ClusterInfoServiceUtils.refresh;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class InternalClusterInfoServiceRefreshTests extends ESTestCase {

    public void testCacheSizesAndCommitmentCollectorSuccessAndFailure() {
        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED
            )
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings)) {
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

            final InternalClusterInfoService clusterInfoService = new InternalClusterInfoService(
                settings,
                new WriteLoadConstraintSettings(clusterService.getClusterSettings()),
                clusterService,
                threadPool,
                new NoOpClient(threadPool),
                EstimatedHeapUsageCollector.EMPTY,
                cacheSizesAndCommitmentCollector,
                NodeUsageStatsForThreadPoolsCollector.EMPTY
            );
            // AsyncRefresh asserts that each refresh notifies at least one registered cluster info listener.
            clusterInfoService.addListener(ignored -> {});

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
}
