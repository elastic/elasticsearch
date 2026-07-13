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

    public void testCacheUsageAndCommitmentCollectorFailureFallbacks() {
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
            final Map<ShardId, BoostedAndUnboostedCacheSizes> shardCacheSizes = Map.of(
                new ShardId("index", "uuid", 0),
                new BoostedAndUnboostedCacheSizes(10L, 20L)
            );
            final Map<String, CurrentCacheUsage> nodeCacheUsage = Map.of("node-id", new CurrentCacheUsage(100L, 30L));
            final AtomicBoolean failShardCacheSizes = new AtomicBoolean();
            final AtomicBoolean failNodeCacheUsage = new AtomicBoolean();
            final CacheUsageAndCommitmentCollector cacheUsageAndCommitmentCollector = mock(CacheUsageAndCommitmentCollector.class);
            doAnswer(invocation -> {
                final ActionListener<Map<ShardId, BoostedAndUnboostedCacheSizes>> listener = invocation.getArgument(1);
                if (failShardCacheSizes.get()) {
                    listener.onFailure(new IllegalStateException("simulated shard cache sizes failure"));
                } else {
                    listener.onResponse(shardCacheSizes);
                }
                return null;
            }).when(cacheUsageAndCommitmentCollector).collectShardCacheSizes(any(), any());
            doAnswer(invocation -> {
                final ActionListener<Map<String, CurrentCacheUsage>> listener = invocation.getArgument(1);
                if (failNodeCacheUsage.get()) {
                    listener.onFailure(new IllegalStateException("simulated node cache usage failure"));
                } else {
                    listener.onResponse(nodeCacheUsage);
                }
                return null;
            }).when(cacheUsageAndCommitmentCollector).collectNodeCacheUsage(any(), any());

            final InternalClusterInfoService clusterInfoService = new InternalClusterInfoService(
                settings,
                new WriteLoadConstraintSettings(clusterService.getClusterSettings()),
                clusterService,
                threadPool,
                new NoOpClient(threadPool),
                EstimatedHeapUsageCollector.EMPTY,
                cacheUsageAndCommitmentCollector,
                NodeUsageStatsForThreadPoolsCollector.EMPTY
            );
            clusterInfoService.addListener(ignored -> {});

            failShardCacheSizes.set(true);
            ClusterInfo clusterInfo = refresh(clusterInfoService);
            verify(cacheUsageAndCommitmentCollector).collectShardCacheSizes(any(), any());
            verify(cacheUsageAndCommitmentCollector).collectNodeCacheUsage(any(), any());
            assertThat(clusterInfo.getShardCacheSizes(), equalTo(Map.of()));
            assertThat(clusterInfo.getNodeCacheUsage(), equalTo(nodeCacheUsage));

            Mockito.clearInvocations(cacheUsageAndCommitmentCollector);
            failShardCacheSizes.set(false);
            failNodeCacheUsage.set(true);
            clusterInfo = refresh(clusterInfoService);
            verify(cacheUsageAndCommitmentCollector).collectShardCacheSizes(any(), any());
            verify(cacheUsageAndCommitmentCollector).collectNodeCacheUsage(any(), any());
            assertThat(clusterInfo.getShardCacheSizes(), equalTo(shardCacheSizes));
            assertThat(clusterInfo.getNodeCacheUsage(), equalTo(Map.of()));
        }
    }
}
