/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MockInternalClusterInfoService extends InternalClusterInfoService {

    /** This is a marker plugin used to trigger MockNode to use this mock info service. */
    public static class TestPlugin extends Plugin {}

    @Nullable // if no fakery should take place
    private volatile Function<ShardRouting, Long> shardSizeFunction;

    @Nullable // if no fakery should take place
    private volatile BiFunction<DiscoveryNode, FsInfo.Path, FsInfo.Path> diskUsageFunction;

    public MockInternalClusterInfoService(Settings settings, ClusterService clusterService, ThreadPool threadPool, NodeClient client) {
        super(settings, clusterService, threadPool, client, ShardHeapUsageCollector.EMPTY);
    }

    public void setDiskUsageFunctionAndRefresh(BiFunction<DiscoveryNode, FsInfo.Path, FsInfo.Path> diskUsageFn) {
        this.diskUsageFunction = diskUsageFn;
        ClusterInfoServiceUtils.refresh(this);
    }

    public void setShardSizeFunctionAndRefresh(Function<ShardRouting, Long> shardSizeFn) {
        this.shardSizeFunction = shardSizeFn;
        ClusterInfoServiceUtils.refresh(this);
    }

    @Override
    List<NodeStats> adjustNodesStats(List<NodeStats> nodesStats) {
        var diskUsageFunctionCopy = this.diskUsageFunction;
        if (diskUsageFunctionCopy == null) {
            return nodesStats;
        }

        return nodesStats.stream().map(nodeStats -> {
            final DiscoveryNode discoveryNode = nodeStats.getNode();
            final FsInfo oldFsInfo = nodeStats.getFs();
            return new NodeStats(
                discoveryNode,
                nodeStats.getTimestamp(),
                nodeStats.getIndices(),
                nodeStats.getOs(),
                nodeStats.getProcess(),
                nodeStats.getJvm(),
                nodeStats.getThreadPool(),
                new FsInfo(
                    oldFsInfo.getTimestamp(),
                    oldFsInfo.getIoStats(),
                    StreamSupport.stream(oldFsInfo.spliterator(), false)
                        .map(fsInfoPath -> diskUsageFunctionCopy.apply(discoveryNode, fsInfoPath))
                        .toArray(FsInfo.Path[]::new)
                ),
                nodeStats.getTransport(),
                nodeStats.getHttp(),
                nodeStats.getBreaker(),
                nodeStats.getScriptStats(),
                nodeStats.getDiscoveryStats(),
                nodeStats.getIngestStats(),
                nodeStats.getAdaptiveSelectionStats(),
                nodeStats.getScriptCacheStats(),
                nodeStats.getIndexingPressureStats(),
                nodeStats.getRepositoriesStats(),
                nodeStats.getNodeAllocationStats()
            );
        }).collect(Collectors.toList());
    }

    @Override
    ShardStats[] adjustShardStats(ShardStats[] shardsStats) {
        var shardSizeFunctionCopy = this.shardSizeFunction;
        if (shardSizeFunctionCopy == null) {
            return shardsStats;
        }

        return Arrays.stream(shardsStats).map(shardStats -> {

            var shardRouting = shardStats.getShardRouting();
            var storeStats = new StoreStats(
                shardSizeFunctionCopy.apply(shardRouting),
                shardSizeFunctionCopy.apply(shardRouting),
                shardStats.getStats().store == null ? 0L : shardStats.getStats().store.reservedSizeInBytes()
            );
            var commonStats = new CommonStats(new CommonStatsFlags(CommonStatsFlags.Flag.Store));
            commonStats.store = storeStats;

            return new ShardStats(
                shardRouting,
                commonStats,
                shardStats.getCommitStats(),
                shardStats.getSeqNoStats(),
                shardStats.getRetentionLeaseStats(),
                shardStats.getDataPath(),
                shardStats.getStatePath(),
                shardStats.isCustomDataPath(),
                shardStats.isSearchIdle(),
                shardStats.getSearchIdleTime()
            );
        }).toArray(ShardStats[]::new);
    }

    @Override
    public void setUpdateFrequency(TimeValue updateFrequency) {
        super.setUpdateFrequency(updateFrequency);
    }
}
