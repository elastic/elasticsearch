/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Sources locally data tier usage stats mainly indices and shard sizes grouped by preferred data tier.
 */
public class NodesDataTiersUsageTransportAction extends TransportNodesAction<
    NodesDataTiersUsageAction.NodesRequest,
    NodesDataTiersUsageAction.NodesResponse,
    NodesDataTiersUsageAction.NodeRequest,
    NodeDataTiersUsage> {

    private final IndicesService indicesService;

    @Inject
    public NodesDataTiersUsageTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters
    ) {
        super(
            NodesDataTiersUsageAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodesDataTiersUsageAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
    }

    @Override
    protected NodesDataTiersUsageAction.NodesResponse newResponse(
        NodesDataTiersUsageAction.NodesRequest request,
        List<NodeDataTiersUsage> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesDataTiersUsageAction.NodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodesDataTiersUsageAction.NodeRequest newNodeRequest(NodesDataTiersUsageAction.NodesRequest request) {
        return NodesDataTiersUsageAction.NodeRequest.INSTANCE;
    }

    @Override
    protected NodeDataTiersUsage newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeDataTiersUsage(in);
    }

    @Override
    protected NodeDataTiersUsage nodeOperation(NodesDataTiersUsageAction.NodeRequest nodeRequest, Task task) {
        assert task instanceof CancellableTask;

        DiscoveryNode localNode = clusterService.localNode();
        CommonStatsFlags flags = CommonStatsFlags.NONE.set(CommonStatsFlags.Flag.Docs, true).set(CommonStatsFlags.Flag.Store, true);
        NodeIndicesStats nodeIndicesStats = indicesService.stats(flags, true);
        ClusterState state = clusterService.state();
        RoutingNode routingNode = state.getRoutingNodes().node(localNode.getId());
        Map<String, NodeDataTiersUsage.UsageStats> usageStatsByTier = calculateUsage(routingNode, state.metadata(), nodeIndicesStats);
        return new NodeDataTiersUsage(clusterService.localNode(), usageStatsByTier);
    }

    // For testing
    static Map<String, NodeDataTiersUsage.UsageStats> calculateUsage(
        RoutingNode routingNode,
        Metadata metadata,
        NodeIndicesStats nodeIndicesStats
    ) {
        if (routingNode == null) {
            return Map.of();
        }
        Map<String, NodeDataTiersUsage.UsageStats> usageStatsByTier = new HashMap<>();
        Set<String> localIndices = StreamSupport.stream(routingNode.spliterator(), false)
            .map(routing -> routing.index().getName())
            .collect(Collectors.toSet());
        for (String indexName : localIndices) {
            IndexMetadata indexMetadata = metadata.index(indexName);
            String tier = findPreferredTier(indexMetadata);
            if (tier != null) {
                NodeDataTiersUsage.UsageStats usageStats = usageStatsByTier.computeIfAbsent(
                    tier,
                    ignored -> new NodeDataTiersUsage.UsageStats()
                );
                usageStats.addIndex(indexName);
                List<IndexShardStats> allShardStats = nodeIndicesStats.getShardStats(indexMetadata.getIndex());
                if (allShardStats != null) {
                    for (IndexShardStats indexShardStats : allShardStats) {
                        usageStats.incrementTotalSize(indexShardStats.getTotal().getStore().totalDataSetSizeInBytes());
                        usageStats.incrementDocCount(indexShardStats.getTotal().getDocs().getCount());

                        ShardRouting shardRouting = routingNode.getByShardId(indexShardStats.getShardId());
                        if (shardRouting != null && shardRouting.state() == ShardRoutingState.STARTED) {
                            usageStats.incrementTotalShardCount(1);

                            // Accumulate stats about started primary shards
                            StoreStats primaryStoreStats = indexShardStats.getPrimary().getStore();
                            if (shardRouting.primary() && primaryStoreStats != null) {
                                usageStats.addPrimaryShardSize(primaryStoreStats.totalDataSetSizeInBytes());
                            }
                        }
                    }
                }
            }
        }
        return usageStatsByTier;
    }

    @Nullable
    static String findPreferredTier(IndexMetadata indexMetadata) {
        List<String> tierPref = DataTier.parseTierList(indexMetadata.getSettings().get(DataTier.TIER_PREFERENCE))
            .stream()
            .filter(DataTier::validTierName)
            .toList();
        if (tierPref.isEmpty() == false) {
            return tierPref.get(0);
        }
        return null;
    }
}
