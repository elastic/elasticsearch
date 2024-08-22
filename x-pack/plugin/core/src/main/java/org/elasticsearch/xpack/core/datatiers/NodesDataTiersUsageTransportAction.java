/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datatiers;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
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
    NodesDataTiersUsageTransportAction.NodesRequest,
    NodesDataTiersUsageTransportAction.NodesResponse,
    NodesDataTiersUsageTransportAction.NodeRequest,
    NodeDataTiersUsage> {

    public static final ActionType<NodesResponse> TYPE = new ActionType<>("cluster:monitor/nodes/data_tier_usage");
    public static final NodeFeature LOCALLY_PRECALCULATED_STATS_FEATURE = new NodeFeature("usage.data_tiers.precalculate_stats");

    private static final CommonStatsFlags STATS_FLAGS = new CommonStatsFlags().clear()
        .set(CommonStatsFlags.Flag.Docs, true)
        .set(CommonStatsFlags.Flag.Store, true);

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
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
    }

    @Override
    protected NodesResponse newResponse(NodesRequest request, List<NodeDataTiersUsage> responses, List<FailedNodeException> failures) {
        return new NodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(NodesRequest request) {
        return new NodeRequest();
    }

    @Override
    protected NodeDataTiersUsage newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeDataTiersUsage(in);
    }

    @Override
    protected NodeDataTiersUsage nodeOperation(NodeRequest nodeRequest, Task task) {
        assert task instanceof CancellableTask;

        DiscoveryNode localNode = clusterService.localNode();
        NodeIndicesStats nodeIndicesStats = indicesService.stats(STATS_FLAGS, true);
        ClusterState state = clusterService.state();
        RoutingNode routingNode = state.getRoutingNodes().node(localNode.getId());
        Map<String, NodeDataTiersUsage.UsageStats> usageStatsByTier = aggregateStats(routingNode, state.metadata(), nodeIndicesStats);
        return new NodeDataTiersUsage(clusterService.localNode(), usageStatsByTier);
    }

    // For bwc & testing purposes
    static Map<String, NodeDataTiersUsage.UsageStats> aggregateStats(
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
            if (indexMetadata == null) {
                continue;
            }
            String tier = indexMetadata.getTierPreference().isEmpty() ? null : indexMetadata.getTierPreference().get(0);
            if (tier != null) {
                NodeDataTiersUsage.UsageStats usageStats = usageStatsByTier.computeIfAbsent(
                    tier,
                    ignored -> new NodeDataTiersUsage.UsageStats()
                );
                List<IndexShardStats> allShardStats = nodeIndicesStats.getShardStats(indexMetadata.getIndex());
                if (allShardStats != null) {
                    for (IndexShardStats indexShardStats : allShardStats) {
                        final StoreStats storeStats = indexShardStats.getTotal().getStore();
                        usageStats.incrementTotalSize(storeStats == null ? 0L : storeStats.totalDataSetSizeInBytes());
                        final DocsStats docsStats = indexShardStats.getTotal().getDocs();
                        usageStats.incrementDocCount(docsStats == null ? 0L : docsStats.getCount());

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

    public static class NodesRequest extends BaseNodesRequest<NodesRequest> {

        public NodesRequest() {
            super((String[]) null);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }
    }

    public static class NodeRequest extends TransportRequest {

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        public NodeRequest() {

        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodesResponse extends BaseNodesResponse<NodeDataTiersUsage> {

        public NodesResponse(ClusterName clusterName, List<NodeDataTiersUsage> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeDataTiersUsage> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeDataTiersUsage::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeDataTiersUsage> nodes) throws IOException {
            out.writeCollection(nodes);
        }
    }
}
