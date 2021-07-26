/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DataTiersFeatureSet implements XPackFeatureSet {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public DataTiersFeatureSet(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.DATA_TIERS;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<Usage> listener) {
        final ClusterState state = clusterService.state();
        client.admin().cluster().prepareNodesStats()
            .all()
            .setIndices(CommonStatsFlags.ALL)
            .execute(ActionListener.wrap(nodesStatsResponse -> {
                final RoutingNodes routingNodes = state.getRoutingNodes();

                // First separate the nodes into separate tiers, note that nodes *may* be duplicated
                Map<String, List<NodeStats>> tierSpecificNodeStats = separateTiers(nodesStatsResponse);

                // Generate tier specific stats for the nodes
                Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = tierSpecificNodeStats.entrySet()
                    .stream().collect(Collectors.toMap(Map.Entry::getKey, ns -> calculateStats(ns.getValue(), routingNodes)));

                listener.onResponse(new DataTiersFeatureSetUsage(tierSpecificStats));
            }, listener::onFailure));
    }

    // Visible for testing
    static Map<String, List<NodeStats>> separateTiers(NodesStatsResponse nodesStatsResponse) {
        Map<String, List<NodeStats>> responses = new HashMap<>();
        DataTier.ALL_DATA_TIERS.forEach(tier ->
            responses.put(tier, nodesStatsResponse.getNodes().stream()
                .filter(stats -> stats.getNode().getRoles().stream()
                    .map(DiscoveryNodeRole::roleName)
                    .anyMatch(rn -> rn.equals(tier)))
                .collect(Collectors.toList())));
        return responses;
    }

    private DataTiersFeatureSetUsage.TierSpecificStats calculateStats(List<NodeStats> nodesStats, RoutingNodes routingNodes) {
        int nodeCount = 0;
        int indexCount = 0;
        int totalShardCount = 0;
        long totalByteCount = 0;
        long docCount = 0;
        final AtomicInteger primaryShardCount = new AtomicInteger(0);
        final AtomicLong primaryByteCount = new AtomicLong(0);
        final TDigestState valueSketch = new TDigestState(1000);
        for (NodeStats nodeStats : nodesStats) {
            nodeCount++;
            totalByteCount += nodeStats.getIndices().getStore().getSizeInBytes();
            docCount += nodeStats.getIndices().getDocs().getCount();
            String nodeId = nodeStats.getNode().getId();
            final RoutingNode node = routingNodes.node(nodeId);
            if (node != null) {
                totalShardCount += node.shardsWithState(ShardRoutingState.STARTED).size();
                Set<Index> indicesOnNode = node.shardsWithState(ShardRoutingState.STARTED).stream()
                    .map(ShardRouting::index)
                    .collect(Collectors.toSet());
                indexCount += indicesOnNode.size();
                indicesOnNode.forEach(index -> {
                    final List<IndexShardStats> allShardStats = nodeStats.getIndices().getShardStats(index);
                    if (allShardStats != null) {
                        allShardStats.stream()
                            .filter(shardStats -> shardStats.getPrimary().getStore() != null)
                            .forEach(shardStats -> {
                                StoreStats primaryStoreStats = shardStats.getPrimary().getStore();
                                // If storeStats is null, it means this is not a replica
                                primaryShardCount.incrementAndGet();
                                long primarySize = primaryStoreStats.getSizeInBytes();
                                primaryByteCount.addAndGet(primarySize);
                                valueSketch.add(primarySize);
                            });
                    }
                });
            }
        }
        long primaryShardSizeMedian = (long) valueSketch.quantile(0.5);
        long primaryShardSizeMAD = computeMedianAbsoluteDeviation(valueSketch);
        return new DataTiersFeatureSetUsage.TierSpecificStats(nodeCount, indexCount, totalShardCount, primaryShardCount.get(), docCount,
            totalByteCount, primaryByteCount.get(), primaryShardSizeMedian, primaryShardSizeMAD);
    }

    // Visible for testing
    static long computeMedianAbsoluteDeviation(TDigestState valuesSketch) {
        if (valuesSketch.size() == 0) {
            return 0;
        } else {
            final double approximateMedian = valuesSketch.quantile(0.5);
            final TDigestState approximatedDeviationsSketch = new TDigestState(valuesSketch.compression());
            valuesSketch.centroids().forEach(centroid -> {
                final double deviation = Math.abs(approximateMedian - centroid.mean());
                approximatedDeviationsSketch.add(deviation, centroid.count());
            });

            return (long) approximatedDeviationsSketch.quantile(0.5);
        }
    }
}
