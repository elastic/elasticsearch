/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datatiers;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DataTiersUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;

    @Inject
    public DataTiersUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client
    ) {
        super(XPackUsageFeatureAction.DATA_TIERS.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = client;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        new ParentTaskAssigningClient(client, clusterService.localNode(), task).admin()
            .cluster()
            .execute(
                NodesDataTiersUsageTransportAction.TYPE,
                new NodesDataTiersUsageTransportAction.NodesRequest(),
                listener.delegateFailureAndWrap((delegate, response) -> {
                    // Generate tier specific stats for the nodes and indices
                    delegate.onResponse(
                        new XPackUsageFeatureResponse(
                            new DataTiersFeatureSetUsage(
                                aggregateStats(response.getNodes(), getIndicesGroupedByTier(state, response.getNodes()))
                            )
                        )
                    );
                })
            );
    }

    // Visible for testing
    static Map<String, Set<String>> getIndicesGroupedByTier(ClusterState state, List<NodeDataTiersUsage> nodes) {
        Set<String> indices = nodes.stream()
            .map(nodeResponse -> state.getRoutingNodes().node(nodeResponse.getNode().getId()))
            .filter(Objects::nonNull)
            .flatMap(node -> StreamSupport.stream(node.spliterator(), false))
            .map(ShardRouting::getIndexName)
            .collect(Collectors.toSet());
        Map<String, Set<String>> indicesByTierPreference = new HashMap<>();
        for (String indexName : indices) {
            IndexMetadata indexMetadata = state.metadata().getProject().index(indexName);
            // If the index was deleted in the meantime, skip
            if (indexMetadata == null) {
                continue;
            }
            List<String> tierPreference = indexMetadata.getTierPreference();
            if (tierPreference.isEmpty() == false) {
                indicesByTierPreference.computeIfAbsent(tierPreference.get(0), ignored -> new HashSet<>()).add(indexName);
            }
        }
        return indicesByTierPreference;
    }

    /**
     * Accumulator to hold intermediate data tier stats before final calculation.
     */
    private static class TierStatsAccumulator {
        int nodeCount = 0;
        Set<String> indexNames = new HashSet<>();
        int totalShardCount = 0;
        long totalByteCount = 0;
        long docCount = 0;
        int primaryShardCount = 0;
        long primaryByteCount = 0L;
        final TDigestState valueSketch = TDigestState.createWithoutCircuitBreaking(1000);
    }

    // Visible for testing
    static Map<String, DataTiersFeatureSetUsage.TierSpecificStats> aggregateStats(
        List<NodeDataTiersUsage> nodeDataTiersUsages,
        Map<String, Set<String>> tierPreference
    ) {
        Map<String, TierStatsAccumulator> statsAccumulators = new HashMap<>();
        for (String tier : tierPreference.keySet()) {
            statsAccumulators.put(tier, new TierStatsAccumulator());
            statsAccumulators.get(tier).indexNames.addAll(tierPreference.get(tier));
        }
        for (NodeDataTiersUsage nodeDataTiersUsage : nodeDataTiersUsages) {
            aggregateDataTierNodeCounts(nodeDataTiersUsage, statsAccumulators);
            aggregateDataTierIndexStats(nodeDataTiersUsage, statsAccumulators);
        }
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> results = new HashMap<>();
        for (Map.Entry<String, TierStatsAccumulator> entry : statsAccumulators.entrySet()) {
            results.put(entry.getKey(), aggregateFinalTierStats(entry.getValue()));
        }
        return results;
    }

    /**
     * Determine which data tiers each node belongs to (if any), and increment the node counts for those tiers.
     */
    private static void aggregateDataTierNodeCounts(NodeDataTiersUsage nodeStats, Map<String, TierStatsAccumulator> tiersStats) {
        nodeStats.getNode()
            .getRoles()
            .stream()
            .map(DiscoveryNodeRole::roleName)
            .filter(DataTier::validTierName)
            .forEach(tier -> tiersStats.computeIfAbsent(tier, k -> new TierStatsAccumulator()).nodeCount++);
    }

    /**
     * Iterate the preferred tiers of the indices for a node and aggregate their stats.
     */
    private static void aggregateDataTierIndexStats(NodeDataTiersUsage nodeDataTiersUsage, Map<String, TierStatsAccumulator> accumulators) {
        for (Map.Entry<String, NodeDataTiersUsage.UsageStats> entry : nodeDataTiersUsage.getUsageStatsByTier().entrySet()) {
            String tier = entry.getKey();
            NodeDataTiersUsage.UsageStats usage = entry.getValue();
            if (DataTier.validTierName(tier)) {
                TierStatsAccumulator accumulator = accumulators.computeIfAbsent(tier, k -> new TierStatsAccumulator());
                accumulator.docCount += usage.getDocCount();
                accumulator.totalByteCount += usage.getTotalSize();
                accumulator.totalShardCount += usage.getTotalShardCount();
                for (Long primaryShardSize : usage.getPrimaryShardSizes()) {
                    accumulator.primaryShardCount += 1;
                    accumulator.primaryByteCount += primaryShardSize;
                    accumulator.valueSketch.add(primaryShardSize);
                }
            }
        }
    }

    private static DataTiersFeatureSetUsage.TierSpecificStats aggregateFinalTierStats(TierStatsAccumulator accumulator) {
        long primaryShardSizeMedian = (long) accumulator.valueSketch.quantile(0.5);
        long primaryShardSizeMAD = computeMedianAbsoluteDeviation(accumulator.valueSketch);
        return new DataTiersFeatureSetUsage.TierSpecificStats(
            accumulator.nodeCount,
            accumulator.indexNames.size(),
            accumulator.totalShardCount,
            accumulator.primaryShardCount,
            accumulator.docCount,
            accumulator.totalByteCount,
            accumulator.primaryByteCount,
            primaryShardSizeMedian,
            primaryShardSizeMAD
        );
    }

    // Visible for testing
    static long computeMedianAbsoluteDeviation(TDigestState valuesSketch) {
        if (valuesSketch.size() == 0) {
            return 0;
        } else {
            final double approximateMedian = valuesSketch.quantile(0.5);
            try (TDigestState approximatedDeviationsSketch = TDigestState.createUsingParamsFrom(valuesSketch)) {
                valuesSketch.centroids().forEach(centroid -> {
                    final double deviation = Math.abs(approximateMedian - centroid.mean());
                    approximatedDeviationsSketch.add(deviation, centroid.count());
                });

                return (long) approximatedDeviationsSketch.quantile(0.5);
            }
        }
    }

    /**
     * In this method we use {@link NodesDataTiersUsageTransportAction#aggregateStats(RoutingNode, Metadata, NodeIndicesStats)}
     * to precalculate the stats we need from {@link NodeStats} just like we do in NodesDataTiersUsageTransportAction.
     * This way we can be backwards compatible without duplicating the calculation. This is only meant to be used to be
     * backwards compatible and it should be removed afterwords.
     */
    private static Map<String, NodeDataTiersUsage.UsageStats> precalculateLocalStatsFromNodeStats(NodeStats nodeStats, ClusterState state) {
        RoutingNode routingNode = state.getRoutingNodes().node(nodeStats.getNode().getId());
        if (routingNode == null) {
            return Map.of();
        }

        return NodesDataTiersUsageTransportAction.aggregateStats(routingNode, state.metadata(), nodeStats.getIndices());
    }
}
