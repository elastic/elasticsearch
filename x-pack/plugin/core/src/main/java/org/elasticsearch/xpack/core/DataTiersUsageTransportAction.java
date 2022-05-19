/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.store.StoreStats;
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
import java.util.Set;
import java.util.stream.StreamSupport;

public class DataTiersUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;

    @Inject
    public DataTiersUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            XPackUsageFeatureAction.DATA_TIERS.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        client.admin()
            .cluster()
            .prepareNodesStats()
            .all()
            .setIndices(CommonStatsFlags.ALL)
            .execute(ActionListener.wrap(nodesStatsResponse -> {
                final RoutingNodes routingNodes = state.getRoutingNodes();
                final ImmutableOpenMap<String, IndexMetadata> indices = state.getMetadata().getIndices();

                // Determine which tiers each index would prefer to be within
                Map<String, String> indicesToTiers = tierIndices(indices);

                // Generate tier specific stats for the nodes and indices
                Map<String, DataTiersFeatureSetUsage.TierSpecificStats> tierSpecificStats = calculateStats(
                    nodesStatsResponse.getNodes(),
                    indicesToTiers,
                    routingNodes
                );

                listener.onResponse(new XPackUsageFeatureResponse(new DataTiersFeatureSetUsage(tierSpecificStats)));
            }, listener::onFailure));
    }

    // Visible for testing
    // Takes a registry of indices and returns a mapping of index name to which tier it most prefers. Always 1 to 1, some may filter out.
    static Map<String, String> tierIndices(ImmutableOpenMap<String, IndexMetadata> indices) {
        Map<String, String> indexByTier = new HashMap<>();
        indices.entrySet().forEach(entry -> {
            String tierPref = entry.getValue().getSettings().get(DataTier.TIER_PREFERENCE);
            if (Strings.hasText(tierPref)) {
                String[] tiers = tierPref.split(",");
                if (tiers.length > 0) {
                    indexByTier.put(entry.getKey(), tiers[0]);
                }
            }
        });
        return indexByTier;
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
        final TDigestState valueSketch = new TDigestState(1000);
    }

    // Visible for testing
    static Map<String, DataTiersFeatureSetUsage.TierSpecificStats> calculateStats(
        List<NodeStats> nodesStats,
        Map<String, String> indexByTier,
        RoutingNodes routingNodes
    ) {
        Map<String, TierStatsAccumulator> statsAccumulators = new HashMap<>();
        for (NodeStats nodeStats : nodesStats) {
            aggregateDataTierNodeCounts(nodeStats, statsAccumulators);
            aggregateDataTierIndexStats(nodeStats, routingNodes, indexByTier, statsAccumulators);
        }
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> results = new HashMap<>();
        for (Map.Entry<String, TierStatsAccumulator> entry : statsAccumulators.entrySet()) {
            results.put(entry.getKey(), calculateFinalTierStats(entry.getValue()));
        }
        return results;
    }

    /**
     * Determine which data tiers this node belongs to (if any), and increment the node counts for those tiers.
     */
    private static void aggregateDataTierNodeCounts(NodeStats nodeStats, Map<String, TierStatsAccumulator> tiersStats) {
        nodeStats.getNode()
            .getRoles()
            .stream()
            .map(DiscoveryNodeRole::roleName)
            .filter(DataTier::validTierName)
            .forEach(tier -> tiersStats.computeIfAbsent(tier, k -> new TierStatsAccumulator()).nodeCount++);
    }

    /**
     * Locate which indices are hosted on the node specified by the NodeStats, then group and aggregate the available index stats by tier.
     */
    private static void aggregateDataTierIndexStats(
        NodeStats nodeStats,
        RoutingNodes routingNodes,
        Map<String, String> indexByTier,
        Map<String, TierStatsAccumulator> accumulators
    ) {
        final RoutingNode node = routingNodes.node(nodeStats.getNode().getId());
        if (node != null) {
            StreamSupport.stream(node.spliterator(), false)
                .map(ShardRouting::index)
                .distinct()
                .forEach(index -> classifyIndexAndCollectStats(index, nodeStats, indexByTier, node, accumulators));
        }
    }

    /**
     * Determine which tier an index belongs in, then accumulate its stats into that tier's stats.
     */
    private static void classifyIndexAndCollectStats(
        Index index,
        NodeStats nodeStats,
        Map<String, String> indexByTier,
        RoutingNode node,
        Map<String, TierStatsAccumulator> accumulators
    ) {
        // Look up which tier this index belongs to (its most preferred)
        String indexTier = indexByTier.get(index.getName());
        if (indexTier != null) {
            final TierStatsAccumulator accumulator = accumulators.computeIfAbsent(indexTier, k -> new TierStatsAccumulator());
            accumulator.indexNames.add(index.getName());
            aggregateDataTierShardStats(nodeStats, index, node, accumulator);
        }
    }

    /**
     * Collect shard-level data tier stats from shard stats contained in the node stats response.
     */
    private static void aggregateDataTierShardStats(NodeStats nodeStats, Index index, RoutingNode node, TierStatsAccumulator accumulator) {
        // Shard based stats
        final List<IndexShardStats> allShardStats = nodeStats.getIndices().getShardStats(index);
        if (allShardStats != null) {
            for (IndexShardStats shardStat : allShardStats) {
                accumulator.totalByteCount += shardStat.getTotal().getStore().totalDataSetSizeInBytes();
                accumulator.docCount += shardStat.getTotal().getDocs().getCount();

                // Accumulate stats about started shards
                if (node.getByShardId(shardStat.getShardId()).state() == ShardRoutingState.STARTED) {
                    accumulator.totalShardCount += 1;

                    // Accumulate stats about started primary shards
                    StoreStats primaryStoreStats = shardStat.getPrimary().getStore();
                    if (primaryStoreStats != null) {
                        // if primaryStoreStats is null, it means there is no primary on the node in question
                        accumulator.primaryShardCount++;
                        long primarySize = primaryStoreStats.totalDataSetSizeInBytes();
                        accumulator.primaryByteCount += primarySize;
                        accumulator.valueSketch.add(primarySize);
                    }
                }
            }
        }
    }

    private static DataTiersFeatureSetUsage.TierSpecificStats calculateFinalTierStats(TierStatsAccumulator accumulator) {
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
            final TDigestState approximatedDeviationsSketch = new TDigestState(valuesSketch.compression());
            valuesSketch.centroids().forEach(centroid -> {
                final double deviation = Math.abs(approximateMedian - centroid.mean());
                approximatedDeviationsSketch.add(deviation, centroid.count());
            });

            return (long) approximatedDeviationsSketch.quantile(0.5);
        }
    }
}
