/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.index.search.stats.SearchStats;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING;

/**
 * Scaler that recommends the number of replicas for each index based on the per index search load.
 */
public class ReplicasLoadBalancingScaler {

    public static final ReplicasLoadBalancingResult EMPTY_RESULT = new ReplicasLoadBalancingResult(Map.of(), Map.of());
    private final Client client;
    private volatile boolean enableReplicasForLoadBalancing;

    public ReplicasLoadBalancingScaler(ClusterService clusterService, Client client) {
        this.client = client;
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.initializeAndWatch(ENABLE_REPLICAS_LOAD_BALANCING, this::updateEnableReplicasForLoadBalancing);
    }

    /**
     * Compute the recommended number of replicas for each index based on the per index search load.
     * If the functionality is disabled it returns an empty result.
     */
    public void getRecommendedReplicas(
        ClusterState state,
        ReplicaRankingContext rankingContext,
        ActionListener<ReplicasLoadBalancingResult> listener
    ) {
        if (enableReplicasForLoadBalancing == false) {
            listener.onResponse(EMPTY_RESULT);
            return;
        }

        final var statsIndicesOptions = new IndicesOptions(
            IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS,
            IndicesOptions.WildcardOptions.builder().matchClosed(true).allowEmptyExpressions(false).build(),
            IndicesOptions.GatekeeperOptions.DEFAULT,
            IndicesOptions.CrossProjectModeOptions.DEFAULT
        );
        IndicesStatsRequest statsRequest = new IndicesStatsRequest().indices("_all")
            .clear()
            .indicesOptions(statsIndicesOptions)
            .search(true);
        client.execute(IndicesStatsAction.INSTANCE, statsRequest, listener.delegateFailureAndWrap((delegate, statsResponse) -> {
            listener.onResponse(calculateDesiredReplicas(getIndicesRelativeSearchLoads(rankingContext, statsResponse::getIndex)));
        }));
    }

    /**
     * The result of replicas load balancing calculations.
     *
     * @param immediateReplicaScaleDown a map of indices to the number of replicas that must be immediately scaled down to obbey the current
     *                                 cluster topology bounds
     * @param desiredReplicasPerIndex a map of indices to the desired number of replicas
     */
    public record ReplicasLoadBalancingResult(
        Map<String, Integer> immediateReplicaScaleDown,
        Map<String, Integer> desiredReplicasPerIndex
    ) {}

    /**
     * Calculates the relative search load for each index compared to the total search load across all indices.
     * <p>
     * This method aggregates search load metrics from search shards for each index,
     * computes the total search load across all indices, and returns the proportional search load for each index
     * as a value between 0.0 and 1.0.
     *
     * @param rankingContext the context containing the set of indices to analyze
     * @param indexStatsSupplier a function that provides {@link IndexStats} for a given index name
     * @return a map where keys are index names and values are their relative search loads (0.0 to 1.0),
     *         where the sum of all values equals 1.0 if there is any search load
     */
    static Map<String, Double> getIndicesRelativeSearchLoads(
        ReplicaRankingContext rankingContext,
        Function<String, IndexStats> indexStatsSupplier
    ) {
        Map<String, Double> indicesSearchLoads = new HashMap<>(rankingContext.indices().size(), 1.0f);
        double totalSearchLoad = populateIndicesSearchLoads(rankingContext, indexStatsSupplier, indicesSearchLoads);
        indicesSearchLoads.replaceAll((k, v) -> totalSearchLoad > 0.0 ? v / totalSearchLoad : 0.0);
        return indicesSearchLoads;
    }

    /**
     * Populates the search load for each index into the provided map and returns the total search load across all indices.
     */
    private static double populateIndicesSearchLoads(
        ReplicaRankingContext rankingContext,
        Function<String, IndexStats> indexStatsSupplier,
        Map<String, Double> indicesSearchLoads
    ) {
        double totalSearchLoad = 0.0;
        for (String index : rankingContext.indices()) {
            IndexStats stats = indexStatsSupplier.apply(index);
            double indexSearchLoad = Arrays.stream(stats.getShards())
                // only take search shards into account
                .filter(shardStats -> shardStats.getShardRouting().isPromotableToPrimary() == false)
                .map(shardStats -> shardStats.getStats().search.getTotal())
                .map(SearchStats.Stats::getSearchLoadRate)
                .reduce(0.0, Double::sum);
            indicesSearchLoads.put(index, indexSearchLoad);
            totalSearchLoad += indexSearchLoad;
        }
        return totalSearchLoad;
    }

    static ReplicasLoadBalancingResult calculateDesiredReplicas(Map<String, Double> indicesRelativeSearchLoads) {
        return EMPTY_RESULT;
    }

    private void updateEnableReplicasForLoadBalancing(boolean newValue) {
        this.enableReplicasForLoadBalancing = newValue;
    }
}
