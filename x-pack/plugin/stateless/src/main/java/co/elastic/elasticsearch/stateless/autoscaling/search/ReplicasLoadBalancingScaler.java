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

import co.elastic.elasticsearch.stateless.autoscaling.DesiredClusterTopology;
import co.elastic.elasticsearch.stateless.autoscaling.search.IndexReplicationRanker.IndexRankingProperties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING;

/**
 * Scaler that recommends the number of replicas for each index based on the per index search load.
 *
 * The formula we use to determine the number of replicas based on search load is:
 *    min(ceil(L*N/(S*X)), N)
 *  where N = desired search nodes, L = index relative search load, S = index number of shards,
 *  X = the maximum relative search load we'd like a single replica to handle, parameter used to avoid hot-spotting
 *
 *  This class also ensures that the number of replicas for each index conforms with the current cluster topology,
 *  by indicating an immediate reduction in number of replicas is needed and should be executed regardless of heuristics that live outside
 *  this class. The topology check should be run first before any further calculations, and can be requested as standalone (to avoid the
 *  more computationally expensive relative search load recommendations).
 *
 *  The topology check computes the current alive search nodes using:
 *      current_alive_search_nodes = current_search_nodes - shutting_down_search_nodes (excluding nodes that are shutting down for restart)
 *
 *  current_alive_search_nodes will temporarily contain newly added nodes to the cluster, even if existing nodes haven't yet been marked for
 *  shutdown (this is why they are only used when looking to reduce the number of replicas, this must not be used when considering
 *  increasing replicas)
 *
 *  Once we have the number of alive search nodes will check the maximum number of replicas allowed for each index using:
 *     max_allowed_replicas =
 *        max(
 *             min(current_replicas, current_alive_search_nodes),
 *             desired_search_nodes
 *        )
 *
 *  If the current number of replicas exceeds max_allowed_replicas, we indicate an immediate scale down to that number of replicas.
 */
public class ReplicasLoadBalancingScaler {
    private static final Logger LOGGER = LogManager.getLogger(ReplicasLoadBalancingScaler.class);

    // the maximum relative search load we'd like a single replica to handle, used to avoid hot-spotting
    public static final Setting<Double> MAX_REPLICA_RELATIVE_SEARCH_LOAD = Setting.doubleSetting(
        "serverless.autoscaling.replicas_load_balancing.max_replica_relative_search_load",
        0.5,
        0.01,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    public static final ReplicasLoadBalancingResult EMPTY_RESULT = new ReplicasLoadBalancingResult(Map.of(), Map.of());
    private final Client client;
    private final ClusterService clusterService;
    private volatile boolean enableReplicasForLoadBalancing;
    private volatile double maxReplicaRelativeSearchLoad;

    public ReplicasLoadBalancingScaler(ClusterService clusterService, Client client) {
        this.client = client;
        this.clusterService = clusterService;
    }

    public void init() {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.initializeAndWatch(ENABLE_REPLICAS_LOAD_BALANCING, this::updateEnableReplicasForLoadBalancing);
        clusterSettings.initializeAndWatch(MAX_REPLICA_RELATIVE_SEARCH_LOAD, this::updateMaxReplicaRelativeSearchLoad);
    }

    /**
     * Compute the recommended number of replicas for each index based on the per index search load.
     * If the functionality is disabled it returns an empty result.
     */
    public void getRecommendedReplicas(
        ClusterState state,
        ReplicaRankingContext rankingContext,
        DesiredClusterTopology desiredClusterTopology,
        boolean onlyScaleDownToTopologyBounds,
        ActionListener<ReplicasLoadBalancingResult> listener
    ) {
        if (enableReplicasForLoadBalancing == false) {
            listener.onResponse(EMPTY_RESULT);
            return;
        }

        List<DiscoveryNode> clusterSearchNodes = state.nodes()
            .stream()
            .filter(node -> node.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()))
            .toList();
        int currentSearchNodes = clusterSearchNodes.size();
        NodesShutdownMetadata shutdownMetadata = state.metadata().nodeShutdowns();
        Set<String> shutDownNodeIds = shutdownMetadata.getAllNodeIds();
        int numberOfShuttingDownSearchNodes = (int) clusterSearchNodes.stream()
            .map(DiscoveryNode::getId)
            .filter(shutDownNodeIds::contains)
            // filter out node restarts as they don't reduce search nodes capacity
            .filter(nodeId -> shutdownMetadata.contains(nodeId, SingleNodeShutdownMetadata.Type.RESTART) == false)
            .count();

        Map<String, Integer> immediateReplicaScaleDown = limitReplicasToCurrentTopology(
            rankingContext,
            desiredClusterTopology,
            currentSearchNodes,
            numberOfShuttingDownSearchNodes
        );

        if (onlyScaleDownToTopologyBounds) {
            listener.onResponse(new ReplicasLoadBalancingResult(immediateReplicaScaleDown, Map.of()));
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

        client.execute(IndicesStatsAction.INSTANCE, statsRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                try {
                    listener.onResponse(
                        calculateDesiredReplicas(
                            rankingContext,
                            getIndicesRelativeSearchLoads(rankingContext, indicesStatsResponse::getIndex),
                            immediateReplicaScaleDown,
                            desiredClusterTopology,
                            numberOfShuttingDownSearchNodes
                        )
                    );
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                // if stats API or processing fails we do always want to return the topology check result
                listener.onResponse(new ReplicasLoadBalancingResult(immediateReplicaScaleDown, Map.of()));
            }
        });
    }

    /**
     * Limits the number of replicas for indices to conform with the current cluster topology.
     * This method ensures that indices do not have more replicas than can be supported by the current
     * alive search nodes, taking into account nodes that are shutting down.
     * It returns a map of indices that need immediate replica reduction to stay within topology bounds.
     */
    // visible for testing
    Map<String, Integer> limitReplicasToCurrentTopology(
        ReplicaRankingContext rankingContext,
        DesiredClusterTopology desiredClusterTopology,
        int currentSearchNodesInCluster,
        int shuttingDownSearchNodes
    ) {
        if (enableReplicasForLoadBalancing == false) {
            return Map.of();
        }
        Map<String, Integer> immediateReplicaScaleDown = new HashMap<>();
        int currentAliveSearchNodes = Math.max(1, currentSearchNodesInCluster - shuttingDownSearchNodes);
        int desiredSearchNodes = desiredClusterTopology.getSearch().getReplicas();
        for (IndexRankingProperties indexProperties : rankingContext.properties()) {
            String index = indexProperties.indexProperties().name();
            int currentReplicas = indexProperties.indexProperties().replicas();

            int maxReplicasAllowed = Math.max(Math.min(currentReplicas, currentAliveSearchNodes), desiredSearchNodes);
            if (currentReplicas > maxReplicasAllowed) {
                immediateReplicaScaleDown.put(index, maxReplicasAllowed);
                LOGGER.debug(
                    "immediately reducing the number of replicas index [{}] to [{}] replicas to conform with current topology",
                    index,
                    maxReplicasAllowed
                );
            }
        }
        return immediateReplicaScaleDown;
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
            double indexSearchLoad = 0.0;
            for (ShardStats shardStats : stats.getShards()) {
                // only take search shards into account
                if (shardStats.getShardRouting().isPromotableToPrimary() == false) {
                    assert shardStats.getStats().search != null : "search stats must be requested";
                    indexSearchLoad += shardStats.getStats().search.getTotal().getSearchLoadRate();
                }
            }
            indicesSearchLoads.put(index, indexSearchLoad);
            totalSearchLoad += indexSearchLoad;
        }
        return totalSearchLoad;
    }

    // visible for testing
    ReplicasLoadBalancingResult calculateDesiredReplicas(
        ReplicaRankingContext rankingContext,
        Map<String, Double> indicesRelativeSearchLoads,
        Map<String, Integer> immediateReplicaScaleDown,
        DesiredClusterTopology desiredClusterTopology,
        int shuttingDownSearchNodes
    ) {
        int desiredSearchNodes = desiredClusterTopology.getSearch().getReplicas();

        Map<String, Integer> desiredReplicasPerIndex = new HashMap<>();

        for (IndexRankingProperties indexProperties : rankingContext.properties()) {
            String index = indexProperties.indexProperties().name();
            if (immediateReplicaScaleDown.containsKey(index)) {
                // if the index is marked for immediate scale down that takes precedence to respect the current topology
                // so no further calculations are needed here
                continue;
            }
            Double relativeLoad = indicesRelativeSearchLoads.get(index);
            if (relativeLoad == null) {
                continue;
            }

            int numberOfShards = indexProperties.indexProperties().shards();

            int desiredReplicas = (int) Math.min(
                Math.ceil(relativeLoad * desiredSearchNodes / (numberOfShards * maxReplicaRelativeSearchLoad)),
                desiredSearchNodes
            );

            int currentReplicas = indexProperties.indexProperties().replicas();
            // if shutdowns are in progress, prevent scale-ups but allow scale-downs
            if (shuttingDownSearchNodes > 0 && desiredReplicas > currentReplicas) {
                LOGGER.debug("increasing the number of replicas for index [{}] prevented due to in-progress shutdowns", index);
                desiredReplicas = currentReplicas;
            }

            if (desiredReplicas != currentReplicas) {
                desiredReplicasPerIndex.put(index, desiredReplicas);
            }
        }

        return new ReplicasLoadBalancingResult(immediateReplicaScaleDown, desiredReplicasPerIndex);
    }

    public void updateEnableReplicasForLoadBalancing(boolean newValue) {
        this.enableReplicasForLoadBalancing = newValue;
    }

    private void updateMaxReplicaRelativeSearchLoad(Double newValue) {
        this.maxReplicaRelativeSearchLoad = newValue;
    }
}
