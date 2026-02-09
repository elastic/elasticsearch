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

package org.elasticsearch.xpack.stateless.autoscaling.search;

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
import org.elasticsearch.xpack.stateless.autoscaling.DesiredClusterTopology;
import org.elasticsearch.xpack.stateless.autoscaling.search.IndexReplicationRanker.IndexRankingProperties;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicaRankingContext.DEFAULT_NUMBER_OF_REPLICAS;

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

    public static final SortedMap<String, Integer> EMPTY_DESIRED_REPLICAS_PER_INDEX = new TreeMap<>();
    public static final ReplicasLoadBalancingResult EMPTY_RESULT = new ReplicasLoadBalancingResult(
        Map.of(),
        EMPTY_DESIRED_REPLICAS_PER_INDEX,
        0
    );
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
        clusterSettings.initializeAndWatch(MAX_REPLICA_RELATIVE_SEARCH_LOAD, this::updateMaxReplicaRelativeSearchLoad);
    }

    /**
     * Compute the recommended number of replicas for each index based on the per index search load.
     * If the functionality is disabled it returns an empty result.
     *
     * It returns a state of all indices and their recommended replicas ordered by relative search load (high to low).
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

        if (desiredClusterTopology == null) {
            LOGGER.debug("desired cluster topology is not set, cannot compute replicas load balancing");
            listener.onResponse(EMPTY_RESULT);
            return;
        } else {
            LOGGER.debug("desired cluster topology is set to {}", desiredClusterTopology);
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
            listener.onResponse(new ReplicasLoadBalancingResult(immediateReplicaScaleDown, EMPTY_DESIRED_REPLICAS_PER_INDEX, 0));
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
                LOGGER.warn("unable to get indices stats for replicas load balancing, returning topology check result", e);
                // if stats API or processing fails, return topology check result plus current state for other indices
                SortedMap<String, Integer> desiredState = new TreeMap<>(); // natural order of keys as we don't have the search loads
                for (IndexRankingProperties props : rankingContext.properties()) {
                    String index = props.indexProperties().name();
                    // some indices might be marked for immediate scale down, we don't double report so skip them in the desired state
                    if (immediateReplicaScaleDown.containsKey(index)) {
                        continue;
                    }
                    desiredState.put(index, props.indexProperties().replicas());
                }

                listener.onResponse(new ReplicasLoadBalancingResult(immediateReplicaScaleDown, desiredState, 0));
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
                    "recommending reducing the number of replicas index [{}] to [{}] replicas to conform with current topology",
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
     * @param desiredReplicasPerIndex a map of indices to the desired number of replicas, ordered by relative search load (high to low)
     * @param indicesBlockedFromScaleUp (used for metrics) the number of indices blocked from scaling up
     */
    public record ReplicasLoadBalancingResult(
        Map<String, Integer> immediateReplicaScaleDown,
        SortedMap<String, Integer> desiredReplicasPerIndex,
        int indicesBlockedFromScaleUp
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
        LOGGER.debug("calculated indices relative search loads: {}", indicesSearchLoads);
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
            if (stats != null) {
                for (ShardStats shardStats : stats.getShards()) {
                    // only take search shards into account
                    if (shardStats.getShardRouting().isPromotableToPrimary() == false) {
                        assert shardStats.getStats().search != null : "search stats must be requested";
                        indexSearchLoad += shardStats.getStats().search.getTotal().getSearchLoadRate();
                    }
                }
            } else {
                LOGGER.debug("no stats found for index [{}], assuming zero search load", index);
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

        Comparator<String> comparator = Comparator.comparingDouble(index -> indicesRelativeSearchLoads.getOrDefault(index, 0.0));
        comparator = comparator.reversed();
        SortedMap<String, Integer> desiredReplicasPerIndex = new TreeMap<>(comparator);

        int indicesBlockedFromScaleUp = 0;
        for (IndexRankingProperties indexProperties : rankingContext.properties()) {
            String index = indexProperties.indexProperties().name();
            if (immediateReplicaScaleDown.containsKey(index)) {
                // if the index is marked for immediate scale down that takes precedence to respect the current topology
                // so no further calculations are needed here
                continue;
            }
            Double relativeLoad = indicesRelativeSearchLoads.get(index);
            if (relativeLoad == null) {
                // no search load for this index, so we recommend the default number of replicas
                // it might be tempting to say "current replicas" here, but if the index has a high count
                // of replicas due to previous load, and now we don't know anything about it, we might
                // end up keeping that high replicas count indefinitely (well, until we get some load
                // information). Scaling down is handled elsewhere, and it's not immediate, just because
                // we said default replicas here.
                desiredReplicasPerIndex.put(index, DEFAULT_NUMBER_OF_REPLICAS);
                continue;
            }

            int numberOfShards = indexProperties.indexProperties().shards();

            if (LOGGER.isDebugEnabled()) {
                double rawCalculation = relativeLoad * desiredSearchNodes / (numberOfShards * maxReplicaRelativeSearchLoad);
                double ceiledValue = Math.ceil(rawCalculation);
                double minWithNodes = Math.min(ceiledValue, desiredSearchNodes);
                int debugDesired = (int) Math.max(1, minWithNodes);
                LOGGER.debug(
                    "desiredReplicas calculation for index [{}]: relativeLoad={}, desiredSearchNodes={}, numberOfShards={}, "
                        + "maxReplicaRelativeSearchLoad={}, rawCalculation={}, ceiledValue={}, minWithNodes={}, desiredReplicas={}, "
                        + "shuttingDownSearchNodes={}",
                    index,
                    relativeLoad,
                    desiredSearchNodes,
                    numberOfShards,
                    maxReplicaRelativeSearchLoad,
                    rawCalculation,
                    ceiledValue,
                    minWithNodes,
                    debugDesired,
                    shuttingDownSearchNodes
                );
            }
            int desiredReplicas = (int) Math.max(
                1, // never go below 1 replica
                Math.min(Math.ceil(relativeLoad * desiredSearchNodes / (numberOfShards * maxReplicaRelativeSearchLoad)), desiredSearchNodes)
            );

            int currentReplicas = indexProperties.indexProperties().replicas();
            // if shutdowns are in progress, prevent scale-ups but allow scale-downs
            if (shuttingDownSearchNodes > 0 && desiredReplicas > currentReplicas) {
                indicesBlockedFromScaleUp++;
                LOGGER.debug("increasing the number of replicas for index [{}] prevented due to in-progress shutdowns", index);
                desiredReplicas = currentReplicas;
            }

            desiredReplicasPerIndex.put(index, desiredReplicas);
        }

        return new ReplicasLoadBalancingResult(immediateReplicaScaleDown, desiredReplicasPerIndex, indicesBlockedFromScaleUp);
    }

    public void updateEnableReplicasForLoadBalancing(boolean newValue) {
        this.enableReplicasForLoadBalancing = newValue;
    }

    public boolean isEnabled() {
        return enableReplicasForLoadBalancing;
    }

    private void updateMaxReplicaRelativeSearchLoad(Double newValue) {
        this.maxReplicaRelativeSearchLoad = newValue;
    }
}
