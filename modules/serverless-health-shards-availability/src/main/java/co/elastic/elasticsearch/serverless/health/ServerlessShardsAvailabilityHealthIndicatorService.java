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

package co.elastic.elasticsearch.serverless.health;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.indices.SystemIndices;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getTruncatedIndices;

public class ServerlessShardsAvailabilityHealthIndicatorService extends ShardsAvailabilityHealthIndicatorService {
    public static final String ALL_REPLICAS_UNASSIGNED_IMPACT_ID = "all_replicas_unassigned";

    private static final String SHARD_ROLE_DECIDER_NAME = "stateless_shard_role";

    private static final List<String> SHARD_ROLES = List.of(
        DiscoveryNodeRole.SEARCH_ROLE.roleName(),
        DiscoveryNodeRole.INDEX_ROLE.roleName()
    );

    private static final String DEBUG_NODES_ACTION_GUIDE = "https://ela.st/serverless-debug-nodes";
    private static final Map<String, Diagnosis.Definition> ACTION_DEBUG_NODES_LOOKUP = SHARD_ROLES.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                role -> role,
                role -> new Diagnosis.Definition(
                    NAME,
                    "debug_node:role:" + role,
                    "Elasticsearch isn't allowed to allocate some shards from these indices "
                        + "because the shards are expected to be allocated to "
                        + role
                        + " nodes and there are no such nodes found in the cluster.",
                    "Ensure that " + role + " nodes are healthy and are able to join the cluster.",
                    DEBUG_NODES_ACTION_GUIDE
                )
            )
        );

    private static final String ADJUST_SEARCH_CAPACITY_ACTION_GUIDE = "https://ela.st/serverless-fix-replicas";
    private static final Diagnosis.Definition ACTION_ADJUST_SEARCH_CAPACITY = new Diagnosis.Definition(
        NAME,
        "update_shards:role:" + DiscoveryNodeRole.SEARCH_ROLE.roleName(),
        "Elasticsearch isn't allowed to allocate some shards from these indices to any of the "
            + DiscoveryNodeRole.SEARCH_ROLE.roleName()
            + " nodes because there are not enough nodes to allocate each shard copy on a different node.",
        "Ensure that all the "
            + DiscoveryNodeRole.SEARCH_ROLE.roleName()
            + " nodes configured have joined the cluster or decrease the number of replica shards in the affected indices.",
        ADJUST_SEARCH_CAPACITY_ACTION_GUIDE
    );

    private static final String INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE = "https://ela.st/serverless-cluster-total-shards";
    private static final Map<String, Diagnosis.Definition> ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP = SHARD_ROLES.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                role -> role,
                role -> new Diagnosis.Definition(
                    NAME,
                    "increase_shard_limit_cluster_setting:role:" + role,
                    "Elasticsearch isn't allowed to allocate some shards from these indices because each node in the '"
                        + role
                        + "' role has reached the cluster shard limit. ",
                    "Ensure that all  "
                        + role
                        + " nodes have joined the cluster or increase the values for the '"
                        + CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
                        + "' cluster setting.",
                    INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE
                )
            )
        );

    private static final String INCREASE_INDEX_SHARD_LIMIT_ACTION_GUIDE = "https://ela.st/serverless-index-total-shards";
    private static final Map<String, Diagnosis.Definition> ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP = SHARD_ROLES.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                role -> role,
                role -> new Diagnosis.Definition(
                    NAME,
                    "increase_shard_limit_index_setting:role:" + role,
                    "Elasticsearch isn't allowed to allocate some shards from these indices because each node with the '"
                        + role
                        + "' role has reached the index shard limit.",
                    "Ensure that all  "
                        + role
                        + " nodes have joined the cluster or increase the values for the '"
                        + INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
                        + "' index setting on each index.",
                    INCREASE_INDEX_SHARD_LIMIT_ACTION_GUIDE
                )
            )
        );

    public ServerlessShardsAvailabilityHealthIndicatorService(
        ClusterService clusterService,
        AllocationService allocationService,
        SystemIndices systemIndices
    ) {
        super(clusterService, allocationService, systemIndices);
    }

    @Override
    public ShardsAvailabilityHealthIndicatorService.ShardAllocationStatus createNewStatus(Metadata metadata) {
        return new ServerlessShardAllocationStatus(metadata);
    }

    public class ServerlessShardAllocationStatus extends ShardsAvailabilityHealthIndicatorService.ShardAllocationStatus {

        ServerlessShardAllocationStatus(Metadata clusterMetadata) {
            super(clusterMetadata);
        }

        /**
         * Overrides the existing status to indicate that the cluster is red when all replicas for a shard are not assigned
         */
        @Override
        public HealthStatus getStatus() {
            if (primaries.areAllAvailable() == false || primaries.searchableSnapshotsState.getRedSearchableSnapshots().isEmpty() == false) {
                return RED;
            } else if (replicas.areAllAvailable() == false) {
                if (replicas.doAnyIndicesHaveAllUnavailable()) {
                    // There are some indices where *all* indices are unavailable
                    return RED;
                } else {
                    return YELLOW;
                }
            } else {
                return GREEN;
            }
        }

        @Override
        public HealthIndicatorDetails getDetails(boolean verbose) {
            final HealthIndicatorDetails details = super.getDetails(verbose);
            if (details == HealthIndicatorDetails.EMPTY) {
                return details;
            }
            assert details instanceof SimpleHealthIndicatorDetails : details.getClass().getName();
            if (primaries.indicesWithUnavailableShards.isEmpty() && replicas.indicesWithUnavailableShards.isEmpty()) {
                return details;
            }

            final Map<String, Object> map = new HashMap<>(((SimpleHealthIndicatorDetails) details).details());
            if (primaries.indicesWithUnavailableShards.isEmpty() == false) {
                map.put("indices_with_unavailable_primaries", getTruncatedIndices(primaries.indicesWithUnavailableShards, clusterMetadata));
            }
            if (replicas.indicesWithUnavailableShards.isEmpty() == false) {
                map.put("indices_with_unavailable_replicas", getTruncatedIndices(replicas.indicesWithUnavailableShards, clusterMetadata));
            }
            return new SimpleHealthIndicatorDetails(Map.copyOf(map));
        }

        @Override
        public List<HealthIndicatorImpact> getImpacts() {
            List<HealthIndicatorImpact> impacts = new ArrayList<>(super.getImpacts());
            if (replicas.doAnyIndicesHaveAllUnavailable()) {
                String impactDescription = String.format(
                    Locale.ROOT,
                    "Not all data is searchable. No searchable copies of the data exist on %d %s [%s].",
                    replicas.indicesWithAllShardsUnavailable.size(),
                    replicas.indicesWithAllShardsUnavailable.size() == 1 ? "index" : "indices",
                    getTruncatedIndices(replicas.indicesWithAllShardsUnavailable, clusterMetadata)
                );
                impacts.add(
                    new HealthIndicatorImpact(NAME, ALL_REPLICAS_UNASSIGNED_IMPACT_ID, 1, impactDescription, List.of(ImpactArea.SEARCH))
                );
                if (replicas.indicesWithUnavailableShards.equals(replicas.indicesWithAllShardsUnavailable)) {
                    // Remove the other replica message, because all indices are already covered by the impact added above
                    impacts.removeIf(indicator -> indicator.id().equals(REPLICA_UNASSIGNED_IMPACT_ID));
                }
            }
            return impacts;
        }
    }

    @Override
    public List<Diagnosis.Definition> checkNodeRoleRelatedIssues(
        IndexMetadata indexMetadata,
        List<NodeAllocationResult> nodeAllocationResults,
        ClusterState clusterState,
        ShardRouting shardRouting
    ) {
        String role = shardRouting.primary() ? DiscoveryNodeRole.INDEX_ROLE.roleName() : DiscoveryNodeRole.SEARCH_ROLE.roleName();
        List<Diagnosis.Definition> diagnosisDefs = new ArrayList<>();
        List<NodeAllocationResult> shardRoleAllocationResults = nodeAllocationResults.stream()
            .filter(hasDeciderResult(SHARD_ROLE_DECIDER_NAME, Decision.Type.YES))
            .toList();
        if (shardRoleAllocationResults.isEmpty()) {
            // No nodes were found with the specific role.
            Optional.ofNullable(ACTION_DEBUG_NODES_LOOKUP.get(role)).ifPresent(diagnosisDefs::add);
        } else {
            // Collect the nodes the index is allowed on
            Set<DiscoveryNode> candidateNodes = shardRoleAllocationResults.stream()
                .map(NodeAllocationResult::getNode)
                .filter(node -> node.hasRole(role))
                .collect(Collectors.toSet());

            // Run checks for node role specific problems
            diagnosisDefs.addAll(
                checkNodesWithRoleAtShardLimit(indexMetadata, clusterState, shardRoleAllocationResults, candidateNodes, role)
            );
            checkNotEnoughNodesWithRole(shardRoleAllocationResults, role).ifPresent(diagnosisDefs::add);
        }
        return diagnosisDefs;
    }

    @Override
    public Diagnosis.Definition getIncreaseNodeWithRoleCapacityAction(String role) {
        assert DiscoveryNodeRole.SEARCH_ROLE.roleName().equals(role);
        return ACTION_ADJUST_SEARCH_CAPACITY;
    }

    @Override
    public Diagnosis.Definition getIncreaseShardLimitClusterSettingAction(String role) {
        return ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP.get(role);
    }

    @Override
    public Diagnosis.Definition getIncreaseShardLimitIndexSettingAction(String role) {
        return ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP.get(role);
    }
}
