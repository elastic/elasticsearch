/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.cluster.health.ClusterShardHealth.getInactivePrimaryHealth;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;

/**
 * This indicator reports health for shards.
 * <p>
 * Indicator will report:
 * * RED when one or more primary shards are not available
 * * YELLOW when one or more replica shards are not available
 * * GREEN otherwise
 * <p>
 * Each shard needs to be available and replicated in order to guarantee high availability and prevent data loses.
 * Shards allocated on nodes scheduled for restart (using nodes shutdown API) will not degrade this indicator health.
 */
public class ShardsAvailabilityHealthIndicatorService implements HealthIndicatorService {

    private static final Logger LOGGER = LogManager.getLogger(ShardsAvailabilityHealthIndicatorService.class);

    public static final String NAME = "shards_availability";

    private static final String DATA_TIER_ALLOCATION_DECIDER_NAME = "data_tier";

    private final ClusterService clusterService;
    private final AllocationService allocationService;

    public ShardsAvailabilityHealthIndicatorService(ClusterService clusterService, AllocationService allocationService) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {
        var state = clusterService.state();
        var shutdown = state.getMetadata().custom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY);
        var status = new ShardAllocationStatus(state.getMetadata());

        for (IndexRoutingTable indexShardRouting : state.routingTable()) {
            for (int i = 0; i < indexShardRouting.size(); i++) {
                IndexShardRoutingTable shardRouting = indexShardRouting.shard(i);
                status.addPrimary(shardRouting.primaryShard(), state, shutdown, explain);
                for (ShardRouting replicaShard : shardRouting.replicaShards()) {
                    status.addReplica(replicaShard, state, shutdown, explain);
                }
            }
        }
        return createIndicator(
            status.getStatus(),
            status.getSummary(),
            status.getDetails(explain),
            status.getImpacts(),
            status.getUserActions(explain)
        );
    }

    public static final String RESTORE_FROM_SNAPSHOT_ACTION_GUIDE = "http://ela.st/restore-snapshot";
    public static final Diagnosis.Definition ACTION_RESTORE_FROM_SNAPSHOT = new Diagnosis.Definition(
        "restore_from_snapshot",
        "Elasticsearch isn't allowed to allocate some shards because there are no copies of the shards in the cluster. Elasticsearch will "
            + "allocate these shards when nodes holding good copies of the data join the cluster.",
        "If no such node is available, restore these indices from a recent snapshot.",
        RESTORE_FROM_SNAPSHOT_ACTION_GUIDE
    );

    public static final String DIAGNOSE_SHARDS_ACTION_GUIDE = "http://ela.st/diagnose-shards";
    public static final Diagnosis.Definition ACTION_CHECK_ALLOCATION_EXPLAIN_API = new Diagnosis.Definition(
        "explain_allocations",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any of the nodes in the cluster.",
        "Diagnose the issue by calling the allocation explain API for an index [GET _cluster/allocation/explain]. Choose a node to which "
            + "you expect a shard to be allocated, find this node in the node-by-node explanation, and address the reasons which prevent "
            + "Elasticsearch from allocating the shard.",
        DIAGNOSE_SHARDS_ACTION_GUIDE
    );

    public static final String ENABLE_INDEX_ALLOCATION_GUIDE = "http://ela.st/fix-index-allocation";
    public static final Diagnosis.Definition ACTION_ENABLE_INDEX_ROUTING_ALLOCATION = new Diagnosis.Definition(
        "enable_index_allocations",
        "Elasticsearch isn't allowed to allocate some shards from these indices because allocation for those shards has been disabled at "
            + "the index level.",
        "Check that the ["
            + INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()
            + "] index settings are set to ["
            + EnableAllocationDecider.Allocation.ALL.toString().toLowerCase(Locale.getDefault())
            + "].",
        ENABLE_INDEX_ALLOCATION_GUIDE
    );
    public static final String ENABLE_CLUSTER_ALLOCATION_ACTION_GUIDE = "http://ela.st/fix-cluster-allocation";
    public static final Diagnosis.Definition ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION = new Diagnosis.Definition(
        "enable_cluster_allocations",
        "Elasticsearch isn't allowed to allocate some shards from these indices because allocation for those shards has been disabled at "
            + "the cluster level.",
        "Check that the ["
            + EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()
            + "] cluster setting is set to ["
            + EnableAllocationDecider.Allocation.ALL.toString().toLowerCase(Locale.getDefault())
            + "].",
        ENABLE_CLUSTER_ALLOCATION_ACTION_GUIDE
    );

    public static final String ENABLE_TIER_ACTION_GUIDE = "http://ela.st/enable-tier";
    public static final Map<String, Diagnosis.Definition> ACTION_ENABLE_TIERS_LOOKUP = DataTier.ALL_DATA_TIERS.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    "enable_data_tiers_" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices because the indices expect to be allocated to "
                        + "data tier nodes, but there were not any nodes with the expected tiers found in the cluster.",
                    "Add nodes with the [" + tier + "] role to the cluster.",
                    ENABLE_TIER_ACTION_GUIDE
                )
            )
        );

    public static final String INCREASE_SHARD_LIMIT_ACTION_GUIDE = "http://ela.st/index-total-shards";
    public static final Diagnosis.Definition ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING = new Diagnosis.Definition(
        "increase_shard_limit_index_setting",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any data nodes because each node has reached the index "
            + "shard limit. ",
        "Increase the values for the ["
            + INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
            + "] index setting on each index or add more nodes to the target tiers.",
        INCREASE_SHARD_LIMIT_ACTION_GUIDE
    );

    public static final Map<String, Diagnosis.Definition> ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    "increase_shard_limit_index_setting_" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices because each node in the ["
                        + tier
                        + "] tier has reached the index shard limit. ",
                    "Increase the values for the ["
                        + INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
                        + "] index setting on each index or add more nodes to the target tiers.",
                    INCREASE_SHARD_LIMIT_ACTION_GUIDE
                )
            )
        );

    public static final String INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE = "http://ela.st/cluster-total-shards";
    public static final Diagnosis.Definition ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING = new Diagnosis.Definition(
        "increase_shard_limit_cluster_setting",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any data nodes because each node has reached the "
            + "cluster shard limit.",
        "Increase the values for the ["
            + CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
            + "] cluster setting or add more nodes to the target tiers.",
        INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE
    );

    public static final Map<String, Diagnosis.Definition> ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    "increase_shard_limit_cluster_setting_" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices because each node in the ["
                        + tier
                        + "] tier has reached the cluster shard limit. ",
                    "Increase the values for the ["
                        + CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
                        + "] cluster setting or add more nodes to the target tiers.",
                    INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE
                )
            )
        );

    public static final String MIGRATE_TO_TIERS_ACTION_GUIDE = "http://ela.st/migrate-to-tiers";
    public static final Diagnosis.Definition ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA = new Diagnosis.Definition(
        "migrate_data_tiers_require_data",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any nodes in the desired data tiers because the "
            + "indices are configured with allocation filter rules that are incompatible with the nodes in this tier.",
        "Remove ["
            + INDEX_ROUTING_REQUIRE_GROUP_PREFIX
            + ".data] from the index settings or try migrating to data tiers by first stopping ILM [POST /_ilm/stop] and then using "
            + "the data tier migration action [POST /_ilm/migrate_to_data_tiers]. Finally, restart ILM [POST /_ilm/start].",
        MIGRATE_TO_TIERS_ACTION_GUIDE
    );

    public static final Map<String, Diagnosis.Definition> ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    "migrate_data_tiers_require_data_" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices to any nodes in the ["
                        + tier
                        + "] data tier because the indices are configured with allocation filter rules that are incompatible with the "
                        + "nodes in this tier.",
                    "Remove ["
                        + INDEX_ROUTING_REQUIRE_GROUP_PREFIX
                        + ".data] from the index settings or try migrating to data tiers by first stopping ILM [POST /_ilm/stop] and then "
                        + "using the data tier migration action [POST /_ilm/migrate_to_data_tiers]. "
                        + "Finally, restart ILM [POST /_ilm/start].",
                    MIGRATE_TO_TIERS_ACTION_GUIDE
                )
            )
        );

    public static final Diagnosis.Definition ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA = new Diagnosis.Definition(
        "migrate_data_tiers_include_data",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any nodes in the desired data tiers because the "
            + "indices are configured with allocation filter rules that are incompatible with the nodes in this tier. ",
        "Remove ["
            + INDEX_ROUTING_INCLUDE_GROUP_PREFIX
            + ".data] from the index settings or try migrating to data tiers by first stopping ILM [POST /_ilm/stop] and then using "
            + "the data tier migration action [POST /_ilm/migrate_to_data_tiers]. Finally, restart ILM [POST /_ilm/start].",
        MIGRATE_TO_TIERS_ACTION_GUIDE
    );

    public static final Map<String, Diagnosis.Definition> ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    "migrate_data_tiers_include_data_" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices to any nodes in the ["
                        + tier
                        + "] data tier because the indices are configured with allocation filter rules that are incompatible with the "
                        + "nodes in this tier.",
                    "Remove ["
                        + INDEX_ROUTING_INCLUDE_GROUP_PREFIX
                        + ".data] from the index settings or try migrating to data tiers by first stopping ILM [POST /_ilm/stop] and then "
                        + "using the data tier migration action [POST /_ilm/migrate_to_data_tiers]. Finally, restart ILM "
                        + "[POST /_ilm/start].",
                    MIGRATE_TO_TIERS_ACTION_GUIDE
                )
            )
        );

    public static final String TIER_CAPACITY_ACTION_GUIDE = "http://ela.st/tier-capacity";
    public static final Diagnosis.Definition ACTION_INCREASE_NODE_CAPACITY = new Diagnosis.Definition(
        "increase_node_capacity_for_allocations",
        "Elasticsearch isn't allowed to allocate some shards from these indices because there are not enough nodes in the cluster to "
            + "allocate each shard copy on a different node.",
        "Increase the number of nodes in the cluster or decrease the number of replica shards in the affected indices.",
        TIER_CAPACITY_ACTION_GUIDE
    );

    public static final Map<String, Diagnosis.Definition> ACTION_INCREASE_TIER_CAPACITY_LOOKUP = DataTier.ALL_DATA_TIERS.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    "increase_tier_capacity_for_allocations_" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices to any of the nodes in the desired data tier "
                        + "because there are not enough nodes in the ["
                        + tier
                        + "] tier to allocate each shard copy on a different node.",
                    "Increase the number of nodes in this tier or decrease the number of replica shards in the affected indices.",
                    TIER_CAPACITY_ACTION_GUIDE
                )
            )
        );

    private class ShardAllocationCounts {
        private boolean available = true; // This will be true even if no replicas are expected, as long as none are unavailable
        private int unassigned = 0;
        private int unassigned_new = 0;
        private int unassigned_restarting = 0;
        private int initializing = 0;
        private int started = 0;
        private int relocating = 0;
        private final Set<String> indicesWithUnavailableShards = new HashSet<>();
        private final Map<Diagnosis.Definition, Set<String>> userActions = new HashMap<>();

        public void increment(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns, boolean explain) {
            boolean isNew = isUnassignedDueToNewInitialization(routing);
            boolean isRestarting = isUnassignedDueToTimelyRestart(routing, shutdowns);
            available &= routing.active() || isRestarting || isNew;
            if ((routing.active() || isRestarting || isNew) == false) {
                indicesWithUnavailableShards.add(routing.getIndexName());
            }

            switch (routing.state()) {
                case UNASSIGNED -> {
                    if (isNew) {
                        unassigned_new++;
                    } else if (isRestarting) {
                        unassigned_restarting++;
                    } else {
                        unassigned++;
                        if (explain) {
                            diagnoseUnassignedShardRouting(routing, state).forEach(
                                definition -> addUserAction(definition, routing.getIndexName())
                            );
                        }
                    }
                }
                case INITIALIZING -> initializing++;
                case STARTED -> started++;
                case RELOCATING -> relocating++;
            }
        }

        private void addUserAction(Diagnosis.Definition actionDef, String indexName) {
            userActions.computeIfAbsent(actionDef, (k) -> new HashSet<>()).add(indexName);
        }
    }

    private static boolean isUnassignedDueToTimelyRestart(ShardRouting routing, NodesShutdownMetadata shutdowns) {
        var info = routing.unassignedInfo();
        if (info == null || info.getReason() != UnassignedInfo.Reason.NODE_RESTARTING) {
            return false;
        }
        var shutdown = shutdowns.getAllNodeMetadataMap().get(info.getLastAllocatedNodeId());
        if (shutdown == null || shutdown.getType() != SingleNodeShutdownMetadata.Type.RESTART) {
            return false;
        }
        var now = System.nanoTime();
        var restartingAllocationDelayExpiration = info.getUnassignedTimeInNanos() + shutdown.getAllocationDelay().nanos();
        return now <= restartingAllocationDelayExpiration;
    }

    private static boolean isUnassignedDueToNewInitialization(ShardRouting routing) {
        return routing.primary() && routing.active() == false && getInactivePrimaryHealth(routing) == ClusterHealthStatus.YELLOW;
    }

    /**
     * Generate a list of actions for a user to take that should allow this shard to be assigned.
     * @param shardRouting An unassigned shard routing
     * @param state State of the cluster
     * @return A list of actions for the user to take for this shard
     */
    List<Diagnosis.Definition> diagnoseUnassignedShardRouting(ShardRouting shardRouting, ClusterState state) {
        List<Diagnosis.Definition> actions = new ArrayList<>();
        LOGGER.trace("Diagnosing unassigned shard [{}] due to reason [{}]", shardRouting.shardId(), shardRouting.unassignedInfo());
        switch (shardRouting.unassignedInfo().getLastAllocationStatus()) {
            case NO_VALID_SHARD_COPY:
                if (UnassignedInfo.Reason.NODE_LEFT == shardRouting.unassignedInfo().getReason()) {
                    actions.add(ACTION_RESTORE_FROM_SNAPSHOT);
                }
                break;
            case DECIDERS_NO:
                actions.addAll(explainAllocationsAndDiagnoseDeciders(shardRouting, state));
                break;
            default:
                break;
        }
        if (actions.isEmpty()) {
            actions.add(ACTION_CHECK_ALLOCATION_EXPLAIN_API);
        }
        return actions;
    }

    /**
     * For a shard that is unassigned due to a DECIDERS_NO result, this will explain the allocation and attempt to generate
     * user actions that should allow the shard to be assigned.
     * @param shardRouting The shard routing that is unassigned with a last status of DECIDERS_NO
     * @param state Current cluster state
     * @return a list of actions for the user to take
     */
    private List<Diagnosis.Definition> explainAllocationsAndDiagnoseDeciders(ShardRouting shardRouting, ClusterState state) {
        LOGGER.trace("Executing allocation explain on shard [{}]", shardRouting.shardId());
        RoutingAllocation allocation = new RoutingAllocation(
            allocationService.getAllocationDeciders(),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
        allocation.setDebugMode(RoutingAllocation.DebugMode.ON);
        ShardAllocationDecision shardAllocationDecision = allocationService.explainShardAllocation(shardRouting, allocation);
        AllocateUnassignedDecision allocateDecision = shardAllocationDecision.getAllocateDecision();
        if (LOGGER.isTraceEnabled()) {
            if (allocateDecision.isDecisionTaken()) {
                LOGGER.trace("[{}]: Allocation decision [{}]", shardRouting.shardId(), allocateDecision.getAllocationDecision());
            } else {
                LOGGER.trace("[{}]: Decision taken [false]", shardRouting.shardId());
            }
        }
        if (allocateDecision.isDecisionTaken() && AllocationDecision.NO == allocateDecision.getAllocationDecision()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "[{}]: Working with decisions: [{}]",
                    shardRouting.shardId(),
                    allocateDecision.getNodeDecisions()
                        .stream()
                        .map(
                            n -> n.getCanAllocateDecision()
                                .getDecisions()
                                .stream()
                                .map(d -> d.label() + ": " + d.type())
                                .collect(Collectors.toList())
                        )
                        .collect(Collectors.toList())
                );
            }
            List<NodeAllocationResult> nodeAllocationResults = allocateDecision.getNodeDecisions();
            return diagnoseAllocationResults(shardRouting, state, nodeAllocationResults);
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Generates a list of user actions to take for an unassigned shard by inspecting a list of NodeAllocationResults for
     * well known problems.
     * @param shardRouting The unassigned shard.
     * @param state Current cluster state.
     * @param nodeAllocationResults A list of results for each node in the cluster from the allocation explain api
     * @return A list of user actions to take.
     */
    List<Diagnosis.Definition> diagnoseAllocationResults(
        ShardRouting shardRouting,
        ClusterState state,
        List<NodeAllocationResult> nodeAllocationResults
    ) {
        IndexMetadata index = state.metadata().index(shardRouting.index());
        List<Diagnosis.Definition> actions = new ArrayList<>();
        if (index != null) {
            actions.addAll(checkIsAllocationDisabled(index, nodeAllocationResults));
            actions.addAll(checkDataTierRelatedIssues(index, nodeAllocationResults, state));
        }
        if (actions.isEmpty()) {
            actions.add(ACTION_CHECK_ALLOCATION_EXPLAIN_API);
        }
        return actions;
    }

    /**
     * Convenience method for filtering node allocation results by decider outcomes.
     * @param deciderName The decider that is being checked
     * @param outcome The outcome expected
     * @return A predicate that returns true if the decision exists and matches the expected outcome, false otherwise.
     */
    private static Predicate<NodeAllocationResult> hasDeciderResult(String deciderName, Decision.Type outcome) {
        return (nodeResult) -> nodeResult.getCanAllocateDecision()
            .getDecisions()
            .stream()
            .anyMatch(decision -> deciderName.equals(decision.label()) && outcome == decision.type());
    }

    /**
     * Generates a user action if a shard cannot be allocated anywhere because allocation is disabled for that shard
     * @param indexMetadata from the index shard being diagnosed
     * @param nodeAllocationResults allocation decision results for all nodes in the cluster.
     * @return A list of user actions to take.
     */
    List<Diagnosis.Definition> checkIsAllocationDisabled(IndexMetadata indexMetadata, List<NodeAllocationResult> nodeAllocationResults) {
        List<Diagnosis.Definition> actions = new ArrayList<>();
        if (nodeAllocationResults.stream().allMatch(hasDeciderResult(EnableAllocationDecider.NAME, Decision.Type.NO))) {
            // Check the routing settings for index
            Settings indexSettings = indexMetadata.getSettings();
            EnableAllocationDecider.Allocation indexLevelAllocation = INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.get(indexSettings);
            ClusterSettings clusterSettings = clusterService.getClusterSettings();
            EnableAllocationDecider.Allocation clusterLevelAllocation = clusterSettings.get(
                EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING
            );
            if (EnableAllocationDecider.Allocation.ALL != indexLevelAllocation) {
                // Index setting is not ALL
                actions.add(ACTION_ENABLE_INDEX_ROUTING_ALLOCATION);
            }
            if (EnableAllocationDecider.Allocation.ALL != clusterLevelAllocation) {
                // Cluster setting is not ALL
                actions.add(ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION);
            }
        }
        return actions;
    }

    /**
     * Generates user actions for common problems that keep a shard from allocating to nodes in a data tier.
     * @param indexMetadata Index metadata for the shard being diagnosed.
     * @param nodeAllocationResults allocation decision results for all nodes in the cluster.
     * @param clusterState the current cluster state.
     * @return a list of user actions to take.
     */
    List<Diagnosis.Definition> checkDataTierRelatedIssues(
        IndexMetadata indexMetadata,
        List<NodeAllocationResult> nodeAllocationResults,
        ClusterState clusterState
    ) {
        List<Diagnosis.Definition> actions = new ArrayList<>();
        if (indexMetadata.getTierPreference().size() > 0) {
            List<NodeAllocationResult> dataTierAllocationResults = nodeAllocationResults.stream()
                .filter(hasDeciderResult(DATA_TIER_ALLOCATION_DECIDER_NAME, Decision.Type.YES))
                .toList();
            if (dataTierAllocationResults.isEmpty()) {
                // Shard must be allocated on specific tiers but no nodes were enabled for those tiers.
                for (String tier : indexMetadata.getTierPreference()) {
                    Optional.ofNullable(ACTION_ENABLE_TIERS_LOOKUP.get(tier)).ifPresent(actions::add);
                }
            } else {
                // Collect the nodes from the tiers this index is allowed on
                Set<DiscoveryNode> dataTierNodes = dataTierAllocationResults.stream()
                    .map(NodeAllocationResult::getNode)
                    .collect(Collectors.toSet());

                // Determine the unique roles available on the allowed tier nodes
                Set<String> dataTierRolesAvailable = dataTierNodes.stream()
                    .map(DiscoveryNode::getRoles)
                    .flatMap(Set::stream)
                    .map(DiscoveryNodeRole::roleName)
                    .collect(Collectors.toSet());

                // Determine which tier this index would most prefer to live on
                String preferredTier = indexMetadata.getTierPreference()
                    .stream()
                    .filter(dataTierRolesAvailable::contains)
                    .findFirst()
                    .orElse(null);

                // Run checks for data tier specific problems
                actions.addAll(
                    checkDataTierAtShardLimit(indexMetadata, clusterState, dataTierAllocationResults, dataTierNodes, preferredTier)
                );
                actions.addAll(checkDataTierShouldMigrate(indexMetadata, dataTierAllocationResults, preferredTier, dataTierNodes));
                checkNotEnoughNodesInDataTier(dataTierAllocationResults, preferredTier).ifPresent(actions::add);
            }
        }
        return actions;
    }

    private List<Diagnosis.Definition> checkDataTierAtShardLimit(
        IndexMetadata indexMetadata,
        ClusterState clusterState,
        List<NodeAllocationResult> dataTierAllocationResults,
        Set<DiscoveryNode> dataTierNodes,
        @Nullable String preferredTier
    ) {
        // All tier nodes at shards limit?
        if (dataTierAllocationResults.stream().allMatch(hasDeciderResult(ShardsLimitAllocationDecider.NAME, Decision.Type.NO))) {
            List<Diagnosis.Definition> actions = new ArrayList<>();
            // We need the routing nodes for the tiers this index is allowed on to determine the offending shard limits
            List<RoutingNode> dataTierRoutingNodes = clusterState.getRoutingNodes()
                .stream()
                .filter(routingNode -> dataTierNodes.contains(routingNode.node()))
                .toList();

            // Determine which total_shards_per_node settings are present
            Integer clusterShardsPerNode = clusterService.getClusterSettings().get(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING);
            Integer indexShardsPerNode = INDEX_TOTAL_SHARDS_PER_NODE_SETTING.get(indexMetadata.getSettings());
            assert (clusterShardsPerNode > 0 || indexShardsPerNode > 0) : "shards per node must exist if allocation decision is NO";

            // Determine which total_shards_per_node settings are keeping things from allocating
            boolean clusterShardsPerNodeShouldChange = false;
            if (clusterShardsPerNode > 0) {
                int minShardCountInTier = dataTierRoutingNodes.stream()
                    .map(RoutingNode::numberOfOwningShards)
                    .min(Integer::compareTo)
                    .orElse(-1);
                clusterShardsPerNodeShouldChange = minShardCountInTier >= clusterShardsPerNode;
            }
            boolean indexShardsPerNodeShouldChange = false;
            if (indexShardsPerNode > 0) {
                int minShardCountInTier = dataTierRoutingNodes.stream()
                    .map(routingNode -> routingNode.numberOfOwningShardsForIndex(indexMetadata.getIndex()))
                    .min(Integer::compareTo)
                    .orElse(-1);
                indexShardsPerNodeShouldChange = minShardCountInTier >= indexShardsPerNode;
            }

            // Add appropriate user action
            if (preferredTier != null) {
                // We cannot allocate the shard to the most preferred tier because a shard limit is reached.
                if (clusterShardsPerNodeShouldChange) {
                    Optional.ofNullable(ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP.get(preferredTier)).ifPresent(actions::add);
                }
                if (indexShardsPerNodeShouldChange) {
                    Optional.ofNullable(ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP.get(preferredTier)).ifPresent(actions::add);
                }
            } else {
                // We couldn't determine a desired tier. This is likely because there are no tiers in the cluster,
                // only `data` nodes. Give a generic ask for increasing the shard limit.
                if (clusterShardsPerNodeShouldChange) {
                    actions.add(ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING);
                }
                if (indexShardsPerNodeShouldChange) {
                    actions.add(ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING);
                }
            }
            return actions;
        } else {
            return Collections.emptyList();
        }
    }

    private List<Diagnosis.Definition> checkDataTierShouldMigrate(
        IndexMetadata indexMetadata,
        List<NodeAllocationResult> dataTierAllocationResults,
        @Nullable String preferredTier,
        Set<DiscoveryNode> dataTierNodes
    ) {
        // Check if index has filter requirements on the old "data" attribute that might be keeping it from allocating.
        if (dataTierAllocationResults.stream().allMatch(hasDeciderResult(FilterAllocationDecider.NAME, Decision.Type.NO))) {
            List<Diagnosis.Definition> actions = new ArrayList<>();
            Map<String, List<String>> requireAttributes = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(indexMetadata.getSettings());
            List<String> requireDataAttributes = requireAttributes.get("data");
            DiscoveryNodeFilters requireFilter = requireDataAttributes == null
                ? null
                : DiscoveryNodeFilters.buildFromKeyValues(DiscoveryNodeFilters.OpType.AND, Map.of("data", requireDataAttributes));

            Map<String, List<String>> includeAttributes = INDEX_ROUTING_INCLUDE_GROUP_SETTING.getAsMap(indexMetadata.getSettings());
            List<String> includeDataAttributes = includeAttributes.get("data");
            DiscoveryNodeFilters includeFilter = includeDataAttributes == null
                ? null
                : DiscoveryNodeFilters.buildFromKeyValues(DiscoveryNodeFilters.OpType.OR, Map.of("data", includeDataAttributes));
            if (requireFilter != null || includeFilter != null) {
                // Check if the data tier nodes this shard is allowed on have data attributes that match
                if (requireFilter != null && dataTierNodes.stream().noneMatch(requireFilter::match)) {
                    // No data tier nodes match the required data attribute
                    actions.add(
                        Optional.ofNullable(preferredTier)
                            .map(ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA_LOOKUP::get)
                            .orElse(ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA)
                    );
                }
                if (includeFilter != null && dataTierNodes.stream().noneMatch(includeFilter::match)) {
                    // No data tier nodes match the included data attributes
                    actions.add(
                        Optional.ofNullable(preferredTier)
                            .map(ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA_LOOKUP::get)
                            .orElse(ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA)
                    );
                }
            }
            return actions;
        } else {
            return Collections.emptyList();
        }
    }

    private Optional<Diagnosis.Definition> checkNotEnoughNodesInDataTier(
        List<NodeAllocationResult> dataTierAllocationResults,
        @Nullable String preferredTier
    ) {
        // Not enough tier nodes to hold shards on different nodes?
        if (dataTierAllocationResults.stream().allMatch(hasDeciderResult(SameShardAllocationDecider.NAME, Decision.Type.NO))) {
            // We couldn't determine a desired tier. This is likely because there are no tiers in the cluster,
            // only `data` nodes. Give a generic ask for increasing the shard limit.
            if (preferredTier != null) {
                return Optional.ofNullable(ACTION_INCREASE_TIER_CAPACITY_LOOKUP.get(preferredTier));
            } else {
                return Optional.of(ACTION_INCREASE_NODE_CAPACITY);
            }
        } else {
            return Optional.empty();
        }
    }

    private class ShardAllocationStatus {
        private final ShardAllocationCounts primaries = new ShardAllocationCounts();
        private final ShardAllocationCounts replicas = new ShardAllocationCounts();
        private final Metadata clusterMetadata;

        ShardAllocationStatus(Metadata clusterMetadata) {
            this.clusterMetadata = clusterMetadata;
        }

        public void addPrimary(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns, boolean explain) {
            primaries.increment(routing, state, shutdowns, explain);
        }

        public void addReplica(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns, boolean explain) {
            replicas.increment(routing, state, shutdowns, explain);
        }

        public HealthStatus getStatus() {
            if (primaries.available == false) {
                return RED;
            } else if (replicas.available == false) {
                return YELLOW;
            } else {
                return GREEN;
            }
        }

        public String getSummary() {
            var builder = new StringBuilder("This cluster has ");
            if (primaries.unassigned > 0
                || primaries.unassigned_new > 0
                || primaries.unassigned_restarting > 0
                || replicas.unassigned > 0
                || replicas.unassigned_restarting > 0) {
                builder.append(
                    Stream.of(
                        createMessage(primaries.unassigned, "unavailable primary", "unavailable primaries"),
                        createMessage(primaries.unassigned_new, "creating primary", "creating primaries"),
                        createMessage(primaries.unassigned_restarting, "restarting primary", "restarting primaries"),
                        createMessage(replicas.unassigned, "unavailable replica", "unavailable replicas"),
                        createMessage(replicas.unassigned_restarting, "restarting replica", "restarting replicas")
                    ).flatMap(Function.identity()).collect(joining(", "))
                ).append(".");
            } else {
                builder.append("all shards available.");
            }
            return builder.toString();
        }

        private static Stream<String> createMessage(int count, String singular, String plural) {
            return switch (count) {
                case 0 -> Stream.empty();
                case 1 -> Stream.of("1 " + singular);
                default -> Stream.of(count + " " + plural);
            };
        }

        public HealthIndicatorDetails getDetails(boolean explain) {
            if (explain) {
                return new SimpleHealthIndicatorDetails(
                    Map.of(
                        "unassigned_primaries",
                        primaries.unassigned,
                        "initializing_primaries",
                        primaries.initializing,
                        "creating_primaries",
                        primaries.unassigned_new,
                        "restarting_primaries",
                        primaries.unassigned_restarting,
                        "started_primaries",
                        primaries.started + primaries.relocating,
                        "unassigned_replicas",
                        replicas.unassigned,
                        "initializing_replicas",
                        replicas.initializing,
                        "restarting_replicas",
                        replicas.unassigned_restarting,
                        "started_replicas",
                        replicas.started + replicas.relocating
                    )
                );
            } else {
                return HealthIndicatorDetails.EMPTY;
            }
        }

        public List<HealthIndicatorImpact> getImpacts() {
            final List<HealthIndicatorImpact> impacts = new ArrayList<>();
            if (primaries.indicesWithUnavailableShards.isEmpty() == false) {
                String impactDescription = String.format(
                    Locale.ROOT,
                    "Cannot add data to %d %s [%s]. Searches might return incomplete results.",
                    primaries.indicesWithUnavailableShards.size(),
                    primaries.indicesWithUnavailableShards.size() == 1 ? "index" : "indices",
                    getTruncatedIndicesString(primaries.indicesWithUnavailableShards, clusterMetadata)
                );
                impacts.add(new HealthIndicatorImpact(1, impactDescription, List.of(ImpactArea.INGEST, ImpactArea.SEARCH)));
            }
            /*
             * It is possible that we're working with an intermediate cluster state, and that for an index we have no primary but a replica
             * that is reported as unavailable. That replica is likely being promoted to primary. The only impact that matters at this
             * point is the one above, which has already been reported for this index.
             */
            Set<String> indicesWithUnavailableReplicasOnly = new HashSet<>(replicas.indicesWithUnavailableShards);
            indicesWithUnavailableReplicasOnly.removeAll(primaries.indicesWithUnavailableShards);
            if (indicesWithUnavailableReplicasOnly.isEmpty() == false) {
                String impactDescription = String.format(
                    Locale.ROOT,
                    "Searches might be slower than usual. Fewer redundant copies of the data exist on %d %s [%s].",
                    indicesWithUnavailableReplicasOnly.size(),
                    indicesWithUnavailableReplicasOnly.size() == 1 ? "index" : "indices",
                    getTruncatedIndicesString(indicesWithUnavailableReplicasOnly, clusterMetadata)
                );
                impacts.add(new HealthIndicatorImpact(2, impactDescription, List.of(ImpactArea.SEARCH)));
            }
            return impacts;
        }

        /**
         * Summarizes the user actions that are needed to solve unassigned primary and replica shards.
         * @param explain true if user actions should be generated, false if they should be omitted.
         * @return A summary of user actions. Alternatively, an empty list if none were found or explain is false.
         */
        public List<Diagnosis> getUserActions(boolean explain) {
            if (explain) {
                Map<Diagnosis.Definition, Set<String>> actionsToAffectedIndices = new HashMap<>(primaries.userActions);
                replicas.userActions.forEach((actionDefinition, indicesWithReplicasUnassigned) -> {
                    Set<String> indicesWithPrimariesUnassigned = actionsToAffectedIndices.get(actionDefinition);
                    if (indicesWithPrimariesUnassigned == null) {
                        actionsToAffectedIndices.put(actionDefinition, indicesWithReplicasUnassigned);
                    } else {
                        indicesWithPrimariesUnassigned.addAll(indicesWithReplicasUnassigned);
                    }
                });
                if (actionsToAffectedIndices.isEmpty()) {
                    return Collections.emptyList();
                } else {
                    return actionsToAffectedIndices.entrySet()
                        .stream()
                        .map(
                            e -> new Diagnosis(
                                e.getKey(),
                                e.getValue().stream().sorted(byPriorityThenByName(clusterMetadata)).collect(Collectors.toList())
                            )
                        )
                        .collect(Collectors.toList());
                }
            } else {
                return Collections.emptyList();
            }
        }
    }

    private static String getTruncatedIndicesString(Set<String> indices, Metadata clusterMetadata) {
        final int maxIndices = 10;
        String truncatedIndicesString = indices.stream()
            .sorted(byPriorityThenByName(clusterMetadata))
            .limit(maxIndices)
            .collect(joining(", "));
        if (maxIndices < indices.size()) {
            truncatedIndicesString = truncatedIndicesString + ", ...";
        }
        return truncatedIndicesString;
    }

    /**
     * Sorts index names by their priority first, then alphabetically by name. If the priority cannot be determined for an index then
     * a priority of -1 is used to sort it behind other index names.
     * @param clusterMetadata Used to look up index priority.
     * @return Comparator instance
     */
    private static Comparator<String> byPriorityThenByName(Metadata clusterMetadata) {
        // We want to show indices with a numerically higher index.priority first (since lower priority ones might get truncated):
        return Comparator.comparingInt((String indexName) -> {
            IndexMetadata indexMetadata = clusterMetadata.index(indexName);
            return indexMetadata == null ? -1 : indexMetadata.priority();
        }).reversed().thenComparing(Comparator.naturalOrder());
    }
}
