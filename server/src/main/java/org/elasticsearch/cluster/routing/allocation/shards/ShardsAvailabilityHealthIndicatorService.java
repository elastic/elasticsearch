/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.shards;

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
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.ArrayList;
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
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.health.ClusterShardHealth.getInactivePrimaryHealth;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.health.Diagnosis.Resource.Type.FEATURE_STATE;
import static org.elasticsearch.health.Diagnosis.Resource.Type.INDEX;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getTruncatedIndices;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.indicesComparatorByPriorityAndName;

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

    private final SystemIndices systemIndices;

    public ShardsAvailabilityHealthIndicatorService(
        ClusterService clusterService,
        AllocationService allocationService,
        SystemIndices systemIndices
    ) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.systemIndices = systemIndices;
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * Creates a new {@link ShardAllocationStatus} that will be used to track
     * primary and replica availability, providing the color, diagnosis, and
     * messages about the available or unavailable shards in the cluster.
     * @param metadata Metadata for the cluster
     * @return A new ShardAllocationStatus that has not yet been filled.
     */
    public ShardAllocationStatus createNewStatus(Metadata metadata) {
        return new ShardAllocationStatus(metadata);
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        var state = clusterService.state();
        var shutdown = state.getMetadata().custom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY);
        var status = createNewStatus(state.getMetadata());
        updateShardAllocationStatus(status, state, shutdown, verbose);
        return createIndicator(
            status.getStatus(),
            status.getSymptom(),
            status.getDetails(verbose),
            status.getImpacts(),
            status.getDiagnosis(verbose, maxAffectedResourcesCount)
        );
    }

    static void updateShardAllocationStatus(
        ShardAllocationStatus status,
        ClusterState state,
        NodesShutdownMetadata shutdown,
        boolean verbose
    ) {
        for (IndexRoutingTable indexShardRouting : state.routingTable()) {
            for (int i = 0; i < indexShardRouting.size(); i++) {
                IndexShardRoutingTable shardRouting = indexShardRouting.shard(i);
                status.addPrimary(shardRouting.primaryShard(), state, shutdown, verbose);
                for (ShardRouting replicaShard : shardRouting.replicaShards()) {
                    status.addReplica(replicaShard, state, shutdown, verbose);
                }
            }
        }

        status.updateSearchableSnapshotsOfAvailableIndices();
    }

    // Impact IDs
    public static final String PRIMARY_UNASSIGNED_IMPACT_ID = "primary_unassigned";
    public static final String READ_ONLY_PRIMARY_UNASSIGNED_IMPACT_ID = "read_only_primary_unassigned";
    public static final String REPLICA_UNASSIGNED_IMPACT_ID = "replica_unassigned";

    public static final String RESTORE_FROM_SNAPSHOT_ACTION_GUIDE = "https://ela.st/restore-snapshot";
    public static final Diagnosis.Definition ACTION_RESTORE_FROM_SNAPSHOT = new Diagnosis.Definition(
        NAME,
        "restore_from_snapshot",
        "Elasticsearch isn't allowed to allocate some shards because there are no copies of the shards in the cluster. Elasticsearch will "
            + "allocate these shards when nodes holding good copies of the data join the cluster.",
        "If no such node is available, restore these indices from a recent snapshot.",
        RESTORE_FROM_SNAPSHOT_ACTION_GUIDE
    );

    public static final String DIAGNOSE_SHARDS_ACTION_GUIDE = "https://ela.st/diagnose-shards";
    public static final Diagnosis.Definition ACTION_CHECK_ALLOCATION_EXPLAIN_API = new Diagnosis.Definition(
        NAME,
        "explain_allocations",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any of the nodes in the cluster.",
        "Diagnose the issue by calling the allocation explain API for an index [GET _cluster/allocation/explain]. Choose a node to which "
            + "you expect a shard to be allocated, find this node in the node-by-node explanation, and address the reasons which prevent "
            + "Elasticsearch from allocating the shard.",
        DIAGNOSE_SHARDS_ACTION_GUIDE
    );

    public static final String FIX_DELAYED_SHARDS_GUIDE = "https://ela.st/fix-delayed-shard-allocation";
    public static final Diagnosis.Definition DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS = new Diagnosis.Definition(
        NAME,
        "delayed_shard_allocations",
        "Elasticsearch is not allocating some shards because they are marked for delayed allocation. Shards that have become "
            + "unavailable are usually marked for delayed allocation because it is more efficient to wait and see if the shards return "
            + "on their own than to recover the shard immediately.",
        "Elasticsearch will reallocate the shards when the delay has elapsed. No action is required by the user.",
        FIX_DELAYED_SHARDS_GUIDE
    );

    public static final String WAIT_FOR_INITIALIZATION_GUIDE = "https://ela.st/wait-for-shard-initialization";
    public static final Diagnosis.Definition DIAGNOSIS_WAIT_FOR_INITIALIZATION = new Diagnosis.Definition(
        NAME,
        "initializing_shards",
        "Elasticsearch is currently initializing the unavailable shards. Please wait for the initialization to finish.",
        "The shards will become available as long as the initialization completes. No action is required by the user, you can"
            + " monitor the progress of the initializing shards at "
            + WAIT_FOR_INITIALIZATION_GUIDE
            + ".",
        WAIT_FOR_INITIALIZATION_GUIDE
    );

    public static final String ENABLE_INDEX_ALLOCATION_GUIDE = "https://ela.st/fix-index-allocation";
    public static final Diagnosis.Definition ACTION_ENABLE_INDEX_ROUTING_ALLOCATION = new Diagnosis.Definition(
        NAME,
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
    public static final String ENABLE_CLUSTER_ALLOCATION_ACTION_GUIDE = "https://ela.st/fix-cluster-allocation";
    public static final Diagnosis.Definition ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION = new Diagnosis.Definition(
        NAME,
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

    public static final String ENABLE_TIER_ACTION_GUIDE = "https://ela.st/enable-tier";
    private static final Map<String, Diagnosis.Definition> ACTION_ENABLE_TIERS_LOOKUP = DataTier.ALL_DATA_TIERS.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "enable_data_tiers:tier:" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices because the indices expect to be allocated to "
                        + "data tier nodes, but there were not any nodes with the expected tiers found in the cluster.",
                    "Add nodes with the [" + tier + "] role to the cluster.",
                    ENABLE_TIER_ACTION_GUIDE
                )
            )
        );

    public static final String INCREASE_SHARD_LIMIT_ACTION_GUIDE = "https://ela.st/index-total-shards";
    public static final Diagnosis.Definition ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING = new Diagnosis.Definition(
        NAME,
        "increase_shard_limit_index_setting",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any data nodes because each node has reached the index "
            + "shard limit. ",
        "Increase the values for the ["
            + INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
            + "] index setting on each index or add more nodes to the target tiers.",
        INCREASE_SHARD_LIMIT_ACTION_GUIDE
    );

    private static final Map<String, Diagnosis.Definition> ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "increase_shard_limit_index_setting:tier:" + tier,
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

    public static final String INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE = "https://ela.st/cluster-total-shards";
    public static final Diagnosis.Definition ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING = new Diagnosis.Definition(
        NAME,
        "increase_shard_limit_cluster_setting",
        "Elasticsearch isn't allowed to allocate some shards from these indices to any data nodes because each node has reached the "
            + "cluster shard limit.",
        "Increase the values for the ["
            + CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey()
            + "] cluster setting or add more nodes to the target tiers.",
        INCREASE_CLUSTER_SHARD_LIMIT_ACTION_GUIDE
    );

    private static final Map<String, Diagnosis.Definition> ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP = DataTier.ALL_DATA_TIERS
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "increase_shard_limit_cluster_setting:tier:" + tier,
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

    public static final String MIGRATE_TO_TIERS_ACTION_GUIDE = "https://ela.st/migrate-to-tiers";
    public static final Diagnosis.Definition ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA = new Diagnosis.Definition(
        NAME,
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
                    NAME,
                    "migrate_data_tiers_require_data:tier:" + tier,
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
        NAME,
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
                    NAME,
                    "migrate_data_tiers_include_data:tier:" + tier,
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

    public static final String TIER_CAPACITY_ACTION_GUIDE = "https://ela.st/tier-capacity";
    public static final Diagnosis.Definition ACTION_INCREASE_NODE_CAPACITY = new Diagnosis.Definition(
        NAME,
        "increase_node_capacity_for_allocations",
        "Elasticsearch isn't allowed to allocate some shards from these indices because there are not enough nodes in the cluster to "
            + "allocate each shard copy on a different node.",
        "Increase the number of nodes in the cluster or decrease the number of replica shards in the affected indices.",
        TIER_CAPACITY_ACTION_GUIDE
    );

    // Visible for testing
    public static final Map<String, Diagnosis.Definition> ACTION_INCREASE_TIER_CAPACITY_LOOKUP = DataTier.ALL_DATA_TIERS.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                tier -> tier,
                tier -> new Diagnosis.Definition(
                    NAME,
                    "increase_tier_capacity_for_allocations:tier:" + tier,
                    "Elasticsearch isn't allowed to allocate some shards from these indices to any of the nodes in the desired data tier "
                        + "because there are not enough nodes in the ["
                        + tier
                        + "] tier to allocate each shard copy on a different node.",
                    "Increase the number of nodes in this tier or decrease the number of replica shards in the affected indices.",
                    TIER_CAPACITY_ACTION_GUIDE
                )
            )
        );

    public class ShardAllocationCounts {
        int unassigned = 0;
        int unassigned_new = 0;
        int unassigned_restarting = 0;
        int initializing = 0;
        int started = 0;
        int relocating = 0;
        public final Set<String> indicesWithUnavailableShards = new HashSet<>();
        public final Set<String> indicesWithAllShardsUnavailable = new HashSet<>();
        // We keep the searchable snapshots separately as long as the original index is still available
        // This is checked during the post-processing
        public SearchableSnapshotsState searchableSnapshotsState = new SearchableSnapshotsState();
        final Map<Diagnosis.Definition, Set<String>> diagnosisDefinitions = new HashMap<>();

        public void increment(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns, boolean verbose) {
            boolean isNew = isUnassignedDueToNewInitialization(routing, state);
            boolean isRestarting = isUnassignedDueToTimelyRestart(routing, shutdowns);
            boolean allUnavailable = areAllShardsOfThisTypeUnavailable(routing, state);
            if (allUnavailable) {
                indicesWithAllShardsUnavailable.add(routing.getIndexName());
            }
            if ((routing.active() || isRestarting || isNew) == false) {
                String indexName = routing.getIndexName();
                Settings indexSettings = state.getMetadata().index(indexName).getSettings();
                if (SearchableSnapshotsSettings.isSearchableSnapshotStore(indexSettings)) {
                    searchableSnapshotsState.addSearchableSnapshotWithUnavailableShard(indexName);
                } else {
                    indicesWithUnavailableShards.add(indexName);
                }
            }

            switch (routing.state()) {
                case UNASSIGNED -> {
                    if (isNew) {
                        unassigned_new++;
                    } else if (isRestarting) {
                        unassigned_restarting++;
                    } else {
                        unassigned++;
                        if (verbose) {
                            diagnoseUnassignedShardRouting(routing, state).forEach(
                                definition -> addDefinition(definition, routing.getIndexName())
                            );
                        }
                    }
                }
                case INITIALIZING -> {
                    initializing++;
                    if (verbose) {
                        addDefinition(DIAGNOSIS_WAIT_FOR_INITIALIZATION, routing.getIndexName());
                    }
                }
                case STARTED -> started++;
                case RELOCATING -> relocating++;
            }
        }

        public boolean areAllAvailable() {
            return indicesWithUnavailableShards.isEmpty();
        }

        public boolean doAnyIndicesHaveAllUnavailable() {
            return indicesWithAllShardsUnavailable.isEmpty() == false;
        }

        private void addDefinition(Diagnosis.Definition diagnosisDefinition, String indexName) {
            diagnosisDefinitions.computeIfAbsent(diagnosisDefinition, (k) -> new HashSet<>()).add(indexName);
        }
    }

    /**
     * Returns true if all the shards of the same type (primary or replica) are unassigned. For
     * example: if a replica is passed then this will return true if ALL replicas are unassigned,
     * but if at least one is assigned, it will return false.
     */
    private boolean areAllShardsOfThisTypeUnavailable(ShardRouting routing, ClusterState state) {
        return StreamSupport.stream(
            state.routingTable().allActiveShardsGrouped(new String[] { routing.getIndexName() }, true).spliterator(),
            false
        )
            .flatMap(shardIter -> shardIter.getShardRoutings().stream())
            .filter(sr -> sr.shardId().equals(routing.shardId()))
            .filter(sr -> sr.primary() == routing.primary())
            .allMatch(ShardRouting::unassigned);
    }

    private static boolean isUnassignedDueToTimelyRestart(ShardRouting routing, NodesShutdownMetadata shutdowns) {
        var info = routing.unassignedInfo();
        if (info == null || info.getReason() != UnassignedInfo.Reason.NODE_RESTARTING) {
            return false;
        }
        var shutdown = shutdowns.get(info.getLastAllocatedNodeId(), SingleNodeShutdownMetadata.Type.RESTART);
        if (shutdown == null) {
            return false;
        }
        var now = System.nanoTime();
        var restartingAllocationDelayExpiration = info.getUnassignedTimeInNanos() + shutdown.getAllocationDelay().nanos();
        return now - restartingAllocationDelayExpiration <= 0;
    }

    private static boolean isUnassignedDueToNewInitialization(ShardRouting routing, ClusterState state) {
        if (routing.active()) {
            return false;
        }
        // If the primary is inactive for unexceptional events in the cluster lifecycle, both the primary and the
        // replica are considered new initializations.
        ShardRouting primary = routing.primary() ? routing : state.routingTable().shardRoutingTable(routing.shardId()).primaryShard();
        return primary.active() == false && getInactivePrimaryHealth(primary) == ClusterHealthStatus.YELLOW;
    }

    /**
     * Generate a list of diagnoses that'll contain the instructions for a user to take to allow this shard to be assigned.
     * @param shardRouting An unassigned shard routing
     * @param state State of the cluster
     * @return A list of diagnoses for the provided unassigned shard
     */
    List<Diagnosis.Definition> diagnoseUnassignedShardRouting(ShardRouting shardRouting, ClusterState state) {
        List<Diagnosis.Definition> diagnosisDefs = new ArrayList<>();
        LOGGER.trace("Diagnosing unassigned shard [{}] due to reason [{}]", shardRouting.shardId(), shardRouting.unassignedInfo());
        switch (shardRouting.unassignedInfo().getLastAllocationStatus()) {
            case NO_VALID_SHARD_COPY -> diagnosisDefs.add(ACTION_RESTORE_FROM_SNAPSHOT);
            case NO_ATTEMPT -> {
                if (shardRouting.unassignedInfo().isDelayed()) {
                    diagnosisDefs.add(DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS);
                } else {
                    diagnosisDefs.addAll(explainAllocationsAndDiagnoseDeciders(shardRouting, state));
                }
            }
            case DECIDERS_NO -> diagnosisDefs.addAll(explainAllocationsAndDiagnoseDeciders(shardRouting, state));
            case DELAYED_ALLOCATION -> diagnosisDefs.add(DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS);
        }
        if (diagnosisDefs.isEmpty()) {
            diagnosisDefs.add(ACTION_CHECK_ALLOCATION_EXPLAIN_API);
        }
        return diagnosisDefs;
    }

    /**
     * For a shard that is unassigned due to a DECIDERS_NO result, this will explain the allocation and attempt to generate
     * a list of diagnoses that should allow the shard to be assigned.
     * @param shardRouting The shard routing that is unassigned with a last status of DECIDERS_NO
     * @param state Current cluster state
     * @return A list of diagnoses for the provided unassigned shard
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
            return List.of();
        }
    }

    /**
     * Generates a list of diagnoses for an unassigned shard by inspecting a list of NodeAllocationResults for
     * well known problems.
     * @param shardRouting The unassigned shard.
     * @param state Current cluster state.
     * @param nodeAllocationResults A list of results for each node in the cluster from the allocation explain api
     * @return A list of diagnoses for the provided unassigned shard
     */
    List<Diagnosis.Definition> diagnoseAllocationResults(
        ShardRouting shardRouting,
        ClusterState state,
        List<NodeAllocationResult> nodeAllocationResults
    ) {
        IndexMetadata indexMetadata = state.metadata().index(shardRouting.index());
        List<Diagnosis.Definition> diagnosisDefs = new ArrayList<>();
        if (indexMetadata != null) {
            diagnosisDefs.addAll(checkIsAllocationDisabled(indexMetadata, nodeAllocationResults));
            diagnosisDefs.addAll(checkNodeRoleRelatedIssues(indexMetadata, nodeAllocationResults, state, shardRouting));
        }
        if (diagnosisDefs.isEmpty()) {
            diagnosisDefs.add(ACTION_CHECK_ALLOCATION_EXPLAIN_API);
        }
        return diagnosisDefs;
    }

    /**
     * Convenience method for filtering node allocation results by decider outcomes.
     * @param deciderName The decider that is being checked
     * @param outcome The outcome expected
     * @return A predicate that returns true if the decision exists and matches the expected outcome, false otherwise.
     */
    protected static Predicate<NodeAllocationResult> hasDeciderResult(String deciderName, Decision.Type outcome) {
        return (nodeResult) -> {
            Decision decision = nodeResult.getCanAllocateDecision();
            return decision != null && decision.getDecisions().stream().anyMatch(d -> deciderName.equals(d.label()) && outcome == d.type());
        };
    }

    /**
     * Generates a list of diagnoses if a shard cannot be allocated anywhere because allocation is disabled for that shard
     * @param indexMetadata from the index shard being diagnosed
     * @param nodeAllocationResults allocation decision results for all nodes in the cluster.
     * @return A list of diagnoses for the provided unassigned shard
     */
    List<Diagnosis.Definition> checkIsAllocationDisabled(IndexMetadata indexMetadata, List<NodeAllocationResult> nodeAllocationResults) {
        List<Diagnosis.Definition> diagnosisDefs = new ArrayList<>();
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
                diagnosisDefs.add(ACTION_ENABLE_INDEX_ROUTING_ALLOCATION);
            }
            if (EnableAllocationDecider.Allocation.ALL != clusterLevelAllocation) {
                // Cluster setting is not ALL
                diagnosisDefs.add(ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION);
            }
        }
        return diagnosisDefs;
    }

    /**
     * Generates a list of diagnoses for common problems that keep a shard from allocating to nodes depending on their role;
     * a very common example of such a case are data tiers.
     * @param indexMetadata Index metadata for the shard being diagnosed.
     * @param nodeAllocationResults allocation decision results for all nodes in the cluster.
     * @param clusterState the current cluster state.
     * @param shardRouting the shard the nodeAllocationResults refer to
     * @return A list of diagnoses for the provided unassigned shard
     */
    protected List<Diagnosis.Definition> checkNodeRoleRelatedIssues(
        IndexMetadata indexMetadata,
        List<NodeAllocationResult> nodeAllocationResults,
        ClusterState clusterState,
        ShardRouting shardRouting
    ) {
        List<Diagnosis.Definition> diagnosisDefs = new ArrayList<>();
        if (indexMetadata.getTierPreference().isEmpty() == false) {
            List<NodeAllocationResult> dataTierAllocationResults = nodeAllocationResults.stream()
                .filter(hasDeciderResult(DATA_TIER_ALLOCATION_DECIDER_NAME, Decision.Type.YES))
                .toList();
            if (dataTierAllocationResults.isEmpty()) {
                // Shard must be allocated on specific tiers but no nodes were enabled for those tiers.
                for (String tier : indexMetadata.getTierPreference()) {
                    Optional.ofNullable(getAddNodesWithRoleAction(tier)).ifPresent(diagnosisDefs::add);
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
                diagnosisDefs.addAll(
                    checkNodesWithRoleAtShardLimit(indexMetadata, clusterState, dataTierAllocationResults, dataTierNodes, preferredTier)
                );
                diagnosisDefs.addAll(checkDataTierShouldMigrate(indexMetadata, dataTierAllocationResults, preferredTier, dataTierNodes));
                checkNotEnoughNodesWithRole(dataTierAllocationResults, preferredTier).ifPresent(diagnosisDefs::add);
            }
        }
        return diagnosisDefs;
    }

    protected List<Diagnosis.Definition> checkNodesWithRoleAtShardLimit(
        IndexMetadata indexMetadata,
        ClusterState clusterState,
        List<NodeAllocationResult> nodeRoleAllocationResults,
        Set<DiscoveryNode> nodesWithRoles,
        @Nullable String role
    ) {
        // All applicable nodes at shards limit?
        if (nodeRoleAllocationResults.stream().allMatch(hasDeciderResult(ShardsLimitAllocationDecider.NAME, Decision.Type.NO))) {
            List<Diagnosis.Definition> diagnosisDefs = new ArrayList<>();
            // We need the routing nodes for the role this index is allowed on to determine the offending shard limits
            List<RoutingNode> candidateNodes = clusterState.getRoutingNodes()
                .stream()
                .filter(routingNode -> nodesWithRoles.contains(routingNode.node()))
                .toList();

            // Determine which total_shards_per_node settings are present
            Integer clusterShardsPerNode = clusterService.getClusterSettings().get(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING);
            Integer indexShardsPerNode = INDEX_TOTAL_SHARDS_PER_NODE_SETTING.get(indexMetadata.getSettings());
            assert (clusterShardsPerNode > 0 || indexShardsPerNode > 0) : "shards per node must exist if allocation decision is NO";

            // Determine which total_shards_per_node settings are keeping things from allocating
            boolean clusterShardsPerNodeShouldChange = false;
            if (clusterShardsPerNode > 0) {
                int minShardCount = candidateNodes.stream().map(RoutingNode::numberOfOwningShards).min(Integer::compareTo).orElse(-1);
                clusterShardsPerNodeShouldChange = minShardCount >= clusterShardsPerNode;
            }
            boolean indexShardsPerNodeShouldChange = false;
            if (indexShardsPerNode > 0) {
                int minShardCount = candidateNodes.stream()
                    .map(routingNode -> routingNode.numberOfOwningShardsForIndex(indexMetadata.getIndex()))
                    .min(Integer::compareTo)
                    .orElse(-1);
                indexShardsPerNodeShouldChange = minShardCount >= indexShardsPerNode;
            }

            // Add appropriate diagnosis
            if (role != null) {
                // We cannot allocate the shard to the most preferred role because a shard limit is reached.
                if (clusterShardsPerNodeShouldChange) {
                    Optional.ofNullable(getIncreaseShardLimitClusterSettingAction(role)).ifPresent(diagnosisDefs::add);
                }
                if (indexShardsPerNodeShouldChange) {
                    Optional.ofNullable(getIncreaseShardLimitIndexSettingAction(role)).ifPresent(diagnosisDefs::add);
                }
            } else {
                // We couldn't determine a desired role. Give a generic ask for increasing the shard limit.
                if (clusterShardsPerNodeShouldChange) {
                    diagnosisDefs.add(ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING);
                }
                if (indexShardsPerNodeShouldChange) {
                    diagnosisDefs.add(ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING);
                }
            }
            return diagnosisDefs;
        } else {
            return List.of();
        }
    }

    private static List<Diagnosis.Definition> checkDataTierShouldMigrate(
        IndexMetadata indexMetadata,
        List<NodeAllocationResult> dataTierAllocationResults,
        @Nullable String preferredTier,
        Set<DiscoveryNode> dataTierNodes
    ) {
        // Check if index has filter requirements on the old "data" attribute that might be keeping it from allocating.
        if (dataTierAllocationResults.stream().allMatch(hasDeciderResult(FilterAllocationDecider.NAME, Decision.Type.NO))) {
            List<Diagnosis.Definition> diagnosisDefs = new ArrayList<>();
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
                    diagnosisDefs.add(
                        Optional.ofNullable(preferredTier)
                            .map(ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA_LOOKUP::get)
                            .orElse(ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA)
                    );
                }
                if (includeFilter != null && dataTierNodes.stream().noneMatch(includeFilter::match)) {
                    // No data tier nodes match the included data attributes
                    diagnosisDefs.add(
                        Optional.ofNullable(preferredTier)
                            .map(ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA_LOOKUP::get)
                            .orElse(ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA)
                    );
                }
            }
            return diagnosisDefs;
        } else {
            return List.of();
        }
    }

    protected Optional<Diagnosis.Definition> checkNotEnoughNodesWithRole(
        List<NodeAllocationResult> nodeAllocationResults,
        @Nullable String role
    ) {
        // Not enough nodes to hold shards on different nodes?
        if (nodeAllocationResults.stream().allMatch(hasDeciderResult(SameShardAllocationDecider.NAME, Decision.Type.NO))) {
            // We couldn't determine a desired role. This is likely because there are no nodes with the relevant role in the cluster.
            // Give a generic ask for increasing the shard limit.
            if (role != null) {
                return Optional.ofNullable(getIncreaseNodeWithRoleCapacityAction(role));
            } else {
                return Optional.of(ACTION_INCREASE_NODE_CAPACITY);
            }
        } else {
            return Optional.empty();
        }
    }

    @Nullable
    public Diagnosis.Definition getAddNodesWithRoleAction(String role) {
        return ACTION_ENABLE_TIERS_LOOKUP.get(role);
    }

    @Nullable
    public Diagnosis.Definition getIncreaseShardLimitIndexSettingAction(String role) {
        return ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP.get(role);
    }

    @Nullable
    public Diagnosis.Definition getIncreaseShardLimitClusterSettingAction(String role) {
        return ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP.get(role);
    }

    @Nullable
    public Diagnosis.Definition getIncreaseNodeWithRoleCapacityAction(String role) {
        return ACTION_INCREASE_TIER_CAPACITY_LOOKUP.get(role);
    }

    public class ShardAllocationStatus {
        protected final ShardAllocationCounts primaries = new ShardAllocationCounts();
        protected final ShardAllocationCounts replicas = new ShardAllocationCounts();
        protected final Metadata clusterMetadata;

        public ShardAllocationStatus(Metadata clusterMetadata) {
            this.clusterMetadata = clusterMetadata;
        }

        void addPrimary(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns, boolean verbose) {
            primaries.increment(routing, state, shutdowns, verbose);
        }

        void addReplica(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns, boolean verbose) {
            replicas.increment(routing, state, shutdowns, verbose);
        }

        void updateSearchableSnapshotsOfAvailableIndices() {
            // Searchable snapshots do not have replicas, so this post-processing is not applicable for the replicas
            primaries.searchableSnapshotsState.updateSearchableSnapshotWithAvailableIndices(
                clusterMetadata,
                primaries.indicesWithUnavailableShards
            );
        }

        public HealthStatus getStatus() {
            if (primaries.areAllAvailable() == false || primaries.searchableSnapshotsState.getRedSearchableSnapshots().isEmpty() == false) {
                return RED;
            } else if (replicas.areAllAvailable() == false) {
                return YELLOW;
            } else {
                return GREEN;
            }
        }

        public String getSymptom() {
            var builder = new StringBuilder("This cluster has ");
            if (primaries.unassigned > 0
                || primaries.unassigned_new > 0
                || primaries.unassigned_restarting > 0
                || replicas.unassigned > 0
                || replicas.unassigned_new > 0
                || replicas.unassigned_restarting > 0
                || primaries.initializing > 0
                || replicas.initializing > 0) {
                builder.append(
                    Stream.of(
                        createMessage(primaries.unassigned, "unavailable primary shard", "unavailable primary shards"),
                        createMessage(primaries.unassigned_new, "creating primary shard", "creating primary shards"),
                        createMessage(replicas.unassigned_new, "creating replica shard", "creating replica shards"),
                        createMessage(primaries.unassigned_restarting, "restarting primary shard", "restarting primary shards"),
                        createMessage(replicas.unassigned, "unavailable replica shard", "unavailable replica shards"),
                        createMessage(primaries.initializing, "initializing primary shard", "initializing primary shards"),
                        createMessage(replicas.initializing, "initializing replica shard", "initializing replica shards"),
                        createMessage(replicas.unassigned_restarting, "restarting replica shard", "restarting replica shards")
                    ).flatMap(Function.identity()).collect(joining(", "))
                ).append(".");
            } else {
                builder.append("all shards available.");
            }
            if (primaries.areAllAvailable()
                && primaries.searchableSnapshotsState.searchableSnapshotWithOriginalIndexAvailable.isEmpty() == false) {
                if (primaries.unassigned == 1) {
                    builder.append(
                        " This is a mounted shard and the original shard is available, so there are no data availability problems."
                    );
                } else {
                    builder.append(
                        " These are mounted shards and the original shards are available, so there are no data availability problems."
                    );
                }
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

        public HealthIndicatorDetails getDetails(boolean verbose) {
            if (verbose) {
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
                        "creating_replicas",
                        replicas.unassigned_new,
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
                    getTruncatedIndices(primaries.indicesWithUnavailableShards, clusterMetadata)
                );
                impacts.add(
                    new HealthIndicatorImpact(
                        NAME,
                        PRIMARY_UNASSIGNED_IMPACT_ID,
                        1,
                        impactDescription,
                        List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                    )
                );
            }
            Set<String> readOnlyIndicesWithUnavailableShards = primaries.searchableSnapshotsState.getRedSearchableSnapshots();
            if (readOnlyIndicesWithUnavailableShards.isEmpty() == false) {
                String impactDescription = String.format(
                    Locale.ROOT,
                    "Searching %d %s [%s] might return incomplete results.",
                    readOnlyIndicesWithUnavailableShards.size(),
                    readOnlyIndicesWithUnavailableShards.size() == 1 ? "index" : "indices",
                    getTruncatedIndices(readOnlyIndicesWithUnavailableShards, clusterMetadata)
                );
                impacts.add(
                    new HealthIndicatorImpact(
                        NAME,
                        READ_ONLY_PRIMARY_UNASSIGNED_IMPACT_ID,
                        1,
                        impactDescription,
                        List.of(ImpactArea.SEARCH)
                    )
                );
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
                    getTruncatedIndices(indicesWithUnavailableReplicasOnly, clusterMetadata)
                );
                impacts.add(
                    new HealthIndicatorImpact(NAME, REPLICA_UNASSIGNED_IMPACT_ID, 2, impactDescription, List.of(ImpactArea.SEARCH))
                );
            }
            return impacts;
        }

        /**
         * Returns the diagnosis for unassigned primary and replica shards.
         * @param verbose true if the diagnosis should be generated, false if they should be omitted.
         * @param maxAffectedResourcesCount the max number of affected resources to be returned as part of the diagnosis
         * @return The diagnoses list the indicator identified. Alternatively, an empty list if none were found or verbose is false.
         */
        public List<Diagnosis> getDiagnosis(boolean verbose, int maxAffectedResourcesCount) {
            if (verbose) {
                Map<Diagnosis.Definition, Set<String>> diagnosisToAffectedIndices = new HashMap<>(primaries.diagnosisDefinitions);
                replicas.diagnosisDefinitions.forEach((diagnosisDef, indicesWithReplicasUnassigned) -> {
                    Set<String> indicesWithPrimariesUnassigned = diagnosisToAffectedIndices.get(diagnosisDef);
                    if (indicesWithPrimariesUnassigned == null) {
                        diagnosisToAffectedIndices.put(diagnosisDef, indicesWithReplicasUnassigned);
                    } else {
                        indicesWithPrimariesUnassigned.addAll(indicesWithReplicasUnassigned);
                    }
                });
                if (diagnosisToAffectedIndices.isEmpty()) {
                    return List.of();
                } else {

                    return diagnosisToAffectedIndices.entrySet().stream().map(e -> {
                        List<Diagnosis.Resource> affectedResources = new ArrayList<>(1);
                        if (e.getKey().equals(ACTION_RESTORE_FROM_SNAPSHOT)) {
                            Set<String> restoreFromSnapshotIndices = e.getValue();
                            if (restoreFromSnapshotIndices != null && restoreFromSnapshotIndices.isEmpty() == false) {
                                affectedResources = getRestoreFromSnapshotAffectedResources(
                                    clusterMetadata,
                                    systemIndices,
                                    restoreFromSnapshotIndices,
                                    maxAffectedResourcesCount
                                );
                            }
                        } else {
                            affectedResources.add(
                                new Diagnosis.Resource(
                                    INDEX,
                                    e.getValue()
                                        .stream()
                                        .sorted(indicesComparatorByPriorityAndName(clusterMetadata))
                                        .limit(Math.min(e.getValue().size(), maxAffectedResourcesCount))
                                        .collect(Collectors.toList())
                                )
                            );
                        }
                        return new Diagnosis(e.getKey(), affectedResources);
                    }).collect(Collectors.toList());
                }
            } else {
                return List.of();
            }
        }

        /**
         * The restore from snapshot operation requires the user to specify indices and feature states.
         * The indices that are part of the feature states must not be specified. This method loops through all the
         * identified unassigned indices and returns the affected {@link Diagnosis.Resource}s of type `INDEX`
         * and if applicable `FEATURE_STATE`
         */
        static List<Diagnosis.Resource> getRestoreFromSnapshotAffectedResources(
            Metadata metadata,
            SystemIndices systemIndices,
            Set<String> restoreFromSnapshotIndices,
            int maxAffectedResourcesCount
        ) {
            List<Diagnosis.Resource> affectedResources = new ArrayList<>(2);

            Set<String> affectedIndices = new HashSet<>(restoreFromSnapshotIndices);
            Set<String> affectedFeatureStates = new HashSet<>();
            Map<String, Set<String>> featureToSystemIndices = systemIndices.getFeatures()
                .stream()
                .collect(
                    toMap(
                        SystemIndices.Feature::getName,
                        feature -> feature.getIndexDescriptors()
                            .stream()
                            .flatMap(descriptor -> descriptor.getMatchingIndices(metadata).stream())
                            .collect(toSet())
                    )
                );

            for (Map.Entry<String, Set<String>> featureToIndices : featureToSystemIndices.entrySet()) {
                for (String featureIndex : featureToIndices.getValue()) {
                    if (restoreFromSnapshotIndices.contains(featureIndex)) {
                        affectedFeatureStates.add(featureToIndices.getKey());
                        affectedIndices.remove(featureIndex);
                    }
                }
            }

            Map<String, Set<String>> featureToDsBackingIndices = systemIndices.getFeatures()
                .stream()
                .collect(
                    toMap(
                        SystemIndices.Feature::getName,
                        feature -> feature.getDataStreamDescriptors()
                            .stream()
                            .flatMap(descriptor -> descriptor.getBackingIndexNames(metadata).stream())
                            .collect(toSet())
                    )
                );

            // the shards_availability indicator works with indices so let's remove the feature states data streams backing indices from
            // the list of affected indices (the feature state will cover the restore of these indices too)
            for (Map.Entry<String, Set<String>> featureToBackingIndices : featureToDsBackingIndices.entrySet()) {
                for (String featureIndex : featureToBackingIndices.getValue()) {
                    if (restoreFromSnapshotIndices.contains(featureIndex)) {
                        affectedFeatureStates.add(featureToBackingIndices.getKey());
                        affectedIndices.remove(featureIndex);
                    }
                }
            }

            if (affectedIndices.isEmpty() == false) {
                affectedResources.add(new Diagnosis.Resource(INDEX, affectedIndices.stream().limit(maxAffectedResourcesCount).toList()));
            }
            if (affectedFeatureStates.isEmpty() == false) {
                affectedResources.add(
                    new Diagnosis.Resource(FEATURE_STATE, affectedFeatureStates.stream().limit(maxAffectedResourcesCount).toList())
                );
            }
            return affectedResources;
        }
    }

    public static class SearchableSnapshotsState {
        private final Set<String> searchableSnapshotWithUnavailableShard = new HashSet<>();
        private final Set<String> searchableSnapshotWithOriginalIndexAvailable = new HashSet<>();

        void addSearchableSnapshotWithUnavailableShard(String indexName) {
            searchableSnapshotWithUnavailableShard.add(indexName);
        }

        void addSearchableSnapshotWithOriginalIndexAvailable(String indexName) {
            searchableSnapshotWithOriginalIndexAvailable.add(indexName);
        }

        public Set<String> getRedSearchableSnapshots() {
            return Sets.difference(searchableSnapshotWithUnavailableShard, searchableSnapshotWithOriginalIndexAvailable);
        }

        // If the original index of a searchable snapshot with unavailable shards is available then we remove the searchable snapshot
        // from the list of the unavailable searchable snapshots because the data is available via the original index.
        void updateSearchableSnapshotWithAvailableIndices(Metadata clusterMetadata, Set<String> indicesWithUnavailableShards) {
            for (String index : searchableSnapshotWithUnavailableShard) {
                assert clusterMetadata.index(index) != null : "Index metadata of index '" + index + "' should not be null";
                Settings indexSettings = clusterMetadata.index(index).getSettings();
                String originalIndex = indexSettings.get(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_INDEX_NAME_SETTING_KEY);
                if (originalIndex != null
                    && clusterMetadata.indices().containsKey(originalIndex) != false
                    && indicesWithUnavailableShards.contains(originalIndex) == false) {
                    addSearchableSnapshotWithOriginalIndexAvailable(index);
                }
            }
        }
    }
}
