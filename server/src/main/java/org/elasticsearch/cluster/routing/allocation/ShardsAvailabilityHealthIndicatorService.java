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
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.UserAction;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.cluster.health.ClusterShardHealth.getInactivePrimaryHealth;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;

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
    private final AllocationDeciders allocationDeciders;

    public ShardsAvailabilityHealthIndicatorService(
        ClusterService clusterService,
        AllocationService allocationService,
        AllocationDeciders allocationDeciders
    ) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.allocationDeciders = allocationDeciders;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return DATA;
    }

    @Override
    public HealthIndicatorResult calculate() {
        var state = clusterService.state();
        var shutdown = state.getMetadata().custom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY);
        var status = new ShardAllocationStatus();

        for (IndexRoutingTable indexShardRouting : state.routingTable()) {
            for (int i = 0; i < indexShardRouting.size(); i++) {
                IndexShardRoutingTable shardRouting = indexShardRouting.shard(i);
                status.addPrimary(shardRouting.primaryShard(), state, shutdown);
                for (ShardRouting replicaShard : shardRouting.replicaShards()) {
                    status.addReplica(replicaShard, state, shutdown);
                }
            }
        }

        return new HealthIndicatorResult(name(), component(), status.getStatus(), status.getSummary(), status.getDetails(),
            status.getImpacts(), status.getUserActions());
    }

    // TODO: Fill in messages and help URLs
    public static final UserAction.Definition ACTION_RESTORE_FROM_SNAPSHOT = new UserAction.Definition("restore_from_snapshot", "", "");
    public static final UserAction.Definition ACTION_SHARD_LIMIT = new UserAction.Definition("increase_shard_limit", "", "");
    public static final UserAction.Definition ACTION_ENABLE_ALLOCATIONS = new UserAction.Definition("enable_allocations", "", "");
    public static final UserAction.Definition ACTION_MIGRATE_TIERS = new UserAction.Definition("migrate_data_tiers", "", "");
    public static final UserAction.Definition ACTION_INCREASE_TIER_CAPACITY = new UserAction.Definition(
        "increase_tier_capacity_for_allocations", "", "");

    private class ShardAllocationCounts {
        private boolean available = true; // This will be true even if no replicas are expected, as long as none are unavailable
        private int unassigned = 0;
        private int unassigned_new = 0;
        private int unassigned_restarting = 0;
        private int initializing = 0;
        private int started = 0;
        private int relocating = 0;
        private final Set<String> indicesWithUnavailableShards = new HashSet<>();
        private final Map<String, UserAction> userActions = new HashMap<>();

        public void increment(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns) {
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
                        diagnoseUnassigned(this::addUserAction, routing, state);
                    }
                }
                case INITIALIZING -> initializing++;
                case STARTED -> started++;
                case RELOCATING -> relocating++;
            }
        }

        public void addUserAction(UserAction.Definition actionDef, ShardRouting routing) {
            userActions.computeIfAbsent(actionDef.id(), (k) -> new UserAction(actionDef, new HashSet<>()))
                .affectedResources()
                .add(routing.getIndexName());
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

    private void diagnoseUnassigned(
        BiConsumer<UserAction.Definition, ShardRouting> diagnosisOutput,
        ShardRouting shardRouting,
        ClusterState state
    ) {
        switch (shardRouting.unassignedInfo().getLastAllocationStatus()) {
            case NO_VALID_SHARD_COPY:
                if (UnassignedInfo.Reason.NODE_LEFT == shardRouting.unassignedInfo().getReason()) {
                    diagnosisOutput.accept(ACTION_RESTORE_FROM_SNAPSHOT, shardRouting);
                }
                break;
            case DECIDERS_NO:
                diagnoseDeciders(diagnosisOutput, shardRouting, state);
                break;
            default:
                break;
        }
    }

    private void diagnoseDeciders(
        BiConsumer<UserAction.Definition, ShardRouting> diagnosisOutput,
        ShardRouting shardRouting,
        ClusterState state
    ) {
        LOGGER.trace("Diagnosing shard [{}]", shardRouting.shardId());
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
        allocation.setDebugMode(RoutingAllocation.DebugMode.ON);
        ShardAllocationDecision shardAllocationDecision = allocationService.explainShardAllocation(shardRouting, allocation);
        AllocateUnassignedDecision allocateDecision = shardAllocationDecision.getAllocateDecision();
        LOGGER.trace("[{}]: Obtained decision: [{}/{}]",
            shardRouting.shardId(),
            allocateDecision.isDecisionTaken(),
            allocateDecision.getAllocationDecision()
        );
        if (allocateDecision.isDecisionTaken() && AllocationDecision.NO == allocateDecision.getAllocationDecision()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[{}]: Working with decisions: [{}]", shardRouting.shardId(),
                    allocateDecision.getNodeDecisions().stream()
                        .map(n -> n.getCanAllocateDecision().getDecisions().stream()
                            .map(d -> d.label() + ": " + d.type())
                            .collect(Collectors.toList()))
                        .collect(Collectors.toList()));
            }
            List<NodeAllocationResult> nodeAllocationResults = allocateDecision.getNodeDecisions();
            IndexMetadata index = state.metadata().index(shardRouting.index());
            if (index != null) {
                checkIsAllocationDisabled(diagnosisOutput, shardRouting, nodeAllocationResults);
                checkDataTierRelatedIssues(diagnosisOutput, index, shardRouting, nodeAllocationResults);
            }
        }
    }

    private static Predicate<NodeAllocationResult> nodeHasDeciderResult(String deciderName, Decision.Type outcome) {
        return (nodeResult) -> nodeResult.getCanAllocateDecision().getDecisions().stream()
            .anyMatch(decision -> deciderName.equals(decision.label()) && outcome == decision.type());
    }

    private void checkIsAllocationDisabled(
        BiConsumer<UserAction.Definition, ShardRouting> diagnosisOutput,
        ShardRouting shardRouting,
        List<NodeAllocationResult> nodeAllocationResults
    ) {
        if (nodeAllocationResults.stream().allMatch(nodeHasDeciderResult(EnableAllocationDecider.NAME, Decision.Type.NO))) {
            diagnosisOutput.accept(ACTION_ENABLE_ALLOCATIONS, shardRouting);
        }
    }

    private void checkDataTierRelatedIssues(
        BiConsumer<UserAction.Definition, ShardRouting> diagnosisOutput,
        IndexMetadata indexMetadata,
        ShardRouting shardRouting,
        List<NodeAllocationResult> nodeAllocationResults
    ) {
        if (indexMetadata.getTierPreference().size() > 0) {
            List<NodeAllocationResult> dataTierNodes = nodeAllocationResults.stream()
                .filter(nodeHasDeciderResult(DATA_TIER_ALLOCATION_DECIDER_NAME, Decision.Type.YES))
                .collect(Collectors.toList());
            if (dataTierNodes.isEmpty() == false) {
                // All tier nodes at shards limit?
                if (dataTierNodes.stream().allMatch(nodeHasDeciderResult(ShardsLimitAllocationDecider.NAME, Decision.Type.NO))) {
                    diagnosisOutput.accept(ACTION_SHARD_LIMIT, shardRouting);
                }

                // All tier nodes conflict with allocation filters?
                if (dataTierNodes.stream().allMatch(nodeHasDeciderResult(FilterAllocationDecider.NAME, Decision.Type.NO))) {
                    // TODO: Check if shard is part of an index that can be migrated to data tiers?
                    diagnosisOutput.accept(ACTION_MIGRATE_TIERS, shardRouting);
                }

                // Not enough tier nodes to hold shards on different nodes?
                if (dataTierNodes.stream().allMatch(nodeHasDeciderResult(SameShardAllocationDecider.NAME, Decision.Type.NO))) {
                    diagnosisOutput.accept(ACTION_INCREASE_TIER_CAPACITY, shardRouting);
                }
            }
        }
    }

    private class ShardAllocationStatus {
        private final ShardAllocationCounts primaries = new ShardAllocationCounts();
        private final ShardAllocationCounts replicas = new ShardAllocationCounts();

        public void addPrimary(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns) {
            primaries.increment(routing, state, shutdowns);
        }

        public void addReplica(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns) {
            replicas.increment(routing, state, shutdowns);
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
                        createMessage(primaries.unassigned, "unavailable primary", " unavailable primaries"),
                        createMessage(primaries.unassigned_new, "creating primary", " creating primaries"),
                        createMessage(primaries.unassigned_restarting, "restarting primary", " restarting primaries"),
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

        public SimpleHealthIndicatorDetails getDetails() {
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
        }

        public List<HealthIndicatorImpact> getImpacts() {
            final List<HealthIndicatorImpact> impacts = new ArrayList<>();
            if (primaries.indicesWithUnavailableShards.isEmpty() == false) {
                String impactDescription = String.format(
                    Locale.ROOT,
                    "Cannot add data to %d %s [%s]. Searches might return incomplete results.",
                    primaries.indicesWithUnavailableShards.size(),
                    primaries.indicesWithUnavailableShards.size() == 1 ? "index" : "indices",
                    getTruncatedIndicesString(primaries.indicesWithUnavailableShards)
                );
                impacts.add(new HealthIndicatorImpact(1, impactDescription));
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
                    "Searches might return slower than usual. Fewer redundant copies of the data exist on %d %s [%s].",
                    indicesWithUnavailableReplicasOnly.size(),
                    indicesWithUnavailableReplicasOnly.size() == 1 ? "index" : "indices",
                    getTruncatedIndicesString(indicesWithUnavailableReplicasOnly)
                );
                impacts.add(new HealthIndicatorImpact(3, impactDescription));
            }
            return impacts;
        }

        public List<UserAction> getUserActions() {
            Map<String, UserAction> allActions = new HashMap<>();
            primaries.userActions.forEach(allActions::put);
            replicas.userActions.forEach((actionId, replicaUserActions) -> {
                UserAction existingAction = allActions.get(actionId);
                if (existingAction == null) {
                    allActions.put(actionId, replicaUserActions);
                } else {
                    existingAction.affectedResources().addAll(replicaUserActions.affectedResources());
                }
            });
            if (allActions.isEmpty()) {
                return null;
            } else {
                return allActions.values().stream().toList();
            }
        }
    }

    private static String getTruncatedIndicesString(Set<String> indices) {
        final int maxIndices = 10;
        String truncatedIndicesString = indices.stream().limit(maxIndices).collect(joining(", "));
        if (maxIndices < indices.size()) {
            truncatedIndicesString = truncatedIndicesString + ", ...";
        }
        return truncatedIndicesString;
    }
}
