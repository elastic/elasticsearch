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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.UserAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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

    private final ClusterService clusterService;
    private final ShardsLimitAllocationDecider shardsLimitAllocationDecider;
    private final EnableAllocationDecider enableAllocationDecider;
    private final FilterAllocationDecider filterAllocationDecider;
    private final DataTierAllocationDecider dataTierAllocationDecider;

    public ShardsAvailabilityHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.shardsLimitAllocationDecider = new ShardsLimitAllocationDecider(
            clusterService.getSettings(),
            clusterService.getClusterSettings()
        );
        this.enableAllocationDecider = new EnableAllocationDecider(clusterService.getSettings(), clusterService.getClusterSettings());
        this.filterAllocationDecider = new FilterAllocationDecider(clusterService.getSettings(), clusterService.getClusterSettings());
        this.dataTierAllocationDecider = DataTierAllocationDecider.INSTANCE;

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

        return new HealthIndicatorResult(name(), component(), status.getStatus(), status.getSummary(), status.getDetails());
    }

    private static final String ACTION_SHARD_LIMIT_ID = "increase_shard_limit";
    private static final String ACTION_SHARD_LIMIT_MESSAGE = ""; // TODO
    private static final String ACTION_SHARD_LIMIT_URL = ""; // TODO

    // TODO: This should be split into 4 options based on the setting keeping it from allocating
    private static final String ACTION_ENABLE_ALLOCATIONS_ID = "enable_allocations";
    private static final String ACTION_ENABLE_ALLOCATIONS_MESSAGE = ""; // TODO
    private static final String ACTION_ENABLE_ALLOCATIONS_URL = ""; // TODO

    private static final String ACTION_MIGRATE_TIERS_ID = "migrate_data_tiers";
    private static final String ACTION_MIGRATE_TIERS_MESSAGE = ""; // TODO
    private static final String ACTION_MIGRATE_TIERS_URL = ""; // TODO

    private static final String ACTION_RESTORE_FROM_SNAPSHOT_ID = "restore_from_snapshot";
    private static final String ACTION_RESTORE_FROM_SNAPSHOT_MESSAGE = ""; // TODO
    private static final String ACTION_RESTORE_FROM_SNAPSHOT_URL = ""; // TODO

    private class ShardAllocationCounts {
        private boolean available = true;
        private int unassigned = 0;
        private int unassigned_new = 0;
        private int unassigned_restarting = 0;
        private int initializing = 0;
        private int started = 0;
        private int relocating = 0;
        private final Map<String, List<ShardRouting>> diagnoses = new HashMap<>();

        public void increment(ShardRouting routing, ClusterState state, NodesShutdownMetadata shutdowns) {
            boolean isNew = isUnassignedDueToNewInitialization(routing);
            boolean isRestarting = isUnassignedDueToTimelyRestart(routing, shutdowns);
            available &= routing.active() || isRestarting || isNew;

            switch (routing.state()) {
                case UNASSIGNED -> {
                    if (isNew) {
                        unassigned_new++;
                    } else if (isRestarting) {
                        unassigned_restarting++;
                    } else {
                        unassigned++;
                        maybeGetDiagnosis(routing, state)
                            .ifPresent(action -> diagnoses.computeIfAbsent(action, (k) -> new ArrayList<>()).add(routing));
                    }
                    LOGGER.info("Found unassigned info: " + routing.unassignedInfo());
                }
                case INITIALIZING -> initializing++;
                case STARTED -> started++;
                case RELOCATING -> relocating++;
            }
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

    private Optional<String> maybeGetDiagnosis(ShardRouting routing, ClusterState state) {
        IndexMetadata index = state.metadata().index(routing.index());
        if (index == null) {
            // TODO: routing without index? Ever possible? Probably shouldn't just blow up
            return Optional.empty();
        } else {
            return isShardsLimitHit(routing, index, state)
                .or(() -> isAllocationDisabled(routing, index, state))
                .or(() -> isDataTierFilterConflict(routing, index, state));
        }
    }

    private Optional<String> isShardsLimitHit(ShardRouting routing, IndexMetadata indexMetadata, ClusterState state) {
        if (UnassignedInfo.AllocationStatus.DECIDERS_NO == routing.unassignedInfo().getLastAllocationStatus()) {
            List<String> tierPreference = indexMetadata.getTierPreference();
            if (tierPreference.size() > 0) {
                AllocationDeciders allocationDeciders = new AllocationDeciders(
                    List.of(shardsLimitAllocationDecider, dataTierAllocationDecider)
                );
                RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, System.nanoTime());
                Optional<RoutingNode> possibleHomes = state.getRoutingNodes().stream()
                    .filter(node -> allocationDeciders.canAllocate(routing, node, allocation).type() != Decision.Type.NO)
                    .findAny();
                if (possibleHomes.isEmpty()) {
                    return Optional.of(ACTION_SHARD_LIMIT_ID);
                }
            }
        }
        return Optional.empty();
    }

    private Optional<String> isAllocationDisabled(ShardRouting routing, IndexMetadata indexMetadata, ClusterState state) {
        if (UnassignedInfo.AllocationStatus.DECIDERS_NO == routing.unassignedInfo().getLastAllocationStatus()) {
            List<String> tierPreference = indexMetadata.getTierPreference();
            if (tierPreference.size() > 0) {
                AllocationDeciders allocationDeciders = new AllocationDeciders(List.of(enableAllocationDecider));
                RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, System.nanoTime());
                Optional<RoutingNode> possibleHomes = state.getRoutingNodes().stream()
                    .filter(node -> allocationDeciders.canAllocate(routing, node, allocation).type() != Decision.Type.NO)
                    .findAny();
                if (possibleHomes.isEmpty()) {
                    return Optional.of(ACTION_ENABLE_ALLOCATIONS_ID);
                }
            }
        }
        return Optional.empty();
    }

    private Optional<String> isDataTierFilterConflict(ShardRouting routing, IndexMetadata indexMetadata, ClusterState state) {
        if (UnassignedInfo.AllocationStatus.DECIDERS_NO == routing.unassignedInfo().getLastAllocationStatus()) {
            List<String> tierPreference = indexMetadata.getTierPreference();
            if (tierPreference.size() > 0) {
                AllocationDeciders allocationDeciders = new AllocationDeciders(List.of(filterAllocationDecider, dataTierAllocationDecider));
                RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, System.nanoTime());
                Optional<RoutingNode> possibleHome = state.getRoutingNodes().stream()
                    .filter(node -> Decision.Type.YES == allocationDeciders.canAllocate(routing, node, allocation).type())
                    .findAny();
                if (possibleHome.isEmpty()) {
                    // Todo: Check if the shard is part of an index that can be migrated to data tiers
                    return Optional.of(ACTION_MIGRATE_TIERS_ID);
                }
            }
        }
        return Optional.empty();
    }

    private Optional<String> isNodeLeftDataLoss(ShardRouting routing, IndexMetadata indexMetadata, ClusterState state) {
        if (UnassignedInfo.Reason.NODE_LEFT == routing.unassignedInfo().getReason() &&
            UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY == routing.unassignedInfo().getLastAllocationStatus()) {
            return Optional.of(ACTION_RESTORE_FROM_SNAPSHOT_ID);
        }
        return Optional.empty();
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
                    ).flatMap(Function.identity()).collect(joining(" , "))
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

        public List<UserAction> getUserActions() {
            Map<String, List<ShardRouting>> allDiagnoses = new HashMap<>();
            primaries.diagnoses.forEach(allDiagnoses::put);
            replicas.diagnoses.forEach((action, replicaShards) -> {
                List<ShardRouting> shards = allDiagnoses.get(action);
                if (shards == null) {
                    allDiagnoses.put(action, replicaShards);
                } else {
                    shards.addAll(replicaShards);
                }
            });

            return allDiagnoses.entrySet().stream()
                .map(entry -> {
                    List<ShardRouting> shardRoutings = entry.getValue();
                    Set<String> indices = shardRoutings.stream().map(ShardRouting::getIndexName).collect(Collectors.toSet());
                    return createUserAction(entry.getKey(), indices);
                })
                .collect(Collectors.toList());
        }

        private UserAction createUserAction(String id, Collection<String> indices) {
            return switch (id) {
                case ACTION_SHARD_LIMIT_ID ->
                    new UserAction(ACTION_SHARD_LIMIT_ID, ACTION_SHARD_LIMIT_MESSAGE, indices, ACTION_SHARD_LIMIT_URL);
                case ACTION_ENABLE_ALLOCATIONS_ID ->
                    new UserAction(ACTION_ENABLE_ALLOCATIONS_ID, ACTION_ENABLE_ALLOCATIONS_MESSAGE, indices, ACTION_ENABLE_ALLOCATIONS_URL);
                case ACTION_MIGRATE_TIERS_ID ->
                    new UserAction(ACTION_MIGRATE_TIERS_ID, ACTION_MIGRATE_TIERS_MESSAGE, indices, ACTION_MIGRATE_TIERS_URL);
                case ACTION_RESTORE_FROM_SNAPSHOT_ID ->
                    new UserAction(ACTION_RESTORE_FROM_SNAPSHOT_ID, ACTION_RESTORE_FROM_SNAPSHOT_MESSAGE, indices,
                        ACTION_RESTORE_FROM_SNAPSHOT_URL);
                default -> throw new IllegalArgumentException("Invalid action id [" + id + "]");
            };
        }
    }
}
