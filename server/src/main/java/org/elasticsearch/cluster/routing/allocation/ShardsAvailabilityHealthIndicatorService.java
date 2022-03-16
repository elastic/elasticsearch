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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
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

    public ShardsAvailabilityHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
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

        return createIndicator(status.getStatus(), status.getSummary(), status.getDetails());
    }

    private static record Diagnosis(String identifier, String troubleshootingURL) {}
    private static final Diagnosis UNKNOWN = new Diagnosis("UNKNOWN", "");
    private static final Diagnosis TIER_ROLE_CLASH = new Diagnosis("TIER_CLASH", "https://"); // TODO: Need a URL for this

    private static class ShardAllocationCounts {
        private boolean available = true;
        private int unassigned = 0;
        private int unassigned_new = 0;
        private int unassigned_restarting = 0;
        private int initializing = 0;
        private int started = 0;
        private int relocating = 0;

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
                        LOGGER.info("Diagnosis: " + diagnoseReason(routing, state));
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

    /**
     * Diagnose well known error states that we know will never result in a successful allocation.
     * @return Diagnosis result to be returned with actions for a user to take
     */
    private static Diagnosis diagnoseReason(ShardRouting routing, ClusterState state) {
        return Optional.ofNullable(state.metadata().index(routing.index()))
            .map(indexMetadata -> {
                // Check: Data Tier conflicts with required attributes
                if (UnassignedInfo.AllocationStatus.DECIDERS_NO == routing.unassignedInfo().getLastAllocationStatus()) {
                    List<String> tierPreference = indexMetadata.getTierPreference();
                    if (tierPreference.size() > 0) {
                        Optional<DiscoveryNode> discoveryNode = allocateBasedOnTiersAndFilters(state, indexMetadata);
                        if (discoveryNode.isPresent() == false) {
                            return TIER_ROLE_CLASH;
                        } else {
                            LOGGER.debug(
                                "Tier Check Outcome for [{}]: Found a viable node for tier: {}",
                                routing.shardId(),
                                discoveryNode.get().getId()
                            );
                        }
                    } else {
                        LOGGER.debug(
                            "Tier Check Outcome for [{}]: Unknown - No tier configured: [{}]] from setting: All settings: [{}]",
                            routing.shardId(),
                            tierPreference,
                            indexMetadata.getSettings()
                        );
                    }
                } else {
                    LOGGER.debug(
                        "Tier Check Outcome for [{}]: Unknown - status: {}",
                        routing.shardId(),
                        routing.unassignedInfo().getLastAllocationStatus()
                    );
                }
                return UNKNOWN;
            })
            .orElse(UNKNOWN);
    }

    /**
     * Determines if a shard can be allocated to any node based on JUST tier preference and node filtering rules.
     */
    private static Optional<DiscoveryNode> allocateBasedOnTiersAndFilters(ClusterState state, IndexMetadata indexMetadata) {
        List<String> tierPreference = indexMetadata.getTierPreference();
        DiscoveryNodeFilters requireFilters = indexMetadata.requireFilters();
        DiscoveryNodeFilters includeFilters = indexMetadata.includeFilters();
        DiscoveryNodeFilters excludeFilters = indexMetadata.excludeFilters();

        return state.nodes().getDataNodes().values().stream()
            .filter(node -> {
                if (node.getRoles().contains(DiscoveryNodeRole.DATA_ROLE)) {
                    return true;
                } else {
                    for (String tierName : tierPreference) {
                        for (DiscoveryNodeRole role : node.getRoles()) {
                            if (role.roleName().equals(tierName)) {
                                return true;
                            }
                        }
                    }
                }
                return false;
            })
            .filter(node -> {if (requireFilters != null) return requireFilters.match(node); else return true;})
            .filter(node -> {if (includeFilters != null) return includeFilters.match(node); else return true;})
            .filter(node -> {if (excludeFilters != null) return excludeFilters.match(node); else return true;})
            .findAny();
    }

    private static class ShardAllocationStatus {
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
    }
}
