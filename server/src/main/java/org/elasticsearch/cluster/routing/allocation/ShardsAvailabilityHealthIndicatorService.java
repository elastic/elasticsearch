/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
                status.addPrimary(shardRouting.primaryShard(), shutdown);
                for (ShardRouting replicaShard : shardRouting.replicaShards()) {
                    status.addReplica(replicaShard, shutdown);
                }
            }
        }

        return createIndicator(status.getStatus(), status.getSummary(), status.getDetails(), status.getImpacts());
    }

    private static class ShardAllocationCounts {
        private boolean available = true; // This will be true even if no replicas are expected, as long as none are unavailable
        private int unassigned = 0;
        private int unassigned_new = 0;
        private int unassigned_restarting = 0;
        private int initializing = 0;
        private int started = 0;
        private int relocating = 0;
        private Set<String> indicesWithUnavailableShards = new HashSet<>();

        public void increment(ShardRouting routing, NodesShutdownMetadata shutdowns) {
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
                    }
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

    private static class ShardAllocationStatus {
        private final ShardAllocationCounts primaries = new ShardAllocationCounts();
        private final ShardAllocationCounts replicas = new ShardAllocationCounts();

        public void addPrimary(ShardRouting routing, NodesShutdownMetadata shutdowns) {
            primaries.increment(routing, shutdowns);
        }

        public void addReplica(ShardRouting routing, NodesShutdownMetadata shutdowns) {
            replicas.increment(routing, shutdowns);
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
