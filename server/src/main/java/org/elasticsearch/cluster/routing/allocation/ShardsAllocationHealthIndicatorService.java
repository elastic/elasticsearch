/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;

/**
 * This indicator reports health for shards.
 * <p>
 * Indicator will report:
 * * RED when one or more primary shards are not available
 * * YELLOW when one or more replica shards are not replicated
 * * GREEN otherwise
 * <p>
 * Each shard needs to be available and replicated in order to guarantee high availability and prevent data loses.
 */
public class ShardsAllocationHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "shards_allocation";

    private final ClusterService clusterService;

    public ShardsAllocationHealthIndicatorService(ClusterService clusterService) {
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
        var status = new ShardAllocationStatus();

        for (IndexRoutingTable indexShardRouting : state.routingTable()) {
            for (IndexShardRoutingTable shardRouting : indexShardRouting) {
                status.addPrimary(shardRouting.primaryShard());
                for (ShardRouting replicaShard : shardRouting.replicaShards()) {
                    status.addReplica(replicaShard);
                }
            }
        }

        return createIndicator(status.getStatus(), status.getSummary(), status.getDetails());
    }

    private static class ShardAllocationCounts {
        private int unassigned = 0;
        private int unassigned_restarting = 0;
        private int initializing = 0;
        private int started = 0;
        private int relocating = 0;

        public void increment(ShardRouting routing) {
            switch (routing.state()) {
                case UNASSIGNED -> {
                    unassigned++;
                    if (routing.unassignedInfo() != null && routing.unassignedInfo().getReason() == UnassignedInfo.Reason.NODE_RESTARTING) {
                        unassigned_restarting++;
                    }
                }
                case INITIALIZING -> initializing++;
                case STARTED -> started++;
                case RELOCATING -> relocating++;
            }
        }
    }

    private static class ShardAllocationStatus {
        private final ShardAllocationCounts primaries = new ShardAllocationCounts();
        private final ShardAllocationCounts replicas = new ShardAllocationCounts();

        public void addPrimary(ShardRouting routing) {
            primaries.increment(routing);
        }

        public void addReplica(ShardRouting routing) {
            replicas.increment(routing);
        }

        public HealthStatus getStatus() {
            if (primaries.unassigned > 0) {
                return RED;
            } else if (replicas.unassigned > replicas.unassigned_restarting) {
                return YELLOW;
            } else {
                return GREEN;
            }
        }

        public String getSummary() {
            var builder = new StringBuilder("This cluster has ");
            if (primaries.unassigned > 0 || replicas.unassigned > 0) {

                builder.append(
                    Stream.of(
                        Stream.of("1 unassigned primary").filter(it -> primaries.unassigned == 1),
                        Stream.of(primaries.unassigned + " unassigned primaries").filter(it -> primaries.unassigned > 1),
                        Stream.of("1 unassigned replica").filter(it -> replicas.unassigned == 1),
                        Stream.of(replicas.unassigned + " unassigned replicas").filter(it -> replicas.unassigned > 1)
                    ).flatMap(Function.identity()).collect(joining(" and "))
                ).append(".");
            } else {
                builder.append("no unassigned shards.");
            }
            return builder.toString();
        }

        public SimpleHealthIndicatorDetails getDetails() {
            return new SimpleHealthIndicatorDetails(
                Map.of(
                    "unassigned_primaries",
                    primaries.unassigned,
                    "initializing_primaries",
                    primaries.initializing,
                    "started_primaries",
                    primaries.started,
                    "relocating_primaries",
                    primaries.relocating,
                    "unassigned_replicas",
                    replicas.unassigned,
                    "initializing_replicas",
                    replicas.initializing,
                    "started_replicas",
                    replicas.started,
                    "relocating_replicas",
                    replicas.relocating
                )
            );
        }
    }
}
