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
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.Strings.collectionToDelimitedStringWithLimit;
import static org.elasticsearch.common.util.CollectionUtils.limitSize;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;

/**
 * This indicator reports health for shards.
 *
 * Indicator will report:
 * * RED when one or more shards are not available
 * * YELLOW when one or more shards are not replicated
 * * GREEN otherwise
 *
 * Each shard needs to be available and replicated in order to guarantee high availability and prevent data loses.
 */
public class ShardsHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "shards";

    private final ClusterService clusterService;

    public ShardsHealthIndicatorService(ClusterService clusterService) {
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
        var stats = new ShardAllocationStats();

        for (IndexRoutingTable indexShardRouting : state.routingTable()) {
            for (IndexShardRoutingTable shardRouting : indexShardRouting) {
                var primaryShard = shardRouting.primaryShard();

                if (primaryShard.active()) {
                    stats.allocatedPrimaries++;
                } else {
                    stats.unallocatedPrimaries.add(primaryShard.shardId());
                }

                for (ShardRouting replicaShard : shardRouting.replicaShards()) {
                    if (replicaShard.active()) {
                        stats.allocatedReplicas++;
                    } else if (isRestarting(replicaShard)) {
                        stats.restartingReplicas.add(replicaShard.shardId());
                    } else {
                        stats.unallocatedReplicas.add(replicaShard.shardId());
                    }
                }

                if (shardRouting.replicaShards().isEmpty()) {
                    stats.unreplicatedPrimaries.add(primaryShard.shardId());
                }
            }
        }
        return createIndicator(stats.getStatus(), stats.getSummary(), stats.getDetails());
    }

    private static boolean isRestarting(ShardRouting shard) {
        return shard.unassignedInfo() != null && shard.unassignedInfo().getReason() == UnassignedInfo.Reason.NODE_RESTARTING;
    }

    private static final class ShardAllocationStats {
        // green
        private int allocatedPrimaries = 0;
        private int allocatedReplicas = 0;
        private final List<ShardId> restartingReplicas = new ArrayList<>();
        // yellow
        private final List<ShardId> unreplicatedPrimaries = new ArrayList<>();
        private final List<ShardId> unallocatedReplicas = new ArrayList<>();
        // red
        private final List<ShardId> unallocatedPrimaries = new ArrayList<>();

        HealthStatus getStatus() {
            if (unallocatedPrimaries.isEmpty() == false) {
                return RED;
            } else if (unallocatedReplicas.isEmpty() == false || unreplicatedPrimaries.isEmpty() == false) {
                return YELLOW;
            } else {
                return GREEN;
            }
        }

        String getSummary() {
            var primaries = allocatedPrimaries + unallocatedPrimaries.size();
            var replicas = allocatedReplicas + unallocatedReplicas.size() + restartingReplicas.size();
            var builder = new StringBuilder("This cluster has ").append(primaries + replicas).append(" shards");
            builder.append(" including ").append(primaries).append(" primaries");
            collectionToDelimitedStringWithLimit(unreplicatedPrimaries, ",", " (", " unreplicated)", 1024, builder);
            collectionToDelimitedStringWithLimit(unallocatedPrimaries, ",", " (", " unallocated)", 1024, builder);
            builder.append(" and ").append(replicas).append(" replicas");
            collectionToDelimitedStringWithLimit(unallocatedReplicas, ",", " (", " unallocated)", 1024, builder);
            collectionToDelimitedStringWithLimit(
                restartingReplicas,
                ",",
                " (",
                " temporary unallocated due to node restarting)",
                1024,
                builder
            );
            return builder.append(".").toString();
        }

        HealthIndicatorDetails getDetails() {
            return new SimpleHealthIndicatorDetails(
                Map.of(
                    "allocated_primaries_count",
                    allocatedPrimaries,
                    "allocated_replicas_count",
                    allocatedReplicas,
                    "unreplicated_primaries_count",
                    unreplicatedPrimaries.size(),
                    "unreplicated_primaries",
                    limitSize(unreplicatedPrimaries, 10),
                    "unallocated_replicas_count",
                    unallocatedReplicas.size(),
                    "unallocated_replicas",
                    limitSize(unallocatedReplicas, 10),
                    "restarting_replicas_count",
                    restartingReplicas.size(),
                    "restarting_replicas",
                    limitSize(restartingReplicas, 10),
                    "unallocated_primaries_count",
                    unallocatedPrimaries.size(),
                    "unallocated_primaries",
                    limitSize(unallocatedPrimaries, 10)
                )
            );
        }
    }
}
