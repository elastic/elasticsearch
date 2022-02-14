/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    /**
     * After timeout inactive shards assigned to the restarted node would be considered unavailable causing yellow or red health status.
     * During normal operations fast restarts should not cause health indicator become yellow or red.
     */
    private static final TimeValue RESTART_HEALTH_DEGRADATION_DELAY = TimeValue.timeValueMinutes(5);

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
        var shutdown = state.metadata().custom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY);
        var now = System.currentTimeMillis();

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

    private static boolean isRestarting(String nodeId, NodesShutdownMetadata metadata, long now) {
        var shutdown = metadata.getAllNodeMetadataMap().get(nodeId);
        return shutdown != null
            && shutdown.getType() == SingleNodeShutdownMetadata.Type.RESTART
            && (now - shutdown.getStartedAtMillis()) <= RESTART_HEALTH_DEGRADATION_DELAY.getMillis();
    }

    private static final class ShardAllocationStats {
        // green
        private int allocatedPrimaries = 0;
        private int allocatedReplicas = 0;
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
            return "TODO 83240";
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
                    CollectionUtils.limitSize(unreplicatedPrimaries, 10),
                    "unallocated_replicas_count",
                    unallocatedReplicas.size(),
                    "unallocated_replicas",
                    CollectionUtils.limitSize(unallocatedReplicas, 10),
                    "unallocated_primaries_count",
                    unallocatedPrimaries.size(),
                    "unallocated_primaries",
                    CollectionUtils.limitSize(unallocatedPrimaries, 10)
                )
            );
        }
    }
}
