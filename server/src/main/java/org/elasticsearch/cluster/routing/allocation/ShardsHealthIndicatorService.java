/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
                } else if (isInitializing(primaryShard)) {
                    stats.initializingPrimaries.add(primaryShard.shardId());
                } else {
                    stats.unallocatedPrimaries.add(primaryShard.shardId());
                }

                if (shardRouting.replicaShards().isEmpty()) {
                    stats.unreplicatedPrimaries.add(primaryShard.shardId());
                } else {
                    for (ShardRouting replicaShard : shardRouting.replicaShards()) {
                        if (replicaShard.active()) {
                            stats.allocatedReplicas++;
                        } else if (isRestarting(replicaShard)) {
                            stats.restartingReplicas.add(replicaShard.shardId());
                        } else {
                            stats.unallocatedReplicas.add(replicaShard.shardId());
                        }
                    }
                }
            }
        }
        return createIndicator(stats.getStatus(), stats.getSummary(), stats.getDetails());
    }

    private static boolean isInitializing(ShardRouting shard) {
        return ClusterShardHealth.getInactivePrimaryHealth(shard) == ClusterHealthStatus.YELLOW;
    }

    private static boolean isRestarting(ShardRouting shard) {
        return shard.unassignedInfo() != null && shard.unassignedInfo().getReason() == UnassignedInfo.Reason.NODE_RESTARTING;
    }

    protected static final class ShardAllocationStats {
        // green
        public int allocatedPrimaries = 0;
        public int allocatedReplicas = 0;
        public final List<ShardId> restartingReplicas = new ArrayList<>();
        // yellow
        public final List<ShardId> initializingPrimaries = new ArrayList<>();
        public final List<ShardId> unreplicatedPrimaries = new ArrayList<>();
        public final List<ShardId> unallocatedReplicas = new ArrayList<>();
        // red
        public final List<ShardId> unallocatedPrimaries = new ArrayList<>();

        HealthStatus getStatus() {
            if (unallocatedPrimaries.isEmpty() == false) {
                return RED;
            } else if (unallocatedReplicas.isEmpty() == false
                || unreplicatedPrimaries.isEmpty() == false
                || initializingPrimaries.isEmpty() == false) {
                    return YELLOW;
                } else {
                    return GREEN;
                }
        }

        String getSummary() {
            var primaries = allocatedPrimaries + unallocatedPrimaries.size() + initializingPrimaries.size();
            var replicas = allocatedReplicas + unallocatedReplicas.size() + restartingReplicas.size();
            var builder = new StringBuilder("This cluster has ").append(primaries + replicas).append(" shards");
            builder.append(" including ").append(primaries).append(" primaries");
            collectionToDelimitedStringWithLimit(initializingPrimaries, ",", " (", " initializing)", 1024, builder);
            collectionToDelimitedStringWithLimit(unreplicatedPrimaries, ",", " (", " unreplicated)", 1024, builder);
            collectionToDelimitedStringWithLimit(unallocatedPrimaries, ",", " (", " unallocated)", 1024, builder);
            builder.append(" and ").append(replicas).append(" replicas");
            collectionToDelimitedStringWithLimit(unallocatedReplicas, ",", " (", " unallocated)", 1024, builder);
            collectionToDelimitedStringWithLimit(
                restartingReplicas,
                ",",
                " (",
                " temporary unallocated, node is restarting)",
                1024,
                builder
            );
            return builder.append(".").toString();
        }

        HealthIndicatorDetails getDetails() {
            return new ShardHealthIndicatorDetails(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardAllocationStats that = (ShardAllocationStats) o;
            return allocatedPrimaries == that.allocatedPrimaries
                && allocatedReplicas == that.allocatedReplicas
                && restartingReplicas.equals(that.restartingReplicas)
                && initializingPrimaries.equals(that.initializingPrimaries)
                && unreplicatedPrimaries.equals(that.unreplicatedPrimaries)
                && unallocatedReplicas.equals(that.unallocatedReplicas)
                && unallocatedPrimaries.equals(that.unallocatedPrimaries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                allocatedPrimaries,
                allocatedReplicas,
                restartingReplicas,
                initializingPrimaries,
                unreplicatedPrimaries,
                unallocatedReplicas,
                unallocatedPrimaries
            );
        }

        @Override
        public String toString() {
            return "ShardAllocationStats{"
                + "allocatedPrimaries="
                + allocatedPrimaries
                + ", allocatedReplicas="
                + allocatedReplicas
                + ", restartingReplicas="
                + restartingReplicas
                + ", initializingPrimaries="
                + initializingPrimaries
                + ", unreplicatedPrimaries="
                + unreplicatedPrimaries
                + ", unallocatedReplicas="
                + unallocatedReplicas
                + ", unallocatedPrimaries="
                + unallocatedPrimaries
                + '}';
        }
    }

    protected static final class ShardHealthIndicatorDetails implements HealthIndicatorDetails {
        private final ShardAllocationStats stats;

        public ShardHealthIndicatorDetails(ShardAllocationStats stats) {
            this.stats = stats;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("allocated_primaries_count", stats.allocatedPrimaries);
            builder.field("allocated_replicas_count", stats.allocatedReplicas);
            addListAndCount(builder, "unreplicated_primaries", stats.unreplicatedPrimaries);
            addListAndCount(builder, "initializing_primaries", stats.initializingPrimaries);
            addListAndCount(builder, "unallocated_replicas", stats.unallocatedReplicas);
            addListAndCount(builder, "restarting_replicas", stats.restartingReplicas);
            addListAndCount(builder, "unallocated_primaries", stats.unallocatedPrimaries);
            builder.endObject();
            return builder;
        }

        private void addListAndCount(XContentBuilder builder, String name, List<ShardId> list) throws IOException {
            builder.field(name + "_count", list.size()).field(name, limitSize(list, 10));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardHealthIndicatorDetails that = (ShardHealthIndicatorDetails) o;
            return stats.equals(that.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stats);
        }

        @Override
        public String toString() {
            return stats.toString();
        }
    }
}
