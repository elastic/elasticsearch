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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashSet;
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
public class AllocationHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "shards";

    private final ClusterService clusterService;

    public AllocationHealthIndicatorService(ClusterService clusterService) {
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

        var greenShards = 0;
        var yellowShards = new HashSet<ShardId>();
        var redShards = new HashSet<ShardId>();

        for (IndexRoutingTable indexShardRouting : state.routingTable()) {
            for (IndexShardRoutingTable shardRouting : indexShardRouting) {
                var health = calculateShardHealthStatus(shardRouting);
                switch (health) {
                    case GREEN -> greenShards++;
                    case YELLOW -> yellowShards.add(shardRouting.shardId());
                    case RED -> redShards.add(shardRouting.shardId());
                }
            }
        }

        var status = redShards.isEmpty() ? (yellowShards.isEmpty() ? GREEN : YELLOW) : RED;

        // TODO 83240 add some kind of explain like summary for unallocated shards
        return createIndicator(
            status,
            "TODO 83240",
            new SimpleHealthIndicatorDetails(
                Map.of(
                    "green-shards-count",
                    greenShards,
                    "yellow-shards-count",
                    yellowShards.size(),
                    "yellow-shards",
                    yellowShards.stream().limit(10).toList(),
                    "red-shards-count",
                    redShards.size(),
                    "red-shards",
                    redShards.stream().limit(10).toList()
                )
            )
        );
    }

    private static HealthStatus calculateShardHealthStatus(IndexShardRoutingTable shardRoutingTable) {
        boolean primaryActive = shardRoutingTable.primaryShard().active();
        boolean replicaActive = shardRoutingTable.replicaShards().stream().anyMatch(ShardRouting::active);
        if (primaryActive == false) {
            return RED;
        } else if (replicaActive == false) {
            return YELLOW;
        } else {
            return GREEN;
        }
    }
}
