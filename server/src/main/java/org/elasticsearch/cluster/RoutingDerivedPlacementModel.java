/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.List;

/**
 * Routing-derived replacement for the delta simulators behind {@link ClusterInfoSimulator} (disk, write load, cache commitments), which
 * replaced the {@code maybeSimulateAlreadyStartedShards} heuristic.
 *
 * <p>Drives a set of {@link PlacementAspect}s from the routing table and the poll-time placement ({@link ClusterInfo#dataPath}), then
 * from each shard the computation starts. A shard that moved since the poll needs no source-node inference: it is simply absent from
 * the old node's current placement and present in its poll placement. The model holds no aggregate itself; callers keep a typed
 * reference to each aspect and read {@link PlacementAspect#simulated()}.
 */
public class RoutingDerivedPlacementModel {

    private final List<PlacementAspect<?>> aspects;

    public RoutingDerivedPlacementModel(RoutingAllocation allocation, PlacementAspect<?>... aspects) {
        this.aspects = List.of(aspects);
        final ClusterInfo polled = allocation.clusterInfo();

        // current placement: everything except initializing shards, which are added by shardStarted()
        for (RoutingNode routingNode : allocation.routingNodes()) {
            for (ShardRouting shard : routingNode) {
                if (shard.initializing() == false) {
                    for (var aspect : this.aspects) {
                        aspect.shardPlaced(shard);
                    }
                }
            }
        }
        // poll placement
        for (ClusterInfo.NodeAndShard nodeAndShard : polled.dataPath.keySet()) {
            final RoutingNode routingNode = allocation.routingNodes().node(nodeAndShard.nodeId());
            final ShardRouting current = routingNode == null ? null : routingNode.getByShardId(nodeAndShard.shardId());
            for (var aspect : this.aspects) {
                aspect.shardPlacedAtPoll(nodeAndShard.nodeId(), nodeAndShard.shardId(), current);
            }
        }
    }

    /** Must be called for each initializing shard the computation starts, before it is started in the routing table. */
    public void shardStarted(ShardRouting target) {
        assert target.initializing() : target;
        for (var aspect : aspects) {
            aspect.shardStarted(target);
        }
    }
}
