/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.List;

public class StatelessExistingShardsAllocator implements ExistingShardsAllocator {

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {

    }

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {

    }

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        // In stateful implementation this method is called to determine the node that used to store the shard
        // and assign it using `UnassignedIterator#initialize(...)` or postpone initialization via
        // `UnassignedIterator#removeAndIgnore(...)` if the file list is not available yet to make a decision.

        // In stateless implementation all data is kept in the object store and is downloaded before initializing.
        // Existing shard is not ignored nor initialized here so that it would be assigned from scratch by
        // ShardsAllocator implementation.
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    @Override
    public void cleanCaches() {

    }

    @Override
    public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {

    }

    @Override
    public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {

    }

    @Override
    public int getNumberOfInFlightFetches() {
        return 0;
    }
}
