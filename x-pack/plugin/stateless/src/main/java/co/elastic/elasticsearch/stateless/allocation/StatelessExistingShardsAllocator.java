/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.List;
import java.util.function.Predicate;

public class StatelessExistingShardsAllocator implements ExistingShardsAllocator {

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {}

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation, Predicate<ShardRouting> isRelevantShardPredicate) {}

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
    public void cleanCaches() {}

    @Override
    public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {}

    @Override
    public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {}

    @Override
    public int getNumberOfInFlightFetches() {
        return 0;
    }
}
