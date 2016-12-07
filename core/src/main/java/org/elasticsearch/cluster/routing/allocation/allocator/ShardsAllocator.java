/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.MoveDecision;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;

import java.util.Map;
/**
 * <p>
 * A {@link ShardsAllocator} is the main entry point for shard allocation on nodes in the cluster.
 * The allocator makes basic decision where a shard instance will be allocated, if already allocated instances
 * need to relocate to other nodes due to node failures or due to rebalancing decisions.
 * </p>
 */
public interface ShardsAllocator {

    /**
     * Allocates shards to nodes in the cluster. An implementation of this method should:
     * - assign unassigned shards
     * - relocate shards that cannot stay on a node anymore
     * - relocate shards to find a good shard balance in the cluster
     *
     * @param allocation current node allocation
     */
    void allocate(RoutingAllocation allocation);

    /**
     * Returns a map of node to a float "weight" of where the allocator would like to place the shard.
     * Higher weights signify greater desire to place the shard on that node.
     * Does not modify the allocation at all.
     *
     * @param allocation current node allocation
     * @param shard shard to weigh
     * @return map of nodes to float weights
     */
    @Deprecated
    Map<DiscoveryNode, Float> weighShard(RoutingAllocation allocation, ShardRouting shard);

    /**
     * Returns the decision for where a shard should reside in the cluster.  If the shard is unassigned,
     * then the {@link AllocateUnassignedDecision} will be non-null.  If the shard is not in the unassigned
     * state, then the {@link MoveDecision} will be non-null.
     *
     * This method is primarily used by the cluster allocation explain API to provide detailed explanations
     * for the allocation of a single shard.  Implementations of the {@link #allocate(RoutingAllocation)} method
     * may use the results of this method implementation to decide on allocating shards in the routing table
     * to the cluster.
     */
    default ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        throw new UnsupportedOperationException(getClass().getName() + " must implement decideShardAllocation");
    }
}
