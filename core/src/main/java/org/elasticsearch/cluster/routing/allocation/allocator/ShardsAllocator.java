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

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;

/**
 * <p>
 * A {@link ShardsAllocator} is the main entry point for shard allocation on nodes in the cluster.
 * The allocator makes basic decision where a shard instance will be allocated, if already allocated instances
 * need relocate to other nodes due to node failures or due to rebalancing decisions.
 * </p>
 */
public interface ShardsAllocator {

    /**
     * Applies changes on started nodes based on the implemented algorithm. For example if a 
     * shard has changed to {@link ShardRoutingState#STARTED} from {@link ShardRoutingState#RELOCATING} 
     * this allocator might apply some cleanups on the node that used to hold the shard.
     * @param allocation all started {@link ShardRouting shards}
     */
    void applyStartedShards(StartedRerouteAllocation allocation);

    /**
     * Applies changes on failed nodes based on the implemented algorithm. 
     * @param allocation all failed {@link ShardRouting shards}
     */
    void applyFailedShards(FailedRerouteAllocation allocation);

    /**
     * Assign all unassigned shards to nodes 
     * 
     * @param allocation current node allocation
     * @return <code>true</code> if the allocation has changed, otherwise <code>false</code>
     */
    boolean allocateUnassigned(RoutingAllocation allocation);

    /**
     * Rebalancing number of shards on all nodes
     *   
     * @param allocation current node allocation
     * @return <code>true</code> if the allocation has changed, otherwise <code>false</code>
     */
    boolean rebalance(RoutingAllocation allocation);

    /**
     * Moves a shard from the given node to other node.
     * 
     * @param shardRouting the shard to move
     * @param node A node containing the shard
     * @param allocation current node allocation
     * @return <code>true</code> if the allocation has changed, otherwise <code>false</code>
     */
    boolean move(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation);
}
