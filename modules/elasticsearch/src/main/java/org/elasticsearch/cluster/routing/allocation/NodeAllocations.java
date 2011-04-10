/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

/**
 * Holds several {@link NodeAllocation}s and combines them into a single allocation decision.
 *
 * @author kimchy (shay.banon)
 */
public class NodeAllocations extends NodeAllocation {

    private final NodeAllocation[] allocations;

    public NodeAllocations(Settings settings) {
        this(settings, ImmutableSet.<NodeAllocation>builder()
                .add(new SameShardNodeAllocation(settings))
                .add(new ReplicaAfterPrimaryActiveNodeAllocation(settings))
                .add(new ThrottlingNodeAllocation(settings))
                .add(new RebalanceOnlyWhenActiveNodeAllocation(settings))
                .add(new ClusterRebalanceNodeAllocation(settings))
                .add(new ConcurrentRebalanceNodeAllocation(settings))
                .build()
        );
    }

    @Inject public NodeAllocations(Settings settings, Set<NodeAllocation> allocations) {
        super(settings);
        this.allocations = allocations.toArray(new NodeAllocation[allocations.size()]);
    }

    @Override public void applyStartedShards(NodeAllocations nodeAllocations, StartedRerouteAllocation allocation) {
        for (NodeAllocation allocation1 : allocations) {
            allocation1.applyStartedShards(nodeAllocations, allocation);
        }
    }

    @Override public void applyFailedShards(NodeAllocations nodeAllocations, FailedRerouteAllocation allocation) {
        for (NodeAllocation allocation1 : allocations) {
            allocation1.applyFailedShards(nodeAllocations, allocation);
        }
    }

    @Override public boolean canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        for (NodeAllocation allocation1 : allocations) {
            if (!allocation1.canRebalance(shardRouting, allocation)) {
                return false;
            }
        }
        return true;
    }

    @Override public boolean allocateUnassigned(NodeAllocations nodeAllocations, RoutingAllocation allocation) {
        boolean changed = false;
        for (NodeAllocation allocation1 : allocations) {
            changed |= allocation1.allocateUnassigned(nodeAllocations, allocation);
        }
        return changed;
    }

    @Override public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        Decision ret = Decision.YES;
        // first, check if its in the ignored, if so, return NO
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            return Decision.NO;
        }
        // now, go over the registered allocations
        for (NodeAllocation allocation1 : allocations) {
            Decision decision = allocation1.canAllocate(shardRouting, node, allocation);
            if (decision == Decision.NO) {
                return Decision.NO;
            } else if (decision == Decision.THROTTLE) {
                ret = Decision.THROTTLE;
            }
        }
        return ret;
    }
}
