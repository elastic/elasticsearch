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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * {@link TieredAllocationDecider} is an abstract base class that allows to make
 * dynamic cluster- or index-wide shard allocation decisions on a per-node
 * basis based on the decisions of lower tiered allocation deciders.
 */
public abstract class TieredAllocationDecider extends AllocationDecider {

    public abstract Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation,
                                 LowerTierDecider lowerTierDecider);

    public abstract Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation,
                              LowerTierDecider lowerTierDecider);

    @Override
    public final Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        assert shardRouting.unassigned() : "must not call canForceAllocatePrimary on an assigned shard " + shardRouting;
        Decision decision = canAllocate(shardRouting, node, allocation, new LowerTierDecider() {
            @Override
            public Set<RoutingNode> candidates() {
                return StreamSupport.stream(allocation.routingNodes().spliterator(), false).collect(Collectors.toSet());
            }

            @Override
            public Decision decisionFor(RoutingNode node) {
                return Decision.YES;
            }
        });
        if (decision.type() == Decision.Type.NO) {
            // On a NO decision, by default, we allow force allocating the primary.
            return allocation.decision(Decision.YES,
                decision.label(),
                "primary shard [%s] allowed to force allocate on node [%s]",
                shardRouting.shardId(), node.nodeId());
        } else {
            // On a THROTTLE/YES decision, we use the same decision instead of forcing allocation
            return decision;
        }
    }

    public interface LowerTierDecider {
        Set<RoutingNode> candidates();
        Decision decisionFor(RoutingNode node);
    }
}
