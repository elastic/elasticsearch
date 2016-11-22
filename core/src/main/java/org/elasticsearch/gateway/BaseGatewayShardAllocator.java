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

package org.elasticsearch.gateway;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

/**
 * An abstract class that implements basic functionality for allocating
 * shards to nodes based on shard copies that already exist in the cluster.
 *
 * Individual implementations of this class are responsible for providing
 * the logic to determine to which nodes (if any) those shards are allocated.
 */
public abstract class BaseGatewayShardAllocator extends AbstractComponent {

    public BaseGatewayShardAllocator(Settings settings) {
        super(settings);
    }

    /**
     * Allocate unassigned shards to nodes (if any) where valid copies of the shard already exist.
     * It is up to the individual implementations of {@link #makeAllocationDecision(ShardRouting, RoutingAllocation, Logger)}
     * to make decisions on assigning shards to nodes.
     *
     * @param allocation the allocation state container object
     */
    public void allocateUnassigned(RoutingAllocation allocation) {
        final RoutingNodes routingNodes = allocation.routingNodes();
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            final ShardRouting shard = unassignedIterator.next();
            final AllocateUnassignedDecision allocateUnassignedDecision = makeAllocationDecision(shard, allocation, logger);

            if (allocateUnassignedDecision.isDecisionTaken() == false) {
                // no decision was taken by this allocator
                continue;
            }

            if (allocateUnassignedDecision.getFinalDecisionSafe() == Decision.Type.YES) {
                unassignedIterator.initialize(allocateUnassignedDecision.getAssignedNodeId(),
                    allocateUnassignedDecision.getAllocationId(),
                    shard.primary() ? ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE :
                                      allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                    allocation.changes());
            } else {
                unassignedIterator.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
            }
        }
    }

    /**
     * Make a decision on the allocation of an unassigned shard.  This method is used by
     * {@link #allocateUnassigned(RoutingAllocation)} to make decisions about whether or not
     * the shard can be allocated by this allocator and if so, to which node it will be allocated.
     *
     * @param unassignedShard  the unassigned shard to allocate
     * @param allocation       the current routing state
     * @param logger           the logger
     * @return an {@link AllocateUnassignedDecision} with the final decision of whether to allocate and details of the decision
     */
    public abstract AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard,
                                                                      RoutingAllocation allocation,
                                                                      Logger logger);
}
