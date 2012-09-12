/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.shard.ShardId;

/**
 * A command that moves a shard from a specific node to another node. Note, the shards
 * need to be in "started" state in order to be moved if from is specified.
 */
public class MoveAllocationCommand implements AllocationCommand {

    private final ShardId shardId;
    @Nullable
    private final String fromNode;
    private final String toNode;

    public MoveAllocationCommand(ShardId shardId, @Nullable String fromNode, String toNode) {
        this.shardId = shardId;
        this.fromNode = fromNode;
        this.toNode = toNode;
    }

    @Override
    public void execute(RoutingAllocation allocation) throws ElasticSearchException {
        DiscoveryNode from = allocation.nodes().resolveNode(fromNode);
        DiscoveryNode to = allocation.nodes().resolveNode(toNode);

        boolean found = false;
        for (MutableShardRouting shardRouting : allocation.routingNodes().node(from.id())) {
            if (!shardRouting.shardId().equals(shardId)) {
                continue;
            }
            found = true;

            // TODO we can possibly support also relocating cases, where we cancel relocation and move...
            if (!shardRouting.started()) {
                throw new ElasticSearchIllegalArgumentException("[move_allocation] can't move " + shardId + ", shard is not started (state = " + shardRouting.state() + "]");
            }

            RoutingNode toRoutingNode = allocation.routingNodes().node(to.id());
            AllocationDecider.Decision decision = allocation.deciders().canAllocate(shardRouting, toRoutingNode, allocation);
            if (!decision.allowed()) {
                throw new ElasticSearchIllegalArgumentException("[move_allocation] can't move " + shardId + ", from " + from + ", to " + to + ", since its not allowed");
            }
            if (!decision.allocate()) {
                // its being throttled, maybe have a flag to take it into account and fail? for now, just do it since the "user" wants it...
            }

            toRoutingNode.add(new MutableShardRouting(shardRouting.index(), shardRouting.id(),
                    toRoutingNode.nodeId(), shardRouting.currentNodeId(),
                    shardRouting.primary(), ShardRoutingState.INITIALIZING, shardRouting.version() + 1));

            shardRouting.relocate(toRoutingNode.nodeId());
        }

        if (!found) {
            throw new ElasticSearchIllegalArgumentException("[move_allocation] can't move " + shardId + ", failed to find it on node " + from);
        }
    }
}
