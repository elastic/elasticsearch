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
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.shard.ShardId;

import java.util.Iterator;

/**
 * Allocates an unassigned shard to a specific node. Note, primary allocation will "force"
 * allocation which might mean one will loose data if using local gateway..., use with care
 * with the <tt>allowPrimary</tt> flag.
 */
public class AllocateAllocationCommand implements AllocationCommand {

    private final ShardId shardId;
    private final String nodeId;
    private final boolean allowPrimary;

    public AllocateAllocationCommand(ShardId shardId, String nodeId, boolean allowPrimary) {
        this.shardId = shardId;
        this.nodeId = nodeId;
        this.allowPrimary = allowPrimary;
    }

    @Override
    public void execute(RoutingAllocation allocation) throws ElasticSearchException {
        DiscoveryNode node = allocation.nodes().resolveNode(nodeId);

        MutableShardRouting shardRouting = null;
        for (MutableShardRouting routing : allocation.routingNodes().unassigned()) {
            if (routing.shardId().equals(shardId)) {
                // prefer primaries first to allocate
                if (shardRouting == null || routing.primary()) {
                    shardRouting = routing;
                }
            }
        }

        if (shardRouting == null) {
            throw new ElasticSearchIllegalArgumentException("[allocate] failed to find " + shardId + " on the list of unassigned shards");
        }

        if (shardRouting.primary() && !allowPrimary) {
            throw new ElasticSearchIllegalArgumentException("[allocate] trying to allocate a primary shard " + shardId + "], which is disabled");
        }

        RoutingNode routingNode = allocation.routingNodes().node(node.id());
        allocation.addIgnoreDisable(shardRouting.shardId(), routingNode.nodeId());
        if (!allocation.deciders().canAllocate(shardRouting, routingNode, allocation).allowed()) {
            throw new ElasticSearchIllegalArgumentException("[allocate] allocation of " + shardId + " on node " + node + " is not allowed");
        }
        // go over and remove it from the unassigned
        for (Iterator<MutableShardRouting> it = allocation.routingNodes().unassigned().iterator(); it.hasNext(); ) {
            if (it.next() != shardRouting) {
                continue;
            }
            it.remove();
            routingNode.add(shardRouting);
            break;
        }
    }
}
