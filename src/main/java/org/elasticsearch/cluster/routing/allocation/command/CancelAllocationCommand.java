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
import org.elasticsearch.index.shard.ShardId;

import java.util.Iterator;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;

/**
 * A command that cancel relocation, or recovery of a given shard on a node.
 */
public class CancelAllocationCommand implements AllocationCommand {

    private final ShardId shardId;

    private final String nodeId;

    public CancelAllocationCommand(ShardId shardId, String node) {
        this.shardId = shardId;
        this.nodeId = node;
    }

    @Override
    public void execute(RoutingAllocation allocation) throws ElasticSearchException {
        DiscoveryNode node = allocation.nodes().resolveNode(nodeId);

        boolean found = false;
        for (Iterator<MutableShardRouting> it = allocation.routingNodes().node(node.id()).iterator(); it.hasNext(); ) {
            MutableShardRouting shardRouting = it.next();
            if (!shardRouting.shardId().equals(shardId)) {
                continue;
            }
            found = true;
            if (shardRouting.relocatingNodeId() != null) {
                if (shardRouting.initializing()) {
                    // the shard is initializing and recovering from another node, simply cancel the recovery
                    it.remove();
                    shardRouting.deassignNode();
                    // and cancel the relocating state from the shard its being relocated from
                    RoutingNode relocatingFromNode = allocation.routingNodes().node(shardRouting.relocatingNodeId());
                    if (relocatingFromNode != null) {
                        for (MutableShardRouting fromShardRouting : relocatingFromNode) {
                            if (fromShardRouting.shardId().equals(shardRouting.shardId()) && shardRouting.state() == RELOCATING) {
                                fromShardRouting.cancelRelocation();
                                break;
                            }
                        }
                    }
                } else if (shardRouting.relocating()) {
                    // the shard is relocating to another node, cancel the recovery on the other node, and deallocate this one
                    if (shardRouting.primary()) {
                        // can't cancel a primary shard being initialized
                        throw new ElasticSearchIllegalArgumentException("[cancel_allocation] can't cancel " + shardId + " on node " + node + ", shard is primary and initializing its state");
                    }
                    it.remove();
                    allocation.routingNodes().unassigned().add(new MutableShardRouting(shardRouting.index(), shardRouting.id(),
                            null, shardRouting.primary(), ShardRoutingState.UNASSIGNED, shardRouting.version() + 1));

                    // now, go and find the shard that is initializing on the target node, and cancel it as well...
                    RoutingNode initializingNode = allocation.routingNodes().node(shardRouting.relocatingNodeId());
                    if (initializingNode != null) {
                        for (Iterator<MutableShardRouting> itX = initializingNode.iterator(); itX.hasNext(); ) {
                            MutableShardRouting initializingShardRouting = itX.next();
                            if (initializingShardRouting.shardId().equals(shardRouting.shardId()) && initializingShardRouting.state() == INITIALIZING) {
                                shardRouting.deassignNode();
                                itX.remove();
                            }
                        }
                    }
                }
            } else {
                // the shard is not relocating, its either started, or initializing, just cancel it and move on...
                if (shardRouting.primary()) {
                    // can't cancel a primary shard being initialized
                    throw new ElasticSearchIllegalArgumentException("[cancel_allocation] can't cancel " + shardId + " on node " + node + ", shard is primary and initializing its state");
                }
                it.remove();
                allocation.routingNodes().unassigned().add(new MutableShardRouting(shardRouting.index(), shardRouting.id(),
                        null, shardRouting.primary(), ShardRoutingState.UNASSIGNED, shardRouting.version() + 1));
            }
        }

        if (!found) {
            throw new ElasticSearchIllegalArgumentException("[cancel_allocation] can't cancel " + shardId + ", failed to find it on node " + node);
        }
    }
}
