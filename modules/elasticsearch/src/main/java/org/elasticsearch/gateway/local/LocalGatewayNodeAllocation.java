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

package org.elasticsearch.gateway.local;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.NodeAllocation;
import org.elasticsearch.cluster.routing.allocation.NodeAllocations;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;

/**
 * @author kimchy (shay.banon)
 */
public class LocalGatewayNodeAllocation extends NodeAllocation {

    private final TransportNodesListGatewayState listGatewayState;

    @Inject public LocalGatewayNodeAllocation(Settings settings, TransportNodesListGatewayState listGatewayState) {
        super(settings);
        this.listGatewayState = listGatewayState;
    }

    @Override public void applyFailedShards(NodeAllocations nodeAllocations, RoutingNodes routingNodes, DiscoveryNodes nodes, List<? extends ShardRouting> failedShards) {
        for (ShardRouting failedShard : failedShards) {
            IndexRoutingTable indexRoutingTable = routingNodes.routingTable().index(failedShard.index());
            if (!routingNodes.blocks().hasIndexBlock(indexRoutingTable.index(), LocalGateway.INDEX_NOT_RECOVERED_BLOCK)) {
                continue;
            }

            // we are still in the initial allocation, find another node with existing shards
            // all primary are unassigned for the index, see if we can allocate it on existing nodes, if not, don't assign
            Set<String> nodesIds = Sets.newHashSet();
            nodesIds.addAll(nodes.dataNodes().keySet());
            nodesIds.addAll(nodes.masterNodes().keySet());
            TransportNodesListGatewayState.NodesLocalGatewayState nodesState = listGatewayState.list(nodesIds, null).actionGet();

            // make a list of ShardId to Node, each one from the latest version
            Tuple<DiscoveryNode, Long> t = null;
            for (TransportNodesListGatewayState.NodeLocalGatewayState nodeState : nodesState) {
                // we don't want to reallocate to the node we failed on
                if (nodeState.node().id().equals(failedShard.currentNodeId())) {
                    continue;
                }
                // go and find
                for (Map.Entry<ShardId, Long> entry : nodeState.state().shards().entrySet()) {
                    if (entry.getKey().equals(failedShard.shardId())) {
                        if (t == null || entry.getValue() > t.v2().longValue()) {
                            t = new Tuple<DiscoveryNode, Long>(nodeState.node(), entry.getValue());
                        }
                    }
                }
            }
            if (t != null) {
                // we found a node to allocate to, do it
                RoutingNode currentRoutingNode = routingNodes.nodesToShards().get(failedShard.currentNodeId());
                if (currentRoutingNode == null) {
                    // already failed (might be called several times for the same shard)
                    continue;
                }

                // find the shard and cancel relocation
                Iterator<MutableShardRouting> shards = currentRoutingNode.iterator();
                while (shards.hasNext()) {
                    MutableShardRouting shard = shards.next();
                    if (shard.shardId().equals(failedShard.shardId())) {
                        shard.deassignNode();
                        shards.remove();
                        break;
                    }
                }

                RoutingNode targetNode = routingNodes.nodesToShards().get(t.v1().id());
                targetNode.add(new MutableShardRouting(failedShard.index(), failedShard.id(),
                        targetNode.nodeId(), failedShard.relocatingNodeId(),
                        failedShard.primary(), INITIALIZING));
            }
        }
    }

    @Override public boolean allocateUnassigned(NodeAllocations nodeAllocations, RoutingNodes routingNodes, DiscoveryNodes nodes) {
        boolean changed = false;

        for (IndexRoutingTable indexRoutingTable : routingNodes.routingTable()) {
            // only do the allocation if there is a local "INDEX NOT RECOVERED" block
            if (!routingNodes.blocks().hasIndexBlock(indexRoutingTable.index(), LocalGateway.INDEX_NOT_RECOVERED_BLOCK)) {
                continue;
            }

            if (indexRoutingTable.allPrimaryShardsUnassigned()) {
                // all primary are unassigned for the index, see if we can allocate it on existing nodes, if not, don't assign
                Set<String> nodesIds = Sets.newHashSet();
                nodesIds.addAll(nodes.dataNodes().keySet());
                nodesIds.addAll(nodes.masterNodes().keySet());
                TransportNodesListGatewayState.NodesLocalGatewayState nodesState = listGatewayState.list(nodesIds, null).actionGet();

                // make a list of ShardId to Node, each one from the latest version
                Map<ShardId, Tuple<DiscoveryNode, Long>> shards = Maps.newHashMap();
                for (TransportNodesListGatewayState.NodeLocalGatewayState nodeState : nodesState) {
                    for (Map.Entry<ShardId, Long> entry : nodeState.state().shards().entrySet()) {
                        if (entry.getKey().index().name().equals(indexRoutingTable.index())) {
                            Tuple<DiscoveryNode, Long> t = shards.get(entry.getKey());
                            if (t == null || entry.getValue() > t.v2().longValue()) {
                                t = new Tuple<DiscoveryNode, Long>(nodeState.node(), entry.getValue());
                                shards.put(entry.getKey(), t);
                            }
                        }
                    }
                }

                // check if we managed to allocate to all of them, if not, move all relevant shards to ignored
                if (shards.size() < indexRoutingTable.shards().size()) {
                    for (Iterator<MutableShardRouting> it = routingNodes.unassigned().iterator(); it.hasNext();) {
                        MutableShardRouting shardRouting = it.next();
                        if (shardRouting.index().equals(indexRoutingTable.index())) {
                            it.remove();
                            routingNodes.ignoredUnassigned().add(shardRouting);
                        }
                    }
                } else {
                    changed = true;
                    // we found all nodes to allocate to, do the allocation
                    for (Iterator<MutableShardRouting> it = routingNodes.unassigned().iterator(); it.hasNext();) {
                        MutableShardRouting shardRouting = it.next();
                        if (shardRouting.primary()) {
                            DiscoveryNode node = shards.get(shardRouting.shardId()).v1();
                            RoutingNode routingNode = routingNodes.node(node.id());
                            routingNode.add(shardRouting);
                            it.remove();
                        }
                    }
                }
            }
        }

        // TODO optimize replica allocation to existing work locations

        return changed;
    }
}
