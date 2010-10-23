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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.*;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.ExtTObjectIntHasMap;
import org.elasticsearch.common.trove.TObjectIntHashMap;
import org.elasticsearch.common.trove.TObjectIntIterator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;

/**
 * @author kimchy (shay.banon)
 */
public class LocalGatewayNodeAllocation extends NodeAllocation {

    private final TransportNodesListGatewayStartedShards listGatewayStartedShards;

    private final TransportNodesListShardStoreMetaData listShardStoreMetaData;

    private final ConcurrentMap<ShardId, ConcurrentMap<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData>> cachedStores = ConcurrentCollections.newConcurrentMap();

    private final TimeValue listTimeout;

    private final String initialShards;

    @Inject public LocalGatewayNodeAllocation(Settings settings,
                                              TransportNodesListGatewayStartedShards listGatewayStartedShards, TransportNodesListShardStoreMetaData listShardStoreMetaData) {
        super(settings);
        this.listGatewayStartedShards = listGatewayStartedShards;
        this.listShardStoreMetaData = listShardStoreMetaData;

        this.listTimeout = componentSettings.getAsTime("list_timeout", TimeValue.timeValueSeconds(30));
        this.initialShards = componentSettings.get("initial_shards", "quorum");
    }

    @Override public void applyStartedShards(NodeAllocations nodeAllocations, StartedRerouteAllocation allocation) {
        for (ShardRouting shardRouting : allocation.startedShards()) {
            cachedStores.remove(shardRouting.shardId());
        }
    }

    @Override public void applyFailedShards(NodeAllocations nodeAllocations, FailedRerouteAllocation allocation) {
        for (ShardRouting shardRouting : allocation.failedShards()) {
            cachedStores.remove(shardRouting.shardId());
        }
        for (ShardRouting failedShard : allocation.failedShards()) {
            IndexRoutingTable indexRoutingTable = allocation.routingTable().index(failedShard.index());
            if (!allocation.routingNodes().blocks().hasIndexBlock(indexRoutingTable.index(), GatewayService.INDEX_NOT_RECOVERED_BLOCK)) {
                continue;
            }

            // we are still in the initial allocation, find another node with existing shards
            // all primary are unassigned for the index, see if we can allocate it on existing nodes, if not, don't assign
            Set<String> nodesIds = Sets.newHashSet();
            nodesIds.addAll(allocation.nodes().dataNodes().keySet());
            TransportNodesListGatewayStartedShards.NodesLocalGatewayStartedShards nodesState = listGatewayStartedShards.list(nodesIds, null).actionGet();

            // make a list of ShardId to Node, each one from the latest version
            Tuple<DiscoveryNode, Long> t = null;
            for (TransportNodesListGatewayStartedShards.NodeLocalGatewayStartedShards nodeState : nodesState) {
                if (nodeState.state() == null) {
                    continue;
                }
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
                RoutingNode currentRoutingNode = allocation.routingNodes().nodesToShards().get(failedShard.currentNodeId());
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

                RoutingNode targetNode = allocation.routingNodes().nodesToShards().get(t.v1().id());
                targetNode.add(new MutableShardRouting(failedShard.index(), failedShard.id(),
                        targetNode.nodeId(), failedShard.relocatingNodeId(),
                        failedShard.primary(), INITIALIZING));
            }
        }
    }

    @Override public boolean allocateUnassigned(NodeAllocations nodeAllocations, RoutingAllocation allocation) {
        boolean changed = false;
        DiscoveryNodes nodes = allocation.nodes();
        RoutingNodes routingNodes = allocation.routingNodes();

        for (IndexRoutingTable indexRoutingTable : routingNodes.routingTable()) {
            // only do the allocation if there is a local "INDEX NOT RECOVERED" block
            // we check this here since it helps distinguish between index creation though an API, where the below logic
            // should not apply, and when recovering from the gateway, where we should apply this logic
            if (!routingNodes.blocks().hasIndexBlock(indexRoutingTable.index(), GatewayService.INDEX_NOT_RECOVERED_BLOCK)) {
                continue;
            }

            // the index might be created, but shards not instantiated yet, ignore this state
            if (indexRoutingTable.shards().isEmpty()) {
                continue;
            }

            if (indexRoutingTable.allPrimaryShardsUnassigned()) {
                // all primary are unassigned for the index, see if we can allocate it on existing nodes, if not, don't assign
                Set<String> nodesIds = Sets.newHashSet();
                nodesIds.addAll(nodes.dataNodes().keySet());
                TransportNodesListGatewayStartedShards.NodesLocalGatewayStartedShards nodesState = listGatewayStartedShards.list(nodesIds, null).actionGet();

                if (nodesState.failures().length > 0) {
                    for (FailedNodeException failedNodeException : nodesState.failures()) {
                        logger.warn("failed to fetch shards state from node", failedNodeException);
                    }
                }

                // make a list of ShardId to Node, each one from the latest version
                Map<ShardId, Tuple<DiscoveryNode, Long>> shards = Maps.newHashMap();
                // and a list of the number of shard instances
                TObjectIntHashMap<ShardId> shardsCounts = new ExtTObjectIntHasMap<ShardId>().defaultReturnValue(-1);
                for (TransportNodesListGatewayStartedShards.NodeLocalGatewayStartedShards nodeState : nodesState) {
                    if (nodeState.state() == null) {
                        continue;
                    }
                    for (Map.Entry<ShardId, Long> entry : nodeState.state().shards().entrySet()) {
                        ShardId shardId = entry.getKey();
                        if (shardId.index().name().equals(indexRoutingTable.index())) {
                            shardsCounts.adjustOrPutValue(shardId, 1, 1);

                            Tuple<DiscoveryNode, Long> t = shards.get(shardId);
                            if (t == null || entry.getValue() > t.v2().longValue()) {
                                t = new Tuple<DiscoveryNode, Long>(nodeState.node(), entry.getValue());
                                shards.put(shardId, t);
                            }
                        }
                    }
                }

                // check if we managed to allocate to all of them, if not, move all relevant shards to ignored
                if (shards.size() < indexRoutingTable.shards().size()) {
                    moveIndexToIgnoreUnassigned(routingNodes, indexRoutingTable);
                } else {
                    // check if the counts meets the minimum set
                    int requiredNumber = 1;
                    IndexMetaData indexMetaData = routingNodes.metaData().index(indexRoutingTable.index());
                    if ("quorum".equals(initialShards)) {
                        if (indexMetaData.numberOfReplicas() > 1) {
                            requiredNumber = ((1 + indexMetaData.numberOfReplicas()) / 2) + 1;
                        }
                    } else if ("full".equals(initialShards)) {
                        requiredNumber = indexMetaData.numberOfReplicas() + 1;
                    } else if ("full-1".equals(initialShards)) {
                        if (indexMetaData.numberOfReplicas() > 1) {
                            requiredNumber = indexMetaData.numberOfReplicas();
                        }
                    } else {
                        requiredNumber = Integer.parseInt(initialShards);
                    }

                    boolean allocate = true;
                    for (TObjectIntIterator<ShardId> it = shardsCounts.iterator(); it.hasNext();) {
                        it.advance();
                        if (it.value() < requiredNumber) {
                            allocate = false;
                        }
                    }

                    if (allocate) {
                        changed = true;
                        // we found all nodes to allocate to, do the allocation (but only for the index we are working on)
                        for (Iterator<MutableShardRouting> it = routingNodes.unassigned().iterator(); it.hasNext();) {
                            MutableShardRouting shardRouting = it.next();
                            if (shardRouting.index().equals(indexRoutingTable.index())) {
                                if (shardRouting.primary()) {
                                    DiscoveryNode node = shards.get(shardRouting.shardId()).v1();
                                    logger.debug("[{}][{}] initial allocation to [{}]", shardRouting.index(), shardRouting.id(), node);
                                    RoutingNode routingNode = routingNodes.node(node.id());
                                    routingNode.add(shardRouting);
                                    it.remove();
                                }
                            }
                        }
                    } else {
                        moveIndexToIgnoreUnassigned(routingNodes, indexRoutingTable);
                    }
                }
            }
        }

        if (!routingNodes.hasUnassigned()) {
            return changed;
        }

        Iterator<MutableShardRouting> unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            MutableShardRouting shard = unassignedIterator.next();

            // pre-check if it can be allocated to any node that currently exists, so we won't list the store for it for nothing
            boolean canBeAllocatedToAtLeastOneNode = false;
            for (DiscoveryNode discoNode : nodes.dataNodes().values()) {
                RoutingNode node = routingNodes.node(discoNode.id());
                if (node == null) {
                    continue;
                }
                // if its THROTTLING, we are not going to allocate it to this node, so ignore it as well
                if (nodeAllocations.canAllocate(shard, node, allocation).allocate()) {
                    canBeAllocatedToAtLeastOneNode = true;
                    break;
                }
            }

            if (!canBeAllocatedToAtLeastOneNode) {
                continue;
            }

            ConcurrentMap<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> shardStores = buildShardStores(nodes, shard);

            long lastSizeMatched = 0;
            DiscoveryNode lastDiscoNodeMatched = null;
            RoutingNode lastNodeMatched = null;

            for (Map.Entry<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> nodeStoreEntry : shardStores.entrySet()) {
                DiscoveryNode discoNode = nodeStoreEntry.getKey();
                TransportNodesListShardStoreMetaData.StoreFilesMetaData storeFilesMetaData = nodeStoreEntry.getValue();
                logger.trace("{}: checking node [{}]", shard, discoNode);

                if (storeFilesMetaData == null) {
                    // already allocated on that node...
                    continue;
                }

                RoutingNode node = routingNodes.node(discoNode.id());
                if (node == null) {
                    continue;
                }

                // check if we can allocate on that node...
                // we only check for NO, since if this node is THROTTLING and it has enough "same data"
                // then we will try and assign it next time
                if (nodeAllocations.canAllocate(shard, node, allocation) == Decision.NO) {
                    continue;
                }

                // if it is already allocated, we can't assign to it...
                if (storeFilesMetaData.allocated()) {
                    continue;
                }

                if (!shard.primary()) {
                    MutableShardRouting primaryShard = routingNodes.findPrimaryForReplica(shard);
                    if (primaryShard != null && primaryShard.active()) {
                        DiscoveryNode primaryNode = nodes.get(primaryShard.currentNodeId());
                        if (primaryNode != null) {
                            TransportNodesListShardStoreMetaData.StoreFilesMetaData primaryNodeStore = shardStores.get(primaryNode);
                            if (primaryNodeStore != null && primaryNodeStore.allocated()) {
                                long sizeMatched = 0;

                                for (StoreFileMetaData storeFileMetaData : storeFilesMetaData) {
                                    if (primaryNodeStore.fileExists(storeFileMetaData.name()) && primaryNodeStore.file(storeFileMetaData.name()).length() == storeFileMetaData.length()) {
                                        sizeMatched += storeFileMetaData.length();
                                    }
                                }
                                if (sizeMatched > lastSizeMatched) {
                                    lastSizeMatched = sizeMatched;
                                    lastDiscoNodeMatched = discoNode;
                                    lastNodeMatched = node;
                                }
                            }
                        }
                    }
                }
            }

            if (lastNodeMatched != null) {
                if (nodeAllocations.canAllocate(shard, lastNodeMatched, allocation) == NodeAllocation.Decision.THROTTLE) {
                    if (logger.isTraceEnabled()) {
                        logger.debug("[{}][{}]: throttling allocation [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
                    }
                    // we are throttling this, but we have enough to allocate to this node, ignore it for now
                    unassignedIterator.remove();
                    routingNodes.ignoredUnassigned().add(shard);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
                    }
                    // we found a match
                    changed = true;
                    lastNodeMatched.add(shard);
                    unassignedIterator.remove();
                }
            }
        }

        return changed;
    }

    private void moveIndexToIgnoreUnassigned(RoutingNodes routingNodes, IndexRoutingTable indexRoutingTable) {
        for (Iterator<MutableShardRouting> it = routingNodes.unassigned().iterator(); it.hasNext();) {
            MutableShardRouting shardRouting = it.next();
            if (shardRouting.index().equals(indexRoutingTable.index())) {
                it.remove();
                routingNodes.ignoredUnassigned().add(shardRouting);
            }
        }
    }

    private ConcurrentMap<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> buildShardStores(DiscoveryNodes nodes, MutableShardRouting shard) {
        ConcurrentMap<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> shardStores = cachedStores.get(shard.shardId());
        if (shardStores == null) {
            shardStores = ConcurrentCollections.newConcurrentMap();
            TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData nodesStoreFilesMetaData = listShardStoreMetaData.list(shard.shardId(), false, nodes.dataNodes().keySet(), listTimeout).actionGet();
            if (logger.isDebugEnabled()) {
                if (nodesStoreFilesMetaData.failures().length > 0) {
                    StringBuilder sb = new StringBuilder(shard + ": failures when trying to list stores on nodes:");
                    for (int i = 0; i < nodesStoreFilesMetaData.failures().length; i++) {
                        Throwable cause = ExceptionsHelper.unwrapCause(nodesStoreFilesMetaData.failures()[i]);
                        if (cause instanceof ConnectTransportException) {
                            continue;
                        }
                        sb.append("\n    -> ").append(nodesStoreFilesMetaData.failures()[i].getDetailedMessage());
                    }
                    logger.debug(sb.toString());
                }
            }

            for (TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData nodeStoreFilesMetaData : nodesStoreFilesMetaData) {
                if (nodeStoreFilesMetaData.storeFilesMetaData() != null) {
                    shardStores.put(nodeStoreFilesMetaData.node(), nodeStoreFilesMetaData.storeFilesMetaData());
                }
            }
            cachedStores.put(shard.shardId(), shardStores);
        } else {
            // clean nodes that have failed
            for (DiscoveryNode node : shardStores.keySet()) {
                if (!nodes.nodeExists(node.id())) {
                    shardStores.remove(node);
                }
            }

            // we have stored cached from before, see if the nodes changed, if they have, go fetch again
            Set<String> fetchedNodes = Sets.newHashSet();
            for (DiscoveryNode node : nodes.dataNodes().values()) {
                if (!shardStores.containsKey(node)) {
                    fetchedNodes.add(node.id());
                }
            }

            if (!fetchedNodes.isEmpty()) {
                TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData nodesStoreFilesMetaData = listShardStoreMetaData.list(shard.shardId(), false, fetchedNodes, listTimeout).actionGet();
                if (logger.isTraceEnabled()) {
                    if (nodesStoreFilesMetaData.failures().length > 0) {
                        StringBuilder sb = new StringBuilder(shard + ": failures when trying to list stores on nodes:");
                        for (int i = 0; i < nodesStoreFilesMetaData.failures().length; i++) {
                            Throwable cause = ExceptionsHelper.unwrapCause(nodesStoreFilesMetaData.failures()[i]);
                            if (cause instanceof ConnectTransportException) {
                                continue;
                            }
                            sb.append("\n    -> ").append(nodesStoreFilesMetaData.failures()[i].getDetailedMessage());
                        }
                        logger.trace(sb.toString());
                    }
                }

                for (TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData nodeStoreFilesMetaData : nodesStoreFilesMetaData) {
                    if (nodeStoreFilesMetaData.storeFilesMetaData() != null) {
                        shardStores.put(nodeStoreFilesMetaData.node(), nodeStoreFilesMetaData.storeFilesMetaData());
                    }
                }
            }
        }
        return shardStores;
    }
}
