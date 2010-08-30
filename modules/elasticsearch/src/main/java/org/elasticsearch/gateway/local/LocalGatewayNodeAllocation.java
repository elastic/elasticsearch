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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;

/**
 * @author kimchy (shay.banon)
 */
public class LocalGatewayNodeAllocation extends NodeAllocation {

    private final IndicesService indicesService;

    private final TransportNodesListGatewayState listGatewayState;

    private final TransportNodesListShardStoreMetaData listShardStoreMetaData;

    private final ConcurrentMap<ShardId, ConcurrentMap<DiscoveryNode, IndexStore.StoreFilesMetaData>> cachedStores = ConcurrentCollections.newConcurrentMap();

    private final TimeValue listTimeout;

    @Inject public LocalGatewayNodeAllocation(Settings settings, IndicesService indicesService,
                                              TransportNodesListGatewayState listGatewayState, TransportNodesListShardStoreMetaData listShardStoreMetaData) {
        super(settings);
        this.indicesService = indicesService;
        this.listGatewayState = listGatewayState;
        this.listShardStoreMetaData = listShardStoreMetaData;

        this.listTimeout = componentSettings.getAsTime("list_timeout", TimeValue.timeValueSeconds(30));
    }

    @Override public void applyStartedShards(NodeAllocations nodeAllocations, RoutingNodes routingNodes, DiscoveryNodes nodes, List<? extends ShardRouting> startedShards) {
        for (ShardRouting shardRouting : startedShards) {
            cachedStores.remove(shardRouting.shardId());
        }
    }

    @Override public void applyFailedShards(NodeAllocations nodeAllocations, RoutingNodes routingNodes, DiscoveryNodes nodes, List<? extends ShardRouting> failedShards) {
        for (ShardRouting shardRouting : failedShards) {
            cachedStores.remove(shardRouting.shardId());
        }
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
                            logger.debug("[{}][{}] initial allocation to [{}]", shardRouting.index(), shardRouting.id(), node);
                            RoutingNode routingNode = routingNodes.node(node.id());
                            routingNode.add(shardRouting);
                            it.remove();
                        }
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
            InternalIndexService indexService = (InternalIndexService) indicesService.indexService(shard.index());
            if (indexService == null) {
                continue;
            }
            // if the store is not persistent, it makes no sense to test for special allocation
            if (!indexService.store().persistent()) {
                continue;
            }

            // pre-check if it can be allocated to any node that currently exists, so we won't list the store for it for nothing
            boolean canBeAllocatedToAtLeastOneNode = false;
            for (DiscoveryNode discoNode : nodes.dataNodes().values()) {
                RoutingNode node = routingNodes.node(discoNode.id());
                if (node == null) {
                    continue;
                }
                // if its THROTTLING, we are not going to allocate it to this node, so ignore it as well
                if (nodeAllocations.canAllocate(shard, node, routingNodes).allocate()) {
                    canBeAllocatedToAtLeastOneNode = true;
                    break;
                }
            }

            if (!canBeAllocatedToAtLeastOneNode) {
                continue;
            }

            ConcurrentMap<DiscoveryNode, IndexStore.StoreFilesMetaData> shardStores = buildShardStores(nodes, shard);

            long lastSizeMatched = 0;
            DiscoveryNode lastDiscoNodeMatched = null;
            RoutingNode lastNodeMatched = null;

            for (Map.Entry<DiscoveryNode, IndexStore.StoreFilesMetaData> nodeStoreEntry : shardStores.entrySet()) {
                DiscoveryNode discoNode = nodeStoreEntry.getKey();
                IndexStore.StoreFilesMetaData storeFilesMetaData = nodeStoreEntry.getValue();
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
                if (nodeAllocations.canAllocate(shard, node, routingNodes) == Decision.NO) {
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
                            IndexStore.StoreFilesMetaData primaryNodeStore = shardStores.get(primaryNode);
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
                if (nodeAllocations.canAllocate(shard, lastNodeMatched, routingNodes) == NodeAllocation.Decision.THROTTLE) {
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

    private ConcurrentMap<DiscoveryNode, IndexStore.StoreFilesMetaData> buildShardStores(DiscoveryNodes nodes, MutableShardRouting shard) {
        ConcurrentMap<DiscoveryNode, IndexStore.StoreFilesMetaData> shardStores = cachedStores.get(shard.shardId());
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
                shardStores.put(nodeStoreFilesMetaData.node(), nodeStoreFilesMetaData.storeFilesMetaData());
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
                    shardStores.put(nodeStoreFilesMetaData.node(), nodeStoreFilesMetaData.storeFilesMetaData());
                }
            }
        }
        return shardStores;
    }
}
