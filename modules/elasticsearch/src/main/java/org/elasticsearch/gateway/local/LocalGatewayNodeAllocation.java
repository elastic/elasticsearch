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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.*;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.iterator.TObjectLongIterator;
import org.elasticsearch.common.trove.map.hash.TObjectLongHashMap;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kimchy (shay.banon)
 */
public class LocalGatewayNodeAllocation extends NodeAllocation {

    private final TransportNodesListGatewayStartedShards listGatewayStartedShards;

    private final TransportNodesListShardStoreMetaData listShardStoreMetaData;

    private final ConcurrentMap<ShardId, Map<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData>> cachedStores = ConcurrentCollections.newConcurrentMap();

    private final ConcurrentMap<ShardId, TObjectLongHashMap<DiscoveryNode>> cachedShardsState = ConcurrentCollections.newConcurrentMap();

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
            cachedShardsState.remove(shardRouting.shardId());
        }
    }

    @Override public void applyFailedShards(NodeAllocations nodeAllocations, FailedRerouteAllocation allocation) {
        ShardRouting failedShard = allocation.failedShard();
        cachedStores.remove(failedShard.shardId());
        cachedShardsState.remove(failedShard.shardId());
    }

    @Override public boolean allocateUnassigned(NodeAllocations nodeAllocations, RoutingAllocation allocation) {
        boolean changed = false;
        DiscoveryNodes nodes = allocation.nodes();
        RoutingNodes routingNodes = allocation.routingNodes();

        // First, handle primaries, they must find a place to be allocated on here
        Iterator<MutableShardRouting> unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            MutableShardRouting shard = unassignedIterator.next();

            if (!shard.primary()) {
                continue;
            }

            // this is an API allocation, ignore since we know there is no data...
            if (!routingNodes.routingTable().index(shard.index()).shard(shard.id()).allocatedPostApi()) {
                continue;
            }

            TObjectLongHashMap<DiscoveryNode> nodesState = buildShardStates(nodes, shard);

            int numberOfAllocationsFound = 0;
            long highestVersion = -1;
            DiscoveryNode nodeWithHighestVersion = null;
            for (TObjectLongIterator<DiscoveryNode> it = nodesState.iterator(); it.hasNext();) {
                it.advance();
                DiscoveryNode node = it.key();
                long version = it.value();
                // since we don't check in NO allocation, we need to double check here
                if (allocation.shouldIgnoreShardForNode(shard.shardId(), node.id())) {
                    continue;
                }
                if (version != -1) {
                    numberOfAllocationsFound++;
                    if (highestVersion == -1) {
                        nodeWithHighestVersion = node;
                        highestVersion = version;
                    } else {
                        if (version > highestVersion) {
                            nodeWithHighestVersion = node;
                            highestVersion = version;
                        }
                    }
                }
            }

            // check if the counts meets the minimum set
            int requiredAllocation = 1;
            IndexMetaData indexMetaData = routingNodes.metaData().index(shard.index());
            if ("quorum".equals(initialShards)) {
                if (indexMetaData.numberOfReplicas() > 1) {
                    requiredAllocation = ((1 + indexMetaData.numberOfReplicas()) / 2) + 1;
                }
            } else if ("full".equals(initialShards)) {
                requiredAllocation = indexMetaData.numberOfReplicas() + 1;
            } else if ("full-1".equals(initialShards)) {
                if (indexMetaData.numberOfReplicas() > 1) {
                    requiredAllocation = indexMetaData.numberOfReplicas();
                }
            } else {
                requiredAllocation = Integer.parseInt(initialShards);
            }

            // not enough found for this shard, continue...
            if (numberOfAllocationsFound < requiredAllocation) {
                // we can't really allocate, so ignore it and continue
                unassignedIterator.remove();
                routingNodes.ignoredUnassigned().add(shard);
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}]: not allocating, number_of_allocated_shards_found [{}], required_number [{}]", shard.index(), shard.id(), numberOfAllocationsFound, requiredAllocation);
                }
                continue;
            }

            RoutingNode node = routingNodes.node(nodeWithHighestVersion.id());
            // check if we need to throttle, NOTE, we don't check on NO since it does not apply
            // since this is our master data!
            if (nodeAllocations.canAllocate(shard, node, allocation) == NodeAllocation.Decision.THROTTLE) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}]: throttling allocation [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, nodeWithHighestVersion);
                }
                // we are throttling this, but we have enough to allocate to this node, ignore it for now
                unassignedIterator.remove();
                routingNodes.ignoredUnassigned().add(shard);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}]: allocating [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, nodeWithHighestVersion);
                }
                // we found a match
                changed = true;
                node.add(shard);
                unassignedIterator.remove();
            }
        }

        if (!routingNodes.hasUnassigned()) {
            return changed;
        }

        // Now, handle replicas, try to assign them to nodes that are similar to the one the primary was allocated on
        unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            MutableShardRouting shard = unassignedIterator.next();

            // pre-check if it can be allocated to any node that currently exists, so we won't list the store for it for nothing
            boolean canBeAllocatedToAtLeastOneNode = false;
            for (DiscoveryNode discoNode : nodes.dataNodes().values()) {
                RoutingNode node = routingNodes.node(discoNode.id());
                if (node == null) {
                    continue;
                }
                // if we can't allocate it on a node, ignore it, for example, this handles
                // cases for only allocating a replica after a primary
                if (nodeAllocations.canAllocate(shard, node, allocation).allocate()) {
                    canBeAllocatedToAtLeastOneNode = true;
                    break;
                }
            }

            if (!canBeAllocatedToAtLeastOneNode) {
                continue;
            }

            Map<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> shardStores = buildShardStores(nodes, shard);

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
                                    if (primaryNodeStore.fileExists(storeFileMetaData.name()) && primaryNodeStore.file(storeFileMetaData.name()).isSame(storeFileMetaData)) {
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
                // we only check on THROTTLE since we checked before before on NO
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

    private TObjectLongHashMap<DiscoveryNode> buildShardStates(DiscoveryNodes nodes, MutableShardRouting shard) {
        TObjectLongHashMap<DiscoveryNode> shardStates = cachedShardsState.get(shard.shardId());
        Set<String> nodeIds;
        if (shardStates == null) {
            shardStates = new TObjectLongHashMap<DiscoveryNode>();
            cachedShardsState.put(shard.shardId(), shardStates);
            nodeIds = nodes.dataNodes().keySet();
        } else {
            // clean nodes that have failed
            for (DiscoveryNode node : shardStates.keySet()) {
                if (!nodes.nodeExists(node.id())) {
                    shardStates.remove(node);
                }
            }
            nodeIds = Sets.newHashSet();
            // we have stored cached from before, see if the nodes changed, if they have, go fetch again
            for (DiscoveryNode node : nodes.dataNodes().values()) {
                if (!shardStates.containsKey(node)) {
                    nodeIds.add(node.id());
                }
            }
        }
        if (nodeIds.isEmpty()) {
            return shardStates;
        }

        TransportNodesListGatewayStartedShards.NodesLocalGatewayStartedShards response = listGatewayStartedShards.list(shard.shardId(), nodes.dataNodes().keySet(), listTimeout).actionGet();
        if (logger.isDebugEnabled()) {
            if (response.failures().length > 0) {
                StringBuilder sb = new StringBuilder(shard + ": failures when trying to list shards on nodes:");
                for (int i = 0; i < response.failures().length; i++) {
                    Throwable cause = ExceptionsHelper.unwrapCause(response.failures()[i]);
                    if (cause instanceof ConnectTransportException) {
                        continue;
                    }
                    sb.append("\n    -> ").append(response.failures()[i].getDetailedMessage());
                }
                logger.debug(sb.toString());
            }
        }

        for (TransportNodesListGatewayStartedShards.NodeLocalGatewayStartedShards nodeShardState : response) {
            // -1 version means it does not exists, which is what the API returns, and what we expect to
            shardStates.put(nodeShardState.node(), nodeShardState.version());
        }
        return shardStates;
    }

    private Map<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> buildShardStores(DiscoveryNodes nodes, MutableShardRouting shard) {
        Map<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> shardStores = cachedStores.get(shard.shardId());
        Set<String> nodesIds;
        if (shardStores == null) {
            shardStores = Maps.newHashMap();
            cachedStores.put(shard.shardId(), shardStores);
            nodesIds = nodes.dataNodes().keySet();
        } else {
            nodesIds = Sets.newHashSet();
            // clean nodes that have failed
            for (DiscoveryNode node : shardStores.keySet()) {
                if (!nodes.nodeExists(node.id())) {
                    shardStores.remove(node);
                }
            }

            for (DiscoveryNode node : nodes.dataNodes().values()) {
                if (!shardStores.containsKey(node)) {
                    nodesIds.add(node.id());
                }
            }
        }

        if (!nodesIds.isEmpty()) {
            TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData nodesStoreFilesMetaData = listShardStoreMetaData.list(shard.shardId(), false, nodesIds, listTimeout).actionGet();
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

        return shardStores;
    }
}
