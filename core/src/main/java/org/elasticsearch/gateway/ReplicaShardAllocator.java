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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;

import java.util.Iterator;
import java.util.Map;

/**
 */
public abstract class ReplicaShardAllocator extends AbstractComponent {

    public ReplicaShardAllocator(Settings settings) {
        super(settings);
    }

    public boolean allocateUnassigned(RoutingAllocation allocation) {
        boolean changed = false;
        final RoutingNodes routingNodes = allocation.routingNodes();
        final MetaData metaData = routingNodes.metaData();

        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shard = unassignedIterator.next();
            if (shard.primary()) {
                continue;
            }

            // pre-check if it can be allocated to any node that currently exists, so we won't list the store for it for nothing
            boolean canBeAllocatedToAtLeastOneNode = false;
            for (ObjectCursor<DiscoveryNode> cursor : allocation.nodes().dataNodes().values()) {
                RoutingNode node = routingNodes.node(cursor.value.id());
                if (node == null) {
                    continue;
                }
                // if we can't allocate it on a node, ignore it, for example, this handles
                // cases for only allocating a replica after a primary
                Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
                if (decision.type() == Decision.Type.YES) {
                    canBeAllocatedToAtLeastOneNode = true;
                    break;
                }
            }

            if (!canBeAllocatedToAtLeastOneNode) {
                logger.trace("{}: ignoring allocation, can't be allocated on any node", shard);
                unassignedIterator.removeAndIgnore();
                continue;
            }

            AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> shardStores = fetchData(shard, allocation);
            if (shardStores.hasData() == false) {
                logger.trace("{}: ignoring allocation, still fetching shard stores", shard);
                unassignedIterator.removeAndIgnore();
                continue; // still fetching
            }

            long lastSizeMatched = 0;
            DiscoveryNode lastDiscoNodeMatched = null;
            RoutingNode lastNodeMatched = null;
            boolean hasReplicaData = false;
            IndexMetaData indexMetaData = metaData.index(shard.getIndex());

            for (Map.Entry<DiscoveryNode, TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> nodeStoreEntry : shardStores.getData().entrySet()) {
                DiscoveryNode discoNode = nodeStoreEntry.getKey();
                TransportNodesListShardStoreMetaData.StoreFilesMetaData storeFilesMetaData = nodeStoreEntry.getValue().storeFilesMetaData();
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
                Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
                if (decision.type() == Decision.Type.NO) {
                    continue;
                }

                // if it is already allocated, we can't assign to it...
                if (storeFilesMetaData.allocated()) {
                    continue;
                }

                if (!shard.primary()) {
                    hasReplicaData |= storeFilesMetaData.iterator().hasNext();
                    ShardRouting primaryShard = routingNodes.activePrimary(shard);
                    if (primaryShard != null) {
                        assert primaryShard.active();
                        DiscoveryNode primaryNode = allocation.nodes().get(primaryShard.currentNodeId());
                        if (primaryNode != null) {
                            TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData primaryNodeFilesStore = shardStores.getData().get(primaryNode);
                            if (primaryNodeFilesStore != null) {
                                TransportNodesListShardStoreMetaData.StoreFilesMetaData primaryNodeStore = primaryNodeFilesStore.storeFilesMetaData();
                                if (primaryNodeStore != null && primaryNodeStore.allocated()) {
                                    long sizeMatched = 0;

                                    String primarySyncId = primaryNodeStore.syncId();
                                    String replicaSyncId = storeFilesMetaData.syncId();
                                    // see if we have a sync id we can make use of
                                    if (replicaSyncId != null && replicaSyncId.equals(primarySyncId)) {
                                        logger.trace("{}: node [{}] has same sync id {} as primary", shard, discoNode.name(), replicaSyncId);
                                        lastNodeMatched = node;
                                        lastSizeMatched = Long.MAX_VALUE;
                                        lastDiscoNodeMatched = discoNode;
                                    } else {
                                        for (StoreFileMetaData storeFileMetaData : storeFilesMetaData) {
                                            String metaDataFileName = storeFileMetaData.name();
                                            if (primaryNodeStore.fileExists(metaDataFileName) && primaryNodeStore.file(metaDataFileName).isSame(storeFileMetaData)) {
                                                sizeMatched += storeFileMetaData.length();
                                            }
                                        }
                                        logger.trace("{}: node [{}] has [{}/{}] bytes of re-usable data",
                                                shard, discoNode.name(), new ByteSizeValue(sizeMatched), sizeMatched);
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
                }
            }

            if (lastNodeMatched != null) {
                // we only check on THROTTLE since we checked before before on NO
                Decision decision = allocation.deciders().canAllocate(shard, lastNodeMatched, allocation);
                if (decision.type() == Decision.Type.THROTTLE) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}]: throttling allocation [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
                    }
                    // we are throttling this, but we have enough to allocate to this node, ignore it for now
                    unassignedIterator.removeAndIgnore();
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
                    }
                    // we found a match
                    changed = true;
                    unassignedIterator.initialize(lastNodeMatched.nodeId());
                }
            } else if (hasReplicaData == false) {
                // if we didn't manage to find *any* data (regardless of matching sizes), check if the allocation
                // of the replica shard needs to be delayed, and if so, add it to the ignore unassigned list
                // note: we only care about replica in delayed allocation, since if we have an unassigned primary it
                //       will anyhow wait to find an existing copy of the shard to be allocated
                // note: the other side of the equation is scheduling a reroute in a timely manner, which happens in the RoutingService
                long delay = shard.unassignedInfo().getDelayAllocationExpirationIn(settings, indexMetaData.getSettings());
                if (delay > 0) {
                    logger.debug("[{}][{}]: delaying allocation of [{}] for [{}]", shard.index(), shard.id(), shard, TimeValue.timeValueMillis(delay));
                    /**
                     * mark it as changed, since we want to kick a publishing to schedule future allocation,
                     * see {@link org.elasticsearch.cluster.routing.RoutingService#clusterChanged(ClusterChangedEvent)}).
                     */
                    changed = true;
                    unassignedIterator.removeAndIgnore();
                }
            }
        }
        return changed;
    }

    protected abstract AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetchData(ShardRouting shard, RoutingAllocation allocation);
}
