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

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public abstract class ReplicaShardAllocator extends AbstractComponent {

    public ReplicaShardAllocator(Settings settings) {
        super(settings);
    }

    /**
     * Process existing recoveries of replicas and see if we need to cancel them if we find a better
     * match. Today, a better match is one that has full sync id match compared to not having one in
     * the previous recovery.
     */
    public void processExistingRecoveries(RoutingAllocation allocation) {
        MetaData metaData = allocation.metaData();
        RoutingNodes routingNodes = allocation.routingNodes();
        List<Runnable> shardCancellationActions = new ArrayList<>();
        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shard : routingNode) {
                if (shard.primary() == true) {
                    continue;
                }
                if (shard.initializing() == false) {
                    continue;
                }
                if (shard.relocatingNodeId() != null) {
                    continue;
                }

                // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...
                if (shard.unassignedInfo() != null && shard.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
                    continue;
                }

                AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> shardStores = fetchData(shard, allocation);
                if (shardStores.hasData() == false) {
                    logger.trace("{}: fetching new stores for initializing shard", shard);
                    continue; // still fetching
                }

                ShardRouting primaryShard = allocation.routingNodes().activePrimary(shard.shardId());
                assert primaryShard != null : "the replica shard can be allocated on at least one node, so there must be an active primary";
                TransportNodesListShardStoreMetaData.StoreFilesMetaData primaryStore = findStore(primaryShard, allocation, shardStores);
                if (primaryStore == null) {
                    // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
                    // just let the recovery find it out, no need to do anything about it for the initializing shard
                    logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", shard);
                    continue;
                }

                MatchingNodes matchingNodes = findMatchingNodes(shard, allocation, primaryStore, shardStores);
                if (matchingNodes.getNodeWithHighestMatch() != null) {
                    DiscoveryNode currentNode = allocation.nodes().get(shard.currentNodeId());
                    DiscoveryNode nodeWithHighestMatch = matchingNodes.getNodeWithHighestMatch();
                    // current node will not be in matchingNodes as it is filtered away by SameShardAllocationDecider
                    final String currentSyncId;
                    if (shardStores.getData().containsKey(currentNode)) {
                        currentSyncId = shardStores.getData().get(currentNode).storeFilesMetaData().syncId();
                    } else {
                        currentSyncId = null;
                    }
                    if (currentNode.equals(nodeWithHighestMatch) == false
                            && Objects.equals(currentSyncId, primaryStore.syncId()) == false
                            && matchingNodes.isNodeMatchBySyncID(nodeWithHighestMatch) == true) {
                        // we found a better match that has a full sync id match, the existing allocation is not fully synced
                        // so we found a better one, cancel this one
                        logger.debug("cancelling allocation of replica on [{}], sync id match found on node [{}]",
                                currentNode, nodeWithHighestMatch);
                        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.REALLOCATED_REPLICA,
                            "existing allocation of replica to [" + currentNode + "] cancelled, sync id match found on node ["+ nodeWithHighestMatch + "]",
                            null, 0, allocation.getCurrentNanoTime(), System.currentTimeMillis(), false, UnassignedInfo.AllocationStatus.NO_ATTEMPT);
                        // don't cancel shard in the loop as it will cause a ConcurrentModificationException
                        shardCancellationActions.add(() -> routingNodes.failShard(logger, shard, unassignedInfo, metaData.getIndexSafe(shard.index()), allocation.changes()));
                    }
                }
            }
        }
        for (Runnable action : shardCancellationActions) {
            action.run();
        }
    }

    public void allocateUnassigned(RoutingAllocation allocation) {
        final RoutingNodes routingNodes = allocation.routingNodes();
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shard = unassignedIterator.next();
            if (shard.primary()) {
                continue;
            }

            // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...
            if (shard.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
                continue;
            }

            // pre-check if it can be allocated to any node that currently exists, so we won't list the store for it for nothing
            Decision decision = canBeAllocatedToAtLeastOneNode(shard, allocation);
            if (decision.type() != Decision.Type.YES) {
                logger.trace("{}: ignoring allocation, can't be allocated on any node", shard);
                unassignedIterator.removeAndIgnore(UnassignedInfo.AllocationStatus.fromDecision(decision), allocation.changes());
                continue;
            }

            AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> shardStores = fetchData(shard, allocation);
            if (shardStores.hasData() == false) {
                logger.trace("{}: ignoring allocation, still fetching shard stores", shard);
                allocation.setHasPendingAsyncFetch();
                unassignedIterator.removeAndIgnore(AllocationStatus.FETCHING_SHARD_DATA, allocation.changes());
                continue; // still fetching
            }

            ShardRouting primaryShard = routingNodes.activePrimary(shard.shardId());
            assert primaryShard != null : "the replica shard can be allocated on at least one node, so there must be an active primary";
            TransportNodesListShardStoreMetaData.StoreFilesMetaData primaryStore = findStore(primaryShard, allocation, shardStores);
            if (primaryStore == null) {
                // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
                // we want to let the replica be allocated in order to expose the actual problem with the primary that the replica
                // will try and recover from
                // Note, this is the existing behavior, as exposed in running CorruptFileTest#testNoPrimaryData
                logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", shard);
                continue;
            }

            MatchingNodes matchingNodes = findMatchingNodes(shard, allocation, primaryStore, shardStores);

            if (matchingNodes.getNodeWithHighestMatch() != null) {
                RoutingNode nodeWithHighestMatch = allocation.routingNodes().node(matchingNodes.getNodeWithHighestMatch().getId());
                // we only check on THROTTLE since we checked before before on NO
                decision = allocation.deciders().canAllocate(shard, nodeWithHighestMatch, allocation);
                if (decision.type() == Decision.Type.THROTTLE) {
                    logger.debug("[{}][{}]: throttling allocation [{}] to [{}] in order to reuse its unallocated persistent store", shard.index(), shard.id(), shard, nodeWithHighestMatch.node());
                    // we are throttling this, but we have enough to allocate to this node, ignore it for now
                    unassignedIterator.removeAndIgnore(UnassignedInfo.AllocationStatus.fromDecision(decision), allocation.changes());
                } else {
                    logger.debug("[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store", shard.index(), shard.id(), shard, nodeWithHighestMatch.node());
                    // we found a match
                    unassignedIterator.initialize(nodeWithHighestMatch.nodeId(), null, allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE), allocation.changes());
                }
            } else if (matchingNodes.hasAnyData() == false) {
                // if we didn't manage to find *any* data (regardless of matching sizes), check if the allocation of the replica shard needs to be delayed
                ignoreUnassignedIfDelayed(unassignedIterator, shard, allocation.changes());
            }
        }
    }

    /**
     * Check if the allocation of the replica is to be delayed. Compute the delay and if it is delayed, add it to the ignore unassigned list
     * Note: we only care about replica in delayed allocation, since if we have an unassigned primary it
     *       will anyhow wait to find an existing copy of the shard to be allocated
     * Note: the other side of the equation is scheduling a reroute in a timely manner, which happens in the RoutingService
     *
     * PUBLIC FOR TESTS!
     *
     * @param unassignedIterator iterator over unassigned shards
     * @param shard the shard which might be delayed
     */
    public void ignoreUnassignedIfDelayed(RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator, ShardRouting shard, RoutingChangesObserver changes) {
        if (shard.unassignedInfo().isDelayed()) {
            logger.debug("{}: allocation of [{}] is delayed", shard.shardId(), shard);
            unassignedIterator.removeAndIgnore(AllocationStatus.DELAYED_ALLOCATION, changes);
        }
    }

    /**
     * Determines if the shard can be allocated on at least one node based on the allocation deciders.
     *
     * Returns the best allocation decision for allocating the shard on any node (i.e. YES if at least one
     * node decided YES, THROTTLE if at least one node decided THROTTLE, and NO if none of the nodes decided
     * YES or THROTTLE.
     */
    private Decision canBeAllocatedToAtLeastOneNode(ShardRouting shard, RoutingAllocation allocation) {
        Decision madeDecision = Decision.NO;
        for (ObjectCursor<DiscoveryNode> cursor : allocation.nodes().getDataNodes().values()) {
            RoutingNode node = allocation.routingNodes().node(cursor.value.getId());
            if (node == null) {
                continue;
            }
            // if we can't allocate it on a node, ignore it, for example, this handles
            // cases for only allocating a replica after a primary
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            if (decision.type() == Decision.Type.YES) {
                return decision;
            } else if (madeDecision.type() == Decision.Type.NO && decision.type() == Decision.Type.THROTTLE) {
                madeDecision = decision;
            }
        }
        return madeDecision;
    }

    /**
     * Finds the store for the assigned shard in the fetched data, returns null if none is found.
     */
    private TransportNodesListShardStoreMetaData.StoreFilesMetaData findStore(ShardRouting shard, RoutingAllocation allocation, AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> data) {
        assert shard.currentNodeId() != null;
        DiscoveryNode primaryNode = allocation.nodes().get(shard.currentNodeId());
        if (primaryNode == null) {
            return null;
        }
        TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData primaryNodeFilesStore = data.getData().get(primaryNode);
        if (primaryNodeFilesStore == null) {
            return null;
        }
        return primaryNodeFilesStore.storeFilesMetaData();
    }

    private MatchingNodes findMatchingNodes(ShardRouting shard, RoutingAllocation allocation,
                                            TransportNodesListShardStoreMetaData.StoreFilesMetaData primaryStore,
                                            AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> data) {
        ObjectLongMap<DiscoveryNode> nodesToSize = new ObjectLongHashMap<>();
        for (Map.Entry<DiscoveryNode, TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> nodeStoreEntry : data.getData().entrySet()) {
            DiscoveryNode discoNode = nodeStoreEntry.getKey();
            TransportNodesListShardStoreMetaData.StoreFilesMetaData storeFilesMetaData = nodeStoreEntry.getValue().storeFilesMetaData();
            // we don't have any files at all, it is an empty index
            if (storeFilesMetaData.isEmpty()) {
                continue;
            }

            RoutingNode node = allocation.routingNodes().node(discoNode.getId());
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

            String primarySyncId = primaryStore.syncId();
            String replicaSyncId = storeFilesMetaData.syncId();
            // see if we have a sync id we can make use of
            if (replicaSyncId != null && replicaSyncId.equals(primarySyncId)) {
                logger.trace("{}: node [{}] has same sync id {} as primary", shard, discoNode.getName(), replicaSyncId);
                nodesToSize.put(discoNode, Long.MAX_VALUE);
            } else {
                long sizeMatched = 0;
                for (StoreFileMetaData storeFileMetaData : storeFilesMetaData) {
                    String metaDataFileName = storeFileMetaData.name();
                    if (primaryStore.fileExists(metaDataFileName) && primaryStore.file(metaDataFileName).isSame(storeFileMetaData)) {
                        sizeMatched += storeFileMetaData.length();
                    }
                }
                logger.trace("{}: node [{}] has [{}/{}] bytes of re-usable data",
                        shard, discoNode.getName(), new ByteSizeValue(sizeMatched), sizeMatched);
                nodesToSize.put(discoNode, sizeMatched);
            }
        }

        return new MatchingNodes(nodesToSize);
    }

    protected abstract AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetchData(ShardRouting shard, RoutingAllocation allocation);

    static class MatchingNodes {
        private final ObjectLongMap<DiscoveryNode> nodesToSize;
        private final DiscoveryNode nodeWithHighestMatch;

        public MatchingNodes(ObjectLongMap<DiscoveryNode> nodesToSize) {
            this.nodesToSize = nodesToSize;

            long highestMatchSize = 0;
            DiscoveryNode highestMatchNode = null;

            for (ObjectLongCursor<DiscoveryNode> cursor : nodesToSize) {
                if (cursor.value > highestMatchSize) {
                    highestMatchSize = cursor.value;
                    highestMatchNode = cursor.key;
                }
            }
            this.nodeWithHighestMatch = highestMatchNode;
        }

        /**
         * Returns the node with the highest "non zero byte" match compared to
         * the primary.
         */
        @Nullable
        public DiscoveryNode getNodeWithHighestMatch() {
            return this.nodeWithHighestMatch;
        }

        public boolean isNodeMatchBySyncID(DiscoveryNode node) {
            return nodesToSize.get(node) == Long.MAX_VALUE;
        }

        /**
         * Did we manage to find any data, regardless how well they matched or not.
         */
        public boolean hasAnyData() {
            return nodesToSize.isEmpty() == false;
        }
    }
}
