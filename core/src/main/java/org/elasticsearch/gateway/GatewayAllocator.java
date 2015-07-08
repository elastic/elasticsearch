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
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class GatewayAllocator extends AbstractComponent {

    public static final String INDEX_RECOVERY_INITIAL_SHARDS = "index.recovery.initial_shards";

    private final String initialShards;

    private final TransportNodesListGatewayStartedShards startedAction;
    private final TransportNodesListShardStoreMetaData storeAction;
    private RoutingService routingService;

    private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>> asyncFetchStarted = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData>> asyncFetchStore = ConcurrentCollections.newConcurrentMap();

    @Inject
    public GatewayAllocator(Settings settings, TransportNodesListGatewayStartedShards startedAction, TransportNodesListShardStoreMetaData storeAction) {
        super(settings);
        this.startedAction = startedAction;
        this.storeAction = storeAction;

        this.initialShards = settings.get("gateway.initial_shards", settings.get("gateway.local.initial_shards", "quorum"));

        logger.debug("using initial_shards [{}]", initialShards);
    }

    public void setReallocation(final ClusterService clusterService, final RoutingService routingService) {
        this.routingService = routingService;
        clusterService.add(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                boolean cleanCache = false;
                DiscoveryNode localNode = event.state().nodes().localNode();
                if (localNode != null) {
                    if (localNode.masterNode() == true && event.localNodeMaster() == false) {
                        cleanCache = true;
                    }
                } else {
                    cleanCache = true;
                }
                if (cleanCache) {
                    Releasables.close(asyncFetchStarted.values());
                    asyncFetchStarted.clear();
                    Releasables.close(asyncFetchStore.values());
                    asyncFetchStore.clear();
                }
            }
        });
    }

    public int getNumberOfInFlightFetch() {
        int count = 0;
        for (AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        for (AsyncShardFetch<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetch : asyncFetchStore.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        return count;
    }

    public void applyStartedShards(StartedRerouteAllocation allocation) {
        for (ShardRouting shard : allocation.startedShards()) {
            Releasables.close(asyncFetchStarted.remove(shard.shardId()));
            Releasables.close(asyncFetchStore.remove(shard.shardId()));
        }
    }

    public void applyFailedShards(FailedRerouteAllocation allocation) {
        for (FailedRerouteAllocation.FailedShard shard : allocation.failedShards()) {
            Releasables.close(asyncFetchStarted.remove(shard.shard.shardId()));
            Releasables.close(asyncFetchStore.remove(shard.shard.shardId()));
        }
    }

    /**
     * Return {@code true} if the index is configured to allow shards to be
     * recovered on any node
     */
    private boolean recoverOnAnyNode(@IndexSettings Settings idxSettings) {
        return IndexMetaData.isOnSharedFilesystem(idxSettings) &&
                idxSettings.getAsBoolean(IndexMetaData.SETTING_SHARED_FS_ALLOW_RECOVERY_ON_ANY_NODE, false);
    }

    public boolean allocateUnassigned(RoutingAllocation allocation) {
        boolean changed = false;
        DiscoveryNodes nodes = allocation.nodes();
        RoutingNodes routingNodes = allocation.routingNodes();

        // First, handle primaries, they must find a place to be allocated on here
        final MetaData metaData = routingNodes.metaData();
        RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
        unassigned.sort(new PriorityComparator() {

            @Override
            protected Settings getIndexSettings(String index) {
                IndexMetaData indexMetaData = metaData.index(index);
                return indexMetaData.getSettings();
            }
        }); // sort for priority ordering
        Iterator<ShardRouting> unassignedIterator = unassigned.iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shard = unassignedIterator.next();

            if (!shard.primary()) {
                continue;
            }

            // this is an API allocation, ignore since we know there is no data...
            if (!routingNodes.routingTable().index(shard.index()).shard(shard.id()).primaryAllocatedPostApi()) {
                continue;
            }

            AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch = asyncFetchStarted.get(shard.shardId());
            if (fetch == null) {
                fetch = new InternalAsyncFetch<>(logger, "shard_started", shard.shardId(), startedAction);
                asyncFetchStarted.put(shard.shardId(), fetch);
            }
            AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> shardState = fetch.fetchData(nodes, metaData, allocation.getIgnoreNodes(shard.shardId()));
            if (shardState.hasData() == false) {
                logger.trace("{}: ignoring allocation, still fetching shard started state", shard);
                unassignedIterator.remove();
                routingNodes.ignoredUnassigned().add(shard);
                continue;
            }
            shardState.processAllocation(allocation);

            IndexMetaData indexMetaData = metaData.index(shard.getIndex());

            /**
             * Build a map of DiscoveryNodes to shard state number for the given shard.
             * A state of -1 means the shard does not exist on the node, where any
             * shard state >= 0 is the state version of the shard on that node's disk.
             *
             * A shard on shared storage will return at least shard state 0 for all
             * nodes, indicating that the shard can be allocated to any node.
             */
            ObjectLongHashMap<DiscoveryNode> nodesState = new ObjectLongHashMap<>();
            for (TransportNodesListGatewayStartedShards.NodeGatewayStartedShards nodeShardState : shardState.getData().values()) {
                long version = nodeShardState.version();
                // -1 version means it does not exists, which is what the API returns, and what we expect to
                logger.trace("[{}] on node [{}] has version [{}] of shard", shard, nodeShardState.getNode(), version);
                nodesState.put(nodeShardState.getNode(), version);
            }

            int numberOfAllocationsFound = 0;
            long highestVersion = -1;
            final Map<DiscoveryNode, Long> nodesWithVersion = Maps.newHashMap();

            assert !nodesState.containsKey(null);
            final Object[] keys = nodesState.keys;
            final long[] values = nodesState.values;
            Settings idxSettings = indexMetaData.settings();
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] == null) {
                    continue;
                }

                DiscoveryNode node = (DiscoveryNode) keys[i];
                long version = values[i];
                // since we don't check in NO allocation, we need to double check here
                if (allocation.shouldIgnoreShardForNode(shard.shardId(), node.id())) {
                    continue;
                }
                if (recoverOnAnyNode(idxSettings)) {
                    numberOfAllocationsFound++;
                    if (version > highestVersion) {
                        highestVersion = version;
                    }
                    // We always put the node without clearing the map
                    nodesWithVersion.put(node, version);
                } else if (version != -1) {
                    numberOfAllocationsFound++;
                    // If we've found a new "best" candidate, clear the
                    // current candidates and add it
                    if (version > highestVersion) {
                        highestVersion = version;
                        nodesWithVersion.clear();
                        nodesWithVersion.put(node, version);
                    } else if (version == highestVersion) {
                        // If the candidate is the same, add it to the
                        // list, but keep the current candidate
                        nodesWithVersion.put(node, version);
                    }
                }
            }
            // Now that we have a map of nodes to versions along with the
            // number of allocations found (and not ignored), we need to sort
            // it so the node with the highest version is at the beginning
            List<DiscoveryNode> nodesWithHighestVersion = Lists.newArrayList();
            nodesWithHighestVersion.addAll(nodesWithVersion.keySet());
            CollectionUtil.timSort(nodesWithHighestVersion, new Comparator<DiscoveryNode>() {
                @Override
                public int compare(DiscoveryNode o1, DiscoveryNode o2) {
                    return Long.compare(nodesWithVersion.get(o2), nodesWithVersion.get(o1));
                }
            });

            if (logger.isDebugEnabled()) {
                logger.debug("[{}][{}] found {} allocations of {}, highest version: [{}]",
                        shard.index(), shard.id(), numberOfAllocationsFound, shard, highestVersion);
            }
            if (logger.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder("[");
                for (DiscoveryNode n : nodesWithHighestVersion) {
                    sb.append("[");
                    sb.append(n.getName());
                    sb.append("]");
                    sb.append(" -> ");
                    sb.append(nodesWithVersion.get(n));
                    sb.append(", ");
                }
                sb.append("]");
                logger.trace("{} candidates for allocation: {}", shard, sb.toString());
            }

            // check if the counts meets the minimum set
            int requiredAllocation = 1;
            // if we restore from a repository one copy is more then enough
            if (shard.restoreSource() == null) {
                try {
                    String initialShards = indexMetaData.settings().get(INDEX_RECOVERY_INITIAL_SHARDS, settings.get(INDEX_RECOVERY_INITIAL_SHARDS, this.initialShards));
                    if ("quorum".equals(initialShards)) {
                        if (indexMetaData.numberOfReplicas() > 1) {
                            requiredAllocation = ((1 + indexMetaData.numberOfReplicas()) / 2) + 1;
                        }
                    } else if ("quorum-1".equals(initialShards) || "half".equals(initialShards)) {
                        if (indexMetaData.numberOfReplicas() > 2) {
                            requiredAllocation = ((1 + indexMetaData.numberOfReplicas()) / 2);
                        }
                    } else if ("one".equals(initialShards)) {
                        requiredAllocation = 1;
                    } else if ("full".equals(initialShards) || "all".equals(initialShards)) {
                        requiredAllocation = indexMetaData.numberOfReplicas() + 1;
                    } else if ("full-1".equals(initialShards) || "all-1".equals(initialShards)) {
                        if (indexMetaData.numberOfReplicas() > 1) {
                            requiredAllocation = indexMetaData.numberOfReplicas();
                        }
                    } else {
                        requiredAllocation = Integer.parseInt(initialShards);
                    }
                } catch (Exception e) {
                    logger.warn("[{}][{}] failed to derived initial_shards from value {}, ignore allocation for {}", shard.index(), shard.id(), initialShards, shard);
                }
            }

            // not enough found for this shard, continue...
            if (numberOfAllocationsFound < requiredAllocation) {
                // if we are restoring this shard we still can allocate
                if (shard.restoreSource() == null) {
                    // we can't really allocate, so ignore it and continue
                    unassignedIterator.remove();
                    routingNodes.ignoredUnassigned().add(shard);
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}]: not allocating, number_of_allocated_shards_found [{}], required_number [{}]", shard.index(), shard.id(), numberOfAllocationsFound, requiredAllocation);
                    }
                } else if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}]: missing local data, will restore from [{}]", shard.index(), shard.id(), shard.restoreSource());
                }
                continue;
            }

            Set<DiscoveryNode> throttledNodes = Sets.newHashSet();
            Set<DiscoveryNode> noNodes = Sets.newHashSet();
            for (DiscoveryNode discoNode : nodesWithHighestVersion) {
                RoutingNode node = routingNodes.node(discoNode.id());
                if (node == null) {
                    continue;
                }

                Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
                if (decision.type() == Decision.Type.THROTTLE) {
                    throttledNodes.add(discoNode);
                } else if (decision.type() == Decision.Type.NO) {
                    noNodes.add(discoNode);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}]: allocating [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, discoNode);
                    }
                    // we found a match
                    changed = true;
                    // make sure we create one with the version from the recovered state
                    routingNodes.assign(new ShardRouting(shard, highestVersion), node.nodeId());
                    unassignedIterator.remove();

                    // found a node, so no throttling, no "no", and break out of the loop
                    throttledNodes.clear();
                    noNodes.clear();
                    break;
                }
            }
            if (throttledNodes.isEmpty()) {
                // if we have a node that we "can't" allocate to, force allocation, since this is our master data!
                if (!noNodes.isEmpty()) {
                    DiscoveryNode discoNode = noNodes.iterator().next();
                    RoutingNode node = routingNodes.node(discoNode.id());
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}]: forcing allocating [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, discoNode);
                    }
                    // we found a match
                    changed = true;
                    // make sure we create one with the version from the recovered state
                    routingNodes.assign(new ShardRouting(shard, highestVersion), node.nodeId());
                    unassignedIterator.remove();
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}]: throttling allocation [{}] to [{}] on primary allocation", shard.index(), shard.id(), shard, throttledNodes);
                }
                // we are throttling this, but we have enough to allocate to this node, ignore it for now
                unassignedIterator.remove();
                routingNodes.ignoredUnassigned().add(shard);
            }
        }

        if (!routingNodes.hasUnassigned()) {
            return changed;
        }

        // Now, handle replicas, try to assign them to nodes that are similar to the one the primary was allocated on
        unassignedIterator = unassigned.iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shard = unassignedIterator.next();
            if (shard.primary()) {
                continue;
            }

            // pre-check if it can be allocated to any node that currently exists, so we won't list the store for it for nothing
            boolean canBeAllocatedToAtLeastOneNode = false;
            for (ObjectCursor<DiscoveryNode> cursor : nodes.dataNodes().values()) {
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
                unassignedIterator.remove();
                routingNodes.ignoredUnassigned().add(shard);
                continue;
            }

            AsyncShardFetch<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetch = asyncFetchStore.get(shard.shardId());
            if (fetch == null) {
                fetch = new InternalAsyncFetch<>(logger, "shard_store", shard.shardId(), storeAction);
                asyncFetchStore.put(shard.shardId(), fetch);
            }
            AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> shardStores = fetch.fetchData(nodes, metaData, allocation.getIgnoreNodes(shard.shardId()));
            if (shardStores.hasData() == false) {
                logger.trace("{}: ignoring allocation, still fetching shard stores", shard);
                unassignedIterator.remove();
                routingNodes.ignoredUnassigned().add(shard);
                continue; // still fetching
            }
            shardStores.processAllocation(allocation);

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
                        DiscoveryNode primaryNode = nodes.get(primaryShard.currentNodeId());
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
                    unassignedIterator.remove();
                    routingNodes.ignoredUnassigned().add(shard);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
                    }
                    // we found a match
                    changed = true;
                    routingNodes.assign(shard, lastNodeMatched.nodeId());
                    unassignedIterator.remove();
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
                    unassignedIterator.remove();
                    routingNodes.ignoredUnassigned().add(shard);
                }
            }
        }
        return changed;
    }

    class InternalAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {

        public InternalAsyncFetch(ESLogger logger, String type, ShardId shardId, List<? extends BaseNodesResponse<T>, T> action) {
            super(logger, type, shardId, action);
        }

        @Override
        protected void reroute(ShardId shardId, String reason) {
            logger.trace("{} scheduling reroute for {}", shardId, reason);
            routingService.reroute("async_shard_fetch");
        }
    }

}
