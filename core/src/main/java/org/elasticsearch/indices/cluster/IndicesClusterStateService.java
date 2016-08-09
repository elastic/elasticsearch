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

package org.elasticsearch.indices.cluster;

import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoverySource;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class IndicesClusterStateService extends AbstractLifecycleComponent implements ClusterStateListener {

    final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RecoveryTargetService recoveryTargetService;
    private final ShardStateAction shardStateAction;
    private final NodeMappingRefreshAction nodeMappingRefreshAction;
    private final NodeServicesProvider nodeServicesProvider;

    private static final ShardStateAction.Listener SHARD_STATE_ACTION_LISTENER = new ShardStateAction.Listener() {
    };

    // a list of shards that failed during recovery
    // we keep track of these shards in order to prevent repeated recovery of these shards on each cluster state update
    final ConcurrentMap<ShardId, ShardRouting> failedShardsCache = ConcurrentCollections.newConcurrentMap();
    private final RestoreService restoreService;
    private final RepositoriesService repositoriesService;

    private final FailedShardHandler failedShardHandler = new FailedShardHandler();

    private final boolean sendRefreshMapping;
    private final List<IndexEventListener> buildInIndexListener;

    @Inject
    public IndicesClusterStateService(Settings settings, IndicesService indicesService, ClusterService clusterService,
                                      ThreadPool threadPool, RecoveryTargetService recoveryTargetService,
                                      ShardStateAction shardStateAction,
                                      NodeMappingRefreshAction nodeMappingRefreshAction,
                                      RepositoriesService repositoriesService, RestoreService restoreService,
                                      SearchService searchService, SyncedFlushService syncedFlushService,
                                      RecoverySource recoverySource, NodeServicesProvider nodeServicesProvider) {
        this(settings, (AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>>) indicesService,
            clusterService, threadPool, recoveryTargetService, shardStateAction,
            nodeMappingRefreshAction, repositoriesService, restoreService, searchService, syncedFlushService, recoverySource,
            nodeServicesProvider);
    }

    // for tests
    IndicesClusterStateService(Settings settings,
                               AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService,
                               ClusterService clusterService,
                               ThreadPool threadPool, RecoveryTargetService recoveryTargetService,
                               ShardStateAction shardStateAction,
                               NodeMappingRefreshAction nodeMappingRefreshAction,
                               RepositoriesService repositoriesService, RestoreService restoreService,
                               SearchService searchService, SyncedFlushService syncedFlushService,
                               RecoverySource recoverySource, NodeServicesProvider nodeServicesProvider) {
        super(settings);
        this.buildInIndexListener = Arrays.asList(recoverySource, recoveryTargetService, searchService, syncedFlushService);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTargetService = recoveryTargetService;
        this.shardStateAction = shardStateAction;
        this.nodeMappingRefreshAction = nodeMappingRefreshAction;
        this.restoreService = restoreService;
        this.repositoriesService = repositoriesService;
        this.sendRefreshMapping = this.settings.getAsBoolean("indices.cluster.send_refresh_mapping", true);
        this.nodeServicesProvider = nodeServicesProvider;
    }

    @Override
    protected void doStart() {
        clusterService.addFirst(this);
    }

    @Override
    protected void doStop() {
        clusterService.remove(this);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public synchronized void clusterChanged(final ClusterChangedEvent event) {
        if (!lifecycle.started()) {
            return;
        }

        final ClusterState state = event.state();

        // we need to clean the shards and indices we have on this node, since we
        // are going to recover them again once state persistence is disabled (no master / not recovered)
        // TODO: feels hacky, a block disables state persistence, and then we clean the allocated shards, maybe another flag in blocks?
        if (state.blocks().disableStatePersistence()) {
            for (AllocatedIndex<? extends Shard> indexService : indicesService) {
                indicesService.removeIndex(indexService.index(), "cleaning index (disabled block persistence)"); // also cleans shards
            }
            return;
        }

        updateFailedShardsCache(state);

        deleteIndices(event); // also deletes shards of deleted indices

        removeUnallocatedIndices(event); // also removes shards of removed indices

        failMissingShards(state);

        removeShards(state);

        updateIndices(event); // can also fail shards, but these are then guaranteed to be in failedShardsCache

        createIndices(state);

        createOrUpdateShards(state);
    }

    /**
     * Removes shard entries from the failed shards cache that are no longer allocated to this node by the master.
     * Sends shard failures for shards that are marked as actively allocated to this node but don't actually exist on the node.
     * Resends shard failures for shards that are still marked as allocated to this node but previously failed.
     *
     * @param state new cluster state
     */
    private void updateFailedShardsCache(final ClusterState state) {
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            failedShardsCache.clear();
            return;
        }

        DiscoveryNode masterNode = state.nodes().getMasterNode();

        // remove items from cache which are not in our routing table anymore and resend failures that have not executed on master yet
        for (Iterator<Map.Entry<ShardId, ShardRouting>> iterator = failedShardsCache.entrySet().iterator(); iterator.hasNext(); ) {
            ShardRouting failedShardRouting = iterator.next().getValue();
            ShardRouting matchedRouting = localRoutingNode.getByShardId(failedShardRouting.shardId());
            if (matchedRouting == null || matchedRouting.isSameAllocation(failedShardRouting) == false) {
                iterator.remove();
            } else {
                if (masterNode != null) { // TODO: can we remove this? Is resending shard failures the responsibility of shardStateAction?
                    String message = "master " + masterNode + " has not removed previously failed shard. resending shard failure";
                    logger.trace("[{}] re-sending failed shard [{}], reason [{}]", matchedRouting.shardId(), matchedRouting, message);
                    shardStateAction.localShardFailed(matchedRouting, message, null, SHARD_STATE_ACTION_LISTENER);
                }
            }
        }
    }

    /**
     * Deletes indices (with shard data).
     *
     * @param event cluster change event
     */
    private void deleteIndices(final ClusterChangedEvent event) {
        final ClusterState previousState = event.previousState();
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        for (Index index : event.indicesDeleted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] cleaning index, no longer part of the metadata", index);
            }
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
            final IndexSettings indexSettings;
            if (indexService != null) {
                indexSettings = indexService.getIndexSettings();
                indicesService.deleteIndex(index, "index no longer part of the metadata");
            } else if (previousState.metaData().hasIndex(index.getName())) {
                // The deleted index was part of the previous cluster state, but not loaded on the local node
                final IndexMetaData metaData = previousState.metaData().index(index);
                indexSettings = new IndexSettings(metaData, settings);
                indicesService.deleteUnassignedIndex("deleted index was not assigned to local node", metaData, state);
            } else {
                // The previous cluster state's metadata also does not contain the index,
                // which is what happens on node startup when an index was deleted while the
                // node was not part of the cluster.  In this case, try reading the index
                // metadata from disk.  If its not there, there is nothing to delete.
                // First, though, verify the precondition for applying this case by
                // asserting that the previous cluster state is not initialized/recovered.
                assert previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
                final IndexMetaData metaData = indicesService.verifyIndexIsDeleted(index, event.state());
                if (metaData != null) {
                    indexSettings = new IndexSettings(metaData, settings);
                } else {
                    indexSettings = null;
                }
            }
            if (indexSettings != null) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("[{}] failed to complete pending deletion for index", e, index);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        try {
                            // we are waiting until we can lock the index / all shards on the node and then we ack the delete of the store
                            // to the master. If we can't acquire the locks here immediately there might be a shard of this index still
                            // holding on to the lock due to a "currently canceled recovery" or so. The shard will delete itself BEFORE the
                            // lock is released so it's guaranteed to be deleted by the time we get the lock
                            indicesService.processPendingDeletes(index, indexSettings, new TimeValue(30, TimeUnit.MINUTES));
                        } catch (LockObtainFailedException exc) {
                            logger.warn("[{}] failed to lock all shards for index - timed out after 30 seconds", index);
                        } catch (InterruptedException e) {
                            logger.warn("[{}] failed to lock all shards for index - interrupted", index);
                        }
                    }
                });
            }
        }
    }

    /**
     * Removes indices that have no shards allocated to this node. This does not delete the shard data as we wait for enough
     * shard copies to exist in the cluster before deleting shard data (triggered by {@link org.elasticsearch.indices.store.IndicesStore}).
     *
     * @param event the cluster changed event
     */
    private void removeUnallocatedIndices(final ClusterChangedEvent event) {
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        Set<Index> indicesWithShards = new HashSet<>();
        RoutingNode localRoutingNode = state.getRoutingNodes().node(localNodeId);
        if (localRoutingNode != null) { // null e.g. if we are not a data node
            for (ShardRouting shardRouting : localRoutingNode) {
                indicesWithShards.add(shardRouting.index());
            }
        }

        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            Index index = indexService.index();
            if (indicesWithShards.contains(index) == false) {
                // if the cluster change indicates a brand new cluster, we only want
                // to remove the in-memory structures for the index and not delete the
                // contents on disk because the index will later be re-imported as a
                // dangling index
                assert state.metaData().index(index) != null || event.isNewCluster() :
                    "index " + index + " does not exist in the cluster state, it should either " +
                    "have been deleted or the cluster must be new";
                logger.debug("{} removing index, no shards allocated", index);
                indicesService.removeIndex(index, "removing index (no shards allocated)");
            }
        }
    }

    /**
     * Notifies master about shards that don't exist but are supposed to be active on this node.
     *
     * @param state new cluster state
     */
    private void failMissingShards(final ClusterState state) {
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        for (final ShardRouting shardRouting : localRoutingNode) {
            ShardId shardId = shardRouting.shardId();
            if (shardRouting.initializing() == false &&
                failedShardsCache.containsKey(shardId) == false &&
                indicesService.getShardOrNull(shardId) == null) {
                // the master thinks we are active, but we don't have this shard at all, mark it as failed
                sendFailShard(shardRouting, "master marked shard as active, but shard has not been created, mark shard as failed", null);
            }
        }
    }

    /**
     * Removes shards that are currently loaded by indicesService but have disappeared from the routing table of the current node.
     * Also removes shards where the recovery source node has changed.
     * This method does not delete the shard data.
     *
     * @param state new cluster state
     */
    private void removeShards(final ClusterState state) {
        final RoutingTable routingTable = state.routingTable();
        final DiscoveryNodes nodes = state.nodes();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        // remove shards based on routing nodes (no deletion of data)
        RoutingNode localRoutingNode = state.getRoutingNodes().node(localNodeId);
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            for (Shard shard : indexService) {
                ShardRouting currentRoutingEntry = shard.routingEntry();
                ShardId shardId = currentRoutingEntry.shardId();
                ShardRouting newShardRouting = localRoutingNode == null ? null : localRoutingNode.getByShardId(shardId);
                if (newShardRouting == null || newShardRouting.isSameAllocation(currentRoutingEntry) == false) {
                    // we can just remove the shard without cleaning it locally, since we will clean it in IndicesStore
                    // once all shards are allocated
                    logger.debug("{} removing shard (not allocated)", shardId);
                    indexService.removeShard(shardId.id(), "removing shard (not allocated)");
                } else {
                    // remove shards where recovery source has changed. This re-initializes shards later in createOrUpdateShards
                    if (newShardRouting.isPeerRecovery()) {
                        RecoveryState recoveryState = shard.recoveryState();
                        final DiscoveryNode sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, newShardRouting);
                        if (recoveryState.getSourceNode().equals(sourceNode) == false) {
                            if (recoveryTargetService.cancelRecoveriesForShard(shardId, "recovery source node changed")) {
                                // getting here means that the shard was still recovering
                                logger.debug("{} removing shard (recovery source changed), current [{}], global [{}], shard [{}])",
                                    shardId, recoveryState.getSourceNode(), sourceNode, newShardRouting);
                                indexService.removeShard(shardId.id(), "removing shard (recovery source node changed)");
                            }
                        }
                    }
                }
            }
        }
    }

    private void createIndices(final ClusterState state) {
        // we only create indices for shards that are allocated
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        // create map of indices to create with shards to fail if index creation fails
        final Map<Index, List<ShardRouting>> indicesToCreate = new HashMap<>();
        for (ShardRouting shardRouting : localRoutingNode) {
            if (failedShardsCache.containsKey(shardRouting.shardId()) == false) {
                final Index index = shardRouting.index();
                if (indicesService.indexService(index) == null) {
                    indicesToCreate.computeIfAbsent(index, k -> new ArrayList<>()).add(shardRouting);
                }
            }
        }

        for (Map.Entry<Index, List<ShardRouting>> entry : indicesToCreate.entrySet()) {
            final Index index = entry.getKey();
            final IndexMetaData indexMetaData = state.metaData().index(index);
            logger.debug("[{}] creating index", index);

            AllocatedIndex<? extends Shard> indexService = null;
            try {
                indexService = indicesService.createIndex(nodeServicesProvider, indexMetaData, buildInIndexListener);
                if (indexService.updateMapping(indexMetaData) && sendRefreshMapping) {
                    nodeMappingRefreshAction.nodeMappingRefresh(state.nodes().getMasterNode(),
                        new NodeMappingRefreshAction.NodeMappingRefreshRequest(indexMetaData.getIndex().getName(),
                            indexMetaData.getIndexUUID(), state.nodes().getLocalNodeId())
                    );
                }
            } catch (Exception e) {
                final String failShardReason;
                if (indexService == null) {
                    failShardReason = "failed to create index";
                } else {
                    failShardReason = "failed to update mapping for index";
                    indicesService.removeIndex(index, "removing index (mapping update failed)");
                }
                for (ShardRouting shardRouting : entry.getValue()) {
                    sendFailShard(shardRouting, failShardReason, e);
                }
            }
        }
    }

    private void updateIndices(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        final ClusterState state = event.state();
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            final Index index = indexService.index();
            final IndexMetaData currentIndexMetaData = indexService.getIndexSettings().getIndexMetaData();
            final IndexMetaData newIndexMetaData = state.metaData().index(index);
            assert newIndexMetaData != null : "index " + index + " should have been removed by deleteIndices";
            if (ClusterChangedEvent.indexMetaDataChanged(currentIndexMetaData, newIndexMetaData)) {
                indexService.updateMetaData(newIndexMetaData);
                try {
                    if (indexService.updateMapping(newIndexMetaData) && sendRefreshMapping) {
                        nodeMappingRefreshAction.nodeMappingRefresh(state.nodes().getMasterNode(),
                            new NodeMappingRefreshAction.NodeMappingRefreshRequest(newIndexMetaData.getIndex().getName(),
                                newIndexMetaData.getIndexUUID(), state.nodes().getLocalNodeId())
                        );
                    }
                } catch (Exception e) {
                    indicesService.removeIndex(indexService.index(), "removing index (mapping update failed)");

                    // fail shards that would be created or updated by createOrUpdateShards
                    RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
                    if (localRoutingNode != null) {
                        for (final ShardRouting shardRouting : localRoutingNode) {
                            if (shardRouting.index().equals(index) && failedShardsCache.containsKey(shardRouting.shardId()) == false) {
                                sendFailShard(shardRouting, "failed to update mapping for index", e);
                            }
                        }
                    }
                }
            }
        }
    }

    private void createOrUpdateShards(final ClusterState state) {
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }

        DiscoveryNodes nodes = state.nodes();
        RoutingTable routingTable = state.routingTable();

        for (final ShardRouting shardRouting : localRoutingNode) {
            ShardId shardId = shardRouting.shardId();
            if (failedShardsCache.containsKey(shardId) == false) {
                AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardId.getIndex());
                assert indexService != null : "index " + shardId.getIndex() + " should have been created by createIndices";
                Shard shard = indexService.getShardOrNull(shardId.id());
                if (shard == null) {
                    assert shardRouting.initializing() : shardRouting + " should have been removed by failMissingShards";
                    createShard(nodes, routingTable, shardRouting, indexService);
                } else {
                    updateShard(nodes, shardRouting, shard);
                }
            }
        }
    }

    private void createShard(DiscoveryNodes nodes, RoutingTable routingTable, ShardRouting shardRouting,
                             AllocatedIndex<? extends Shard> indexService) {
        assert shardRouting.initializing() : "only allow shard creation for initializing shard but was " + shardRouting;

        DiscoveryNode sourceNode = null;
        if (shardRouting.isPeerRecovery()) {
            sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, shardRouting);
            if (sourceNode == null) {
                logger.trace("ignoring initializing shard {} - no source node can be found.", shardRouting.shardId());
                return;
            }
        }

        try {
            logger.debug("{} creating shard", shardRouting.shardId());
            RecoveryState recoveryState = recoveryState(nodes.getLocalNode(), sourceNode, shardRouting,
                indexService.getIndexSettings().getIndexMetaData());
            indicesService.createShard(shardRouting, recoveryState, recoveryTargetService, new RecoveryListener(shardRouting),
                repositoriesService, nodeServicesProvider, failedShardHandler);
        } catch (IndexShardAlreadyExistsException e) {
            // ignore this, the method call can happen several times
            logger.debug("Trying to create shard that already exists", e);
            assert false;
        } catch (Exception e) {
            failAndRemoveShard(shardRouting, true, "failed to create shard", e);
        }
    }

    private void updateShard(DiscoveryNodes nodes, ShardRouting shardRouting, Shard shard) {
        final ShardRouting currentRoutingEntry = shard.routingEntry();
        assert currentRoutingEntry.isSameAllocation(shardRouting) :
            "local shard has a different allocation id but wasn't cleaning by removeShards. "
                + "cluster state: " + shardRouting + " local: " + currentRoutingEntry;

        try {
            shard.updateRoutingEntry(shardRouting);
        } catch (Exception e) {
            failAndRemoveShard(shardRouting, true, "failed updating shard routing entry", e);
            return;
        }

        final IndexShardState state = shard.state();
        if (shardRouting.initializing() && (state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY)) {
            // the master thinks we are initializing, but we are already started or on POST_RECOVERY and waiting
            // for master to confirm a shard started message (either master failover, or a cluster event before
            // we managed to tell the master we started), mark us as started
            if (logger.isTraceEnabled()) {
                logger.trace("{} master marked shard as initializing, but shard has state [{}], resending shard started to {}",
                    shardRouting.shardId(), state, nodes.getMasterNode());
            }
            if (nodes.getMasterNode() != null) {
                shardStateAction.shardStarted(shardRouting, "master " + nodes.getMasterNode() +
                    " marked shard as initializing, but shard state is [" + state + "], mark shard as started",
                    SHARD_STATE_ACTION_LISTENER);
            }
        }
    }

    /**
     * Finds the routing source node for peer recovery, return null if its not found. Note, this method expects the shard
     * routing to *require* peer recovery, use {@link ShardRouting#isPeerRecovery()} to
     * check if its needed or not.
     */
    private static DiscoveryNode findSourceNodeForPeerRecovery(ESLogger logger, RoutingTable routingTable, DiscoveryNodes nodes,
                                                               ShardRouting shardRouting) {
        DiscoveryNode sourceNode = null;
        if (!shardRouting.primary()) {
            ShardRouting primary = routingTable.shardRoutingTable(shardRouting.shardId()).primaryShard();
            // only recover from started primary, if we can't find one, we will do it next round
            if (primary.active()) {
                sourceNode = nodes.get(primary.currentNodeId());
                if (sourceNode == null) {
                    logger.trace("can't find replica source node because primary shard {} is assigned to an unknown node.", primary);
                }
            } else {
                logger.trace("can't find replica source node because primary shard {} is not active.", primary);
            }
        } else if (shardRouting.relocatingNodeId() != null) {
            sourceNode = nodes.get(shardRouting.relocatingNodeId());
            if (sourceNode == null) {
                logger.trace("can't find relocation source node for shard {} because it is assigned to an unknown node [{}].",
                    shardRouting.shardId(), shardRouting.relocatingNodeId());
            }
        } else {
            throw new IllegalStateException("trying to find source node for peer recovery when routing state means no peer recovery: " +
                shardRouting);
        }
        return sourceNode;
    }

    private class RecoveryListener implements RecoveryTargetService.RecoveryListener {

        private final ShardRouting shardRouting;

        private RecoveryListener(ShardRouting shardRouting) {
            this.shardRouting = shardRouting;
        }

        @Override
        public void onRecoveryDone(RecoveryState state) {
            if (state.getType() == RecoveryState.Type.SNAPSHOT) {
                restoreService.indexShardRestoreCompleted(state.getRestoreSource().snapshot(), shardRouting.shardId());
            }
            shardStateAction.shardStarted(shardRouting, message(state), SHARD_STATE_ACTION_LISTENER);
        }

        private String message(RecoveryState state) {
            switch (state.getType()) {
                case SNAPSHOT: return "after recovery from repository";
                case STORE: return "after recovery from store";
                case PRIMARY_RELOCATION: return "after recovery (primary relocation) from node [" + state.getSourceNode() + "]";
                case REPLICA: return "after recovery (replica) from node [" + state.getSourceNode() + "]";
                case LOCAL_SHARDS: return "after recovery from local shards";
                default: throw new IllegalArgumentException("Unknown recovery type: " + state.getType().name());
            }
        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            if (state.getType() == RecoveryState.Type.SNAPSHOT) {
                try {
                    if (Lucene.isCorruptionException(e.getCause())) {
                        restoreService.failRestore(state.getRestoreSource().snapshot(), shardRouting.shardId());
                    }
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                } finally {
                    handleRecoveryFailure(shardRouting, sendShardFailure, e);
                }
            } else {
                handleRecoveryFailure(shardRouting, sendShardFailure, e);
            }
        }
    }

    private RecoveryState recoveryState(DiscoveryNode localNode, DiscoveryNode sourceNode, ShardRouting shardRouting,
                                              IndexMetaData indexMetaData) {
        assert shardRouting.initializing() : "only allow initializing shard routing to be recovered: " + shardRouting;
        if (shardRouting.isPeerRecovery()) {
            assert sourceNode != null : "peer recovery started but sourceNode is null for " + shardRouting;
            RecoveryState.Type type = shardRouting.primary() ? RecoveryState.Type.PRIMARY_RELOCATION : RecoveryState.Type.REPLICA;
            return new RecoveryState(shardRouting.shardId(), shardRouting.primary(), type, sourceNode, localNode);
        } else if (shardRouting.restoreSource() == null) {
            // recover from filesystem store
            Index mergeSourceIndex = indexMetaData.getMergeSourceIndex();
            final boolean recoverFromLocalShards = mergeSourceIndex != null && shardRouting.allocatedPostIndexCreate(indexMetaData) == false
                && shardRouting.primary();
            return new RecoveryState(shardRouting.shardId(), shardRouting.primary(),
                recoverFromLocalShards ? RecoveryState.Type.LOCAL_SHARDS : RecoveryState.Type.STORE, localNode, localNode);
        } else {
            // recover from a snapshot
            return new RecoveryState(shardRouting.shardId(), shardRouting.primary(),
                RecoveryState.Type.SNAPSHOT, shardRouting.restoreSource(), localNode);
        }
    }

    private synchronized void handleRecoveryFailure(ShardRouting shardRouting, boolean sendShardFailure, Exception failure) {
        failAndRemoveShard(shardRouting, sendShardFailure, "failed recovery", failure);
    }

    private void failAndRemoveShard(ShardRouting shardRouting, boolean sendShardFailure, String message, @Nullable Exception failure) {
        try {
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardRouting.shardId().getIndex());
            if (indexService != null) {
                indexService.removeShard(shardRouting.shardId().id(), message);
            }
        } catch (ShardNotFoundException e) {
            // the node got closed on us, ignore it
        } catch (Exception inner) {
            inner.addSuppressed(failure);
            logger.warn(
                "[{}][{}] failed to remove shard after failure ([{}])",
                inner,
                shardRouting.getIndexName(),
                shardRouting.getId(),
                message);
        }
        if (sendShardFailure) {
            sendFailShard(shardRouting, message, failure);
        }
    }

    private void sendFailShard(ShardRouting shardRouting, String message, @Nullable Exception failure) {
        try {
            logger.warn("[{}] marking and sending shard failed due to [{}]", failure, shardRouting.shardId(), message);
            failedShardsCache.put(shardRouting.shardId(), shardRouting);
            shardStateAction.localShardFailed(shardRouting, message, failure, SHARD_STATE_ACTION_LISTENER);
        } catch (Exception inner) {
            if (failure != null) inner.addSuppressed(failure);
            logger.warn(
                    "[{}][{}] failed to mark shard as failed (because of [{}])",
                    inner,
                    shardRouting.getIndexName(),
                    shardRouting.getId(),
                    message);
        }
    }

    private class FailedShardHandler implements Callback<IndexShard.ShardFailure> {
        @Override
        public void handle(final IndexShard.ShardFailure shardFailure) {
            final ShardRouting shardRouting = shardFailure.routing;
            threadPool.generic().execute(() -> {
                synchronized (IndicesClusterStateService.this) {
                    failAndRemoveShard(shardRouting, true, "shard failure, reason [" + shardFailure.reason + "]", shardFailure.cause);
                }
            });
        }
    }

    public interface Shard {

        /**
         * Returns the shard id of this shard.
         */
        ShardId shardId();

        /**
         * Returns the latest cluster routing entry received with this shard.
         */
        ShardRouting routingEntry();

        /**
         * Returns the latest internal shard state.
         */
        IndexShardState state();

        /**
         * Returns the recovery state associated with this shard.
         */
        RecoveryState recoveryState();

        /**
         * Updates the shards routing entry. This mutate the shards internal state depending
         * on the changes that get introduced by the new routing value. This method will persist shard level metadata.
         *
         * @throws IndexShardRelocatedException if shard is marked as relocated and relocation aborted
         * @throws IOException                  if shard state could not be persisted
         */
        void updateRoutingEntry(ShardRouting shardRouting) throws IOException;
    }

    public interface AllocatedIndex<T extends Shard> extends Iterable<T>, IndexComponent {

        /**
         * Returns the index settings of this index.
         */
        IndexSettings getIndexSettings();

        /**
         * Updates the meta data of this index. Changes become visible through {@link #getIndexSettings()}
         */
        void updateMetaData(IndexMetaData indexMetaData);

        /**
         * Checks if index requires refresh from master.
         */
        boolean updateMapping(IndexMetaData indexMetaData) throws IOException;

        /**
         * Returns shard with given id.
         */
        @Nullable T getShardOrNull(int shardId);

        /**
         * Removes shard with given id.
         */
        void removeShard(int shardId, String message);
    }

    public interface AllocatedIndices<T extends Shard, U extends AllocatedIndex<T>> extends Iterable<U> {

        /**
         * Creates a new {@link IndexService} for the given metadata.
         * @param indexMetaData the index metadata to create the index for
         * @param builtInIndexListener a list of built-in lifecycle {@link IndexEventListener} that should should be used along side with
         *                             the per-index listeners
         * @throws IndexAlreadyExistsException if the index already exists.
         */
        U createIndex(NodeServicesProvider nodeServicesProvider, IndexMetaData indexMetaData,
                         List<IndexEventListener> builtInIndexListener) throws IOException;

        /**
         * Verify that the contents on disk for the given index is deleted; if not, delete the contents.
         * This method assumes that an index is already deleted in the cluster state and/or explicitly
         * through index tombstones.
         * @param index {@code Index} to make sure its deleted from disk
         * @param clusterState {@code ClusterState} to ensure the index is not part of it
         * @return IndexMetaData for the index loaded from disk
         */
        IndexMetaData verifyIndexIsDeleted(Index index, ClusterState clusterState);

        /**
         * Deletes the given index. Persistent parts of the index
         * like the shards files, state and transaction logs are removed once all resources are released.
         *
         * Equivalent to {@link #removeIndex(Index, String)} but fires
         * different lifecycle events to ensure pending resources of this index are immediately removed.
         * @param index the index to delete
         * @param reason the high level reason causing this delete
         */
        void deleteIndex(Index index, String reason);

        /**
         * Deletes an index that is not assigned to this node. This method cleans up all disk folders relating to the index
         * but does not deal with in-memory structures. For those call {@link #deleteIndex(Index, String)}
         */
        void deleteUnassignedIndex(String reason, IndexMetaData metaData, ClusterState clusterState);

        /**
         * Removes the given index from this service and releases all associated resources. Persistent parts of the index
         * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
         * @param index the index to remove
         * @param reason  the high level reason causing this removal
         */
        void removeIndex(Index index, String reason);

        /**
         * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
         */
        @Nullable U indexService(Index index);

        /**
         * Creates shard for the specified shard routing and starts recovery,
         */
        T createShard(ShardRouting shardRouting, RecoveryState recoveryState, RecoveryTargetService recoveryTargetService,
                      RecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService,
                      NodeServicesProvider nodeServicesProvider, Callback<IndexShard.ShardFailure> onShardFailure) throws IOException;

        /**
         * Returns shard for the specified id if it exists otherwise returns <code>null</code>.
         */
        default T getShardOrNull(ShardId shardId) {
            U indexRef = indexService(shardId.getIndex());
            if (indexRef != null) {
                return indexRef.getShardOrNull(shardId.id());
            }
            return null;
        }

        void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeValue) throws IOException, InterruptedException;
    }
}
