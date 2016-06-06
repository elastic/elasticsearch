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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 *
 */
public class IndicesClusterStateService extends AbstractLifecycleComponent<IndicesClusterStateService> implements ClusterStateListener {

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RecoveryTargetService recoveryTargetService;
    private final ShardStateAction shardStateAction;
    private final NodeIndexDeletedAction nodeIndexDeletedAction;
    private final NodeMappingRefreshAction nodeMappingRefreshAction;
    private final NodeServicesProvider nodeServicesProvider;
    private final GlobalCheckpointSyncAction globalCheckpointSyncAction;

    private static final ShardStateAction.Listener SHARD_STATE_ACTION_LISTENER = new ShardStateAction.Listener() {
    };

    // a list of shards that failed during recovery
    // we keep track of these shards in order to prevent repeated recovery of these shards on each cluster state update
    private final ConcurrentMap<ShardId, ShardRouting> failedShards = ConcurrentCollections.newConcurrentMap();
    private final RestoreService restoreService;
    private final RepositoriesService repositoriesService;

    private final Object mutex = new Object();
    private final FailedShardHandler failedShardHandler = new FailedShardHandler();

    private final boolean sendRefreshMapping;
    private final List<IndexEventListener> buildInIndexListener;

    @Inject
    public IndicesClusterStateService(Settings settings, IndicesService indicesService, ClusterService clusterService,
                                      ThreadPool threadPool, RecoveryTargetService recoveryTargetService,
                                      ShardStateAction shardStateAction,
                                      NodeIndexDeletedAction nodeIndexDeletedAction,
                                      NodeMappingRefreshAction nodeMappingRefreshAction,
                                      RepositoriesService repositoriesService, RestoreService restoreService,
                                      SearchService searchService, SyncedFlushService syncedFlushService,
                                      RecoverySource recoverySource, NodeServicesProvider nodeServicesProvider,
                                      GlobalCheckpointSyncAction globalCheckpointSyncAction) {
        super(settings);
        this.buildInIndexListener = Arrays.asList(recoverySource, recoveryTargetService, searchService, syncedFlushService);
        this.globalCheckpointSyncAction = globalCheckpointSyncAction;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTargetService = recoveryTargetService;
        this.shardStateAction = shardStateAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
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
    public void clusterChanged(final ClusterChangedEvent event) {
        if (!indicesService.changesAllowed()) {
            return;
        }

        if (!lifecycle.started()) {
            return;
        }

        synchronized (mutex) {
            // we need to clean the shards and indices we have on this node, since we
            // are going to recover them again once state persistence is disabled (no master / not recovered)
            // TODO: this feels a bit hacky here, a block disables state persistence, and then we clean the allocated shards, maybe another flag in blocks?
            if (event.state().blocks().disableStatePersistence()) {
                for (IndexService indexService : indicesService) {
                    Index index = indexService.index();
                    for (Integer shardId : indexService.shardIds()) {
                        logger.debug("{}[{}] removing shard (disabled block persistence)", index, shardId);
                        try {
                            indexService.removeShard(shardId, "removing shard (disabled block persistence)");
                        } catch (Throwable e) {
                            logger.warn("{} failed to remove shard (disabled block persistence)", e, index);
                        }
                    }
                    removeIndex(index, "cleaning index (disabled block persistence)");
                }
                return;
            }

            cleanFailedShards(event);

            // cleaning up indices that are completely deleted so we won't need to worry about them
            // when checking for shards
            applyDeletedIndices(event);
            applyDeletedShards(event);
            // call after deleted shards so indices with no shards will be cleaned
            applyCleanedIndices(event);
            // make sure that newly created shards use the latest meta data
            applyIndexMetaData(event);
            applyNewIndices(event);
            // apply mappings also updates new indices. TODO: make new indices good to begin with
            applyMappings(event);
            applyNewOrUpdatedShards(event);
        }
    }

    private void cleanFailedShards(final ClusterChangedEvent event) {
        RoutingNode routingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        if (routingNode == null) {
            failedShards.clear();
            return;
        }
        for (Iterator<Map.Entry<ShardId, ShardRouting>> iterator = failedShards.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<ShardId, ShardRouting> entry = iterator.next();
            ShardRouting failedShardRouting = entry.getValue();
            ShardRouting matchedShardRouting = routingNode.getByShardId(failedShardRouting.shardId());
            if (matchedShardRouting == null || matchedShardRouting.isSameAllocation(failedShardRouting) == false) {
                iterator.remove();
            }
        }
    }

    private void applyDeletedIndices(final ClusterChangedEvent event) {
        final ClusterState previousState = event.previousState();
        final String localNodeId = event.state().nodes().getLocalNodeId();
        assert localNodeId != null;

        for (Index index : event.indicesDeleted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] cleaning index, no longer part of the metadata", index);
            }
            final IndexService idxService = indicesService.indexService(index);
            final IndexSettings indexSettings;
            if (idxService != null) {
                indexSettings = idxService.getIndexSettings();
                deleteIndex(index, "index no longer part of the metadata");
            } else if (previousState.metaData().hasIndex(index.getName())) {
                // The deleted index was part of the previous cluster state, but not loaded on the local node
                final IndexMetaData metaData = previousState.metaData().index(index);
                indexSettings = new IndexSettings(metaData, settings);
                indicesService.deleteUnassignedIndex("deleted index was not assigned to local node", metaData, event.state());
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
            // indexSettings can only be null if there was no IndexService and no metadata existed
            // on disk for this index, so it won't need to go through the node deleted action anyway
            if (indexSettings != null) {
                try {
                    nodeIndexDeletedAction.nodeIndexDeleted(event.state(), index, indexSettings, localNodeId);
                } catch (Exception e) {
                    logger.debug("failed to send to master index {} deleted event", e, index);
                }
            }
        }

        for (IndexService indexService : indicesService) {
            IndexMetaData indexMetaData = event.state().metaData().index(indexService.index());
            if (indexMetaData == null) {
                assert false : "index" + indexService.index() + " exists locally, doesn't have a metadata but is not part "
                    + " of the delete index list. \nprevious state: " + event.previousState().prettyPrint()
                    + "\n current state:\n" + event.state().prettyPrint();
                logger.warn("[{}] isn't part of metadata but is part of in memory structures. removing",
                    indexService.index());
                deleteIndex(indexService.index(), "isn't part of metadata (explicit check)");
            }
        }
    }

    private void applyDeletedShards(final ClusterChangedEvent event) {
        RoutingNode routingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        if (routingNode == null) {
            return;
        }
        Set<String> newShardAllocationIds = new HashSet<>();
        for (IndexService indexService : indicesService) {
            Index index = indexService.index();
            IndexMetaData indexMetaData = event.state().metaData().index(index);
            assert indexMetaData != null : "local index doesn't have metadata, should have been cleaned up by applyDeletedIndices: " + index;
            // now, go over and delete shards that needs to get deleted
            newShardAllocationIds.clear();
            for (ShardRouting shard : routingNode) {
                if (shard.index().equals(index)) {
                    // use the allocation id and not object so we won't be influence by relocation targets
                    newShardAllocationIds.add(shard.allocationId().getId());
                }
            }
            for (IndexShard existingShard : indexService) {
                if (newShardAllocationIds.contains(existingShard.routingEntry().allocationId().getId()) == false) {
                    if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} removing shard (index is closed)", existingShard.shardId());
                        }
                        indexService.removeShard(existingShard.shardId().id(), "removing shard (index is closed)");
                    } else {
                        // we can just remove the shard, without cleaning it locally, since we will clean it
                        // when all shards are allocated in the IndicesStore
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} removing shard (not allocated)", existingShard.shardId());
                        }
                        indexService.removeShard(existingShard.shardId().id(), "removing shard (not allocated)");
                    }
                }
            }
        }
    }

    private void applyCleanedIndices(final ClusterChangedEvent event) {
        // handle closed indices, since they are not allocated on a node once they are closed
        // so applyDeletedIndices might not take them into account
        for (IndexService indexService : indicesService) {
            Index index = indexService.index();
            IndexMetaData indexMetaData = event.state().metaData().index(index);
            if (indexMetaData != null && indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                for (Integer shardId : indexService.shardIds()) {
                    logger.debug("{}[{}] removing shard (index is closed)", index, shardId);
                    try {
                        indexService.removeShard(shardId, "removing shard (index is closed)");
                    } catch (Throwable e) {
                        logger.warn("{} failed to remove shard (index is closed)", e, index);
                    }
                }
            }
        }

        final Set<Index> hasAllocations = new HashSet<>();
        final RoutingNode node = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        // if no shards are allocated ie. if this node is a master-only node it can return nul
        if (node != null) {
            for (ShardRouting routing : node) {
                hasAllocations.add(routing.index());
            }
        }
        for (IndexService indexService : indicesService) {
            Index index = indexService.index();
            if (hasAllocations.contains(index) == false) {
                assert indexService.shardIds().isEmpty() :
                    "no locally assigned shards, but index wasn't emptied by applyDeletedShards."
                        + " index " + index + ", shards: " + indexService.shardIds();
                if (logger.isDebugEnabled()) {
                    logger.debug("{} cleaning index (no shards allocated)", index);
                }
                // clean the index
                removeIndex(index, "removing index (no shards allocated)");
            }
        }
    }

    private void applyIndexMetaData(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        for (IndexMetaData indexMetaData : event.state().metaData()) {
            if (!indicesService.hasIndex(indexMetaData.getIndex())) {
                // we only create / update here
                continue;
            }
            // if the index meta data didn't change, no need check for refreshed settings
            if (!event.indexMetaDataChanged(indexMetaData)) {
                continue;
            }
            Index index = indexMetaData.getIndex();
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                // already deleted on us, ignore it
                continue;
            }
            indexService.updateMetaData(indexMetaData);
        }
    }

    private void applyNewIndices(final ClusterChangedEvent event) {
        // we only create indices for shards that are allocated
        RoutingNode routingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        if (routingNode == null) {
            return;
        }
        for (ShardRouting shard : routingNode) {
            if (!indicesService.hasIndex(shard.index())) {
                final IndexMetaData indexMetaData = event.state().metaData().getIndexSafe(shard.index());
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] creating index", indexMetaData.getIndex());
                }
                try {
                    indicesService.createIndex(nodeServicesProvider, indexMetaData, buildInIndexListener,
                        globalCheckpointSyncAction::updateCheckpointForShard);
                } catch (Throwable e) {
                    sendFailShard(shard, "failed to create index", e);
                }
            }
        }
    }

    private void applyMappings(ClusterChangedEvent event) {
        // go over and update mappings
        for (IndexMetaData indexMetaData : event.state().metaData()) {
            Index index = indexMetaData.getIndex();
            if (!indicesService.hasIndex(index)) {
                // we only create / update here
                continue;
            }
            boolean requireRefresh = false;
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                // got deleted on us, ignore (closing the node)
                return;
            }
            try {
                MapperService mapperService = indexService.mapperService();
                // go over and add the relevant mappings (or update them)
                for (ObjectCursor<MappingMetaData> cursor : indexMetaData.getMappings().values()) {
                    MappingMetaData mappingMd = cursor.value;
                    String mappingType = mappingMd.type();
                    CompressedXContent mappingSource = mappingMd.source();
                    requireRefresh |= processMapping(index.getName(), mapperService, mappingType, mappingSource);
                }
                if (requireRefresh && sendRefreshMapping) {
                    nodeMappingRefreshAction.nodeMappingRefresh(event.state(),
                        new NodeMappingRefreshAction.NodeMappingRefreshRequest(index.getName(), indexMetaData.getIndexUUID(),
                            event.state().nodes().getLocalNodeId())
                    );
                }
            } catch (Throwable t) {
                // if we failed the mappings anywhere, we need to fail the shards for this index, note, we safeguard
                // by creating the processing the mappings on the master, or on the node the mapping was introduced on,
                // so this failure typically means wrong node level configuration or something similar
                for (IndexShard indexShard : indexService) {
                    ShardRouting shardRouting = indexShard.routingEntry();
                    failAndRemoveShard(shardRouting, indexService, true, "failed to update mappings", t);
                }
            }
        }
    }

    private boolean processMapping(String index, MapperService mapperService, String mappingType, CompressedXContent mappingSource) throws Throwable {
        // refresh mapping can happen when the parsing/merging of the mapping from the metadata doesn't result in the same
        // mapping, in this case, we send to the master to refresh its own version of the mappings (to conform with the
        // merge version of it, which it does when refreshing the mappings), and warn log it.
        boolean requiresRefresh = false;
        try {
            DocumentMapper existingMapper = mapperService.documentMapper(mappingType);

            if (existingMapper == null || mappingSource.equals(existingMapper.mappingSource()) == false) {
                String op = existingMapper == null ? "adding" : "updating";
                if (logger.isDebugEnabled() && mappingSource.compressed().length < 512) {
                    logger.debug("[{}] {} mapping [{}], source [{}]", index, op, mappingType, mappingSource.string());
                } else if (logger.isTraceEnabled()) {
                    logger.trace("[{}] {} mapping [{}], source [{}]", index, op, mappingType, mappingSource.string());
                } else {
                    logger.debug("[{}] {} mapping [{}] (source suppressed due to length, use TRACE level if needed)", index, op, mappingType);
                }
                mapperService.merge(mappingType, mappingSource, MapperService.MergeReason.MAPPING_RECOVERY, true);
                if (!mapperService.documentMapper(mappingType).mappingSource().equals(mappingSource)) {
                    logger.debug("[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}", index, mappingType, mappingSource, mapperService.documentMapper(mappingType).mappingSource());
                    requiresRefresh = true;
                }
            }
        } catch (Throwable e) {
            logger.warn("[{}] failed to add mapping [{}], source [{}]", e, index, mappingType, mappingSource);
            throw e;
        }
        return requiresRefresh;
    }


    private void applyNewOrUpdatedShards(final ClusterChangedEvent event) {
        if (!indicesService.changesAllowed()) {
            return;
        }

        RoutingTable routingTable = event.state().routingTable();
        RoutingNode routingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());

        if (routingNode == null) {
            failedShards.clear();
            return;
        }

        DiscoveryNodes nodes = event.state().nodes();

        for (final ShardRouting shardRouting : routingNode) {
            final IndexService indexService = indicesService.indexService(shardRouting.index());
            if (indexService == null) {
                // creation failed for some reasons
                assert failedShards.containsKey(shardRouting.shardId()) :
                    "index has local allocation but is not created by applyNewIndices and is not failed " + shardRouting;
                continue;
            }
            final IndexMetaData indexMetaData = event.state().metaData().index(shardRouting.index());
            assert indexMetaData != null : "index has local allocation but no meta data. " + shardRouting.index();

            final int shardId = shardRouting.id();

            if (!indexService.hasShard(shardId) && shardRouting.started()) {
                if (failedShards.containsKey(shardRouting.shardId())) {
                    if (nodes.getMasterNode() != null) {
                        String message = "master " + nodes.getMasterNode() + " marked shard as started, but shard has previous failed. resending shard failure";
                        logger.trace("[{}] re-sending failed shard [{}], reason [{}]", shardRouting.shardId(), shardRouting, message);
                        shardStateAction.shardFailed(shardRouting, shardRouting, message, null, SHARD_STATE_ACTION_LISTENER);
                    }
                } else {
                    // the master thinks we are started, but we don't have this shard at all, mark it as failed
                    sendFailShard(shardRouting, "master [" + nodes.getMasterNode() + "] marked shard as started, but shard has not been created, mark shard as failed", null);
                }
                continue;
            }

            IndexShard indexShard = indexService.getShardOrNull(shardId);
            if (indexShard != null) {
                ShardRouting currentRoutingEntry = indexShard.routingEntry();
                // if the current and global routing are initializing, but are still not the same, its a different "shard" being allocated
                // for example: a shard that recovers from one node and now needs to recover to another node,
                //              or a replica allocated and then allocating a primary because the primary failed on another node
                boolean shardHasBeenRemoved = false;
                assert currentRoutingEntry.isSameAllocation(shardRouting) :
                    "local shard has a different allocation id but wasn't cleaning by applyDeletedShards. "
                        + "cluster state: " + shardRouting + " local: " + currentRoutingEntry;
                if (shardRouting.isPeerRecovery()) {
                    RecoveryState recoveryState = indexShard.recoveryState();
                    final DiscoveryNode sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, shardRouting);
                    if (recoveryState.getSourceNode().equals(sourceNode) == false) {
                        if (recoveryTargetService.cancelRecoveriesForShard(currentRoutingEntry.shardId(), "recovery source node changed")) {
                            // getting here means that the shard was still recovering
                            logger.debug("[{}][{}] removing shard (recovery source changed), current [{}], global [{}])", shardRouting.index(), shardRouting.id(), currentRoutingEntry, shardRouting);
                            indexService.removeShard(shardRouting.id(), "removing shard (recovery source node changed)");
                            shardHasBeenRemoved = true;
                        }
                    }
                }

                if (shardHasBeenRemoved == false) {
                    try {
                        indexShard.updateRoutingEntry(shardRouting, event.state().blocks().disableStatePersistence() == false);
                        if (shardRouting.primary()) {
                            final IndexShardRoutingTable shardRoutingTable = routingTable.shardRoutingTable(shardRouting.shardId());
                            Set<String> activeIds = shardRoutingTable.activeShards().stream().map(sr -> sr.allocationId().getId()).collect(Collectors.toSet());
                            Set<String> initializingIds = shardRoutingTable.getAllInitializingShards().stream().map(sr -> sr.allocationId().getId()).collect(Collectors.toSet());
                            indexShard.updateAllocationIdsFromMaster(activeIds, initializingIds);
                        }
                    } catch (Throwable e) {
                        failAndRemoveShard(shardRouting, indexService, true, "failed updating shard routing entry", e);
                    }
                }
            }

            if (shardRouting.initializing()) {
                applyInitializingShard(event.state(), indexService, shardRouting);
            }
        }
    }

    private void applyInitializingShard(final ClusterState state, IndexService indexService, final ShardRouting shardRouting) {
        final RoutingTable routingTable = state.routingTable();
        final DiscoveryNodes nodes = state.getNodes();
        final int shardId = shardRouting.id();

        if (indexService.hasShard(shardId)) {
            IndexShard indexShard = indexService.getShard(shardId);
            if (indexShard.state() == IndexShardState.STARTED || indexShard.state() == IndexShardState.POST_RECOVERY) {
                // the master thinks we are initializing, but we are already started or on POST_RECOVERY and waiting
                // for master to confirm a shard started message (either master failover, or a cluster event before
                // we managed to tell the master we started), mark us as started
                if (logger.isTraceEnabled()) {
                    logger.trace("{} master marked shard as initializing, but shard has state [{}], resending shard started to {}",
                        indexShard.shardId(), indexShard.state(), nodes.getMasterNode());
                }
                if (nodes.getMasterNode() != null) {
                    shardStateAction.shardStarted(shardRouting,
                        "master " + nodes.getMasterNode() + " marked shard as initializing, but shard state is [" + indexShard.state() + "], mark shard as started",
                        SHARD_STATE_ACTION_LISTENER);
                }
                return;
            } else {
                if (indexShard.ignoreRecoveryAttempt()) {
                    logger.trace("ignoring recovery instruction for an existing shard {} (shard state: [{}])", indexShard.shardId(), indexShard.state());
                    return;
                }
            }
        }

        // if we're in peer recovery, try to find out the source node now so in case it fails, we will not create the index shard
        DiscoveryNode sourceNode = null;
        if (shardRouting.isPeerRecovery()) {
            sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, shardRouting);
            if (sourceNode == null) {
                logger.trace("ignoring initializing shard {} - no source node can be found.", shardRouting.shardId());
                return;
            }
        }

        // if there is no shard, create it
        if (!indexService.hasShard(shardId)) {
            if (failedShards.containsKey(shardRouting.shardId())) {
                if (nodes.getMasterNode() != null) {
                    String message = "master " + nodes.getMasterNode() + " marked shard as initializing, but shard is marked as failed, resend shard failure";
                    logger.trace("[{}] re-sending failed shard [{}], reason [{}]", shardRouting.shardId(), shardRouting, message);
                    shardStateAction.shardFailed(shardRouting, shardRouting, message, null, SHARD_STATE_ACTION_LISTENER);
                }
                return;
            }
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}] creating shard", shardRouting.index(), shardId);
                }
                IndexShard indexShard = indexService.createShard(shardRouting);
                indexShard.addShardFailureCallback(failedShardHandler);
            } catch (IndexShardAlreadyExistsException e) {
                // ignore this, the method call can happen several times
            } catch (Throwable e) {
                failAndRemoveShard(shardRouting, indexService, true, "failed to create shard", e);
                return;
            }
        }
        final IndexShard indexShard = indexService.getShard(shardId);

        if (indexShard.ignoreRecoveryAttempt()) {
            // we are already recovering (we can get to this state since the cluster event can happen several
            // times while we recover)
            logger.trace("ignoring recovery instruction for shard {} (shard state: [{}])", indexShard.shardId(), indexShard.state());
            return;
        }

        indexShard.startRecovery(nodes.getLocalNode(), sourceNode, recoveryTargetService,
            new RecoveryListener(shardRouting, indexService), repositoriesService);
    }

    /**
     * Finds the routing source node for peer recovery, return null if its not found. Note, this method expects the shard
     * routing to *require* peer recovery, use {@link ShardRouting#isPeerRecovery()} to
     * check if its needed or not.
     */
    private static DiscoveryNode findSourceNodeForPeerRecovery(ESLogger logger, RoutingTable routingTable, DiscoveryNodes nodes, ShardRouting shardRouting) {
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
                logger.trace("can't find relocation source node for shard {} because it is assigned to an unknown node [{}].", shardRouting.shardId(), shardRouting.relocatingNodeId());
            }
        } else {
            throw new IllegalStateException("trying to find source node for peer recovery when routing state means no peer recovery: " + shardRouting);
        }
        return sourceNode;
    }

    private class RecoveryListener implements RecoveryTargetService.RecoveryListener {

        private final ShardRouting shardRouting;
        private final IndexService indexService;

        private RecoveryListener(ShardRouting shardRouting, IndexService indexService) {
            this.shardRouting = shardRouting;
            this.indexService = indexService;
        }

        @Override
        public void onRecoveryDone(RecoveryState state) {
            if (state.getType() == RecoveryState.Type.SNAPSHOT) {
                restoreService.indexShardRestoreCompleted(state.getRestoreSource().snapshotId(), shardRouting.shardId());
            }
            shardStateAction.shardStarted(shardRouting, message(state), SHARD_STATE_ACTION_LISTENER);
        }

        private String message(RecoveryState state) {
            switch (state.getType()) {
                case SNAPSHOT: return "after recovery from repository";
                case STORE: return "after recovery from store";
                case PRIMARY_RELOCATION: return "after recovery (primary relocation) from node [" + state.getSourceNode() + "]";
                case REPLICA: return "after recovery (replica) from node [" + state.getSourceNode() + "]";
                default: throw new IllegalArgumentException(state.getType().name());
            }
        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            if (state.getType() == RecoveryState.Type.SNAPSHOT) {
                try {
                    if (Lucene.isCorruptionException(e.getCause())) {
                        restoreService.failRestore(state.getRestoreSource().snapshotId(), shardRouting.shardId());
                    }
                } catch (Throwable inner) {
                    e.addSuppressed(inner);
                } finally {
                    handleRecoveryFailure(indexService, shardRouting, sendShardFailure, e);
                }
            } else {
                handleRecoveryFailure(indexService, shardRouting, sendShardFailure, e);
            }
        }
    }

    private void handleRecoveryFailure(IndexService indexService, ShardRouting shardRouting, boolean sendShardFailure, Throwable failure) {
        synchronized (mutex) {
            failAndRemoveShard(shardRouting, indexService, sendShardFailure, "failed recovery", failure);
        }
    }

    private void removeIndex(Index index, String reason) {
        try {
            indicesService.removeIndex(index, reason);
        } catch (Throwable e) {
            logger.warn("failed to clean index ({})", e, reason);
        }
    }

    private void deleteIndex(Index index, String reason) {
        try {
            indicesService.deleteIndex(index, reason);
        } catch (Throwable e) {
            logger.warn("failed to delete index ({})", e, reason);
        }

    }

    private void failAndRemoveShard(ShardRouting shardRouting, @Nullable IndexService indexService, boolean sendShardFailure, String message, @Nullable Throwable failure) {
        if (indexService != null && indexService.hasShard(shardRouting.getId())) {
            // if the indexService is null we can't remove the shard, that's fine since we might have a failure
            // when the index is remove and then we already removed the index service for that shard...
            try {
                indexService.removeShard(shardRouting.getId(), message);
            } catch (ShardNotFoundException e) {
                // the node got closed on us, ignore it
            } catch (Throwable e1) {
                logger.warn("[{}][{}] failed to remove shard after failure ([{}])", e1, shardRouting.getIndexName(), shardRouting.getId(), message);
            }
        }
        if (sendShardFailure) {
            sendFailShard(shardRouting, message, failure);
        }
    }

    private void sendFailShard(ShardRouting shardRouting, String message, @Nullable Throwable failure) {
        try {
            logger.warn("[{}] marking and sending shard failed due to [{}]", failure, shardRouting.shardId(), message);
            failedShards.put(shardRouting.shardId(), shardRouting);
            shardStateAction.shardFailed(shardRouting, shardRouting, message, failure, SHARD_STATE_ACTION_LISTENER);
        } catch (Throwable e1) {
            logger.warn("[{}][{}] failed to mark shard as failed (because of [{}])", e1, shardRouting.getIndexName(), shardRouting.getId(), message);
        }
    }

    private class FailedShardHandler implements Callback<IndexShard.ShardFailure> {
        @Override
        public void handle(final IndexShard.ShardFailure shardFailure) {
            final IndexService indexService = indicesService.indexService(shardFailure.routing.shardId().getIndex());
            final ShardRouting shardRouting = shardFailure.routing;
            threadPool.generic().execute(() -> {
                synchronized (mutex) {
                    failAndRemoveShard(shardRouting, indexService, true, "shard failure, reason [" + shardFailure.reason + "]", shardFailure.cause);
                }
            });
        }
    }
}
