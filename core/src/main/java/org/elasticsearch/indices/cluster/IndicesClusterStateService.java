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
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.snapshots.IndexShardRepository;
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
                                      RecoverySource recoverySource, NodeServicesProvider nodeServicesProvider) {
        super(settings);
        this.buildInIndexListener = Arrays.asList(recoverySource, recoveryTargetService, searchService, syncedFlushService);
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
        RoutingNodes.RoutingNodeIterator routingNode = event.state().getRoutingNodes().routingNodeIter(event.state().nodes().getLocalNodeId());
        if (routingNode == null) {
            failedShards.clear();
            return;
        }
        RoutingTable routingTable = event.state().routingTable();
        for (Iterator<Map.Entry<ShardId, ShardRouting>> iterator = failedShards.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<ShardId, ShardRouting> entry = iterator.next();
            ShardId failedShardId = entry.getKey();
            ShardRouting failedShardRouting = entry.getValue();
            IndexRoutingTable indexRoutingTable = routingTable.index(failedShardId.getIndex());
            if (indexRoutingTable == null) {
                iterator.remove();
                continue;
            }
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(failedShardId.id());
            if (shardRoutingTable == null) {
                iterator.remove();
                continue;
            }
            if (shardRoutingTable.assignedShards().stream().noneMatch(shr -> shr.isSameAllocation(failedShardRouting))) {
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
            } else {
                final IndexMetaData metaData = previousState.metaData().getIndexSafe(index);
                indexSettings = new IndexSettings(metaData, settings);
                indicesService.deleteClosedIndex("closed index no longer part of the metadata", metaData, event.state());
            }
            try {
                nodeIndexDeletedAction.nodeIndexDeleted(event.state(), index, indexSettings, localNodeId);
            } catch (Throwable e) {
                logger.debug("failed to send to master index {} deleted event", e, index);
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
        RoutingNodes.RoutingNodeIterator routingNode = event.state().getRoutingNodes().routingNodeIter(event.state().nodes().getLocalNodeId());
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
        RoutingNodes.RoutingNodeIterator routingNode = event.state().getRoutingNodes().routingNodeIter(event.state().nodes().getLocalNodeId());
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
                    indicesService.createIndex(nodeServicesProvider, indexMetaData, buildInIndexListener);
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
        RoutingNodes.RoutingNodeIterator routingNode = event.state().getRoutingNodes().routingNodeIter(event.state().nodes().getLocalNodeId());

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
                    if (nodes.masterNode() != null) {
                        String message = "master " + nodes.masterNode() + " marked shard as started, but shard has previous failed. resending shard failure";
                        logger.trace("[{}] re-sending failed shard [{}], reason [{}]", shardRouting.shardId(), shardRouting, message);
                        shardStateAction.shardFailed(shardRouting, shardRouting, message, null, SHARD_STATE_ACTION_LISTENER);
                    }
                } else {
                    // the master thinks we are started, but we don't have this shard at all, mark it as failed
                    sendFailShard(shardRouting, "master [" + nodes.masterNode() + "] marked shard as started, but shard has not been created, mark shard as failed", null);
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
                if (isPeerRecovery(shardRouting)) {
                    final DiscoveryNode sourceNode = findSourceNodeForPeerRecovery(routingTable, nodes, shardRouting);
                    // check if there is an existing recovery going, and if so, and the source node is not the same, cancel the recovery to restart it
                    if (recoveryTargetService.cancelRecoveriesForShard(indexShard.shardId(), "recovery source node changed", status -> !status.sourceNode().equals(sourceNode))) {
                        logger.debug("[{}][{}] removing shard (recovery source changed), current [{}], global [{}])", shardRouting.index(), shardRouting.id(), currentRoutingEntry, shardRouting);
                        // closing the shard will also cancel any ongoing recovery.
                        indexService.removeShard(shardRouting.id(), "removing shard (recovery source node changed)");
                        shardHasBeenRemoved = true;
                    }
                }

                if (shardHasBeenRemoved == false) {
                    // shadow replicas do not support primary promotion. The master would reinitialize the shard, giving it a new allocation, meaning we should be there.
                    assert (shardRouting.primary() && currentRoutingEntry.primary() == false) == false || indexShard.allowsPrimaryPromotion() :
                        "shard for doesn't support primary promotion but master promoted it with changing allocation. New routing " + shardRouting + ", current routing " + currentRoutingEntry;
                    try {
                        indexShard.updateRoutingEntry(shardRouting, event.state().blocks().disableStatePersistence() == false);
                    } catch (Throwable e) {
                        failAndRemoveShard(shardRouting, indexService, true, "failed updating shard routing entry", e);
                    }
                }
            }

            if (shardRouting.initializing()) {
                applyInitializingShard(event.state(), indexMetaData, indexService, shardRouting);
            }
        }
    }

    private void applyInitializingShard(final ClusterState state, final IndexMetaData indexMetaData, IndexService indexService, final ShardRouting shardRouting) {
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
                        indexShard.shardId(), indexShard.state(), nodes.masterNode());
                }
                if (nodes.masterNode() != null) {
                    shardStateAction.shardStarted(shardRouting,
                        "master " + nodes.masterNode() + " marked shard as initializing, but shard state is [" + indexShard.state() + "], mark shard as started",
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
        if (isPeerRecovery(shardRouting)) {
            sourceNode = findSourceNodeForPeerRecovery(routingTable, nodes, shardRouting);
            if (sourceNode == null) {
                logger.trace("ignoring initializing shard {} - no source node can be found.", shardRouting.shardId());
                return;
            }
        }

        // if there is no shard, create it
        if (!indexService.hasShard(shardId)) {
            if (failedShards.containsKey(shardRouting.shardId())) {
                if (nodes.masterNode() != null) {
                    String message = "master " + nodes.masterNode() + " marked shard as initializing, but shard is marked as failed, resend shard failure";
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

        final RestoreSource restoreSource = shardRouting.restoreSource();

        if (isPeerRecovery(shardRouting)) {
            try {

                assert sourceNode != null : "peer recovery started but sourceNode is null";

                // we don't mark this one as relocated at the end.
                // For primaries: requests in any case are routed to both when its relocating and that way we handle
                //    the edge case where its mark as relocated, and we might need to roll it back...
                // For replicas: we are recovering a backup from a primary
                RecoveryState.Type type = shardRouting.primary() ? RecoveryState.Type.PRIMARY_RELOCATION : RecoveryState.Type.REPLICA;
                RecoveryState recoveryState = new RecoveryState(indexShard.shardId(), shardRouting.primary(), type, sourceNode, nodes.getLocalNode());
                indexShard.markAsRecovering("from " + sourceNode, recoveryState);
                recoveryTargetService.startRecovery(indexShard, type, sourceNode, new PeerRecoveryListener(shardRouting, indexService, indexMetaData));
            } catch (Throwable e) {
                indexShard.failShard("corrupted preexisting index", e);
                handleRecoveryFailure(indexService, shardRouting, true, e);
            }
        } else if (restoreSource == null) {
            assert indexShard.routingEntry().equals(shardRouting); // should have already be done before
            // recover from filesystem store
            final RecoveryState recoveryState = new RecoveryState(indexShard.shardId(), shardRouting.primary(),
                RecoveryState.Type.STORE,
                nodes.getLocalNode(), nodes.getLocalNode());
            indexShard.markAsRecovering("from store", recoveryState); // mark the shard as recovering on the cluster state thread
            threadPool.generic().execute(() -> {
                try {
                    if (indexShard.recoverFromStore(nodes.getLocalNode())) {
                        shardStateAction.shardStarted(shardRouting, "after recovery from store", SHARD_STATE_ACTION_LISTENER);
                    }
                } catch (Throwable t) {
                    handleRecoveryFailure(indexService, shardRouting, true, t);
                }

            });
        } else {
            // recover from a restore
            final RecoveryState recoveryState = new RecoveryState(indexShard.shardId(), shardRouting.primary(),
                RecoveryState.Type.SNAPSHOT, shardRouting.restoreSource(), nodes.getLocalNode());
            indexShard.markAsRecovering("from snapshot", recoveryState); // mark the shard as recovering on the cluster state thread
            threadPool.generic().execute(() -> {
                final ShardId sId = indexShard.shardId();
                try {
                    final IndexShardRepository indexShardRepository = repositoriesService.indexShardRepository(restoreSource.snapshotId().getRepository());
                    if (indexShard.restoreFromRepository(indexShardRepository, nodes.getLocalNode())) {
                        restoreService.indexShardRestoreCompleted(restoreSource.snapshotId(), sId);
                        shardStateAction.shardStarted(shardRouting, "after recovery from repository", SHARD_STATE_ACTION_LISTENER);
                    }
                } catch (Throwable first) {
                    try {
                        if (Lucene.isCorruptionException(first)) {
                            restoreService.failRestore(restoreSource.snapshotId(), sId);
                        }
                    } catch (Throwable second) {
                        first.addSuppressed(second);
                    } finally {
                        handleRecoveryFailure(indexService, shardRouting, true, first);
                    }
                }
            });
        }
    }

    /**
     * Finds the routing source node for peer recovery, return null if its not found. Note, this method expects the shard
     * routing to *require* peer recovery, use {@link #isPeerRecovery(org.elasticsearch.cluster.routing.ShardRouting)} to
     * check if its needed or not.
     */
    private DiscoveryNode findSourceNodeForPeerRecovery(RoutingTable routingTable, DiscoveryNodes nodes, ShardRouting shardRouting) {
        DiscoveryNode sourceNode = null;
        if (!shardRouting.primary()) {
            IndexShardRoutingTable shardRoutingTable = routingTable.index(shardRouting.index()).shard(shardRouting.id());
            for (ShardRouting entry : shardRoutingTable) {
                if (entry.primary() && entry.active()) {
                    // only recover from started primary, if we can't find one, we will do it next round
                    sourceNode = nodes.get(entry.currentNodeId());
                    if (sourceNode == null) {
                        logger.trace("can't find replica source node because primary shard {} is assigned to an unknown node.", entry);
                        return null;
                    }
                    break;
                }
            }

            if (sourceNode == null) {
                logger.trace("can't find replica source node for {} because a primary shard can not be found.", shardRouting.shardId());
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

    private boolean isPeerRecovery(ShardRouting shardRouting) {
        return !shardRouting.primary() || shardRouting.relocatingNodeId() != null;
    }

    private class PeerRecoveryListener implements RecoveryTargetService.RecoveryListener {

        private final ShardRouting shardRouting;
        private final IndexService indexService;
        private final IndexMetaData indexMetaData;

        private PeerRecoveryListener(ShardRouting shardRouting, IndexService indexService, IndexMetaData indexMetaData) {
            this.shardRouting = shardRouting;
            this.indexService = indexService;
            this.indexMetaData = indexMetaData;
        }

        @Override
        public void onRecoveryDone(RecoveryState state) {
            shardStateAction.shardStarted(shardRouting, "after recovery (replica) from node [" + state.getSourceNode() + "]", SHARD_STATE_ACTION_LISTENER);
        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            handleRecoveryFailure(indexService, shardRouting, sendShardFailure, e);
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
