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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
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
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRecoveryException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.StoreRecoveryService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryStatus;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class IndicesClusterStateService extends AbstractLifecycleComponent<IndicesClusterStateService> implements ClusterStateListener {

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RecoveryTarget recoveryTarget;
    private final ShardStateAction shardStateAction;
    private final NodeIndexDeletedAction nodeIndexDeletedAction;
    private final NodeMappingRefreshAction nodeMappingRefreshAction;

    // a map of mappings type we have seen per index due to cluster state
    // we need this so we won't remove types automatically created as part of the indexing process
    private final ConcurrentMap<Tuple<String, String>, Boolean> seenMappings = ConcurrentCollections.newConcurrentMap();

    // a list of shards that failed during recovery
    // we keep track of these shards in order to prevent repeated recovery of these shards on each cluster state update
    private final ConcurrentMap<ShardId, FailedShard> failedShards = ConcurrentCollections.newConcurrentMap();

    static class FailedShard {
        public final long version;
        public final long timestamp;

        FailedShard(long version) {
            this.version = version;
            this.timestamp = System.currentTimeMillis();
        }
    }

    private final Object mutex = new Object();
    private final FailedEngineHandler failedEngineHandler = new FailedEngineHandler();

    private final boolean sendRefreshMapping;

    @Inject
    public IndicesClusterStateService(Settings settings, IndicesService indicesService, ClusterService clusterService,
                                      ThreadPool threadPool, RecoveryTarget recoveryTarget,
                                      ShardStateAction shardStateAction,
                                      NodeIndexDeletedAction nodeIndexDeletedAction,
                                      NodeMappingRefreshAction nodeMappingRefreshAction) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTarget = recoveryTarget;
        this.shardStateAction = shardStateAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
        this.nodeMappingRefreshAction = nodeMappingRefreshAction;

        this.sendRefreshMapping = this.settings.getAsBoolean("indices.cluster.send_refresh_mapping", true);
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
                    String index = indexService.index().getName();
                    for (Integer shardId : indexService.shardIds()) {
                        logger.debug("[{}][{}] removing shard (disabled block persistence)", index, shardId);
                        try {
                            indexService.removeShard(shardId, "removing shard (disabled block persistence)");
                        } catch (Throwable e) {
                            logger.warn("[{}] failed to remove shard (disabled block persistence)", e, index);
                        }
                    }
                    removeIndex(index, "cleaning index (disabled block persistence)");
                }
                return;
            }

            cleanFailedShards(event);

            applyDeletedIndices(event);
            applyNewIndices(event);
            applyMappings(event);
            applyAliases(event);
            applyNewOrUpdatedShards(event);
            applyDeletedShards(event);
            applyCleanedIndices(event);
            applySettings(event);
        }
    }

    private void applyCleanedIndices(final ClusterChangedEvent event) {
        // handle closed indices, since they are not allocated on a node once they are closed
        // so applyDeletedIndices might not take them into account
        for (IndexService indexService : indicesService) {
            String index = indexService.index().getName();
            IndexMetaData indexMetaData = event.state().metaData().index(index);
            if (indexMetaData != null && indexMetaData.state() == IndexMetaData.State.CLOSE) {
                for (Integer shardId : indexService.shardIds()) {
                    logger.debug("[{}][{}] removing shard (index is closed)", index, shardId);
                    try {
                        indexService.removeShard(shardId, "removing shard (index is closed)");
                    } catch (Throwable e) {
                        logger.warn("[{}] failed to remove shard (index is closed)", e, index);
                    }
                }
            }
        }
        for (IndexService indexService : indicesService) {
            String index = indexService.index().getName();
            if (indexService.shardIds().isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] cleaning index (no shards allocated)", index);
                }
                // clean the index
                removeIndex(index, "removing index (no shards allocated)");
            }
        }
    }

    private void applyDeletedIndices(final ClusterChangedEvent event) {
        final ClusterState previousState = event.previousState();
        final String localNodeId = event.state().nodes().localNodeId();
        assert localNodeId != null;

        for (IndexService indexService : indicesService) {
            IndexMetaData indexMetaData = event.state().metaData().index(indexService.index().name());
            if (indexMetaData != null) {
                if (!indexMetaData.isSameUUID(indexService.indexUUID())) {
                    logger.debug("[{}] mismatch on index UUIDs between cluster state and local state, cleaning the index so it will be recreated", indexMetaData.index());
                    deleteIndex(indexMetaData.index(), "mismatch on index UUIDs between cluster state and local state, cleaning the index so it will be recreated");
                }
            }
        }

        for (String index : event.indicesDeleted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] cleaning index, no longer part of the metadata", index);
            }
            final Settings indexSettings;
            final IndexService idxService = indicesService.indexService(index);
            if (idxService != null) {
                indexSettings = idxService.getIndexSettings();
                deleteIndex(index, "index no longer part of the metadata");
            } else {
                final IndexMetaData metaData = previousState.metaData().index(index);
                assert metaData != null;
                indexSettings = metaData.settings();
                indicesService.deleteClosedIndex("closed index no longer part of the metadata", metaData, event.state());
            }
            try {
                nodeIndexDeletedAction.nodeIndexDeleted(event.state(), index, indexSettings, localNodeId);
            } catch (Throwable e) {
                logger.debug("failed to send to master index {} deleted event", e, index);
            }
        }


    }

    private void applyDeletedShards(final ClusterChangedEvent event) {
        RoutingNodes.RoutingNodeIterator routingNode = event.state().getRoutingNodes().routingNodeIter(event.state().nodes().localNodeId());
        if (routingNode == null) {
            return;
        }
        IntHashSet newShardIds = new IntHashSet();
        for (IndexService indexService : indicesService) {
            String index = indexService.index().name();
            IndexMetaData indexMetaData = event.state().metaData().index(index);
            if (indexMetaData == null) {
                continue;
            }
            // now, go over and delete shards that needs to get deleted
            newShardIds.clear();
            for (ShardRouting shard : routingNode) {
                if (shard.index().equals(index)) {
                    newShardIds.add(shard.id());
                }
            }
            for (Integer existingShardId : indexService.shardIds()) {
                if (!newShardIds.contains(existingShardId)) {
                    if (indexMetaData.state() == IndexMetaData.State.CLOSE) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[{}][{}] removing shard (index is closed)", index, existingShardId);
                        }
                        indexService.removeShard(existingShardId, "removing shard (index is closed)");
                    } else {
                        // we can just remove the shard, without cleaning it locally, since we will clean it
                        // when all shards are allocated in the IndicesStore
                        if (logger.isDebugEnabled()) {
                            logger.debug("[{}][{}] removing shard (not allocated)", index, existingShardId);
                        }
                        indexService.removeShard(existingShardId, "removing shard (not allocated)");
                    }
                }
            }
        }
    }

    private void applyNewIndices(final ClusterChangedEvent event) {
        // we only create indices for shards that are allocated
        RoutingNodes.RoutingNodeIterator routingNode = event.state().getRoutingNodes().routingNodeIter(event.state().nodes().localNodeId());
        if (routingNode == null) {
            return;
        }
        for (ShardRouting shard : routingNode) {
            if (!indicesService.hasIndex(shard.index())) {
                final IndexMetaData indexMetaData = event.state().metaData().index(shard.index());
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] creating index", indexMetaData.index());
                }
                try {
                    indicesService.createIndex(indexMetaData.index(), indexMetaData.settings(), event.state().nodes().localNode().id());
                } catch (Throwable e) {
                    sendFailShard(shard, indexMetaData.getIndexUUID(), "failed to create index", e);
                }
            }
        }
    }

    private void applySettings(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        for (IndexMetaData indexMetaData : event.state().metaData()) {
            if (!indicesService.hasIndex(indexMetaData.index())) {
                // we only create / update here
                continue;
            }
            // if the index meta data didn't change, no need check for refreshed settings
            if (!event.indexMetaDataChanged(indexMetaData)) {
                continue;
            }
            String index = indexMetaData.index();
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                // already deleted on us, ignore it
                continue;
            }
            IndexSettingsService indexSettingsService = indexService.injector().getInstance(IndexSettingsService.class);
            indexSettingsService.refreshSettings(indexMetaData.settings());
        }
    }


    private void applyMappings(ClusterChangedEvent event) {
        // go over and update mappings
        for (IndexMetaData indexMetaData : event.state().metaData()) {
            if (!indicesService.hasIndex(indexMetaData.index())) {
                // we only create / update here
                continue;
            }
            List<String> typesToRefresh = new ArrayList<>();
            String index = indexMetaData.index();
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                // got deleted on us, ignore (closing the node)
                return;
            }
            try {
                MapperService mapperService = indexService.mapperService();
                // first, go over and update the _default_ mapping (if exists)
                if (indexMetaData.mappings().containsKey(MapperService.DEFAULT_MAPPING)) {
                    boolean requireRefresh = processMapping(index, mapperService, MapperService.DEFAULT_MAPPING, indexMetaData.mapping(MapperService.DEFAULT_MAPPING).source());
                    if (requireRefresh) {
                        typesToRefresh.add(MapperService.DEFAULT_MAPPING);
                    }
                }

                // go over and add the relevant mappings (or update them)
                for (ObjectCursor<MappingMetaData> cursor : indexMetaData.mappings().values()) {
                    MappingMetaData mappingMd = cursor.value;
                    String mappingType = mappingMd.type();
                    CompressedXContent mappingSource = mappingMd.source();
                    if (mappingType.equals(MapperService.DEFAULT_MAPPING)) { // we processed _default_ first
                        continue;
                    }
                    boolean requireRefresh = processMapping(index, mapperService, mappingType, mappingSource);
                    if (requireRefresh) {
                        typesToRefresh.add(mappingType);
                    }
                }
                if (!typesToRefresh.isEmpty() && sendRefreshMapping) {
                    nodeMappingRefreshAction.nodeMappingRefresh(event.state(),
                            new NodeMappingRefreshAction.NodeMappingRefreshRequest(index, indexMetaData.indexUUID(),
                                    typesToRefresh.toArray(new String[typesToRefresh.size()]), event.state().nodes().localNodeId())
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
        if (!seenMappings.containsKey(new Tuple<>(index, mappingType))) {
            seenMappings.put(new Tuple<>(index, mappingType), true);
        }

        // refresh mapping can happen for 2 reasons. The first is less urgent, and happens when the mapping on this
        // node is ahead of what there is in the cluster state (yet an update-mapping has been sent to it already,
        // it just hasn't been processed yet and published). Eventually, the mappings will converge, and the refresh
        // mapping sent is more of a safe keeping (assuming the update mapping failed to reach the master, ...)
        // the second case is where the parsing/merging of the mapping from the metadata doesn't result in the same
        // mapping, in this case, we send to the master to refresh its own version of the mappings (to conform with the
        // merge version of it, which it does when refreshing the mappings), and warn log it.
        boolean requiresRefresh = false;
        try {
            if (!mapperService.hasMapping(mappingType)) {
                if (logger.isDebugEnabled() && mappingSource.compressed().length < 512) {
                    logger.debug("[{}] adding mapping [{}], source [{}]", index, mappingType, mappingSource.string());
                } else if (logger.isTraceEnabled()) {
                    logger.trace("[{}] adding mapping [{}], source [{}]", index, mappingType, mappingSource.string());
                } else {
                    logger.debug("[{}] adding mapping [{}] (source suppressed due to length, use TRACE level if needed)", index, mappingType);
                }
                // we don't apply default, since it has been applied when the mappings were parsed initially
                mapperService.merge(mappingType, mappingSource, false, true);
                if (!mapperService.documentMapper(mappingType).mappingSource().equals(mappingSource)) {
                    logger.debug("[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}", index, mappingType, mappingSource, mapperService.documentMapper(mappingType).mappingSource());
                    requiresRefresh = true;
                }
            } else {
                DocumentMapper existingMapper = mapperService.documentMapper(mappingType);
                if (!mappingSource.equals(existingMapper.mappingSource())) {
                    // mapping changed, update it
                    if (logger.isDebugEnabled() && mappingSource.compressed().length < 512) {
                        logger.debug("[{}] updating mapping [{}], source [{}]", index, mappingType, mappingSource.string());
                    } else if (logger.isTraceEnabled()) {
                        logger.trace("[{}] updating mapping [{}], source [{}]", index, mappingType, mappingSource.string());
                    } else {
                        logger.debug("[{}] updating mapping [{}] (source suppressed due to length, use TRACE level if needed)", index, mappingType);
                    }
                    // we don't apply default, since it has been applied when the mappings were parsed initially
                    mapperService.merge(mappingType, mappingSource, false, true);
                    if (!mapperService.documentMapper(mappingType).mappingSource().equals(mappingSource)) {
                        requiresRefresh = true;
                        logger.debug("[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}", index, mappingType, mappingSource, mapperService.documentMapper(mappingType).mappingSource());
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn("[{}] failed to add mapping [{}], source [{}]", e, index, mappingType, mappingSource);
            throw e;
        }
        return requiresRefresh;
    }

    private boolean aliasesChanged(ClusterChangedEvent event) {
        return !event.state().metaData().equalsAliases(event.previousState().metaData()) ||
                !event.state().routingTable().equals(event.previousState().routingTable());
    }

    private void applyAliases(ClusterChangedEvent event) {
        // check if aliases changed
        if (aliasesChanged(event)) {
            // go over and update aliases
            for (IndexMetaData indexMetaData : event.state().metaData()) {
                String index = indexMetaData.index();
                IndexService indexService = indicesService.indexService(index);
                if (indexService == null) {
                    // we only create / update here
                    continue;
                }
                IndexAliasesService indexAliasesService = indexService.aliasesService();
                indexAliasesService.setAliases(indexMetaData.getAliases());
            }
        }
    }

    private void applyNewOrUpdatedShards(final ClusterChangedEvent event) {
        if (!indicesService.changesAllowed()) {
            return;
        }

        RoutingTable routingTable = event.state().routingTable();
        RoutingNodes.RoutingNodeIterator routingNode = event.state().getRoutingNodes().routingNodeIter(event.state().nodes().localNodeId());

        if (routingNode == null) {
            failedShards.clear();
            return;
        }
        DiscoveryNodes nodes = event.state().nodes();

        for (final ShardRouting shardRouting : routingNode) {
            final IndexService indexService = indicesService.indexService(shardRouting.index());
            if (indexService == null) {
                // got deleted on us, ignore
                continue;
            }
            final IndexMetaData indexMetaData = event.state().metaData().index(shardRouting.index());
            if (indexMetaData == null) {
                // the index got deleted on the metadata, we will clean it later in the apply deleted method call
                continue;
            }

            final int shardId = shardRouting.id();

            if (!indexService.hasShard(shardId) && shardRouting.started()) {
                if (failedShards.containsKey(shardRouting.shardId())) {
                    if (nodes.masterNode() != null) {
                        shardStateAction.resendShardFailed(shardRouting, indexMetaData.getIndexUUID(), nodes.masterNode(),
                                "master " + nodes.masterNode() + " marked shard as started, but shard has previous failed. resending shard failure.", null);
                    }
                } else {
                    // the master thinks we are started, but we don't have this shard at all, mark it as failed
                    sendFailShard(shardRouting, indexMetaData.getIndexUUID(), "master [" + nodes.masterNode() + "] marked shard as started, but shard has not been created, mark shard as failed", null);
                }
                continue;
            }

            IndexShard indexShard = indexService.shard(shardId);
            if (indexShard != null) {
                ShardRouting currentRoutingEntry = indexShard.routingEntry();
                // if the current and global routing are initializing, but are still not the same, its a different "shard" being allocated
                // for example: a shard that recovers from one node and now needs to recover to another node,
                //              or a replica allocated and then allocating a primary because the primary failed on another node
                boolean shardHasBeenRemoved = false;
                if (currentRoutingEntry.isSameAllocation(shardRouting) == false) {
                    logger.debug("[{}][{}] removing shard (different instance of it allocated on this node, current [{}], global [{}])", shardRouting.index(), shardRouting.id(), currentRoutingEntry, shardRouting);
                    // closing the shard will also cancel any ongoing recovery.
                    indexService.removeShard(shardRouting.id(), "removing shard (different instance of it allocated on this node)");
                    shardHasBeenRemoved = true;
                } else if (isPeerRecovery(shardRouting)) {
                    final DiscoveryNode sourceNode = findSourceNodeForPeerRecovery(routingTable, nodes, shardRouting);
                    // check if there is an existing recovery going, and if so, and the source node is not the same, cancel the recovery to restart it
                    if (recoveryTarget.cancelRecoveriesForShard(indexShard.shardId(), "recovery source node changed", status -> !status.sourceNode().equals(sourceNode))) {
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
                    indexShard.updateRoutingEntry(shardRouting, event.state().blocks().disableStatePersistence() == false);
                }
            }

            if (shardRouting.initializing()) {
                applyInitializingShard(event.state(), indexMetaData, shardRouting);
            }
        }
    }

    private void cleanFailedShards(final ClusterChangedEvent event) {
        RoutingTable routingTable = event.state().routingTable();
        RoutingNodes.RoutingNodeIterator routingNode = event.state().getRoutingNodes().routingNodeIter(event.state().nodes().localNodeId());
        if (routingNode == null) {
            failedShards.clear();
            return;
        }
        DiscoveryNodes nodes = event.state().nodes();
        long now = System.currentTimeMillis();
        String localNodeId = nodes.localNodeId();
        Iterator<Map.Entry<ShardId, FailedShard>> iterator = failedShards.entrySet().iterator();
        shards:
        while (iterator.hasNext()) {
            Map.Entry<ShardId, FailedShard> entry = iterator.next();
            FailedShard failedShard = entry.getValue();
            IndexRoutingTable indexRoutingTable = routingTable.index(entry.getKey().getIndex());
            if (indexRoutingTable != null) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(entry.getKey().id());
                if (shardRoutingTable != null) {
                    for (ShardRouting shardRouting : shardRoutingTable.assignedShards()) {
                        if (localNodeId.equals(shardRouting.currentNodeId())) {
                            // we have a timeout here just to make sure we don't have dangled failed shards for some reason
                            // its just another safely layer
                            if (shardRouting.version() == failedShard.version && ((now - failedShard.timestamp) < TimeValue.timeValueMinutes(60).millis())) {
                                // It's the same failed shard - keep it if it hasn't timed out
                                continue shards;
                            } else {
                                // Different version or expired, remove it
                                break;
                            }
                        }
                    }
                }
            }
            iterator.remove();
        }
    }

    private void applyInitializingShard(final ClusterState state, final IndexMetaData indexMetaData, final ShardRouting shardRouting) {
        final IndexService indexService = indicesService.indexService(shardRouting.index());
        if (indexService == null) {
            // got deleted on us, ignore
            return;
        }
        final RoutingTable routingTable = state.routingTable();
        final DiscoveryNodes nodes = state.getNodes();
        final int shardId = shardRouting.id();

        if (indexService.hasShard(shardId)) {
            IndexShard indexShard = indexService.shardSafe(shardId);
            if (indexShard.state() == IndexShardState.STARTED || indexShard.state() == IndexShardState.POST_RECOVERY) {
                // the master thinks we are initializing, but we are already started or on POST_RECOVERY and waiting
                // for master to confirm a shard started message (either master failover, or a cluster event before
                // we managed to tell the master we started), mark us as started
                if (logger.isTraceEnabled()) {
                    logger.trace("{} master marked shard as initializing, but shard has state [{}], resending shard started to {}",
                            indexShard.shardId(), indexShard.state(), nodes.masterNode());
                }
                if (nodes.masterNode() != null) {
                    shardStateAction.shardStarted(shardRouting, indexMetaData.getIndexUUID(),
                            "master " + nodes.masterNode() + " marked shard as initializing, but shard state is [" + indexShard.state() + "], mark shard as started",
                            nodes.masterNode());
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
                    shardStateAction.resendShardFailed(shardRouting, indexMetaData.getIndexUUID(), nodes.masterNode(),
                            "master " + nodes.masterNode() + " marked shard as initializing, but shard is marked as failed, resend shard failure", null);
                }
                return;
            }
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}] creating shard", shardRouting.index(), shardId);
                }
                IndexShard indexShard = indexService.createShard(shardId, shardRouting);
                indexShard.updateRoutingEntry(shardRouting, state.blocks().disableStatePersistence() == false);
                indexShard.addFailedEngineListener(failedEngineHandler);
            } catch (IndexShardAlreadyExistsException e) {
                // ignore this, the method call can happen several times
            } catch (Throwable e) {
                failAndRemoveShard(shardRouting, indexService, true, "failed to create shard", e);
                return;
            }
        }
        final IndexShard indexShard = indexService.shardSafe(shardId);

        if (indexShard.ignoreRecoveryAttempt()) {
            // we are already recovering (we can get to this state since the cluster event can happen several
            // times while we recover)
            logger.trace("ignoring recovery instruction for shard {} (shard state: [{}])", indexShard.shardId(), indexShard.state());
            return;
        }

        if (isPeerRecovery(shardRouting)) {
            try {

                assert sourceNode != null : "peer recovery started but sourceNode is null";

                // we don't mark this one as relocated at the end.
                // For primaries: requests in any case are routed to both when its relocating and that way we handle
                //    the edge case where its mark as relocated, and we might need to roll it back...
                // For replicas: we are recovering a backup from a primary
                RecoveryState.Type type = shardRouting.primary() ? RecoveryState.Type.RELOCATION : RecoveryState.Type.REPLICA;
                recoveryTarget.startRecovery(indexShard, type, sourceNode, new PeerRecoveryListener(shardRouting, indexService, indexMetaData));
            } catch (Throwable e) {
                indexShard.failShard("corrupted preexisting index", e);
                handleRecoveryFailure(indexService, shardRouting, true, e);
            }
        } else {
            indexService.shard(shardId).recoverFromStore(shardRouting, new StoreRecoveryService.RecoveryListener() {
                @Override
                public void onRecoveryDone() {
                    shardStateAction.shardStarted(shardRouting, indexMetaData.getIndexUUID(), "after recovery from store");
                }

                @Override
                public void onIgnoreRecovery(String reason) {
                }

                @Override
                public void onRecoveryFailed(IndexShardRecoveryException e) {
                    handleRecoveryFailure(indexService, shardRouting, true, e);
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

    private class PeerRecoveryListener implements RecoveryTarget.RecoveryListener {

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
            shardStateAction.shardStarted(shardRouting, indexMetaData.getIndexUUID(), "after recovery (replica) from node [" + state.getSourceNode() + "]");
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

    private void removeIndex(String index, String reason) {
        try {
            indicesService.removeIndex(index, reason);
        } catch (Throwable e) {
            logger.warn("failed to clean index ({})", e, reason);
        }
        clearSeenMappings(index);

    }

    private void clearSeenMappings(String index) {
        // clear seen mappings as well
        for (Tuple<String, String> tuple : seenMappings.keySet()) {
            if (tuple.v1().equals(index)) {
                seenMappings.remove(tuple);
            }
        }
    }

    private void deleteIndex(String index, String reason) {
        try {
            indicesService.deleteIndex(index, reason);
        } catch (Throwable e) {
            logger.warn("failed to delete index ({})", e, reason);
        }
        // clear seen mappings as well
        clearSeenMappings(index);

    }

    private void failAndRemoveShard(ShardRouting shardRouting, IndexService indexService, boolean sendShardFailure, String message, @Nullable Throwable failure) {
        if (indexService.hasShard(shardRouting.getId())) {
            try {
                indexService.removeShard(shardRouting.getId(), message);
            } catch (ShardNotFoundException e) {
                // the node got closed on us, ignore it
            } catch (Throwable e1) {
                logger.warn("[{}][{}] failed to remove shard after failure ([{}])", e1, shardRouting.getIndex(), shardRouting.getId(), message);
            }
        }
        if (sendShardFailure) {
            sendFailShard(shardRouting, indexService.indexUUID(), message, failure);
        }
    }

    private void sendFailShard(ShardRouting shardRouting, String indexUUID, String message, @Nullable Throwable failure) {
        try {
            logger.warn("[{}] marking and sending shard failed due to [{}]", failure, shardRouting.shardId(), message);
            failedShards.put(shardRouting.shardId(), new FailedShard(shardRouting.version()));
            shardStateAction.shardFailed(shardRouting, indexUUID, message, failure);
        } catch (Throwable e1) {
            logger.warn("[{}][{}] failed to mark shard as failed (because of [{}])", e1, shardRouting.getIndex(), shardRouting.getId(), message);
        }
    }

    private class FailedEngineHandler implements Engine.FailedEngineListener {
        @Override
        public void onFailedEngine(final ShardId shardId, final String reason, final @Nullable Throwable failure) {
            ShardRouting shardRouting = null;
            final IndexService indexService = indicesService.indexService(shardId.index().name());
            if (indexService != null) {
                IndexShard indexShard = indexService.shard(shardId.id());
                if (indexShard != null) {
                    shardRouting = indexShard.routingEntry();
                }
            }
            if (shardRouting == null) {
                logger.warn("[{}][{}] engine failed, but can't find index shard. failure reason: [{}]", failure,
                        shardId.index().name(), shardId.id(), reason);
                return;
            }
            final ShardRouting fShardRouting = shardRouting;
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    synchronized (mutex) {
                        failAndRemoveShard(fShardRouting, indexService, true, "engine failure, reason [" + reason + "]", failure);
                    }
                }
            });
        }
    }
}
