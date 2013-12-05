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

package org.elasticsearch.indices.cluster;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeIndexCreatedAction;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.aliases.IndexAlias;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.gateway.IndexShardGatewayRecoveryException;
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryStatus;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.recovery.StartRecoveryRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.ExceptionsHelper.detailedMessage;

/**
 *
 */
public class IndicesClusterStateService extends AbstractLifecycleComponent<IndicesClusterStateService> implements ClusterStateListener {

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RecoveryTarget recoveryTarget;
    private final ShardStateAction shardStateAction;
    private final NodeIndexCreatedAction nodeIndexCreatedAction;
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
                                      NodeIndexCreatedAction nodeIndexCreatedAction, NodeIndexDeletedAction nodeIndexDeletedAction,
                                      NodeMappingRefreshAction nodeMappingRefreshAction) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTarget = recoveryTarget;
        this.shardStateAction = shardStateAction;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
        this.nodeMappingRefreshAction = nodeMappingRefreshAction;

        this.sendRefreshMapping = componentSettings.getAsBoolean("send_refresh_mapping", true);
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        clusterService.addFirst(this);
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        clusterService.remove(this);
    }

    @Override
    protected void doClose() throws ElasticSearchException {
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
                for (final String index : indicesService.indices()) {
                    IndexService indexService = indicesService.indexService(index);
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
            cleanMismatchedIndexUUIDs(event);
            applyNewIndices(event);
            applyMappings(event);
            applyAliases(event);
            applyNewOrUpdatedShards(event);
            applyDeletedIndices(event);
            applyDeletedShards(event);
            applyCleanedIndices(event);
            applySettings(event);
            sendIndexLifecycleEvents(event);
        }
    }

    private void sendIndexLifecycleEvents(final ClusterChangedEvent event) {
        String localNodeId = event.state().nodes().localNodeId();
        assert localNodeId != null;
        for (String index : event.indicesCreated()) {
            try {
                nodeIndexCreatedAction.nodeIndexCreated(event.state(), index, localNodeId);
            } catch (Throwable e) {
                logger.debug("failed to send to master index {} created event", e, index);
            }
        }
        for (String index : event.indicesDeleted()) {
            try {
                nodeIndexDeletedAction.nodeIndexDeleted(event.state(), index, localNodeId);
            } catch (Throwable e) {
                logger.debug("failed to send to master index {} deleted event", e, index);
            }
        }
    }

    private void cleanMismatchedIndexUUIDs(final ClusterChangedEvent event) {
        for (IndexService indexService : indicesService) {
            IndexMetaData indexMetaData = event.state().metaData().index(indexService.index().name());
            if (indexMetaData == null) {
                // got deleted on us, will be deleted later
                continue;
            }
            if (!indexMetaData.isSameUUID(indexService.indexUUID())) {
                logger.debug("[{}] mismatch on index UUIDs between cluster state and local state, cleaning the index so it will be recreated", indexMetaData.index());
                removeIndex(indexMetaData.index(), "mismatch on index UUIDs between cluster state and local state, cleaning the index so it will be recreated");
            }
        }
    }

    private void applyCleanedIndices(final ClusterChangedEvent event) {
        // handle closed indices, since they are not allocated on a node once they are closed
        // so applyDeletedIndices might not take them into account
        for (final String index : indicesService.indices()) {
            IndexMetaData indexMetaData = event.state().metaData().index(index);
            if (indexMetaData != null && indexMetaData.state() == IndexMetaData.State.CLOSE) {
                IndexService indexService = indicesService.indexService(index);
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
        for (final String index : indicesService.indices()) {
            if (indicesService.indexService(index).shardIds().isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] cleaning index (no shards allocated)", index);
                }
                // clean the index
                removeIndex(index, "removing index (no shards allocated)");
            }
        }
    }

    private void applyDeletedIndices(final ClusterChangedEvent event) {
        for (final String index : indicesService.indices()) {
            if (!event.state().metaData().hasIndex(index)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] cleaning index, no longer part of the metadata", index);
                }
                removeIndex(index, "index no longer part of the metadata");
            }
        }
    }

    private void applyDeletedShards(final ClusterChangedEvent event) {
        RoutingNode routingNode = event.state().readOnlyRoutingNodes().nodesToShards().get(event.state().nodes().localNodeId());
        if (routingNode == null) {
            return;
        }
        IntOpenHashSet newShardIds = new IntOpenHashSet();
        for (IndexService indexService : indicesService) {
            String index = indexService.index().name();
            IndexMetaData indexMetaData = event.state().metaData().index(index);
            if (indexMetaData == null) {
                continue;
            }
            // now, go over and delete shards that needs to get deleted
            newShardIds.clear();
            List<MutableShardRouting> shards = routingNode.shards();
            for (int i = 0; i < shards.size(); i++) {
                ShardRouting shardRouting = shards.get(i);
                if (shardRouting.index().equals(index)) {
                    newShardIds.add(shardRouting.id());
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
        RoutingNode routingNode = event.state().readOnlyRoutingNodes().nodesToShards().get(event.state().nodes().localNodeId());
        if (routingNode == null) {
            return;
        }
        for (MutableShardRouting shard : routingNode) {
            if (!indicesService.hasIndex(shard.index())) {
                final IndexMetaData indexMetaData = event.state().metaData().index(shard.index());
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] creating index", indexMetaData.index());
                }
                indicesService.createIndex(indexMetaData.index(), indexMetaData.settings(), event.state().nodes().localNode().id());
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
            IndexService indexService = indicesService.indexServiceSafe(index);
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
            List<String> typesToRefresh = null;
            String index = indexMetaData.index();
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                // got deleted on us, ignore (closing the node)
                return;
            }
            MapperService mapperService = indexService.mapperService();
            // first, go over and update the _default_ mapping (if exists)
            if (indexMetaData.mappings().containsKey(MapperService.DEFAULT_MAPPING)) {
                processMapping(index, mapperService, MapperService.DEFAULT_MAPPING, indexMetaData.mapping(MapperService.DEFAULT_MAPPING).source());
            }

            // go over and add the relevant mappings (or update them)
            for (ObjectCursor<MappingMetaData> cursor : indexMetaData.mappings().values()) {
                MappingMetaData mappingMd = cursor.value;
                String mappingType = mappingMd.type();
                CompressedString mappingSource = mappingMd.source();
                if (mappingType.equals(MapperService.DEFAULT_MAPPING)) { // we processed _default_ first
                    continue;
                }
                boolean requireRefresh = processMapping(index, mapperService, mappingType, mappingSource);
                if (requireRefresh) {
                    if (typesToRefresh == null) {
                        typesToRefresh = Lists.newArrayList();
                    }
                    typesToRefresh.add(mappingType);
                }
            }
            if (typesToRefresh != null) {
                if (sendRefreshMapping) {
                    nodeMappingRefreshAction.nodeMappingRefresh(event.state(),
                            new NodeMappingRefreshAction.NodeMappingRefreshRequest(index, indexMetaData.uuid(),
                                    typesToRefresh.toArray(new String[typesToRefresh.size()]), event.state().nodes().localNodeId()));
                }
            }
            // go over and remove mappings
            for (DocumentMapper documentMapper : mapperService) {
                if (seenMappings.containsKey(new Tuple<String, String>(index, documentMapper.type())) && !indexMetaData.mappings().containsKey(documentMapper.type())) {
                    // we have it in our mappings, but not in the metadata, and we have seen it in the cluster state, remove it
                    mapperService.remove(documentMapper.type());
                    seenMappings.remove(new Tuple<String, String>(index, documentMapper.type()));
                }
            }
        }
    }

    private boolean processMapping(String index, MapperService mapperService, String mappingType, CompressedString mappingSource) {
        if (!seenMappings.containsKey(new Tuple<String, String>(index, mappingType))) {
            seenMappings.put(new Tuple<String, String>(index, mappingType), true);
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
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] adding mapping [{}], source [{}]", index, mappingType, mappingSource.string());
                }
                // we don't apply default, since it has been applied when the mappings were parsed initially
                mapperService.merge(mappingType, mappingSource, false);
                if (!mapperService.documentMapper(mappingType).mappingSource().equals(mappingSource)) {
                    logger.debug("[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}", index, mappingType, mappingSource, mapperService.documentMapper(mappingType).mappingSource());
                    requiresRefresh = true;
                }
            } else {
                DocumentMapper existingMapper = mapperService.documentMapper(mappingType);
                if (!mappingSource.equals(existingMapper.mappingSource())) {
                    // mapping changed, update it
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] updating mapping [{}], source [{}]", index, mappingType, mappingSource.string());
                    }
                    // we don't apply default, since it has been applied when the mappings were parsed initially
                    mapperService.merge(mappingType, mappingSource, false);
                    if (!mapperService.documentMapper(mappingType).mappingSource().equals(mappingSource)) {
                        requiresRefresh = true;
                        logger.debug("[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}", index, mappingType, mappingSource, mapperService.documentMapper(mappingType).mappingSource());
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn("[{}] failed to add mapping [{}], source [{}]", e, index, mappingType, mappingSource);
        }
        return requiresRefresh;
    }

    private boolean aliasesChanged(ClusterChangedEvent event) {
        return !event.state().metaData().aliases().equals(event.previousState().metaData().aliases()) ||
                !event.state().routingTable().equals(event.previousState().routingTable());
    }

    private void applyAliases(ClusterChangedEvent event) {
        // check if aliases changed
        if (aliasesChanged(event)) {
            // go over and update aliases
            for (IndexMetaData indexMetaData : event.state().metaData()) {
                if (!indicesService.hasIndex(indexMetaData.index())) {
                    // we only create / update here
                    continue;
                }
                String index = indexMetaData.index();
                IndexService indexService = indicesService.indexService(index);
                IndexAliasesService indexAliasesService = indexService.aliasesService();
                processAliases(index, indexMetaData.aliases().values(), indexAliasesService);
                // go over and remove aliases
                for (IndexAlias indexAlias : indexAliasesService) {
                    if (!indexMetaData.aliases().containsKey(indexAlias.alias())) {
                        // we have it in our aliases, but not in the metadata, remove it
                        indexAliasesService.remove(indexAlias.alias());
                    }
                }
            }
        }
    }

    private void processAliases(String index, ObjectContainer<AliasMetaData> aliases, IndexAliasesService indexAliasesService) {
        HashMap<String, IndexAlias> newAliases = newHashMap();
        for (ObjectCursor<AliasMetaData> cursor : aliases) {
            AliasMetaData aliasMd = cursor.value;
            String alias = aliasMd.alias();
            CompressedString filter = aliasMd.filter();
            try {
                if (!indexAliasesService.hasAlias(alias)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] adding alias [{}], filter [{}]", index, alias, filter);
                    }
                    newAliases.put(alias, indexAliasesService.create(alias, filter));
                } else {
                    if ((filter == null && indexAliasesService.alias(alias).filter() != null) ||
                            (filter != null && !filter.equals(indexAliasesService.alias(alias).filter()))) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[{}] updating alias [{}], filter [{}]", index, alias, filter);
                        }
                        newAliases.put(alias, indexAliasesService.create(alias, filter));
                    }
                }
            } catch (Throwable e) {
                logger.warn("[{}] failed to add alias [{}], filter [{}]", e, index, alias, filter);
            }
        }
        indexAliasesService.addAll(newAliases);
    }

    private void applyNewOrUpdatedShards(final ClusterChangedEvent event) throws ElasticSearchException {
        if (!indicesService.changesAllowed()) {
            return;
        }

        RoutingTable routingTable = event.state().routingTable();
        RoutingNode routingNodes = event.state().readOnlyRoutingNodes().nodesToShards().get(event.state().nodes().localNodeId());
        if (routingNodes == null) {
            failedShards.clear();
            return;
        }
        DiscoveryNodes nodes = event.state().nodes();

        for (final ShardRouting shardRouting : routingNodes) {
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
                if (!failedShards.containsKey(shardRouting.shardId())) {
                    // the master thinks we are started, but we don't have this shard at all, mark it as failed
                    logger.warn("[{}][{}] master [{}] marked shard as started, but shard has not been created, mark shard as failed", shardRouting.index(), shardId, nodes.masterNode());
                    failedShards.put(shardRouting.shardId(), new FailedShard(shardRouting.version()));
                    shardStateAction.shardFailed(shardRouting, indexMetaData.getUUID(),
                            "master " + nodes.masterNode() + " marked shard as started, but shard has not been created, mark shard as failed");
                }
                continue;
            }

            if (indexService.hasShard(shardId)) {
                InternalIndexShard indexShard = (InternalIndexShard) indexService.shard(shardId);
                ShardRouting currentRoutingEntry = indexShard.routingEntry();
                // if the current and global routing are initializing, but are still not the same, its a different "shard" being allocated
                // for example: a shard that recovers from one node and now needs to recover to another node,
                //              or a replica allocated and then allocating a primary because the primary failed on another node
                if (currentRoutingEntry.initializing() && shardRouting.initializing() && !currentRoutingEntry.equals(shardRouting)) {
                    logger.debug("[{}][{}] removing shard (different instance of it allocated on this node, current [{}], global [{}])", shardRouting.index(), shardRouting.id(), currentRoutingEntry, shardRouting);
                    // cancel recovery just in case we are in recovery (its fine if we are not in recovery, it will be a noop).
                    recoveryTarget.cancelRecovery(indexShard);
                    indexService.removeShard(shardRouting.id(), "removing shard (different instance of it allocated on this node)");
                }
            }

            if (indexService.hasShard(shardId)) {
                InternalIndexShard indexShard = (InternalIndexShard) indexService.shard(shardId);
                if (!shardRouting.equals(indexShard.routingEntry())) {
                    indexShard.routingEntry(shardRouting);
                    indexService.shardInjector(shardId).getInstance(IndexShardGatewayService.class).routingStateChanged();
                }
            }

            if (shardRouting.initializing()) {
                applyInitializingShard(routingTable, nodes, indexMetaData, routingTable.index(shardRouting.index()).shard(shardRouting.id()), shardRouting);
            }
        }
    }

    private void cleanFailedShards(final ClusterChangedEvent event) {
        RoutingTable routingTable = event.state().routingTable();
        RoutingNode routingNodes = event.state().readOnlyRoutingNodes().nodesToShards().get(event.state().nodes().localNodeId());
        if (routingNodes == null) {
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

    private void applyInitializingShard(final RoutingTable routingTable, final DiscoveryNodes nodes, final IndexMetaData indexMetaData, final IndexShardRoutingTable indexShardRouting, final ShardRouting shardRouting) throws ElasticSearchException {
        final IndexService indexService = indicesService.indexService(shardRouting.index());
        if (indexService == null) {
            // got deleted on us, ignore
            return;
        }
        final int shardId = shardRouting.id();

        if (indexService.hasShard(shardId)) {
            IndexShard indexShard = indexService.shardSafe(shardId);
            if (indexShard.state() == IndexShardState.STARTED || indexShard.state() == IndexShardState.POST_RECOVERY) {
                // the master thinks we are initializing, but we are already started or on POST_RECOVERY and waiting
                // for master to confirm a shard started message (either master failover, or a cluster event before
                // we managed to tell the master we started), mark us as started
                if (logger.isTraceEnabled()) {
                    logger.trace("{} master marked shard as initializing, but shard has state [{}], resending shard started",
                            indexShard.shardId(), indexShard.state());
                }
                shardStateAction.shardStarted(shardRouting, indexMetaData.getUUID(),
                        "master " + nodes.masterNode() + " marked shard as initializing, but shard state is [" + indexShard.state() + "], mark shard as started");
                return;
            } else {
                if (indexShard.ignoreRecoveryAttempt()) {
                    return;
                }
            }
        }
        // if there is no shard, create it
        if (!indexService.hasShard(shardId)) {
            if (failedShards.containsKey(shardRouting.shardId())) {
                // already tried to create this shard but it failed - ignore
                logger.trace("[{}][{}] not initializing, this shards failed to recover on this node before, waiting for reassignment", shardRouting.index(), shardRouting.id());
                return;
            }
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}] creating shard", shardRouting.index(), shardId);
                }
                InternalIndexShard indexShard = (InternalIndexShard) indexService.createShard(shardId);
                indexShard.routingEntry(shardRouting);
                indexShard.engine().addFailedEngineListener(failedEngineHandler);
            } catch (IndexShardAlreadyExistsException e) {
                // ignore this, the method call can happen several times
            } catch (Throwable e) {
                logger.warn("[{}][{}] failed to create shard", e, shardRouting.index(), shardRouting.id());
                try {
                    indexService.removeShard(shardId, "failed to create [" + ExceptionsHelper.detailedMessage(e) + "]");
                } catch (IndexShardMissingException e1) {
                    // ignore
                } catch (Throwable e1) {
                    logger.warn("[{}][{}] failed to remove shard after failed creation", e1, shardRouting.index(), shardRouting.id());
                }
                failedShards.put(shardRouting.shardId(), new FailedShard(shardRouting.version()));
                shardStateAction.shardFailed(shardRouting, indexMetaData.getUUID(), "Failed to create shard, message [" + detailedMessage(e) + "]");
                return;
            }
        }
        final InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(shardId);

        if (indexShard.ignoreRecoveryAttempt()) {
            // we are already recovering (we can get to this state since the cluster event can happen several
            // times while we recover)
            return;
        }


        if (!shardRouting.primary()) {
            // recovery from primary
            IndexShardRoutingTable shardRoutingTable = routingTable.index(shardRouting.index()).shard(shardRouting.id());
            for (ShardRouting entry : shardRoutingTable) {
                if (entry.primary() && entry.started()) {
                    // only recover from started primary, if we can't find one, we will do it next round
                    final DiscoveryNode sourceNode = nodes.get(entry.currentNodeId());
                    try {
                        // we are recovering a backup from a primary, so no need to mark it as relocated
                        final StartRecoveryRequest request = new StartRecoveryRequest(indexShard.shardId(), sourceNode, nodes.localNode(), false, indexShard.store().list());
                        recoveryTarget.startRecovery(request, indexShard, new PeerRecoveryListener(request, shardRouting, indexService, indexMetaData));
                    } catch (Throwable e) {
                        handleRecoveryFailure(indexService, indexMetaData, shardRouting, true, e);
                        break;
                    }
                    break;
                }
            }
        } else {
            if (shardRouting.relocatingNodeId() == null) {
                // we are the first primary, recover from the gateway
                // if its post api allocation, the index should exists
                boolean indexShouldExists = indexShardRouting.primaryAllocatedPostApi();
                IndexShardGatewayService shardGatewayService = indexService.shardInjector(shardId).getInstance(IndexShardGatewayService.class);
                shardGatewayService.recover(indexShouldExists, new IndexShardGatewayService.RecoveryListener() {
                    @Override
                    public void onRecoveryDone() {
                        shardStateAction.shardStarted(shardRouting, indexMetaData.getUUID(), "after recovery from gateway");
                    }

                    @Override
                    public void onIgnoreRecovery(String reason) {
                    }

                    @Override
                    public void onRecoveryFailed(IndexShardGatewayRecoveryException e) {
                        handleRecoveryFailure(indexService, indexMetaData, shardRouting, true, e);
                    }
                });
            } else {
                // relocating primaries, recovery from the relocating shard
                final DiscoveryNode sourceNode = nodes.get(shardRouting.relocatingNodeId());
                try {
                    // we don't mark this one as relocated at the end, requests in any case are routed to both when its relocating
                    // and that way we handle the edge case where its mark as relocated, and we might need to roll it back...
                    final StartRecoveryRequest request = new StartRecoveryRequest(indexShard.shardId(), sourceNode, nodes.localNode(), false, indexShard.store().list());
                    recoveryTarget.startRecovery(request, indexShard, new PeerRecoveryListener(request, shardRouting, indexService, indexMetaData));
                } catch (Throwable e) {
                    handleRecoveryFailure(indexService, indexMetaData, shardRouting, true, e);
                }
            }
        }
    }

    private class PeerRecoveryListener implements RecoveryTarget.RecoveryListener {

        private final StartRecoveryRequest request;
        private final ShardRouting shardRouting;
        private final IndexService indexService;
        private final IndexMetaData indexMetaData;

        private PeerRecoveryListener(StartRecoveryRequest request, ShardRouting shardRouting, IndexService indexService, IndexMetaData indexMetaData) {
            this.request = request;
            this.shardRouting = shardRouting;
            this.indexService = indexService;
            this.indexMetaData = indexMetaData;
        }

        @Override
        public void onRecoveryDone() {
            shardStateAction.shardStarted(shardRouting, indexMetaData.getUUID(), "after recovery (replica) from node [" + request.sourceNode() + "]");
        }

        @Override
        public void onRetryRecovery(TimeValue retryAfter, RecoveryStatus recoveryStatus) {
            recoveryTarget.retryRecovery(request, recoveryStatus, PeerRecoveryListener.this);
        }

        @Override
        public void onIgnoreRecovery(boolean removeShard, String reason) {
            if (!removeShard) {
                return;
            }
            synchronized (mutex) {
                if (indexService.hasShard(shardRouting.shardId().id())) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}] removing shard on ignored recovery, reason [{}]", shardRouting.index(), shardRouting.shardId().id(), reason);
                    }
                    try {
                        indexService.removeShard(shardRouting.shardId().id(), "ignore recovery: " + reason);
                    } catch (IndexShardMissingException e) {
                        // the node got closed on us, ignore it
                    } catch (Throwable e1) {
                        logger.warn("[{}][{}] failed to delete shard after ignore recovery", e1, indexService.index().name(), shardRouting.shardId().id());
                    }
                }
            }
        }

        @Override
        public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
            handleRecoveryFailure(indexService, indexMetaData, shardRouting, sendShardFailure, e);
        }
    }

    private void handleRecoveryFailure(IndexService indexService, IndexMetaData indexMetaData, ShardRouting shardRouting, boolean sendShardFailure, Throwable failure) {
        logger.warn("[{}][{}] failed to start shard", failure, indexService.index().name(), shardRouting.shardId().id());
        synchronized (mutex) {
            if (indexService.hasShard(shardRouting.shardId().id())) {
                try {
                    indexService.removeShard(shardRouting.shardId().id(), "recovery failure [" + ExceptionsHelper.detailedMessage(failure) + "]");
                } catch (IndexShardMissingException e) {
                    // the node got closed on us, ignore it
                } catch (Throwable e1) {
                    logger.warn("[{}][{}] failed to delete shard after failed startup", e1, indexService.index().name(), shardRouting.shardId().id());
                }
            }
            if (sendShardFailure) {
                try {
                    failedShards.put(shardRouting.shardId(), new FailedShard(shardRouting.version()));
                    shardStateAction.shardFailed(shardRouting, indexMetaData.getUUID(), "Failed to start shard, message [" + detailedMessage(failure) + "]");
                } catch (Throwable e1) {
                    logger.warn("[{}][{}] failed to mark shard as failed after a failed start", e1, indexService.index().name(), shardRouting.id());
                }
            }
        }
    }

    private void removeIndex(String index, String reason) {
        try {
            indicesService.removeIndex(index, reason);
        } catch (Throwable e) {
            logger.warn("failed to clean index ({})", e, reason);
        }
        // clear seen mappings as well
        for (Tuple<String, String> tuple : seenMappings.keySet()) {
            if (tuple.v1().equals(index)) {
                seenMappings.remove(tuple);
            }
        }
    }

    private class FailedEngineHandler implements Engine.FailedEngineListener {
        @Override
        public void onFailedEngine(final ShardId shardId, final Throwable failure) {
            ShardRouting shardRouting = null;
            final IndexService indexService = indicesService.indexService(shardId.index().name());
            if (indexService != null) {
                IndexShard indexShard = indexService.shard(shardId.id());
                if (indexShard != null) {
                    shardRouting = indexShard.routingEntry();
                }
            }
            if (shardRouting == null) {
                logger.warn("[{}][{}] engine failed, but can't find index shard", shardId.index().name(), shardId.id());
                return;
            }
            final ShardRouting fShardRouting = shardRouting;
            final String indexUUID = indexService.indexUUID(); // we know indexService is not null here.
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    synchronized (mutex) {
                        if (indexService.hasShard(shardId.id())) {
                            try {
                                indexService.removeShard(shardId.id(), "engine failure [" + ExceptionsHelper.detailedMessage(failure) + "]");
                            } catch (IndexShardMissingException e) {
                                // the node got closed on us, ignore it
                            } catch (Throwable e1) {
                                logger.warn("[{}][{}] failed to delete shard after failed engine", e1, indexService.index().name(), shardId.id());
                            }
                        }
                        try {
                            failedShards.put(fShardRouting.shardId(), new FailedShard(fShardRouting.version()));
                            shardStateAction.shardFailed(fShardRouting, indexUUID, "engine failure, message [" + detailedMessage(failure) + "]");
                        } catch (Throwable e1) {
                            logger.warn("[{}][{}] failed to mark shard as failed after a failed engine", e1, indexService.index().name(), shardId.id());
                        }
                    }
                }
            });
        }
    }
}
