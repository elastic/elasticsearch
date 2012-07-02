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

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.*;
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
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.recovery.StartRecoveryRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.collect.Sets.newHashSet;
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

    private final NodeMappingCreatedAction nodeMappingCreatedAction;

    private final NodeMappingRefreshAction nodeMappingRefreshAction;

    private final NodeAliasesUpdatedAction nodeAliasesUpdatedAction;

    // a map of mappings type we have seen per index due to cluster state
    // we need this so we won't remove types automatically created as part of the indexing process
    private final ConcurrentMap<Tuple<String, String>, Boolean> seenMappings = ConcurrentCollections.newConcurrentMap();

    private final Object mutex = new Object();

    private final FailedEngineHandler failedEngineHandler = new FailedEngineHandler();

    @Inject
    public IndicesClusterStateService(Settings settings, IndicesService indicesService, ClusterService clusterService,
                                      ThreadPool threadPool, RecoveryTarget recoveryTarget,
                                      ShardStateAction shardStateAction,
                                      NodeIndexCreatedAction nodeIndexCreatedAction, NodeIndexDeletedAction nodeIndexDeletedAction,
                                      NodeMappingCreatedAction nodeMappingCreatedAction, NodeMappingRefreshAction nodeMappingRefreshAction,
                                      NodeAliasesUpdatedAction nodeAliasesUpdatedAction) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTarget = recoveryTarget;
        this.shardStateAction = shardStateAction;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
        this.nodeMappingCreatedAction = nodeMappingCreatedAction;
        this.nodeMappingRefreshAction = nodeMappingRefreshAction;
        this.nodeAliasesUpdatedAction = nodeAliasesUpdatedAction;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        clusterService.add(this);
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
        if (!indicesService.changesAllowed())
            return;

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
                        } catch (Exception e) {
                            logger.warn("[{}] failed to remove shard (disabled block persistence)", e, index);
                        }
                    }
                    indicesService.cleanIndex(index, "cleaning index (disabled block persistence)");
                }
                return;
            }

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
        for (String index : event.indicesCreated()) {
            try {
                nodeIndexCreatedAction.nodeIndexCreated(index, event.state().nodes().localNodeId());
            } catch (Exception e) {
                logger.debug("failed to send to master index {} created event", index);
            }
        }
        for (String index : event.indicesDeleted()) {
            try {
                nodeIndexDeletedAction.nodeIndexDeleted(index, event.state().nodes().localNodeId());
            } catch (Exception e) {
                logger.debug("failed to send to master index {} deleted event", index);
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
                    } catch (Exception e) {
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
                try {
                    indicesService.cleanIndex(index, "cleaning index (no shards allocated)");
                } catch (Exception e) {
                    logger.warn("[{}] failed to clean index (no shards of that index are allocated on this node)", e, index);
                }
            }
        }
    }

    private void applyDeletedIndices(final ClusterChangedEvent event) {
        for (final String index : indicesService.indices()) {
            if (!event.state().metaData().hasIndex(index)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] cleaning index, no longer part of the metadata", index);
                }
                try {
                    indicesService.cleanIndex(index, "index no longer part of the metadata");
                } catch (Exception e) {
                    logger.warn("failed to clean index", e);
                }
                // clear seen mappings as well
                for (Tuple<String, String> tuple : seenMappings.keySet()) {
                    if (tuple.v1().equals(index)) {
                        seenMappings.remove(tuple);
                    }
                }
            }
        }
    }

    private void applyDeletedShards(final ClusterChangedEvent event) {
        RoutingNode routingNodes = event.state().readOnlyRoutingNodes().nodesToShards().get(event.state().nodes().localNodeId());
        if (routingNodes == null) {
            return;
        }
        for (final String index : indicesService.indices()) {
            IndexMetaData indexMetaData = event.state().metaData().index(index);
            if (indexMetaData != null) {
                // now, go over and delete shards that needs to get deleted
                Set<Integer> newShardIds = newHashSet();
                for (final ShardRouting shardRouting : routingNodes) {
                    if (shardRouting.index().equals(index)) {
                        newShardIds.add(shardRouting.id());
                    }
                }
                final IndexService indexService = indicesService.indexService(index);
                if (indexService == null) {
                    continue;
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
            IndexService indexService = indicesService.indexServiceSafe(index);
            MapperService mapperService = indexService.mapperService();
            // first, go over and update the _default_ mapping (if exists)
            if (indexMetaData.mappings().containsKey(MapperService.DEFAULT_MAPPING)) {
                processMapping(event, index, mapperService, MapperService.DEFAULT_MAPPING, indexMetaData.mapping(MapperService.DEFAULT_MAPPING).source());
            }

            // go over and add the relevant mappings (or update them)
            for (MappingMetaData mappingMd : indexMetaData.mappings().values()) {
                String mappingType = mappingMd.type();
                CompressedString mappingSource = mappingMd.source();
                if (mappingType.equals(MapperService.DEFAULT_MAPPING)) { // we processed _default_ first
                    continue;
                }
                boolean requireRefresh = processMapping(event, index, mapperService, mappingType, mappingSource);
                if (requireRefresh) {
                    if (typesToRefresh == null) {
                        typesToRefresh = Lists.newArrayList();
                    }
                    typesToRefresh.add(mappingType);
                }
            }
            if (typesToRefresh != null) {
                nodeMappingRefreshAction.nodeMappingRefresh(new NodeMappingRefreshAction.NodeMappingRefreshRequest(index, typesToRefresh.toArray(new String[typesToRefresh.size()]), event.state().nodes().localNodeId()));
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

    private boolean processMapping(ClusterChangedEvent event, String index, MapperService mapperService, String mappingType, CompressedString mappingSource) {
        if (!seenMappings.containsKey(new Tuple<String, String>(index, mappingType))) {
            seenMappings.put(new Tuple<String, String>(index, mappingType), true);
        }

        boolean requiresRefresh = false;
        try {
            if (!mapperService.hasMapping(mappingType)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] adding mapping [{}], source [{}]", index, mappingType, mappingSource.string());
                }
                mapperService.add(mappingType, mappingSource.string());
                if (!mapperService.documentMapper(mappingType).mappingSource().equals(mappingSource)) {
                    // this might happen when upgrading from 0.15 to 0.16
                    logger.debug("[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}", index, mappingType, mappingSource, mapperService.documentMapper(mappingType).mappingSource());
                    requiresRefresh = true;
                }
                nodeMappingCreatedAction.nodeMappingCreated(new NodeMappingCreatedAction.NodeMappingCreatedResponse(index, mappingType, event.state().nodes().localNodeId()));
            } else {
                DocumentMapper existingMapper = mapperService.documentMapper(mappingType);
                if (!mappingSource.equals(existingMapper.mappingSource())) {
                    // mapping changed, update it
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] updating mapping [{}], source [{}]", index, mappingType, mappingSource.string());
                    }
                    mapperService.add(mappingType, mappingSource.string());
                    if (!mapperService.documentMapper(mappingType).mappingSource().equals(mappingSource)) {
                        requiresRefresh = true;
                        // this might happen when upgrading from 0.15 to 0.16
                        logger.debug("[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}", index, mappingType, mappingSource, mapperService.documentMapper(mappingType).mappingSource());
                    }
                    nodeMappingCreatedAction.nodeMappingCreated(new NodeMappingCreatedAction.NodeMappingCreatedResponse(index, mappingType, event.state().nodes().localNodeId()));
                }
            }
        } catch (Exception e) {
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
                for (AliasMetaData aliasesMd : indexMetaData.aliases().values()) {
                    processAlias(index, aliasesMd.alias(), aliasesMd.filter(), indexAliasesService);
                }
                // go over and remove aliases
                for (IndexAlias indexAlias : indexAliasesService) {
                    if (!indexMetaData.aliases().containsKey(indexAlias.alias())) {
                        // we have it in our aliases, but not in the metadata, remove it
                        indexAliasesService.remove(indexAlias.alias());
                    }
                }
            }
            // Notify client that alias changes were applied
            nodeAliasesUpdatedAction.nodeAliasesUpdated(
                    new NodeAliasesUpdatedAction.NodeAliasesUpdatedResponse(event.state().nodes().localNodeId(), event.state().version()));
        }
    }

    private void processAlias(String index, String alias, CompressedString filter, IndexAliasesService indexAliasesService) {
        try {
            if (!indexAliasesService.hasAlias(alias)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] adding alias [{}], filter [{}]", index, alias, filter);
                }
                indexAliasesService.add(alias, filter);
            } else {
                if ((filter == null && indexAliasesService.alias(alias).filter() != null) ||
                        (filter != null && !filter.equals(indexAliasesService.alias(alias).filter()))) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] updating alias [{}], filter [{}]", index, alias, filter);
                    }
                    indexAliasesService.add(alias, filter);
                }
            }
        } catch (Exception e) {
            logger.warn("[{}] failed to add alias [{}], filter [{}]", e, index, alias, filter);
        }

    }

    private void applyNewOrUpdatedShards(final ClusterChangedEvent event) throws ElasticSearchException {
        if (!indicesService.changesAllowed())
            return;

        RoutingTable routingTable = event.state().routingTable();
        RoutingNode routingNodes = event.state().readOnlyRoutingNodes().nodesToShards().get(event.state().nodes().localNodeId());
        if (routingNodes == null) {
            return;
        }
        DiscoveryNodes nodes = event.state().nodes();


        for (final ShardRouting shardRouting : routingNodes) {
            final IndexService indexService = indicesService.indexService(shardRouting.index());
            if (indexService == null) {
                // got deleted on us, ignore
                continue;
            }

            final int shardId = shardRouting.id();

            if (!indexService.hasShard(shardId) && shardRouting.started()) {
                // the master thinks we are started, but we don't have this shard at all, mark it as failed
                logger.warn("[{}][{}] master [{}] marked shard as started, but shard have not been created, mark shard as failed", shardRouting.index(), shardId, nodes.masterNode());
                shardStateAction.shardFailed(shardRouting, "master " + nodes.masterNode() + " marked shard as started, but shard have not been created, mark shard as failed");
                continue;
            }

            if (indexService.hasShard(shardId)) {
                InternalIndexShard indexShard = (InternalIndexShard) indexService.shard(shardId);
                if (!shardRouting.equals(indexShard.routingEntry())) {
                    ShardRouting currentRoutingEntry = indexShard.routingEntry();
                    boolean needToDeleteCurrentShard = false;
                    if (currentRoutingEntry.initializing() && shardRouting.initializing()) {
                        // both are initializing, see if they are different instanceof of the shard routing, so they got switched on us
                        if (currentRoutingEntry.primary() && !shardRouting.primary()) {
                            needToDeleteCurrentShard = true;
                        }
                        // recovering from different nodes..., restart recovery
                        if (currentRoutingEntry.relocatingNodeId() != null && shardRouting.relocatingNodeId() != null &&
                                !currentRoutingEntry.relocatingNodeId().equals(shardRouting.relocatingNodeId())) {
                            needToDeleteCurrentShard = true;
                        }
                    }
                    if (needToDeleteCurrentShard) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[{}][{}] removing shard (different instance of it allocated on this node)", shardRouting.index(), shardRouting.id());
                        }
                        recoveryTarget.cancelRecovery(shardRouting.shardId());
                        indexService.removeShard(shardRouting.id(), "removing shard (different instance of it allocated on this node)");
                    }
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
                applyInitializingShard(routingTable, nodes, routingTable.index(shardRouting.index()).shard(shardRouting.id()), shardRouting);
            }
        }
    }

    private void applyInitializingShard(final RoutingTable routingTable, final DiscoveryNodes nodes, final IndexShardRoutingTable indexShardRouting, final ShardRouting shardRouting) throws ElasticSearchException {
        final IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());
        final int shardId = shardRouting.id();

        if (indexService.hasShard(shardId)) {
            IndexShard indexShard = indexService.shardSafe(shardId);
            if (indexShard.state() == IndexShardState.STARTED) {
                // the master thinks we are initializing, but we are already started
                // (either master failover, or a cluster event before we managed to tell the master we started), mark us as started
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}][{}] master [{}] marked shard as initializing, but shard already created, mark shard as started");
                }
                shardStateAction.shardStarted(shardRouting, "master " + nodes.masterNode() + " marked shard as initializing, but shard already started, mark shard as started");
                return;
            } else {
                if (indexShard.ignoreRecoveryAttempt()) {
                    return;
                }
            }
        }
        // if there is no shard, create it
        if (!indexService.hasShard(shardId)) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}][{}] creating shard", shardRouting.index(), shardId);
                }
                InternalIndexShard indexShard = (InternalIndexShard) indexService.createShard(shardId);
                indexShard.routingEntry(shardRouting);
                indexShard.engine().addFailedEngineListener(failedEngineHandler);
            } catch (IndexShardAlreadyExistsException e) {
                // ignore this, the method call can happen several times
            } catch (Exception e) {
                logger.warn("[{}][{}] failed to create shard", e, shardRouting.index(), shardRouting.id());
                try {
                    indexService.removeShard(shardId, "failed to create [" + ExceptionsHelper.detailedMessage(e) + "]");
                } catch (IndexShardMissingException e1) {
                    // ignore
                } catch (Exception e1) {
                    logger.warn("[{}][{}] failed to remove shard after failed creation", e1, shardRouting.index(), shardRouting.id());
                }
                shardStateAction.shardFailed(shardRouting, "Failed to create shard, message [" + detailedMessage(e) + "]");
                return;
            } catch (OutOfMemoryError e) {
                logger.warn("[{}][{}] failed to create shard", e, shardRouting.index(), shardRouting.id());
                try {
                    indexService.removeShard(shardId, "failed to create [" + ExceptionsHelper.detailedMessage(e) + "]");
                } catch (IndexShardMissingException e1) {
                    // ignore
                } catch (Exception e1) {
                    logger.warn("[{}][{}] failed to remove shard after failed creation", e1, shardRouting.index(), shardRouting.id());
                }
                shardStateAction.shardFailed(shardRouting, "Failed to create shard, message [" + detailedMessage(e) + "]");
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
                        recoveryTarget.startRecovery(request, false, new PeerRecoveryListener(request, shardRouting, indexService));
                    } catch (Exception e) {
                        handleRecoveryFailure(indexService, shardRouting, true, e);
                        break;
                    }
                    break;
                }
            }
        } else {
            if (shardRouting.relocatingNodeId() == null) {
                // we are the first primary, recover from the gateway
                // if its post api allocation, the index should exists
                boolean indexShouldExists = indexShardRouting.allocatedPostApi();
                IndexShardGatewayService shardGatewayService = indexService.shardInjector(shardId).getInstance(IndexShardGatewayService.class);
                shardGatewayService.recover(indexShouldExists, new IndexShardGatewayService.RecoveryListener() {
                    @Override
                    public void onRecoveryDone() {
                        shardStateAction.shardStarted(shardRouting, "after recovery from gateway");
                    }

                    @Override
                    public void onIgnoreRecovery(String reason) {
                    }

                    @Override
                    public void onRecoveryFailed(IndexShardGatewayRecoveryException e) {
                        handleRecoveryFailure(indexService, shardRouting, true, e);
                    }
                });
            } else {
                // relocating primaries, recovery from the relocating shard
                final DiscoveryNode sourceNode = nodes.get(shardRouting.relocatingNodeId());
                try {
                    // we don't mark this one as relocated at the end, requests in any case are routed to both when its relocating
                    // and that way we handle the edge case where its mark as relocated, and we might need to roll it back...
                    final StartRecoveryRequest request = new StartRecoveryRequest(indexShard.shardId(), sourceNode, nodes.localNode(), false, indexShard.store().list());
                    recoveryTarget.startRecovery(request, false, new PeerRecoveryListener(request, shardRouting, indexService));
                } catch (Exception e) {
                    handleRecoveryFailure(indexService, shardRouting, true, e);
                }
            }
        }
    }

    private class PeerRecoveryListener implements RecoveryTarget.RecoveryListener {

        private final StartRecoveryRequest request;

        private final ShardRouting shardRouting;

        private final IndexService indexService;

        private PeerRecoveryListener(StartRecoveryRequest request, ShardRouting shardRouting, IndexService indexService) {
            this.request = request;
            this.shardRouting = shardRouting;
            this.indexService = indexService;
        }

        @Override
        public void onRecoveryDone() {
            shardStateAction.shardStarted(shardRouting, "after recovery (replica) from node [" + request.sourceNode() + "]");
        }

        @Override
        public void onRetryRecovery(TimeValue retryAfter) {
            threadPool.schedule(retryAfter, ThreadPool.Names.GENERIC, new Runnable() {
                @Override
                public void run() {
                    recoveryTarget.startRecovery(request, true, PeerRecoveryListener.this);
                }
            });
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
                    } catch (Exception e1) {
                        logger.warn("[{}][{}] failed to delete shard after ignore recovery", e1, indexService.index().name(), shardRouting.shardId().id());
                    }
                }
            }
        }

        @Override
        public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
            handleRecoveryFailure(indexService, shardRouting, sendShardFailure, e);
        }
    }

    private void handleRecoveryFailure(IndexService indexService, ShardRouting shardRouting, boolean sendShardFailure, Throwable failure) {
        logger.warn("[{}][{}] failed to start shard", failure, indexService.index().name(), shardRouting.shardId().id());
        synchronized (mutex) {
            if (indexService.hasShard(shardRouting.shardId().id())) {
                try {
                    indexService.removeShard(shardRouting.shardId().id(), "recovery failure [" + ExceptionsHelper.detailedMessage(failure) + "]");
                } catch (IndexShardMissingException e) {
                    // the node got closed on us, ignore it
                } catch (Exception e1) {
                    logger.warn("[{}][{}] failed to delete shard after failed startup", e1, indexService.index().name(), shardRouting.shardId().id());
                }
            }
            if (sendShardFailure) {
                try {
                    shardStateAction.shardFailed(shardRouting, "Failed to start shard, message [" + detailedMessage(failure) + "]");
                } catch (Exception e1) {
                    logger.warn("[{}][{}] failed to mark shard as failed after a failed start", e1, indexService.index().name(), shardRouting.id());
                }
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
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    synchronized (mutex) {
                        if (indexService.hasShard(shardId.id())) {
                            try {
                                indexService.removeShard(shardId.id(), "engine failure [" + ExceptionsHelper.detailedMessage(failure) + "]");
                            } catch (IndexShardMissingException e) {
                                // the node got closed on us, ignore it
                            } catch (Exception e1) {
                                logger.warn("[{}][{}] failed to delete shard after failed engine", e1, indexService.index().name(), shardId.id());
                            }
                        }
                        try {
                            shardStateAction.shardFailed(fShardRouting, "engine failure, message [" + detailedMessage(failure) + "]");
                        } catch (Exception e1) {
                            logger.warn("[{}][{}] failed to mark shard as failed after a failed engine", e1, indexService.index().name(), shardId.id());
                        }
                    }
                }
            });
        }
    }
}
