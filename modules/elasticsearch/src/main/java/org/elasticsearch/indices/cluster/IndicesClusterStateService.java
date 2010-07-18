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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeIndexCreatedAction;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.index.NodeMappingCreatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.gateway.IndexShardGatewayRecoveryException;
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.recovery.RecoveryFailedException;
import org.elasticsearch.index.shard.recovery.RecoveryTarget;
import org.elasticsearch.index.shard.recovery.StartRecoveryRequest;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ExceptionsHelper.*;
import static org.elasticsearch.common.collect.Sets.*;

/**
 * @author kimchy (shay.banon)
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

    @Inject public IndicesClusterStateService(Settings settings, IndicesService indicesService, ClusterService clusterService,
                                              ThreadPool threadPool, RecoveryTarget recoveryTarget, ShardStateAction shardStateAction,
                                              NodeIndexCreatedAction nodeIndexCreatedAction, NodeIndexDeletedAction nodeIndexDeletedAction,
                                              NodeMappingCreatedAction nodeMappingCreatedAction) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTarget = recoveryTarget;
        this.shardStateAction = shardStateAction;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
        this.nodeMappingCreatedAction = nodeMappingCreatedAction;
    }

    @Override protected void doStart() throws ElasticSearchException {
        clusterService.add(this);
    }

    @Override protected void doStop() throws ElasticSearchException {
        clusterService.remove(this);
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    @Override public void clusterChanged(final ClusterChangedEvent event) {
        if (!indicesService.changesAllowed())
            return;

        applyNewIndices(event);
        applyMappings(event);
        applyNewOrUpdatedShards(event);
        applyDeletedIndices(event);
        applyDeletedShards(event);
    }

    private void applyDeletedIndices(final ClusterChangedEvent event) {
        for (final String index : indicesService.indices()) {
            if (!event.state().metaData().hasIndex(index)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] deleting index", index);
                }
                indicesService.deleteIndex(index);
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        nodeIndexDeletedAction.nodeIndexDeleted(index, event.state().nodes().localNodeId());
                    }
                });
            }
        }
    }

    private void applyDeletedShards(final ClusterChangedEvent event) {
        RoutingNode routingNodes = event.state().readOnlyRoutingNodes().nodesToShards().get(event.state().nodes().localNodeId());
        if (routingNodes == null) {
            return;
        }
        for (final String index : indicesService.indices()) {
            if (event.state().metaData().hasIndex(index)) {
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
                        if (logger.isDebugEnabled()) {
                            logger.debug("[{}][{}] deleting shard", index, existingShardId);
                        }
                        indexService.cleanShard(existingShardId);
                    }
                }
            }
        }
    }

    private void applyNewIndices(final ClusterChangedEvent event) {
        // first, go over and create and indices that needs to be created
        for (final IndexMetaData indexMetaData : event.state().metaData()) {
            if (!indicesService.hasIndex(indexMetaData.index())) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] creating index", indexMetaData.index());
                }
                indicesService.createIndex(indexMetaData.index(), indexMetaData.settings(), event.state().nodes().localNode().id());
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        nodeIndexCreatedAction.nodeIndexCreated(indexMetaData.index(), event.state().nodes().localNodeId());
                    }
                });
            }
        }
    }


    private void applyMappings(ClusterChangedEvent event) {
        // go over and update mappings
        for (IndexMetaData indexMetaData : event.state().metaData()) {
            if (!indicesService.hasIndex(indexMetaData.index())) {
                // we only create / update here
                continue;
            }
            String index = indexMetaData.index();
            IndexService indexService = indicesService.indexServiceSafe(index);
            MapperService mapperService = indexService.mapperService();
            ImmutableMap<String, String> mappings = indexMetaData.mappings();
            // we don't support removing mappings for now ...
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
                String mappingType = entry.getKey();
                String mappingSource = entry.getValue();

                try {
                    if (!mapperService.hasMapping(mappingType)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[{}] adding mapping [{}], source [{}]", index, mappingType, mappingSource);
                        }
                        mapperService.add(mappingType, mappingSource);
                        nodeMappingCreatedAction.nodeMappingCreated(new NodeMappingCreatedAction.NodeMappingCreatedResponse(index, mappingType, event.state().nodes().localNodeId()));
                    } else {
                        DocumentMapper existingMapper = mapperService.documentMapper(mappingType);
                        if (!mappingSource.equals(existingMapper.mappingSource())) {
                            // mapping changed, update it
                            if (logger.isDebugEnabled()) {
                                logger.debug("[{}] updating mapping [{}], source [{}]", index, mappingType, mappingSource);
                            }
                            mapperService.add(mappingType, mappingSource);
                            nodeMappingCreatedAction.nodeMappingCreated(new NodeMappingCreatedAction.NodeMappingCreatedResponse(index, mappingType, event.state().nodes().localNodeId()));
                        }
                    }
                } catch (Exception e) {
                    logger.warn("[{}] failed to add mapping [{}], source [{}]", index, mappingType, mappingSource);
                }
            }
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
            final IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());

            final int shardId = shardRouting.id();

            if (!indexService.hasShard(shardId) && shardRouting.started()) {
                // the master thinks we are started, but we don't have this shard at all, mark it as failed
                logger.warn("[{}][{}] master [{}] marked shard as started, but shard have not been created, mark shard as failed");
                shardStateAction.shardFailed(shardRouting, "master " + nodes.masterNode() + " marked shard as started, but shard have not been created, mark shard as failed");
                continue;
            }

            if (indexService.hasShard(shardId)) {
                InternalIndexShard indexShard = (InternalIndexShard) indexService.shard(shardId);
                if (!shardRouting.equals(indexShard.routingEntry())) {
                    indexShard.routingEntry(shardRouting);
                    indexService.shardInjector(shardId).getInstance(IndexShardGatewayService.class).routingStateChanged();
                }
            }

            if (shardRouting.initializing()) {
                applyInitializingShard(routingTable, nodes, shardRouting);
            }
        }
    }

    private void applyInitializingShard(final RoutingTable routingTable, final DiscoveryNodes nodes, final ShardRouting shardRouting) throws ElasticSearchException {
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
            } catch (IndexShardAlreadyExistsException e) {
                // ignore this, the method call can happen several times
            } catch (Exception e) {
                logger.warn("[{}][{}] failed to create shard", e, shardRouting.index(), shardRouting.id());
                try {
                    indexService.cleanShard(shardId);
                } catch (IndexShardMissingException e1) {
                    // ignore
                } catch (Exception e1) {
                    logger.warn("[{}][{}] failed to delete shard after failed creation", e1, shardRouting.index(), shardRouting.id());
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
                        final StartRecoveryRequest request = new StartRecoveryRequest(indexShard.shardId(), sourceNode, nodes.localNode(), false, indexShard.store().listWithMd5());
                        recoveryTarget.startRecovery(request, new PeerRecoveryListener(request, shardRouting, indexService));
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
                IndexShardGatewayService shardGatewayService = indexService.shardInjector(shardId).getInstance(IndexShardGatewayService.class);
                shardGatewayService.recover(new IndexShardGatewayService.RecoveryListener() {
                    @Override public void onRecoveryDone() {
                        shardStateAction.shardStarted(shardRouting, "after recovery from gateway");
                    }

                    @Override public void onIgnoreRecovery(String reason) {
                    }

                    @Override public void onRecoveryFailed(IndexShardGatewayRecoveryException e) {
                        handleRecoveryFailure(indexService, shardRouting, true, e);
                    }
                });
            } else {
                // relocating primaries, recovery from the relocating shard
                final DiscoveryNode sourceNode = nodes.get(shardRouting.relocatingNodeId());
                try {
                    // we are recovering a backup from a primary, so no need to mark it as relocated
                    final StartRecoveryRequest request = new StartRecoveryRequest(indexShard.shardId(), sourceNode, nodes.localNode(), false, indexShard.store().listWithMd5());
                    recoveryTarget.startRecovery(request, new PeerRecoveryListener(request, shardRouting, indexService));
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

        @Override public void onRecoveryDone() {
            shardStateAction.shardStarted(shardRouting, "after recovery (backup) from node [" + request.sourceNode() + "]");
        }

        @Override public void onRetryRecovery(TimeValue retryAfter) {
            threadPool.schedule(new Runnable() {
                @Override public void run() {
                    recoveryTarget.startRecovery(request, PeerRecoveryListener.this);
                }
            }, retryAfter);
        }

        @Override public void onIgnoreRecovery(boolean cleanShard, String reason) {
            if (!cleanShard) {
                return;
            }
            if (indexService.hasShard(shardRouting.shardId().id())) {
                try {
                    indexService.cleanShard(shardRouting.shardId().id());
                } catch (IndexShardMissingException e) {
                    // the node got closed on us, ignore it
                } catch (Exception e1) {
                    logger.warn("[{}][{}] failed to delete shard after ignore recovery", e1, indexService.index().name(), shardRouting.shardId().id());
                }
            }
        }

        @Override public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
            handleRecoveryFailure(indexService, shardRouting, sendShardFailure, e);
        }
    }

    private void handleRecoveryFailure(IndexService indexService, ShardRouting shardRouting, boolean sendShardFailure, Throwable failure) {
        logger.warn("[{}][{}] failed to start shard", failure, indexService.index().name(), shardRouting.shardId().id());
        if (indexService.hasShard(shardRouting.shardId().id())) {
            try {
                indexService.cleanShard(shardRouting.shardId().id());
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
