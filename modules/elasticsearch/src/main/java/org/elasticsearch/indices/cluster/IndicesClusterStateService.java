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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeIndexCreatedAction;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.cluster.node.Nodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.gateway.IgnoreGatewayRecoveryException;
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.InternalIndexShard;
import org.elasticsearch.index.shard.recovery.IgnoreRecoveryException;
import org.elasticsearch.index.shard.recovery.RecoveryAction;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.settings.Settings;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.*;

/**
 * @author kimchy (Shay Banon)
 */
public class IndicesClusterStateService extends AbstractComponent implements ClusterStateListener, LifecycleComponent<IndicesClusterStateService> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final IndicesService indicesService;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final ShardStateAction shardStateAction;

    private final NodeIndexCreatedAction nodeIndexCreatedAction;

    private final NodeIndexDeletedAction nodeIndexDeletedAction;

    @Inject public IndicesClusterStateService(Settings settings, IndicesService indicesService, ClusterService clusterService,
                                              ThreadPool threadPool, ShardStateAction shardStateAction,
                                              NodeIndexCreatedAction nodeIndexCreatedAction, NodeIndexDeletedAction nodeIndexDeletedAction) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.shardStateAction = shardStateAction;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public IndicesClusterStateService start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        clusterService.add(this);
        return this;
    }

    @Override public IndicesClusterStateService stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        clusterService.remove(this);
        return this;
    }

    @Override public void close() throws ElasticSearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    @Override public void clusterChanged(final ClusterChangedEvent event) {
        if (!indicesService.changesAllowed())
            return;

        MetaData metaData = event.state().metaData();
        // first, go over and create and indices that needs to be created
        for (final IndexMetaData indexMetaData : metaData) {
            if (!indicesService.hasIndex(indexMetaData.index())) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Index [{}]: Creating", indexMetaData.index());
                }
                indicesService.createIndex(indexMetaData.index(), indexMetaData.settings(), event.state().nodes().localNode().id());
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        nodeIndexCreatedAction.nodeIndexCreated(indexMetaData.index(), event.state().nodes().localNodeId());
                    }
                });
            }
        }

        RoutingTable routingTable = event.state().routingTable();

        RoutingNode routingNodes = event.state().routingNodes().nodesToShards().get(event.state().nodes().localNodeId());
        if (routingNodes != null) {
            applyShards(routingNodes, routingTable, event.state().nodes());
        }

        // go over and update mappings
        for (IndexMetaData indexMetaData : metaData) {
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
                            logger.debug("Index [" + index + "] Adding mapping [" + mappingType + "], source [" + mappingSource + "]");
                        }
                        mapperService.add(mappingType, mappingSource);
                    } else {
                        DocumentMapper existingMapper = mapperService.documentMapper(mappingType);
                        if (!mappingSource.equals(existingMapper.mappingSource())) {
                            // mapping changed, update it
                            if (logger.isDebugEnabled()) {
                                logger.debug("Index [" + index + "] Updating mapping [" + mappingType + "], source [" + mappingSource + "]");
                            }
                            mapperService.add(mappingType, mappingSource);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to add mapping [" + mappingType + "], source [" + mappingSource + "]", e);
                }
            }
        }

        // go over and delete either all indices or specific shards
        for (final String index : indicesService.indices()) {
            if (metaData.index(index) == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Index [{}]: Deleting", index);
                }
                indicesService.deleteIndex(index);
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        nodeIndexDeletedAction.nodeIndexDeleted(index, event.state().nodes().localNodeId());
                    }
                });
            } else if (routingNodes != null) {
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
                            logger.debug("Index [{}]: Deleting shard [{}]", index, existingShardId);
                        }
                        indexService.deleteShard(existingShardId);
                    }
                }
            }
        }
    }

    private void applyShards(final RoutingNode routingNodes, final RoutingTable routingTable, final Nodes nodes) throws ElasticSearchException {
        if (!indicesService.changesAllowed())
            return;

        for (final ShardRouting shardRouting : routingNodes) {
            final IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());

            final int shardId = shardRouting.id();

            if (!indexService.hasShard(shardId) && shardRouting.started()) {
                // the master thinks we are started, but we don't have this shard at all, mark it as failed
                logger.warn("[" + shardRouting.index() + "][" + shardRouting.shardId().id() + "] Master " + nodes.masterNode() + " marked shard as started, but shard have not been created, mark shard as failed");
                shardStateAction.shardFailed(shardRouting);
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

    private void applyInitializingShard(final RoutingTable routingTable, final Nodes nodes, final ShardRouting shardRouting) throws ElasticSearchException {
        final IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());
        final int shardId = shardRouting.id();

        if (indexService.hasShard(shardId)) {
            IndexShard indexShard = indexService.shardSafe(shardId);
            if (indexShard.state() == IndexShardState.STARTED) {
                // the master thinks we are initializing, but we are already started
                // (either master failover, or a cluster event before we managed to tell the master we started), mark us as started
                if (logger.isTraceEnabled()) {
                    logger.trace("[" + shardRouting.index() + "][" + shardRouting.shardId().id() + "] Master " + nodes.masterNode() + " marked shard as initializing, but shard already started, mark shard as started");
                }
                shardStateAction.shardStarted(shardRouting);
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
                    logger.debug("Index [{}]: Creating shard [{}]", shardRouting.index(), shardId);
                }
                InternalIndexShard indexShard = (InternalIndexShard) indexService.createShard(shardId);
                indexShard.routingEntry(shardRouting);
            } catch (IndexShardAlreadyExistsException e) {
                // ignore this, the method call can happen several times
            } catch (Exception e) {
                logger.warn("Failed to create shard for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e);
                try {
                    indexService.deleteShard(shardId);
                } catch (Exception e1) {
                    logger.warn("Failed to delete shard after failed creation for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e1);
                }
                shardStateAction.shardFailed(shardRouting);
                return;
            }
        }
        final InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(shardId);

        if (indexShard.ignoreRecoveryAttempt()) {
            // we are already recovering (we can get to this state since the cluster event can happen several
            // times while we recover)
            return;
        }

        threadPool.execute(new Runnable() {
            @Override public void run() {
                // recheck here, since the cluster event can be called
                if (indexShard.ignoreRecoveryAttempt()) {
                    return;
                }
                try {
                    RecoveryAction recoveryAction = indexService.shardInjector(shardId).getInstance(RecoveryAction.class);
                    if (!shardRouting.primary()) {
                        // recovery from primary
                        IndexShardRoutingTable shardRoutingTable = routingTable.index(shardRouting.index()).shard(shardRouting.id());
                        for (ShardRouting entry : shardRoutingTable) {
                            if (entry.primary() && entry.started()) {
                                // only recover from started primary, if we can't find one, we will do it next round
                                Node node = nodes.get(entry.currentNodeId());
                                try {
                                    // we are recovering a backup from a primary, so no need to mark it as relocated
                                    recoveryAction.startRecovery(nodes.localNode(), node, false);
                                    shardStateAction.shardStarted(shardRouting);
                                } catch (IgnoreRecoveryException e) {
                                    // that's fine, since we might be called concurrently, just ignore this
                                    break;
                                }
                                break;
                            }
                        }
                    } else {
                        if (shardRouting.relocatingNodeId() == null) {
                            // we are the first primary, recover from the gateway
                            IndexShardGatewayService shardGatewayService = indexService.shardInjector(shardId).getInstance(IndexShardGatewayService.class);
                            try {
                                shardGatewayService.recover();
                                shardStateAction.shardStarted(shardRouting);
                            } catch (IgnoreGatewayRecoveryException e) {
                                // that's fine, we might be called concurrently, just ignore this, we already recovered
                            }
                        } else {
                            // relocating primaries, recovery from the relocating shard
                            Node node = nodes.get(shardRouting.relocatingNodeId());
                            try {
                                // we mark the primary we are going to recover from as relocated
                                recoveryAction.startRecovery(nodes.localNode(), node, true);
                                shardStateAction.shardStarted(shardRouting);
                            } catch (IgnoreRecoveryException e) {
                                // that's fine, since we might be called concurrently, just ignore this, we are already recovering
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to start shard for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e);
                    if (indexService.hasShard(shardId)) {
                        try {
                            indexService.deleteShard(shardId);
                        } catch (Exception e1) {
                            logger.warn("Failed to delete shard after failed startup for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e1);
                        }
                    }
                    try {
                        shardStateAction.shardFailed(shardRouting);
                    } catch (Exception e1) {
                        logger.warn("Failed to mark shard as failed after a failed start for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e);
                    }
                }
            }
        });
    }
}
