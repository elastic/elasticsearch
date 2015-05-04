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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.util.*;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 */
public class RecoverySource extends AbstractComponent {

    public static class Actions {
        public static final String START_RECOVERY = "internal:index/shard/recovery/start_recovery";
    }

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final RecoverySettings recoverySettings;
    private final MappingUpdatedAction mappingUpdatedAction;

    private final ClusterService clusterService;

    private final OngoingRecoveres ongoingRecoveries = new OngoingRecoveres();


    @Inject
    public RecoverySource(Settings settings, TransportService transportService, IndicesService indicesService,
                          RecoverySettings recoverySettings, MappingUpdatedAction mappingUpdatedAction, ClusterService clusterService) {
        super(settings);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.clusterService = clusterService;
        this.indicesService.indicesLifecycle().addListener(new IndicesLifecycle.Listener() {
            @Override
            public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                               @IndexSettings Settings indexSettings) {
                if (indexShard != null) {
                    ongoingRecoveries.cancel(indexShard, "shard is closed");
                }
            }
        });

        this.recoverySettings = recoverySettings;

        transportService.registerRequestHandler(Actions.START_RECOVERY, StartRecoveryRequest.class, ThreadPool.Names.GENERIC, new StartRecoveryTransportRequestHandler());
    }

    private RecoveryResponse recover(final StartRecoveryRequest request) {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().index().name());
        final IndexShard shard = indexService.shardSafe(request.shardId().id());

        // starting recovery from that our (the source) shard state is marking the shard to be in recovery mode as well, otherwise
        // the index operations will not be routed to it properly
        RoutingNode node = clusterService.state().readOnlyRoutingNodes().node(request.targetNode().id());
        if (node == null) {
            logger.debug("delaying recovery of {} as source node {} is unknown", request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source node does not have the node [" + request.targetNode() + "] in its state yet..");
        }
        ShardRouting targetShardRouting = null;
        for (ShardRouting shardRouting : node) {
            if (shardRouting.shardId().equals(request.shardId())) {
                targetShardRouting = shardRouting;
                break;
            }
        }
        if (targetShardRouting == null) {
            logger.debug("delaying recovery of {} as it is not listed as assigned to target node {}", request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
        }
        if (!targetShardRouting.initializing()) {
            logger.debug("delaying recovery of {} as it is not listed as initializing on the target node {}. known shards state is [{}]",
                    request.shardId(), request.targetNode(), targetShardRouting.state());
            throw new DelayRecoveryException("source node has the state of the target shard to be [" + targetShardRouting.state() + "], expecting to be [initializing]");
        }

        logger.trace("[{}][{}] starting recovery to {}, mark_as_relocated {}", request.shardId().index().name(), request.shardId().id(), request.targetNode(), request.markAsRelocated());
        final RecoverySourceHandler handler;
        if (IndexMetaData.isOnSharedFilesystem(shard.indexSettings())) {
            handler = new SharedFSRecoverySourceHandler(shard, request, recoverySettings, transportService, clusterService, indicesService, mappingUpdatedAction, logger);
        } else {
            handler = new RecoverySourceHandler(shard, request, recoverySettings, transportService, clusterService, indicesService, mappingUpdatedAction, logger);
        }
        ongoingRecoveries.add(shard, handler);
        try {
            shard.recover(handler);
        } finally {
            ongoingRecoveries.remove(shard, handler);
        }
        return handler.getResponse();
    }

    class StartRecoveryTransportRequestHandler implements TransportRequestHandler<StartRecoveryRequest> {
        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel) throws Exception {
            RecoveryResponse response = recover(request);
            channel.sendResponse(response);
        }
    }


    private static final class OngoingRecoveres {
        private final Map<IndexShard, Set<RecoverySourceHandler>> ongoingRecoveries = new HashMap<>();

        synchronized void add(IndexShard shard, RecoverySourceHandler handler) {
            Set<RecoverySourceHandler> shardRecoveryHandlers = ongoingRecoveries.get(shard);
            if (shardRecoveryHandlers == null) {
                shardRecoveryHandlers = new HashSet<>();
                ongoingRecoveries.put(shard, shardRecoveryHandlers);
            }
            assert shardRecoveryHandlers.contains(handler) == false : "Handler was already registered [" + handler + "]";
            shardRecoveryHandlers.add(handler);
            shard.recoveryStats().incCurrentAsSource();
        }

        synchronized void remove(IndexShard shard, RecoverySourceHandler handler) {
            final Set<RecoverySourceHandler> shardRecoveryHandlers = ongoingRecoveries.get(shard);
            assert shardRecoveryHandlers != null : "Shard was not registered [" + shard + "]";
            boolean remove = shardRecoveryHandlers.remove(handler);
            assert remove : "Handler was not registered [" + handler + "]";
            if (remove) {
                shard.recoveryStats().decCurrentAsSource();
            }
            if (shardRecoveryHandlers.isEmpty()) {
                ongoingRecoveries.remove(shard);
            }
        }

        synchronized void cancel(IndexShard shard, String reason) {
            final Set<RecoverySourceHandler> shardRecoveryHandlers = ongoingRecoveries.get(shard);
            if (shardRecoveryHandlers != null) {
                final List<Exception> failures = new ArrayList<>();
                for (RecoverySourceHandler handlers : shardRecoveryHandlers) {
                    try {
                        handlers.cancel(reason);
                    } catch (Exception ex) {
                        failures.add(ex);
                    } finally {
                        shard.recoveryStats().decCurrentAsSource();
                    }
                }
                ExceptionsHelper.maybeThrowRuntimeAndSuppress(failures);
            }
        }
    }
}

