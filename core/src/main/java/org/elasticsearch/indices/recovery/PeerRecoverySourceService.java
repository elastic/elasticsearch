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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 */
public class PeerRecoverySourceService extends AbstractComponent implements IndexEventListener {

    public static class Actions {
        public static final String START_RECOVERY = "internal:index/shard/recovery/start_recovery";
    }

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final RecoverySettings recoverySettings;

    final OngoingRecoveries ongoingRecoveries = new OngoingRecoveries();

    @Inject
    public PeerRecoverySourceService(Settings settings, TransportService transportService, IndicesService indicesService,
                                     RecoverySettings recoverySettings) {
        super(settings);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        transportService.registerRequestHandler(Actions.START_RECOVERY, StartRecoveryRequest::new, ThreadPool.Names.GENERIC, new StartRecoveryTransportRequestHandler());
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                       Settings indexSettings) {
        if (indexShard != null) {
            ongoingRecoveries.cancel(indexShard, "shard is closed");
        }
    }

    private RecoveryResponse recover(final StartRecoveryRequest request) throws IOException {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());

        final ShardRouting routingEntry = shard.routingEntry();

        if (routingEntry.primary() == false || routingEntry.active() == false) {
            throw new DelayRecoveryException("source shard [" + routingEntry + "] is not an active primary");
        }

        if (request.isPrimaryRelocation() && (routingEntry.relocating() == false || routingEntry.relocatingNodeId().equals(request.targetNode().getId()) == false)) {
            logger.debug("delaying recovery of {} as source shard is not marked yet as relocating to {}", request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source shard is not marked yet as relocating to [" + request.targetNode() + "]");
        }

        RecoverySourceHandler handler = ongoingRecoveries.addNewRecovery(request, shard);
        logger.trace("[{}][{}] starting recovery to {}", request.shardId().getIndex().getName(), request.shardId().id(), request.targetNode());
        try {
            return handler.recoverToTarget();
        } finally {
            ongoingRecoveries.remove(shard, handler);
        }
    }

    class StartRecoveryTransportRequestHandler implements TransportRequestHandler<StartRecoveryRequest> {
        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel) throws Exception {
            RecoveryResponse response = recover(request);
            channel.sendResponse(response);
        }
    }

    final class OngoingRecoveries {
        private final Map<IndexShard, ShardRecoveryContext> ongoingRecoveries = new HashMap<>();

        synchronized RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, IndexShard shard) {
            final ShardRecoveryContext shardContext = ongoingRecoveries.computeIfAbsent(shard, s -> new ShardRecoveryContext());
            RecoverySourceHandler handler = shardContext.addNewRecovery(request, shard);
            shard.recoveryStats().incCurrentAsSource();
            return handler;
        }

        synchronized void remove(IndexShard shard, RecoverySourceHandler handler) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            assert shardRecoveryContext != null : "Shard was not registered [" + shard + "]";
            boolean remove = shardRecoveryContext.recoveryHandlers.remove(handler);
            assert remove : "Handler was not registered [" + handler + "]";
            if (remove) {
                shard.recoveryStats().decCurrentAsSource();
            }
            if (shardRecoveryContext.recoveryHandlers.isEmpty()) {
                ongoingRecoveries.remove(shard);
            }
        }

        synchronized void cancel(IndexShard shard, String reason) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            if (shardRecoveryContext != null) {
                final List<Exception> failures = new ArrayList<>();
                for (RecoverySourceHandler handlers : shardRecoveryContext.recoveryHandlers) {
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

        private final class ShardRecoveryContext {
            final Set<RecoverySourceHandler> recoveryHandlers = new HashSet<>();

            /**
             * Adds recovery source handler.
             */
            synchronized RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, IndexShard shard) {
                for (RecoverySourceHandler existingHandler : recoveryHandlers) {
                    if (existingHandler.getRequest().targetAllocationId().equals(request.targetAllocationId())) {
                        throw new DelayRecoveryException("recovery with same target already registered, waiting for " +
                            "previous recovery attempt to be cancelled or completed");
                    }
                }
                RecoverySourceHandler handler = createRecoverySourceHandler(request, shard);
                recoveryHandlers.add(handler);
                return handler;
            }

            private RecoverySourceHandler createRecoverySourceHandler(StartRecoveryRequest request, IndexShard shard) {
                RecoverySourceHandler handler;
                final RemoteRecoveryTargetHandler recoveryTarget =
                    new RemoteRecoveryTargetHandler(request.recoveryId(), request.shardId(), transportService,
                        request.targetNode(), recoverySettings, throttleTime -> shard.recoveryStats().addThrottleTime(throttleTime));
                handler = new RecoverySourceHandler(shard, recoveryTarget, request, recoverySettings.getChunkSize().bytesAsInt(), settings);
                return handler;
            }
        }
    }
}

