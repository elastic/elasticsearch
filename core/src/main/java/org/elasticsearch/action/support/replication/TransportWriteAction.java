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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 */
public abstract class TransportWriteAction<
            Request extends ReplicatedWriteRequest<Request>,
            Response extends ReplicationResponse & ReplicatedWriteResponse
        > extends TransportReplicationAction<Request, Request, Response> {

    protected TransportWriteAction(Settings settings, String actionName, TransportService transportService,
            ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request,
            String executor) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                indexNameExpressionResolver, request, request, executor);
    }

    /**
     * Called on the primary with a reference to the {@linkplain IndexShard} to modify.
     */
    protected abstract WriteResult<Response> onPrimaryShard(Request request, IndexShard indexShard) throws Exception;

    /**
     * Called once per replica with a reference to the {@linkplain IndexShard} to modify.
     *
     * @return the translog location of the {@linkplain IndexShard} after the write was completed or null if no write occurred
     */
    protected abstract Translog.Location onReplicaShard(Request request, IndexShard indexShard);

    @Override
    protected final WritePrimaryResult shardOperationOnPrimary(Request request) throws Exception {
        final ShardId shardId = request.shardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        WriteResult<Response> result = onPrimaryShard(request, indexShard);
        return new WritePrimaryResult(request, result.getResponse(), result.getLocation(), indexShard);
    }

    @Override
    protected final WriteReplicaResult shardOperationOnReplica(Request request) {
        final ShardId shardId = request.shardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        Translog.Location location = onReplicaShard(request, indexShard);
        return new WriteReplicaResult(indexShard, request, location);
    }

    /**
     * Simple result from a write action. Write actions have static method to return these so they can integrate with bulk.
     */
    public static class WriteResult<Response extends ReplicationResponse> {
        private final Response response;
        private final Translog.Location location;

        public WriteResult(Response response, @Nullable Location location) {
            this.response = response;
            this.location = location;
            // Set the ShardInfo to 0 so we can safely send it to the replicas
            // NOCOMMIT this seems wrong
            response.setShardInfo(new ShardInfo());
        }

        public Response getResponse() {
            return response;
        }

        public Translog.Location getLocation() {
            return location;
        }
    }

    private class WritePrimaryResult extends PrimaryResult {
        volatile boolean refreshPending;
        volatile ActionListener<Response> listener = null;

        public WritePrimaryResult(Request request, Response finalResponse,
                                  @Nullable Translog.Location location,
                                  IndexShard indexShard) {
            super(request, finalResponse);
            refreshPending = finishWrite(indexShard, request, location, forcedRefresh -> {
                synchronized (WritePrimaryResult.this) {
                    if (forcedRefresh) {
                        finalResponse.setForcedRefresh(true);
                        logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
                    }
                    refreshPending = false;
                    respondIfPossible();
                }
            });
        }

        @Override
        public synchronized void respond(ActionListener<Response> listener) {
            this.listener = listener;
            respondIfPossible();
        }

        /**
         * Respond if the refresh has occurred and the listener is ready. Always called while synchronized on {@code this}.
         */
        protected void respondIfPossible() {
            if (refreshPending == false && listener != null) {
                super.respond(listener);
            }
        }
    }

    private class WriteReplicaResult extends ReplicaResult {
        private final IndexShard indexShard;
        private final ReplicatedWriteRequest<?> request;
        private final Translog.Location location;

        public WriteReplicaResult(IndexShard indexShard, ReplicatedWriteRequest<?> request, Translog.Location location) {
            this.indexShard = indexShard;
            this.request = request;
            this.location = location;
        }

        @Override
        public void respond(ActionListener<TransportResponse.Empty> listener) {
            boolean refreshPending = finishWrite(indexShard, request, location, forcedRefresh -> {
                if (forcedRefresh) {
                    logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
                }
                listener.onResponse(TransportResponse.Empty.INSTANCE);
            });
            if (false == refreshPending) {
                listener.onResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    /**
     * Finish up the write by syncing the translog, flushing, and refreshing or waiting for a refresh. Called on both the primary and the
     * replica.
     *
     * @param refreshListener used to signal that a refresh has made this change visible (see
     *        {@link IndexShard#addRefreshListener(Location, Consumer)}). Only called if this method returns true. Otherwise you shouldn't
     *        wait for it because it'll never be called.
     * @return true if this request should wait for the refreshListener to be called or false if this method only took synchronous actions
     *         and won't call the listener at all
     */
    private static boolean finishWrite(IndexShard indexShard, ReplicatedWriteRequest<?> request, Translog.Location location,
            Consumer<Boolean> refreshListener) {
        boolean fsyncTranslog = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null;
        if (fsyncTranslog) {
            indexShard.sync(location);
        }
        indexShard.maybeFlush();
        boolean async = false;
        switch (request.getRefreshPolicy()) {
        case IMMEDIATE:
            indexShard.refresh("refresh_flag_index");
            break;
        case WAIT_UNTIL:
            if (location != null) {
                async = true;
                indexShard.addRefreshListener(location, refreshListener);
            }
            break;
        case NONE:
            break;
        }
        return async;
    }
}
