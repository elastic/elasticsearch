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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
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

import java.util.function.Supplier;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 */
public abstract class TransportWriteAction<
            Request extends ReplicatedWriteRequest<Request>,
            Response extends ReplicationResponse & WriteResponse
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
        IndexShard indexShard = indexShard(request);
        WriteResult<Response> result = onPrimaryShard(request, indexShard);
        return new WritePrimaryResult(request, result.getResponse(), result.getLocation(), indexShard);
    }

    @Override
    protected final WriteReplicaResult shardOperationOnReplica(Request request) {
        IndexShard indexShard = indexShard(request);
        Translog.Location location = onReplicaShard(request, indexShard);
        return new WriteReplicaResult(indexShard, request, location);
    }

    /**
     * Fetch the IndexShard for the request. Protected so it can be mocked in tests.
     */
    protected IndexShard indexShard(Request request) {
        final ShardId shardId = request.shardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getShard(shardId.id());
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
        }

        public Response getResponse() {
            return response;
        }

        public Translog.Location getLocation() {
            return location;
        }
    }

    /**
     * Result of taking the action on the primary.
     */
    class WritePrimaryResult extends PrimaryResult implements RespondingWriteResult {
        boolean finishedAsyncActions;
        ActionListener<Response> listener = null;

        public WritePrimaryResult(Request request, Response finalResponse,
                                  @Nullable Translog.Location location,
                                  IndexShard indexShard) {
            super(request, finalResponse);
            /*
             * We call this before replication because this might wait for a refresh and that can take a while. This way we wait for the
             * refresh in parallel on the primary and on the replica.
             */
            postWriteActions(indexShard, request, location, this, logger);
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
            if (finishedAsyncActions && listener != null) {
                super.respond(listener);
            }
        }

        @Override
        public synchronized void respondAfterAsyncAction(boolean forcedRefresh) {
            finalResponse.setForcedRefresh(forcedRefresh);
            finishedAsyncActions = true;
            respondIfPossible();
        }
    }

    /**
     * Result of taking the action on the replica.
     */
    class WriteReplicaResult extends ReplicaResult implements RespondingWriteResult {
        boolean finishedAsyncActions;
        private ActionListener<TransportResponse.Empty> listener;

        public WriteReplicaResult(IndexShard indexShard, ReplicatedWriteRequest<?> request, Translog.Location location) {
            postWriteActions(indexShard, request, location, this, logger);
        }

        @Override
        public void respond(ActionListener<TransportResponse.Empty> listener) {
            this.listener = listener;
            respondIfPossible();
        }

        /**
         * Respond if the refresh has occurred and the listener is ready. Always called while synchronized on {@code this}.
         */
        protected void respondIfPossible() {
            if (finishedAsyncActions && listener != null) {
                super.respond(listener);
            }
        }

        @Override
        public synchronized void respondAfterAsyncAction(boolean forcedRefresh) {
            finishedAsyncActions = true;
            respondIfPossible();
        }
    }

    private interface RespondingWriteResult {
        void respondAfterAsyncAction(boolean forcedRefresh);
    }

    static void postWriteActions(final IndexShard indexShard,
                                 final WriteRequest<?> request,
                                 @Nullable final Translog.Location location,
                                 final RespondingWriteResult respond,
                                 final ESLogger logger) {
        boolean pendingOps = false;
        boolean immediateRefresh = false;
        switch (request.getRefreshPolicy()) {
            case IMMEDIATE:
                indexShard.refresh("refresh_flag_index");
                immediateRefresh = true;
                break;
            case WAIT_UNTIL:
                if (location != null) {
                    pendingOps = true;
                    indexShard.addRefreshListener(location, forcedRefresh -> {
                        if (forcedRefresh) {
                            logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
                        }
                        respond.respondAfterAsyncAction(forcedRefresh);
                    });
                }
                break;
            case NONE:
                break;
        }
        boolean fsyncTranslog = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null;
        if (fsyncTranslog) {
            indexShard.sync(location);
        }
        indexShard.maybeFlush();
        if (pendingOps == false) {
            respond.respondAfterAsyncAction(immediateRefresh);
        }
    }
}
