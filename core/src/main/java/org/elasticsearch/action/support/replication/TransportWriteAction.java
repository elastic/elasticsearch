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
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
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
        }

        public Response getResponse() {
            return response;
        }

        public Translog.Location getLocation() {
            return location;
        }
    }

    private class WritePrimaryResult extends PrimaryResult implements RespondingWriteResult {
        private final ReplicatedWriteRequest<?> request;
        volatile boolean finishedWrite;
        volatile ActionListener<Response> listener = null;

        public WritePrimaryResult(Request request, Response finalResponse,
                                  @Nullable Translog.Location location,
                                  IndexShard indexShard) {
            super(request, finalResponse);
            this.request = request;
            finishWrite(indexShard, request, location);
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
            if (finishedWrite && listener != null) {
                super.respond(listener);
            }
        }

        @Override
        public void forcedRefresh() {
            finalResponse.setForcedRefresh(true);
        }

        @Override
        public void waitForRefreshForcedRefresh() {
            logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
        }

        @Override
        public synchronized void finished() {
            finishedWrite = true;
            respondIfPossible();
        }
    }

    private class WriteReplicaResult extends ReplicaResult implements RespondingWriteResult {
        private final IndexShard indexShard;
        private final ReplicatedWriteRequest<?> request;
        private final Translog.Location location;
        private ActionListener<TransportResponse.Empty> listener;

        public WriteReplicaResult(IndexShard indexShard, ReplicatedWriteRequest<?> request, Translog.Location location) {
            this.indexShard = indexShard;
            this.request = request;
            this.location = location;
        }

        @Override
        public void respond(ActionListener<TransportResponse.Empty> listener) {
            this.listener = listener;
            finishWrite(indexShard, request, location);
        }

        @Override
        public void forcedRefresh() {
            // We don't have anywhere to store this so we just throw it on the floor for now.
        }

        @Override
        public void waitForRefreshForcedRefresh() {
            logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
        }

        @Override
        public void finished() {
            listener.onResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    private interface RespondingWriteResult {
        /**
         * Finish up the write by syncing the translog, flushing, and refreshing or waiting for a refresh. Called on both the primary and
         * the replica.
         */
        default void finishWrite(IndexShard indexShard, ReplicatedWriteRequest<?> request, Translog.Location location) {
            boolean refreshPending = false;
            switch (request.getRefreshPolicy()) {
            case IMMEDIATE:
                indexShard.refresh("refresh_flag_index");
                forcedRefresh();
                break;
            case WAIT_UNTIL:
                if (location != null) {
                    refreshPending = true;
                    indexShard.addRefreshListener(location, forcedRefresh -> {
                        if (forcedRefresh) {
                            forcedRefresh();
                            waitForRefreshForcedRefresh();
                        }
                        finish(indexShard, location);
                    });
                }
                break;
            case NONE:
                break;
            }
            if (false == refreshPending) {
                finish(indexShard, location);
            }
        }

        default void finish(IndexShard indexShard, Translog.Location location) {
            boolean fsyncTranslog = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null;
            if (fsyncTranslog) {
                indexShard.sync(location);
            }
            indexShard.maybeFlush();
            finished();
        }

        /**
         * Called either when the request forces a refresh via {@link RefreshPolicy#IMMEDIATE} or when we run out of listeners slots while
         * attempting to honor {@link RefreshPolicy#WAIT_UNTIL}.
         */
        void forcedRefresh();

        /**
         * Called when we run out of listeners slots while attempting to honor {@link RefreshPolicy#WAIT_UNTIL}.
         */
        void waitForRefreshForcedRefresh();

        /**
         * Called when we are finished waiting for a refresh. This is never called if we don't wait for a refresh.
         */
        void finished();
    }
}
