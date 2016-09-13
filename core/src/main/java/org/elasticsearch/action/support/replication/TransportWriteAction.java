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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
            new AsyncAfterWriteAction(indexShard, request, location, this, logger).run();
        }

        @Override
        public synchronized void respond(ActionListener<Response> listener) {
            this.listener = listener;
            respondIfPossible(null);
        }

        /**
         * Respond if the refresh has occurred and the listener is ready. Always called while synchronized on {@code this}.
         */
        protected void respondIfPossible(Exception ex) {
            if (finishedAsyncActions && listener != null) {
                if (ex == null) {
                    super.respond(listener);
                } else {
                    listener.onFailure(ex);
                }
            }
        }

        public synchronized void onFailure(Exception exception) {
            finishedAsyncActions = true;
            respondIfPossible(exception);
        }

        @Override
        public synchronized void onSuccess(boolean forcedRefresh) {
            finalResponse.setForcedRefresh(forcedRefresh);
            finishedAsyncActions = true;
            respondIfPossible(null);
        }
    }

    /**
     * Result of taking the action on the replica.
     */
    class WriteReplicaResult extends ReplicaResult implements RespondingWriteResult {
        boolean finishedAsyncActions;
        private ActionListener<TransportResponse.Empty> listener;

        public WriteReplicaResult(IndexShard indexShard, ReplicatedWriteRequest<?> request, Translog.Location location) {
            new AsyncAfterWriteAction(indexShard, request, location, this, logger).run();
        }

        @Override
        public void respond(ActionListener<TransportResponse.Empty> listener) {
            this.listener = listener;
            respondIfPossible(null);
        }

        /**
         * Respond if the refresh has occurred and the listener is ready. Always called while synchronized on {@code this}.
         */
        protected void respondIfPossible(Exception ex) {
            if (finishedAsyncActions && listener != null) {
                if (ex == null) {
                    super.respond(listener);
                } else {
                    listener.onFailure(ex);
                }
            }
        }

        @Override
        public void onFailure(Exception ex) {
            finishedAsyncActions = true;
            respondIfPossible(ex);
        }

        @Override
        public synchronized void onSuccess(boolean forcedRefresh) {
            finishedAsyncActions = true;
            respondIfPossible(null);
        }
    }

    /**
     * callback used by {@link AsyncAfterWriteAction} to notify that all post
     * process actions have been executed
     */
    private interface RespondingWriteResult {
        /**
         * Called on successful processing of all post write actions
         * @param forcedRefresh <code>true</code> iff this write has caused a refresh
         */
        void onSuccess(boolean forcedRefresh);

        /**
         * Called on failure if a post action failed.
         */
        void onFailure(Exception ex);
    }

    /**
     * This class encapsulates post write actions like async waits for
     * translog syncs or waiting for a refresh to happen making the write operation
     * visible.
     */
    static final class AsyncAfterWriteAction {
        private final Location location;
        private final boolean waitUntilRefresh;
        private final boolean sync;
        private final AtomicInteger pendingOps = new AtomicInteger(1);
        private final AtomicBoolean refreshed = new AtomicBoolean(false);
        private final AtomicReference<Exception> syncFailure = new AtomicReference<>(null);
        private final RespondingWriteResult respond;
        private final IndexShard indexShard;
        private final WriteRequest<?> request;
        private final Logger logger;

        AsyncAfterWriteAction(final IndexShard indexShard,
                             final WriteRequest<?> request,
                             @Nullable final Translog.Location location,
                             final RespondingWriteResult respond,
                             final Logger logger) {
            this.indexShard = indexShard;
            this.request = request;
            boolean waitUntilRefresh = false;
            switch (request.getRefreshPolicy()) {
                case IMMEDIATE:
                    indexShard.refresh("refresh_flag_index");
                    refreshed.set(true);
                    break;
                case WAIT_UNTIL:
                    if (location != null) {
                        waitUntilRefresh = true;
                        pendingOps.incrementAndGet();
                    }
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("unknown refresh policy: " + request.getRefreshPolicy());
            }
            this.waitUntilRefresh = waitUntilRefresh;
            this.respond = respond;
            this.location = location;
            if ((sync = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null)) {
                pendingOps.incrementAndGet();
            }
            this.logger = logger;
            assert pendingOps.get() >= 0 && pendingOps.get() <= 3 : "pendingOpts was: " + pendingOps.get();
        }

        /** calls the response listener if all pending operations have returned otherwise it just decrements the pending opts counter.*/
        private void maybeFinish() {
            final int numPending = pendingOps.decrementAndGet();
            if (numPending == 0) {
                if (syncFailure.get() != null) {
                    respond.onFailure(syncFailure.get());
                } else {
                    respond.onSuccess(refreshed.get());
                }
            }
            assert numPending >= 0 && numPending <= 2: "numPending must either 2, 1 or 0 but was " + numPending ;
        }

        void run() {
            // we either respond immediately ie. if we we don't fsync per request or wait for refresh
            // OR we got an pass async operations on and wait for them to return to respond.
            indexShard.maybeFlush();
            maybeFinish(); // decrement the pendingOpts by one, if there is nothing else to do we just respond with success.
            if (waitUntilRefresh) {
                assert pendingOps.get() > 0;
                indexShard.addRefreshListener(location, forcedRefresh -> {
                    if (forcedRefresh) {
                        logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
                    }
                    refreshed.set(forcedRefresh);
                    maybeFinish();
                });
            }
            if (sync) {
                assert pendingOps.get() > 0;
                indexShard.sync(location, (ex) -> {
                    syncFailure.set(ex);
                    maybeFinish();
                });
            }
        }
    }
}
