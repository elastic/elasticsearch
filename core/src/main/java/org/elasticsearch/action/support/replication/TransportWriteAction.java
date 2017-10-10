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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 * Allows performing async actions (e.g. refresh) after performing write operations on primary and replica shards
 */
public abstract class TransportWriteAction<
            Request extends ReplicatedWriteRequest<Request>,
            ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
            Response extends ReplicationResponse & WriteResponse
        > extends TransportReplicationAction<Request, ReplicaRequest, Response> {

    protected TransportWriteAction(Settings settings, String actionName, TransportService transportService,
            ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request,
                                   Supplier<ReplicaRequest> replicaRequest, String executor) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                indexNameExpressionResolver, request, replicaRequest, executor, true);
    }

    /** Syncs operation result to the translog or throws a shard not available failure */
    protected static Location syncOperationResultOrThrow(final Engine.Result operationResult,
                                                         final Location currentLocation) throws Exception {
        final Location location;
        if (operationResult.hasFailure()) {
            // check if any transient write operation failures should be bubbled up
            Exception failure = operationResult.getFailure();
            assert failure instanceof MapperParsingException : "expected mapper parsing failures. got " + failure;
            if (!TransportActions.isShardNotAvailableException(failure)) {
                throw failure;
            } else {
                location = currentLocation;
            }
        } else {
            location = locationToSync(currentLocation, operationResult.getTranslogLocation());
        }
        return location;
    }

    protected static Location locationToSync(Location current, Location next) {
        /* here we are moving forward in the translog with each operation. Under the hood this might
         * cross translog files which is ok since from the user perspective the translog is like a
         * tape where only the highest location needs to be fsynced in order to sync all previous
         * locations even though they are not in the same file. When the translog rolls over files
         * the previous file is fsynced on after closing if needed.*/
        assert next != null : "next operation can't be null";
        assert current == null || current.compareTo(next) < 0 :
                "translog locations are not increasing";
        return next;
    }

    @Override
    protected ReplicationOperation.Replicas newReplicasProxy(long primaryTerm) {
        return new WriteActionReplicasProxy(primaryTerm);
    }

    /**
     * Called on the primary with a reference to the primary {@linkplain IndexShard} to modify.
     *
     * @return the result of the operation on primary, including current translog location and operation response and failure
     * async refresh is performed on the <code>primary</code> shard according to the <code>Request</code> refresh policy
     */
    @Override
    protected abstract WritePrimaryResult<ReplicaRequest, Response> shardOperationOnPrimary(
            Request request, IndexShard primary) throws Exception;

    /**
     * Called once per replica with a reference to the replica {@linkplain IndexShard} to modify.
     *
     * @return the result of the operation on replica, including current translog location and operation response and failure
     * async refresh is performed on the <code>replica</code> shard according to the <code>ReplicaRequest</code> refresh policy
     */
    @Override
    protected abstract WriteReplicaResult<ReplicaRequest> shardOperationOnReplica(
            ReplicaRequest request, IndexShard replica) throws Exception;

    /**
     * Result of taking the action on the primary.
     *
     * NOTE: public for testing
     */
    public static class WritePrimaryResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
            Response extends ReplicationResponse & WriteResponse> extends PrimaryResult<ReplicaRequest, Response>
            implements RespondingWriteResult {
        boolean finishedAsyncActions;
        public final Location location;
        ActionListener<Response> listener = null;

        public WritePrimaryResult(ReplicaRequest request, @Nullable Response finalResponse,
                                  @Nullable Location location, @Nullable Exception operationFailure,
                                  IndexShard primary, Logger logger) {
            super(request, finalResponse, operationFailure);
            this.location = location;
            assert location == null || operationFailure == null
                    : "expected either failure to be null or translog location to be null, " +
                    "but found: [" + location + "] translog location and [" + operationFailure + "] failure";
            if (operationFailure != null) {
                this.finishedAsyncActions = true;
            } else {
                /*
                 * We call this before replication because this might wait for a refresh and that can take a while.
                 * This way we wait for the refresh in parallel on the primary and on the replica.
                 */
                new AsyncAfterWriteAction(primary, request, location, this, logger).run();
            }
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
            finalResponseIfSuccessful.setForcedRefresh(forcedRefresh);
            finishedAsyncActions = true;
            respondIfPossible(null);
        }
    }

    /**
     * Result of taking the action on the replica.
     */
    protected static class WriteReplicaResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>>
            extends ReplicaResult implements RespondingWriteResult {
        public final Location location;
        boolean finishedAsyncActions;
        private ActionListener<TransportResponse.Empty> listener;

        public WriteReplicaResult(ReplicaRequest request, @Nullable Location location,
                                  @Nullable Exception operationFailure, IndexShard replica, Logger logger) {
            super(operationFailure);
            this.location = location;
            if (operationFailure != null) {
                this.finishedAsyncActions = true;
            } else {
                new AsyncAfterWriteAction(replica, request, location, this, logger).run();
            }
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

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    @Override
    protected ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    /**
     * callback used by {@link AsyncAfterWriteAction} to notify that all post
     * process actions have been executed
     */
    interface RespondingWriteResult {
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
            /*
             * We either respond immediately (i.e., if we do not fsync per request or wait for
             * refresh), or we there are past async operations and we wait for them to return to
             * respond.
             */
            indexShard.afterWriteOperation();
            // decrement pending by one, if there is nothing else to do we just respond with success
            maybeFinish();
            if (waitUntilRefresh) {
                assert pendingOps.get() > 0;
                indexShard.addRefreshListener(location, forcedRefresh -> {
                    if (forcedRefresh) {
                        logger.warn(
                                "block until refresh ran out of slots and forced a refresh: [{}]",
                                request);
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

    /**
     * A proxy for <b>write</b> operations that need to be performed on the
     * replicas, where a failure to execute the operation should fail
     * the replica shard and/or mark the replica as stale.
     *
     * This extends {@code TransportReplicationAction.ReplicasProxy} to do the
     * failing and stale-ing.
     */
    class WriteActionReplicasProxy extends ReplicasProxy {

        WriteActionReplicasProxy(long primaryTerm) {
            super(primaryTerm);
        }

        @Override
        public void failShardIfNeeded(ShardRouting replica, String message, Exception exception,
                                      Runnable onSuccess, Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {

            logger.warn((org.apache.logging.log4j.util.Supplier<?>)
                    () -> new ParameterizedMessage("[{}] {}", replica.shardId(), message), exception);
            shardStateAction.remoteShardFailed(replica.shardId(), replica.allocationId().getId(), primaryTerm, message, exception,
                    createListener(onSuccess, onPrimaryDemoted, onIgnoredFailure));
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, Runnable onSuccess,
                                                 Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, "mark copy as stale", null,
                    createListener(onSuccess, onPrimaryDemoted, onIgnoredFailure));
        }

        private ShardStateAction.Listener createListener(final Runnable onSuccess, final Consumer<Exception> onPrimaryDemoted,
                                                         final Consumer<Exception> onIgnoredFailure) {
            return new ShardStateAction.Listener() {
                @Override
                public void onSuccess() {
                    onSuccess.run();
                }

                @Override
                public void onFailure(Exception shardFailedError) {
                    if (shardFailedError instanceof ShardStateAction.NoLongerPrimaryShardException) {
                        onPrimaryDemoted.accept(shardFailedError);
                    } else {
                        // these can occur if the node is shutting down and are okay
                        // any other exception here is not expected and merits investigation
                        assert shardFailedError instanceof TransportException ||
                                shardFailedError instanceof NodeClosedException : shardFailedError;
                        onIgnoredFailure.accept(shardFailedError);
                    }
                }
            };
        }
    }
}
