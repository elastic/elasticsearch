/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 * Allows performing async actions (e.g. refresh) after performing write operations on primary and replica shards
 */
public abstract class TransportWriteAction<
    Request extends ReplicatedWriteRequest<Request>,
    ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
    Response extends ReplicationResponse & WriteResponse> extends TransportReplicationAction<Request, ReplicaRequest, Response> {

    protected final IndexingPressure indexingPressure;
    protected final SystemIndices systemIndices;
    protected final ExecutorSelector executorSelector;

    protected final PostWriteRefresh postWriteRefresh;
    private final BiFunction<ExecutorSelector, IndexShard, Executor> executorFunction;

    protected TransportWriteAction(
        Settings settings,
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Writeable.Reader<ReplicaRequest> replicaRequest,
        BiFunction<ExecutorSelector, IndexShard, Executor> executorFunction,
        PrimaryActionExecution primaryActionExecution,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        ReplicaActionExecution replicaActionExecution
    ) {
        // We pass ThreadPool.Names.SAME to the super class as we control the dispatching to the
        // ThreadPool.Names.WRITE/ThreadPool.Names.SYSTEM_WRITE thread pools in this class.
        super(
            settings,
            actionName,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            request,
            replicaRequest,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            SyncGlobalCheckpointAfterOperation.AttemptAfterSuccess,
            primaryActionExecution,
            replicaActionExecution
        );
        this.executorFunction = executorFunction;
        this.indexingPressure = indexingPressure;
        this.systemIndices = systemIndices;
        this.executorSelector = systemIndices.getExecutorSelector();
        this.postWriteRefresh = new PostWriteRefresh(transportService);
    }

    protected Executor executor(IndexShard shard) {
        return executorFunction.apply(executorSelector, shard);
    }

    @Override
    protected Releasable checkOperationLimits(Request request) {
        return indexingPressure.markPrimaryOperationStarted(primaryOperationCount(request), primaryOperationSize(request), force(request));
    }

    protected boolean force(ReplicatedWriteRequest<?> request) {
        return forceExecutionOnPrimary || isSystemShard(request.shardId);
    }

    protected boolean isSystemShard(ShardId shardId) {
        final IndexAbstraction abstraction = clusterService.state().metadata().getIndicesLookup().get(shardId.getIndexName());
        return abstraction != null ? abstraction.isSystem() : systemIndices.isSystemIndex(shardId.getIndexName());
    }

    @Override
    protected Releasable checkPrimaryLimits(Request request, boolean rerouteWasLocal, boolean localRerouteInitiatedByNodeClient) {
        if (rerouteWasLocal) {
            // If this primary request was received from a local reroute initiated by the node client, we
            // must mark a new primary operation local to the coordinating node.
            if (localRerouteInitiatedByNodeClient) {
                return indexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(
                    primaryOperationCount(request),
                    primaryOperationSize(request)
                );
            } else {
                return () -> {};
            }
        } else {
            // If this primary request was received directly from the network, we must mark a new primary
            // operation. This happens if the write action skips the reroute step (ex: rsync) or during
            // primary delegation, after the primary relocation hand-off.
            return indexingPressure.markPrimaryOperationStarted(
                primaryOperationCount(request),
                primaryOperationSize(request),
                force(request)
            );
        }
    }

    protected long primaryOperationSize(Request request) {
        return 0;
    }

    protected int primaryOperationCount(Request request) {
        return 0;
    }

    @Override
    protected Releasable checkReplicaLimits(ReplicaRequest request) {
        return indexingPressure.markReplicaOperationStarted(replicaOperationCount(request), replicaOperationSize(request), force(request));
    }

    protected long replicaOperationSize(ReplicaRequest request) {
        return 0;
    }

    protected int replicaOperationCount(ReplicaRequest request) {
        return 0;
    }

    /** Syncs operation result to the translog or throws a shard not available failure */
    protected static Location syncOperationResultOrThrow(final Engine.Result operationResult, final Location currentLocation)
        throws Exception {
        final Location location;
        if (operationResult.getFailure() != null) {
            // check if any transient write operation failures should be bubbled up
            Exception failure = operationResult.getFailure();
            assert failure instanceof MapperParsingException : "expected mapper parsing failures. got " + failure;
            throw failure;
        } else {
            location = locationToSync(currentLocation, operationResult.getTranslogLocation());
        }
        return location;
    }

    public static Location locationToSync(Location current, Location next) {
        /* here we are moving forward in the translog with each operation. Under the hood this might
         * cross translog files which is ok since from the user perspective the translog is like a
         * tape where only the highest location needs to be fsynced in order to sync all previous
         * locations even though they are not in the same file. When the translog rolls over files
         * the previous file is fsynced on after closing if needed.*/
        assert next != null : "next operation can't be null";
        assert current == null || current.compareTo(next) < 0 : "translog locations are not increasing";
        return next;
    }

    @Override
    protected ReplicationOperation.Replicas<ReplicaRequest> newReplicasProxy() {
        return new WriteActionReplicasProxy();
    }

    /**
     * Called on the primary with a reference to the primary {@linkplain IndexShard} to modify.
     *
     * @param listener listener for the result of the operation on primary, including current translog location and operation response
     * and failure async refresh is performed on the <code>primary</code> shard according to the <code>Request</code> refresh policy
     */
    @Override
    protected void shardOperationOnPrimary(
        Request request,
        IndexShard primary,
        ActionListener<PrimaryResult<ReplicaRequest, Response>> listener
    ) {
        executorFunction.apply(executorSelector, primary).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                dispatchedShardOperationOnPrimary(request, primary, listener);
            }

            @Override
            public boolean isForceExecution() {
                return force(request);
            }
        });
    }

    protected abstract void dispatchedShardOperationOnPrimary(
        Request request,
        IndexShard primary,
        ActionListener<PrimaryResult<ReplicaRequest, Response>> listener
    );

    /**
     * Called once per replica with a reference to the replica {@linkplain IndexShard} to modify.
     *
     * @param listener listener for the result of the operation on replica, including current translog location and operation
     * response and failure async refresh is performed on the <code>replica</code> shard according to the <code>ReplicaRequest</code>
     * refresh policy
     */
    @Override
    protected void shardOperationOnReplica(ReplicaRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        executorFunction.apply(executorSelector, replica).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                dispatchedShardOperationOnReplica(request, replica, listener);
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        });
    }

    protected abstract void dispatchedShardOperationOnReplica(
        ReplicaRequest request,
        IndexShard replica,
        ActionListener<ReplicaResult> listener
    );

    /**
     * Result of taking the action on the primary.
     *
     * NOTE: public for testing
     */
    public static class WritePrimaryResult<
        ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
        Response extends ReplicationResponse & WriteResponse> extends PrimaryResult<ReplicaRequest, Response> {
        public final Location location;
        public final IndexShard primary;
        private final Logger logger;
        private final PostWriteRefresh postWriteRefresh;
        private final Consumer<Runnable> postWriteAction;

        public WritePrimaryResult(
            ReplicaRequest request,
            @Nullable Response finalResponse,
            @Nullable Location location,
            IndexShard primary,
            Logger logger,
            PostWriteRefresh postWriteRefresh
        ) {
            this(request, finalResponse, location, primary, logger, postWriteRefresh, null);
        }

        public WritePrimaryResult(
            ReplicaRequest request,
            @Nullable Response finalResponse,
            @Nullable Location location,
            IndexShard primary,
            Logger logger,
            PostWriteRefresh postWriteRefresh,
            @Nullable Consumer<Runnable> postWriteAction
        ) {
            super(request, finalResponse);
            this.location = location;
            this.primary = primary;
            this.logger = logger;
            this.postWriteRefresh = postWriteRefresh;
            this.postWriteAction = postWriteAction;
        }

        @Override
        public void runPostReplicationActions(ActionListener<Void> listener) {
            /*
             * We call this after replication because this might wait for a refresh and that can take a while.
             * This way we wait for the refresh in parallel on the primary and on the replica.
             */
            new AsyncAfterWriteAction(primary, replicaRequest(), location, new RespondingWriteResult() {
                @Override
                public void onSuccess(boolean forcedRefresh) {
                    replicationResponse.setForcedRefresh(forcedRefresh);
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception ex) {
                    listener.onFailure(ex);
                }
            }, logger, postWriteRefresh, postWriteAction).run();
        }
    }

    /**
     * Result of taking the action on the replica.
     */
    public static class WriteReplicaResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>> extends ReplicaResult {
        public final Location location;
        private final ReplicaRequest request;
        private final IndexShard replica;
        private final Logger logger;
        private final Consumer<Runnable> postWriteAction;

        public WriteReplicaResult(
            ReplicaRequest request,
            @Nullable Location location,
            @Nullable Exception operationFailure,
            IndexShard replica,
            Logger logger
        ) {
            this(request, location, operationFailure, replica, logger, null);
        }

        public WriteReplicaResult(
            ReplicaRequest request,
            @Nullable Location location,
            @Nullable Exception operationFailure,
            IndexShard replica,
            Logger logger,
            Consumer<Runnable> postWriteAction
        ) {
            super(operationFailure);
            this.location = location;
            this.request = request;
            this.replica = replica;
            this.logger = logger;
            this.postWriteAction = postWriteAction;
        }

        @Override
        public void runPostReplicaActions(ActionListener<Void> listener) {
            if (finalFailure != null) {
                listener.onFailure(finalFailure);
            } else {
                new AsyncAfterWriteAction(replica, request, location, new RespondingWriteResult() {
                    @Override
                    public void onSuccess(boolean forcedRefresh) {
                        listener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception ex) {
                        listener.onFailure(ex);
                    }
                }, logger, null, postWriteAction).run();
            }
        }
    }

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
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
        private final boolean needsRefreshAction;
        private final boolean sync;
        private final AtomicInteger pendingOps = new AtomicInteger(1);
        private final AtomicBoolean refreshed = new AtomicBoolean(false);
        // TODO: Temporary until we fail unpromotable shard
        private final AtomicReference<Exception> refreshFailure = new AtomicReference<>(null);
        private final AtomicReference<Exception> syncFailure = new AtomicReference<>(null);
        private final RespondingWriteResult respond;
        private final IndexShard indexShard;
        private final WriteRequest<?> request;
        private final Logger logger;
        private final PostWriteRefresh postWriteRefresh;
        private final Consumer<Runnable> postWriteAction;
        private final TimeValue postWriteRefreshTimeout;

        AsyncAfterWriteAction(
            final IndexShard indexShard,
            final ReplicatedWriteRequest<?> request,
            @Nullable final Translog.Location location,
            final RespondingWriteResult respond,
            final Logger logger,
            @Nullable final PostWriteRefresh postWriteRefresh,
            @Nullable final Consumer<Runnable> postWriteAction
        ) {
            this.indexShard = indexShard;
            this.request = request;
            this.needsRefreshAction = request.getRefreshPolicy() != WriteRequest.RefreshPolicy.NONE;
            this.respond = respond;
            this.location = location;
            this.postWriteRefresh = postWriteRefresh;
            this.postWriteAction = postWriteAction;
            if (needsRefreshAction) {
                pendingOps.incrementAndGet();
            }
            if ((sync = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null)) {
                pendingOps.incrementAndGet();
            }
            if (postWriteAction != null) {
                pendingOps.incrementAndGet();
            }
            this.logger = logger;
            this.postWriteRefreshTimeout = request.timeout();
            assert pendingOps.get() >= 0 && pendingOps.get() <= 3 : "pendingOps was: " + pendingOps.get();
        }

        /** calls the response listener if all pending operations have returned otherwise it just decrements the pending opts counter.*/
        private void maybeFinish() {
            final int numPending = pendingOps.decrementAndGet();
            if (numPending == 0) {
                if (syncFailure.get() != null) {
                    respond.onFailure(syncFailure.get());
                } else {
                    // TODO: Temporary until we fail unpromotable shard
                    if (refreshFailure.get() != null) {
                        respond.onFailure(refreshFailure.get());
                    } else {
                        respond.onSuccess(refreshed.get());
                    }
                }
            }
            assert numPending >= 0 && numPending <= 2 : "numPending must either 2, 1 or 0 but was " + numPending;
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
            if (needsRefreshAction) {
                assert pendingOps.get() > 0;
                ActionListener<Boolean> refreshListener = new ActionListener<>() {
                    @Override
                    public void onResponse(Boolean forceRefresh) {
                        // TODO: Maybe move this into PostWriteRefresh
                        if (forceRefresh && request.getRefreshPolicy() == WriteRequest.RefreshPolicy.WAIT_UNTIL) {
                            logger.warn("block until refresh ran out of slots and forced a refresh: [{}]", request);
                        }
                        refreshed.set(forceRefresh);
                        maybeFinish();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        refreshFailure.set(e);
                        maybeFinish();
                    }
                };
                // If the post refresh action is null, this is just the replica and we only call the static method
                if (postWriteRefresh != null) {
                    postWriteRefresh.refreshShard(
                        request.getRefreshPolicy(),
                        indexShard,
                        location,
                        refreshListener,
                        postWriteRefreshTimeout
                    );
                } else {
                    PostWriteRefresh.refreshReplicaShard(request.getRefreshPolicy(), indexShard, location, refreshListener);
                }
            }
            if (sync) {
                assert pendingOps.get() > 0;
                indexShard.syncAfterWrite(location, e -> {
                    syncFailure.set(e);
                    maybeFinish();
                });
            }
            if (postWriteAction != null) {
                postWriteAction.accept(this::maybeFinish);
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

        @Override
        public void failShardIfNeeded(
            ShardRouting replica,
            long primaryTerm,
            String message,
            Exception exception,
            ActionListener<Void> listener
        ) {
            if (TransportActions.isShardNotAvailableException(exception) == false) {
                logger.warn(() -> format("[%s] %s", replica.shardId(), message), exception);
            }
            shardStateAction.remoteShardFailed(
                replica.shardId(),
                replica.allocationId().getId(),
                primaryTerm,
                true,
                message,
                exception,
                listener
            );
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }
}
