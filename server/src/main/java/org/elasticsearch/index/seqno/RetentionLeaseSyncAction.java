/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.seqno;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.ReplicationTask;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotInPrimaryModeException;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static org.elasticsearch.core.Strings.format;

/**
 * Write action responsible for syncing retention leases to replicas. This action is deliberately a write action so that if a replica misses
 * a retention lease sync then that shard will be marked as stale.
 */
public class RetentionLeaseSyncAction extends TransportWriteAction<
    RetentionLeaseSyncAction.Request,
    RetentionLeaseSyncAction.Request,
    RetentionLeaseSyncAction.Response> {

    public static final String ACTION_NAME = "indices:admin/seq_no/retention_lease_sync";
    private static final Logger LOGGER = LogManager.getLogger(RetentionLeaseSyncAction.class);

    @Inject
    public RetentionLeaseSyncAction(
        final Settings settings,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final ThreadPool threadPool,
        final ShardStateAction shardStateAction,
        final ActionFilters actionFilters,
        final IndexingPressure indexingPressure,
        final SystemIndices systemIndices,
        final ProjectResolver projectResolver
    ) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            RetentionLeaseSyncAction.Request::new,
            RetentionLeaseSyncAction.Request::new,
            new ManagementOnlyExecutorFunction(threadPool),
            PrimaryActionExecution.Force,
            indexingPressure,
            systemIndices,
            projectResolver,
            ReplicaActionExecution.BypassCircuitBreaker
        );
    }

    @Override
    protected void doExecute(Task parentTask, Request request, ActionListener<Response> listener) {
        assert false : "use RetentionLeaseSyncAction#sync";
    }

    final void sync(
        ShardId shardId,
        String primaryAllocationId,
        long primaryTerm,
        RetentionLeases retentionLeases,
        ActionListener<ReplicationResponse> listener
    ) {
        try (var ignore = threadPool.getThreadContext().newEmptySystemContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            final Request request = new Request(shardId, retentionLeases);
            final ReplicationTask task = (ReplicationTask) taskManager.register("transport", "retention_lease_sync", request);
            transportService.sendChildRequest(
                clusterService.localNode(),
                transportPrimaryAction,
                new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
                task,
                transportOptions,
                new TransportResponseHandler<ReplicationResponse>() {
                    @Override
                    public ReplicationResponse read(StreamInput in) throws IOException {
                        return newResponseInstance(in);
                    }

                    @Override
                    public Executor executor() {
                        return TransportResponseHandler.TRANSPORT_WORKER;
                    }

                    @Override
                    public void handleResponse(ReplicationResponse response) {
                        task.setPhase("finished");
                        taskManager.unregister(task);
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException e) {
                        LOGGER.log(getExceptionLogLevel(e), () -> format("%s retention lease sync failed", shardId), e);
                        task.setPhase("finished");
                        taskManager.unregister(task);
                        listener.onFailure(e);
                    }
                }
            );
        }
    }

    static Level getExceptionLogLevel(Exception e) {
        return ExceptionsHelper.unwrap(
            e,
            NodeClosedException.class,
            IndexNotFoundException.class,
            AlreadyClosedException.class,
            IndexShardClosedException.class,
            ShardNotInPrimaryModeException.class,
            ReplicationOperation.RetryOnPrimaryException.class
        ) == null ? Level.WARN : Level.DEBUG;
    }

    @Override
    protected void dispatchedShardOperationOnPrimary(
        Request request,
        IndexShard primary,
        ActionListener<PrimaryResult<Request, Response>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            assert request.waitForActiveShards().equals(ActiveShardCount.NONE) : request.waitForActiveShards();
            Objects.requireNonNull(request);
            Objects.requireNonNull(primary);
            primary.persistRetentionLeases();
            return new WritePrimaryResult<>(request, new Response(), null, primary, LOGGER, postWriteRefresh);
        });
    }

    @Override
    protected void dispatchedShardOperationOnReplica(Request request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            Objects.requireNonNull(request);
            Objects.requireNonNull(replica);
            replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
            replica.persistRetentionLeases();
            return new WriteReplicaResult<>(request, null, null, replica, LOGGER);
        });
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return null;
    }

    public static final class Request extends ReplicatedWriteRequest<Request> {

        private final RetentionLeases retentionLeases;

        public RetentionLeases getRetentionLeases() {
            return retentionLeases;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            retentionLeases = new RetentionLeases(in);
        }

        public Request(final ShardId shardId, final RetentionLeases retentionLeases) {
            super(Objects.requireNonNull(shardId));
            this.retentionLeases = Objects.requireNonNull(retentionLeases);
            waitForActiveShards(ActiveShardCount.NONE);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(Objects.requireNonNull(out));
            retentionLeases.writeTo(out);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new ReplicationTask(id, type, action, "retention_lease_sync shardId=" + shardId, parentTaskId, headers);
        }

        @Override
        public String toString() {
            return "RetentionLeaseSyncAction.Request{"
                + "retentionLeases="
                + retentionLeases
                + ", shardId="
                + shardId
                + ", timeout="
                + timeout
                + ", index='"
                + index
                + '\''
                + ", waitForActiveShards="
                + waitForActiveShards
                + '}';
        }

    }

    public static final class Response extends ReplicationResponse implements WriteResponse {

        public Response() {}

        Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void setForcedRefresh(final boolean forcedRefresh) {
            // ignore
        }

    }

    @Override
    protected Response newResponseInstance(StreamInput in) throws IOException {
        return new Response(in);
    }

    /**
     * A {@code BiFunction<ExecutorSelector, IndexShard, Executor>} for passing to the super constructor which always returns the
     * MANAGEMENT executor (but looks it up once at construction time and caches the result, unlike how the obvious lambda would work).
     */
    private static class ManagementOnlyExecutorFunction implements BiFunction<ExecutorSelector, IndexShard, Executor> {
        private final Executor executor;

        ManagementOnlyExecutorFunction(ThreadPool threadPool) {
            executor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
        }

        @Override
        public Executor apply(ExecutorSelector executorSelector, IndexShard indexShard) {
            return executor;
        }
    }

}
