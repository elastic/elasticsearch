/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Background global checkpoint sync action initiated when a shard goes inactive. This is needed because while we send the global checkpoint
 * on every replication operation, after the last operation completes the global checkpoint could advance but without a follow-up operation
 * the global checkpoint will never be synced to the replicas.
 */
public class GlobalCheckpointSyncAction extends TransportReplicationAction<
    GlobalCheckpointSyncAction.Request,
    GlobalCheckpointSyncAction.Request,
    ReplicationResponse> {

    public static String ACTION_NAME = "indices:admin/seq_no/global_checkpoint_sync";
    public static ActionType<ReplicationResponse> TYPE = new ActionType<>(ACTION_NAME, ReplicationResponse::new);

    private final Map<IndexShard, PendingSyncs<PrimaryResult<Request, ReplicationResponse>>> primarySyncs = new ConcurrentHashMap<>();
    private final Map<IndexShard, PendingSyncs<ReplicaResult>> replicaSyncs = new ConcurrentHashMap<>();
    private final Function<IndexShard, String> executorProvider;

    @Inject
    public GlobalCheckpointSyncAction(
        final Settings settings,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final ThreadPool threadPool,
        final ShardStateAction shardStateAction,
        final ActionFilters actionFilters,
        final SystemIndices systemIndices
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
            Request::new,
            Request::new,
            ThreadPool.Names.SAME
        );
        executorProvider = (shard) -> ExecutorSelector.getWriteExecutorForShard(systemIndices.getExecutorSelector(), shard);
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(
        Request request,
        IndexShard indexShard,
        ActionListener<PrimaryResult<Request, ReplicationResponse>> listener
    ) {
        scheduleSync(primarySyncs, request, indexShard, () -> new PrimaryResult<>(request, new ReplicationResponse()), listener);
    }

    @Override
    protected void shardOperationOnReplica(Request request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        scheduleSync(replicaSyncs, request, replica, ReplicaResult::new, listener);
    }

    private <R> void scheduleSync(
        Map<IndexShard, PendingSyncs<R>> pendingSyncs,
        Request request,
        IndexShard shard,
        Supplier<R> resultSupplier,
        ActionListener<R> listener
    ) {
        String executor = executorProvider.apply(shard);

        PendingSyncs<R> pending = pendingSyncs.compute(shard, (ignored, existing) -> {
            if (existing == null) {
                return new PendingSyncs<>(request);
            } else {
                existing.toComplete.add(new Tuple<>(resultSupplier, listener));
                return existing;
            }
        });

        if (pending.syncRequest == request) {
            threadPool.executor(executor).execute(new AbstractRunnable() {

                @Override
                public boolean isForceExecution() {
                    return true;
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() {
                    PendingSyncs<R> removed = pendingSyncs.remove(shard);
                    assert removed == pending;

                    ConcurrentLinkedQueue<Tuple<Supplier<R>, ActionListener<R>>> toComplete = removed.toComplete;
                    try {
                        maybeSyncTranslog(shard);
                        for (Tuple<Supplier<R>, ActionListener<R>> tuple : toComplete) {
                            ActionListener.completeWith(tuple.v2(), () -> tuple.v1().get());
                        }
                    } catch (Exception e) {
                        ActionListener.onFailure(toComplete.stream().map(Tuple::v2).collect(Collectors.toList()), e);
                    }
                }
            });
        }
    }

    private static void maybeSyncTranslog(final IndexShard indexShard) throws IOException {
        if (indexShard.getTranslogDurability() == Translog.Durability.REQUEST
            && indexShard.getLastSyncedGlobalCheckpoint() < indexShard.getLastKnownGlobalCheckpoint()) {
            indexShard.sync();
        }
    }

    private static class PendingSyncs<R> {

        private final ConcurrentLinkedQueue<Tuple<Supplier<R>, ActionListener<R>>> toComplete = new ConcurrentLinkedQueue<>();
        private final Request syncRequest;

        private PendingSyncs(Request syncRequest) {
            this.syncRequest = syncRequest;
        }
    }

    public static final class Request extends ReplicationRequest<Request> {

        private Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(final ShardId shardId) {
            super(shardId);
        }

        @Override
        public String toString() {
            return "GlobalCheckpointSyncAction.Request{"
                + "shardId="
                + shardId
                + ", timeout="
                + timeout
                + ", index='"
                + index
                + '\''
                + ", waitForActiveShards="
                + waitForActiveShards
                + "}";
        }
    }

}
