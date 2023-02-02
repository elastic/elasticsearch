/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

/**
 * Extends {@link TransportReplicationAction} to support sending replica requests to unpromotable shards.
 * Subclasses can specify whether to send the replica request from the primary to either the promotable
 * and/or unpromotable shards.
 */
public abstract class TransportReplicationWithUnpromotablesAction<
    Request extends ReplicationRequest<Request>,
    ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
    Response extends ReplicationResponse> extends TransportReplicationAction<Request, ReplicaRequest, Response> {

    protected final String transportUnpromotableReplicaAction;

    protected TransportReplicationWithUnpromotablesAction(
        Settings settings,
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Writeable.Reader<ReplicaRequest> replicaRequestReader,
        String executor
    ) {
        super(
            settings,
            actionName,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            requestReader,
            replicaRequestReader,
            executor
        );

        this.transportUnpromotableReplicaAction = actionName + "[u]";
        transportService.registerRequestHandler(
            transportUnpromotableReplicaAction,
            executor,
            true,
            true,
            in -> replicaRequestReader.read(in),
            this::handleUnpromotableReplicaRequest
        );
    }

    @Override
    protected ReplicationOperation.Replicas<ReplicaRequest> newReplicasProxy() {
        return new UnpromotableReplicasProxy(getReplicaForwardOptions());
    }

    /**
     * Execute the specified unpromotable replica operation.
     *
     * @param shardRequest the request to the unpromotable replica shard
     * @param replica      the unpromotable replica shard to perform the operation on
     */
    protected abstract void shardOperationOnUnpromotableReplica(
        ReplicaRequest shardRequest,
        IndexShard replica,
        ActionListener<ReplicaResult> listener
    );

    /**
     * Returns whether to send the replica request to promotable and/or unpromotable replica shards.
     */
    protected abstract ReplicationOperation.ReplicaForwardOptions getReplicaForwardOptions();

    protected void handleUnpromotableReplicaRequest(final ReplicaRequest replicaRequest, final TransportChannel channel, final Task task) {
        ActionListener<TransportResponse> listener = new ChannelActionListener<>(
            channel,
            transportUnpromotableReplicaAction,
            replicaRequest
        );

        try {
            new AsyncUnpromotableReplicaAction(replicaRequest, listener, (ReplicationTask) task).run();
        } catch (RuntimeException e) {
            listener.onFailure(e);
        }
    }

    private class AsyncUnpromotableReplicaAction extends AbstractRunnable {
        private final ActionListener<TransportResponse> onCompletionListener;
        private final IndexShard replica;
        private final ReplicationTask task;
        private final ReplicaRequest replicaRequest;

        AsyncUnpromotableReplicaAction(
            ReplicaRequest replicaRequest,
            ActionListener<TransportResponse> onCompletionListener,
            ReplicationTask task
        ) {
            this.replicaRequest = replicaRequest;
            this.onCompletionListener = onCompletionListener;
            this.task = task;
            final ShardId shardId = replicaRequest.shardId();
            assert shardId != null : "request shardId must be set";
            this.replica = getIndexShard(shardId);
        }

        @Override
        public void onFailure(Exception e) {
            setPhase(task, "finished");
            onCompletionListener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            setPhase(task, "replica");
            try {
                ActionListener<ReplicaResult> listener = ActionListener.wrap(
                    (replicaResult) -> replicaResult.runPostReplicaActions(ActionListener.wrap(r -> {
                        setPhase(task, "finished");
                        onCompletionListener.onResponse(TransportResponse.Empty.INSTANCE);
                    }, e -> { onFailure(e); })),
                    e -> { onFailure(e); }
                );
                shardOperationOnUnpromotableReplica(replicaRequest, replica, listener);
            } catch (Exception e) {
                onFailure(e);
            }
        }
    }

    /**
     * Extends the {@link org.elasticsearch.action.support.replication.TransportReplicationAction.ReplicasProxy} to perform the actual
     * {@code ReplicaRequest} on the replica shards. Depending on the type of the replica shard (whether it is promotable or unpromotable),
     * the corresponding action is sent.
     */
    protected class UnpromotableReplicasProxy extends ReplicasProxy {

        private final ReplicationOperation.ReplicaForwardOptions replicaForwardOptions;

        public UnpromotableReplicasProxy(ReplicationOperation.ReplicaForwardOptions replicaForwardOptions) {
            this.replicaForwardOptions = replicaForwardOptions;
        }

        @Override
        public ReplicationOperation.ReplicaForwardOptions getReplicaForwardOptions() {
            return replicaForwardOptions;
        }

        @Override
        public void performOnUnpromotable(
            final ShardRouting replica,
            final ReplicaRequest request,
            final ActionListener<TransportResponse.Empty> listener
        ) {
            final DiscoveryNode node = clusterService.state().nodes().get(replica.currentNodeId());
            final var handler = new ActionListenerResponseHandler<>(listener, (streamInput) -> TransportResponse.Empty.INSTANCE);
            transportService.sendRequest(node, transportUnpromotableReplicaAction, request, transportOptions, handler);
        }
    }

}
