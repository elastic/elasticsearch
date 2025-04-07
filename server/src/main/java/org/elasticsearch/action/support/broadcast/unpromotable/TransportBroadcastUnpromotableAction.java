/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.broadcast.unpromotable;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

public abstract class TransportBroadcastUnpromotableAction<Request extends BroadcastUnpromotableRequest, Response extends ActionResponse>
    extends HandledTransportAction<Request, Response> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final ShardStateAction shardStateAction;

    protected final String transportUnpromotableAction;
    protected final Executor executor;

    protected TransportBroadcastUnpromotableAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Executor executor
    ) {
        super(actionName, transportService, actionFilters, requestReader, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.shardStateAction = shardStateAction;
        this.transportService = transportService;
        this.transportUnpromotableAction = actionName + "[u]";
        this.executor = executor;

        transportService.registerRequestHandler(
            transportUnpromotableAction,
            this.executor,
            false,
            false,
            requestReader,
            new UnpromotableTransportHandler()
        );
    }

    protected abstract void unpromotableShardOperation(Task task, Request request, ActionListener<Response> listener);

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var unpromotableShards = request.indexShardRoutingTable.assignedUnpromotableShards();
        final var responses = new ArrayList<Response>(unpromotableShards.size());

        try (var listeners = new RefCountingListener(listener.map(v -> combineUnpromotableShardResponses(responses)))) {
            ActionListener.completeWith(listeners.acquire(), () -> {
                final ClusterState clusterState = clusterService.state();
                if (task != null) {
                    request.setParentTask(clusterService.localNode().getId(), task.getId());
                }
                unpromotableShards.forEach(shardRouting -> {
                    final DiscoveryNode node = clusterState.nodes().get(shardRouting.currentNodeId());
                    final ActionListener<Response> shardRequestListener = listeners.acquire(response -> {
                        synchronized (responses) {
                            responses.add(response);
                        }
                    });
                    transportService.sendRequest(
                        node,
                        transportUnpromotableAction,
                        request,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(
                            request.failShardOnError()
                                ? shardRequestListener.delegateResponse((l, e) -> failShard(shardRouting, clusterState, l, e))
                                : shardRequestListener,
                            this::readResponse,
                            executor
                        )
                    );
                });
                return null;
            });
        }
    }

    protected abstract Response combineUnpromotableShardResponses(List<Response> responses);

    protected abstract Response readResponse(StreamInput in) throws IOException;

    protected abstract Response emptyResponse();

    private void failShard(ShardRouting shardRouting, ClusterState clusterState, ActionListener<Response> l, Exception e) {
        shardStateAction.remoteShardFailed(
            shardRouting.shardId(),
            shardRouting.allocationId().getId(),
            clusterState.metadata().getProject().index(shardRouting.getIndexName()).primaryTerm(shardRouting.shardId().getId()),
            true,
            "mark unpromotable copy as stale after refresh failure",
            e,
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    logger.debug("Marked shard {} as failed", shardRouting.shardId());
                    l.onResponse(emptyResponse());
                }

                @Override
                public void onFailure(Exception sfe) {
                    logger.error(Strings.format("Unable to mark shard [%s] as failed", shardRouting.shardId()), sfe);
                    l.onFailure(e);
                }
            }
        );
    }

    class UnpromotableTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(Request request, TransportChannel channel, Task task) throws Exception {
            final ActionListener<Response> listener = new ChannelActionListener<>(channel);
            ActionListener.run(listener, (l) -> unpromotableShardOperation(task, request, l));
        }

    }
}
