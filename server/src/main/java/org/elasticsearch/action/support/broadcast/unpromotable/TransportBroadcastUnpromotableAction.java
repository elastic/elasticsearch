/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

public abstract class TransportBroadcastUnpromotableAction<Request extends BroadcastUnpromotableRequest> extends HandledTransportAction<
    Request,
    ActionResponse.Empty> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final ShardStateAction shardStateAction;

    protected final String transportUnpromotableAction;
    protected final String executor;

    protected TransportBroadcastUnpromotableAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        String executor
    ) {
        super(actionName, transportService, actionFilters, requestReader);
        this.clusterService = clusterService;
        this.shardStateAction = shardStateAction;
        this.transportService = transportService;
        this.transportUnpromotableAction = actionName + "[u]";
        this.executor = executor;

        transportService.registerRequestHandler(transportUnpromotableAction, executor, requestReader, new UnpromotableTransportHandler());
    }

    protected abstract void unpromotableShardOperation(Task task, Request request, ActionListener<ActionResponse.Empty> listener);

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        try (var listeners = new RefCountingListener(listener.map(v -> ActionResponse.Empty.INSTANCE))) {
            ActionListener.completeWith(listeners.acquire(), () -> {
                final ClusterState clusterState = clusterService.state();
                if (task != null) {
                    request.setParentTask(clusterService.localNode().getId(), task.getId());
                }
                request.indexShardRoutingTable.unpromotableShards().forEach(shardRouting -> {
                    final DiscoveryNode node = clusterState.nodes().get(shardRouting.currentNodeId());
                    final ActionListener<TransportResponse.Empty> acquired = listeners.acquire(ignored -> {});
                    transportService.sendRequest(
                        node,
                        transportUnpromotableAction,
                        request,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(
                            request.failShardOnError()
                                ? acquired.delegateResponse((l, e) -> failShard(shardRouting, clusterState, l, e))
                                : acquired,
                            (in) -> TransportResponse.Empty.INSTANCE,
                            executor
                        )
                    );
                });
                return null;
            });
        }
    }

    private void failShard(ShardRouting shardRouting, ClusterState clusterState, ActionListener<TransportResponse.Empty> l, Exception e) {
        shardStateAction.remoteShardFailed(
            shardRouting.shardId(),
            shardRouting.allocationId().getId(),
            clusterState.metadata().index(shardRouting.getIndexName()).primaryTerm(shardRouting.shardId().getId()),
            true,
            "mark unpromotable copy as stale after refresh failure",
            e,
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    logger.debug("Marked shard {} as failed", shardRouting.shardId());
                    l.onResponse(TransportResponse.Empty.INSTANCE);
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
            final ActionListener<ActionResponse.Empty> listener = new ChannelActionListener<>(channel);
            ActionListener.run(listener, (l) -> unpromotableShardOperation(task, request, l));
        }

    }
}
