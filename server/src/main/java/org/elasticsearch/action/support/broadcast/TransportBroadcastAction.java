/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public abstract class TransportBroadcastAction<
    Request extends BroadcastRequest<Request>,
    Response extends BroadcastResponse,
    ShardRequest extends BroadcastShardRequest,
    ShardResponse extends BroadcastShardResponse> extends HandledTransportAction<Request, Response> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    final String transportShardAction;
    private final String shardExecutor;

    protected TransportBroadcastAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        Writeable.Reader<ShardRequest> shardRequest,
        String shardExecutor
    ) {
        super(actionName, transportService, actionFilters, request);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportShardAction = actionName + "[s]";
        this.shardExecutor = shardExecutor;

        transportService.registerRequestHandler(transportShardAction, ThreadPool.Names.SAME, shardRequest, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncBroadcastAction(task, request, listener).start();
    }

    protected abstract Response newResponse(Request request, AtomicReferenceArray<?> shardsResponses, ClusterState clusterState);

    protected abstract ShardRequest newShardRequest(int numShards, ShardRouting shard, Request request);

    protected abstract ShardResponse readShardResponse(StreamInput in) throws IOException;

    protected abstract ShardResponse shardOperation(ShardRequest request, Task task) throws IOException;

    /**
     * Determines the shards this operation will be executed on. The operation is executed once per shard iterator, typically
     * on the first shard in it. If the operation fails, it will be retried on the next shard in the iterator.
     */
    protected abstract GroupShardsIterator<ShardIterator> shards(ClusterState clusterState, Request request, String[] concreteIndices);

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices);

    protected class AsyncBroadcastAction {

        final Task task;
        final Request request;
        final ActionListener<Response> listener;
        final ClusterState clusterState;
        final DiscoveryNodes nodes;
        final GroupShardsIterator<ShardIterator> shardsIts;
        final int expectedOps;
        final AtomicInteger counterOps = new AtomicInteger();
        // ShardResponse or Exception
        protected final AtomicReferenceArray<Object> shardsResponses;

        protected AsyncBroadcastAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;

            clusterState = clusterService.state();

            ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }
            // update to concrete indices
            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
            blockException = checkRequestBlock(clusterState, request, concreteIndices);
            if (blockException != null) {
                throw blockException;
            }

            nodes = clusterState.nodes();
            logger.trace("resolving shards based on cluster state version [{}]", clusterState.version());
            shardsIts = shards(clusterState, request, concreteIndices);
            expectedOps = shardsIts.size();

            shardsResponses = new AtomicReferenceArray<>(expectedOps);
        }

        public void start() {
            if (shardsIts.size() == 0) {
                // no shards
                try {
                    listener.onResponse(newResponse(request, new AtomicReferenceArray<ShardResponse>(0), clusterState));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
                return;
            }
            // count the local operations, and perform the non local ones
            int shardIndex = -1;
            for (final ShardIterator shardIt : shardsIts) {
                shardIndex++;
                final ShardRouting shard = shardIt.nextOrNull();
                if (shard != null) {
                    performOperation(shardIt, shard, shardIndex);
                } else {
                    // really, no shards active in this group
                    onOperation(null, shardIt, shardIndex, new NoShardAvailableActionException(shardIt.shardId()));
                }
            }
        }

        protected void performOperation(final ShardIterator shardIt, final ShardRouting shard, final int shardIndex) {
            if (shard == null) {
                // no more active shards... (we should not really get here, just safety)
                onOperation(null, shardIt, shardIndex, new NoShardAvailableActionException(shardIt.shardId()));
            } else {
                try {
                    final ShardRequest shardRequest = newShardRequest(shardIt.size(), shard, request);
                    shardRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                    DiscoveryNode node = nodes.get(shard.currentNodeId());
                    if (node == null) {
                        // no node connected, act as failure
                        onOperation(shard, shardIt, shardIndex, new NoShardAvailableActionException(shardIt.shardId()));
                    } else {
                        sendShardRequest(
                            node,
                            shardRequest,
                            ActionListener.wrap(r -> onOperation(shard, shardIndex, r), e -> onOperation(shard, shardIt, shardIndex, e))
                        );
                    }
                } catch (Exception e) {
                    onOperation(shard, shardIt, shardIndex, e);
                }
            }
        }

        protected void sendShardRequest(DiscoveryNode node, ShardRequest shardRequest, ActionListener<ShardResponse> listener) {
            transportService.sendRequest(node, transportShardAction, shardRequest, new TransportResponseHandler<ShardResponse>() {
                @Override
                public ShardResponse read(StreamInput in) throws IOException {
                    return readShardResponse(in);
                }

                @Override
                public void handleResponse(ShardResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }
            });
        }

        protected void onOperation(ShardRouting shard, int shardIndex, ShardResponse response) {
            logger.trace("received response for {}", shard);
            shardsResponses.set(shardIndex, response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim();
            }
        }

        void onOperation(@Nullable ShardRouting shard, final ShardIterator shardIt, int shardIndex, Exception e) {
            // we set the shard failure always, even if its the first in the replication group, and the next one
            // will work (it will just override it...)
            setFailure(shardIt, shardIndex, e);
            ShardRouting nextShard = shardIt.nextOrNull();
            if (nextShard != null) {
                if (e != null) {
                    if (logger.isTraceEnabled()) {
                        if (TransportActions.isShardNotAvailableException(e) == false) {
                            logger.trace(
                                new ParameterizedMessage(
                                    "{}: failed to execute [{}]",
                                    shard != null ? shard.shortSummary() : shardIt.shardId(),
                                    request
                                ),
                                e
                            );
                        }
                    }
                }
                performOperation(shardIt, nextShard, shardIndex);
            } else {
                if (logger.isDebugEnabled()) {
                    if (e != null) {
                        if (TransportActions.isShardNotAvailableException(e) == false) {
                            logger.debug(
                                new ParameterizedMessage(
                                    "{}: failed to execute [{}]",
                                    shard != null ? shard.shortSummary() : shardIt.shardId(),
                                    request
                                ),
                                e
                            );
                        }
                    }
                }
                if (expectedOps == counterOps.incrementAndGet()) {
                    finishHim();
                }
            }
        }

        protected AtomicReferenceArray<Object> shardsResponses() {
            return shardsResponses;
        }

        protected void finishHim() {
            try {
                listener.onResponse(newResponse(request, shardsResponses, clusterState));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        void setFailure(ShardIterator shardIt, int shardIndex, Exception e) {
            if ((e instanceof BroadcastShardOperationFailedException) == false) {
                e = new BroadcastShardOperationFailedException(shardIt.shardId(), e);
            }

            Object response = shardsResponses.get(shardIndex);
            if (response == null) {
                // just override it and return
                shardsResponses.set(shardIndex, e);
            }

            if ((response instanceof Throwable) == false) {
                // we should never really get here...
                return;
            }

            // the failure is already present, try and not override it with an exception that is less meaningless
            // for example, getting illegal shard state
            if (TransportActions.isReadOverrideException(e)) {
                shardsResponses.set(shardIndex, e);
            }
        }
    }

    class ShardTransportHandler implements TransportRequestHandler<ShardRequest> {

        @Override
        public void messageReceived(ShardRequest request, TransportChannel channel, Task task) throws Exception {
            asyncShardOperation(request, task, ActionListener.wrap(channel::sendResponse, e -> {
                try {
                    channel.sendResponse(e);
                } catch (Exception e1) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "Failed to send error response for action [{}] and request [{}]",
                            actionName,
                            request
                        ),
                        e1
                    );
                }
            }));
        }
    }

    private void asyncShardOperation(ShardRequest request, Task task, ActionListener<ShardResponse> listener) {
        transportService.getThreadPool()
            .executor(shardExecutor)
            .execute(ActionRunnable.supply(listener, () -> shardOperation(request, task)));
    }
}
