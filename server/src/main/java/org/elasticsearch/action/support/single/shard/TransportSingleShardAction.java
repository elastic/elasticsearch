/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.single.shard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;
import static org.elasticsearch.core.Strings.format;

/**
 * A base class for operations that need to perform a read operation on a single shard copy. If the operation fails,
 * the read operation can be performed on other shard copies. Concrete implementations can provide their own list
 * of candidate shards to try the read operation on.
 */
public abstract class TransportSingleShardAction<Request extends SingleShardRequest<Request>, Response extends ActionResponse> extends
    TransportAction<Request, Response> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    private final String transportShardAction;
    private final Executor executor;

    @SuppressWarnings("this-escape")
    protected TransportSingleShardAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request,
        Executor executor
    ) {
        // TODO: consider passing the executor, remove it from doExecute and let InboundHandler/TransportAction handle concurrency.
        super(actionName, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        this.transportShardAction = actionName + "[s]";
        this.executor = executor;

        if (isSubAction() == false) {
            transportService.registerRequestHandler(actionName, EsExecutors.DIRECT_EXECUTOR_SERVICE, request, new TransportHandler());
        }
        transportService.registerRequestHandler(
            transportShardAction,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            request,
            new ShardTransportHandler()
        );
    }

    /**
     * Tells whether the action is a main one or a subaction. Used to decide whether we need to register
     * the main transport handler. In fact if the action is a subaction, its execute method
     * will be called locally to its parent action.
     */
    protected boolean isSubAction() {
        return false;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected abstract Response shardOperation(Request request, ShardId shardId) throws IOException;

    protected void asyncShardOperation(Request request, ShardId shardId, ActionListener<Response> listener) throws IOException {
        getExecutor(request, shardId).execute(ActionRunnable.supplyAndDecRef(listener, () -> shardOperation(request, shardId)));
    }

    protected abstract Writeable.Reader<Response> getResponseReader();

    protected abstract boolean resolveIndex(Request request);

    protected static ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.concreteIndex());
    }

    protected void resolveRequest(ClusterState state, InternalRequest request) {

    }

    /**
     * Returns the candidate shards to execute the operation on or <code>null</code> the execute
     * the operation locally (the node that received the request)
     */
    @Nullable
    protected abstract ShardsIterator shards(ClusterState state, InternalRequest request);

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final ShardsIterator shardIt;
        private final InternalRequest internalRequest;
        private final DiscoveryNodes nodes;
        private volatile Exception lastFailure;

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] based on cluster state version [{}]", request, clusterState.version());
            }
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState);
            if (blockException != null) {
                throw blockException;
            }

            String concreteSingleIndex;
            if (resolveIndex(request)) {
                concreteSingleIndex = indexNameExpressionResolver.concreteSingleIndex(clusterState, request).getName();
            } else {
                concreteSingleIndex = request.index();
            }
            this.internalRequest = new InternalRequest(request, concreteSingleIndex);
            resolveRequest(clusterState, internalRequest);

            blockException = checkRequestBlock(clusterState, internalRequest);
            if (blockException != null) {
                throw blockException;
            }

            this.shardIt = shards(clusterState, internalRequest);
        }

        public void start() {
            if (shardIt == null) {
                // just execute it on the local node
                final Writeable.Reader<Response> reader = getResponseReader();
                transportService.sendRequest(
                    clusterService.localNode(),
                    transportShardAction,
                    internalRequest.request(),
                    new ActionListenerResponseHandler<>(listener, reader, TransportResponseHandler.TRANSPORT_WORKER)
                );
            } else {
                perform(null);
            }
        }

        private void onFailure(ShardRouting shardRouting, Exception e) {
            if (e != null) {
                logger.trace(() -> format("%s: failed to execute [%s]", shardRouting, internalRequest.request()), e);
            }
            perform(e);
        }

        private void perform(@Nullable final Exception currentFailure) {
            Exception lastFailure = this.lastFailure;
            if (lastFailure == null || TransportActions.isReadOverrideException(currentFailure)) {
                lastFailure = currentFailure;
                this.lastFailure = currentFailure;
            }
            final ShardRouting shardRouting = shardIt.nextOrNull();
            if (shardRouting == null) {
                Exception failure = lastFailure;
                if (failure == null || isShardNotAvailableException(failure)) {
                    failure = new NoShardAvailableActionException(
                        null,
                        LoggerMessageFormat.format("No shard available for [{}]", internalRequest.request()),
                        failure
                    );
                } else {
                    logger.debug(() -> format("%s: failed to execute [%s]", null, internalRequest.request()), failure);
                }
                listener.onFailure(failure);
                return;
            }
            DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
            if (node == null) {
                onFailure(shardRouting, new NoShardAvailableActionException(shardRouting.shardId()));
            } else {
                internalRequest.request().internalShardId = shardRouting.shardId();
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "sending request [{}] to shard [{}] on node [{}]",
                        internalRequest.request(),
                        internalRequest.request().internalShardId,
                        node
                    );
                }
                final Writeable.Reader<Response> reader = getResponseReader();
                transportService.sendRequest(
                    node,
                    transportShardAction,
                    internalRequest.request(),
                    new ActionListenerResponseHandler<>(listener, reader, TransportResponseHandler.TRANSPORT_WORKER) {
                        @Override
                        public void handleException(TransportException exp) {
                            onFailure(shardRouting, exp);
                        }
                    }
                );
            }
        }
    }

    private class TransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(Request request, final TransportChannel channel, Task task) throws Exception {
            // if we have a local operation, execute it on a thread since we don't spawn
            executeDirect(task, request, new ChannelActionListener<>(channel));
        }
    }

    private class ShardTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] on shard [{}]", request, request.internalShardId);
            }
            asyncShardOperation(request, request.internalShardId, new ChannelActionListener<>(channel));
        }
    }

    /**
     * Internal request class that gets built on each node. Holds the original request plus additional info.
     */
    protected class InternalRequest {
        final Request request;
        final String concreteIndex;

        InternalRequest(Request request, String concreteIndex) {
            this.request = request;
            this.concreteIndex = concreteIndex;
        }

        public Request request() {
            return request;
        }

        public String concreteIndex() {
            return concreteIndex;
        }
    }

    protected Executor getExecutor(Request request, ShardId shardId) {
        return executor;
    }
}
