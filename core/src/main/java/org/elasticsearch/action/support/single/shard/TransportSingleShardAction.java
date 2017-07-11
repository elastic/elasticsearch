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

package org.elasticsearch.action.support.single.shard;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;

/**
 * A base class for operations that need to perform a read operation on a single shard copy. If the operation fails,
 * the read operation can be performed on other shard copies. Concrete implementations can provide their own list
 * of candidate shards to try the read operation on.
 */
public abstract class TransportSingleShardAction<Request extends SingleShardRequest<Request>, Response extends ActionResponse> extends TransportAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    final String transportShardAction;
    final String executor;

    protected TransportSingleShardAction(Settings settings, String actionName, ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         Supplier<Request> request, String executor) {
        super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.clusterService = clusterService;
        this.transportService = transportService;

        this.transportShardAction = actionName + "[s]";
        this.executor = executor;

        if (!isSubAction()) {
            transportService.registerRequestHandler(actionName, request, ThreadPool.Names.SAME, new TransportHandler());
        }
        transportService.registerRequestHandler(transportShardAction, request, executor, new ShardTransportHandler());
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
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected abstract Response shardOperation(Request request, ShardId shardId) throws IOException;

    protected abstract Response newResponse();

    protected abstract boolean resolveIndex(Request request);

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
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
                transportService.sendRequest(clusterService.localNode(), transportShardAction, internalRequest.request(), new TransportResponseHandler<Response>() {
                    @Override
                    public Response newInstance() {
                        return newResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(final Response response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }
                });
            } else {
                perform(null);
            }
        }

        private void onFailure(ShardRouting shardRouting, Exception e) {
            if (logger.isTraceEnabled() && e != null) {
                logger.trace(
                    (org.apache.logging.log4j.util.Supplier<?>)
                        () -> new ParameterizedMessage("{}: failed to execute [{}]", shardRouting, internalRequest.request()), e);
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
                    failure = new NoShardAvailableActionException(null, LoggerMessageFormat.format("No shard available for [{}]", internalRequest.request()), failure);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            (org.apache.logging.log4j.util.Supplier<?>)
                                () -> new ParameterizedMessage("{}: failed to execute [{}]", null, internalRequest.request()), failure);
                    }
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
                transportService.sendRequest(node, transportShardAction, internalRequest.request(), new TransportResponseHandler<Response>() {

                    @Override
                    public Response newInstance() {
                        return newResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(final Response response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        onFailure(shardRouting, exp);
                    }
                });
            }
        }
    }

    private class TransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(Request request, final TransportChannel channel) throws Exception {
            // if we have a local operation, execute it on a thread since we don't spawn
            request.operationThreaded(true);
            execute(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("failed to send response for get", e1);
                    }
                }
            });
        }
    }

    private class ShardTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] on shard [{}]", request, request.internalShardId);
            }
            Response response = shardOperation(request, request.internalShardId);
            channel.sendResponse(response);
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
}
