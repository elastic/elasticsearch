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
package org.elasticsearch.action.support.single.custom;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

/**
 * Transport action used to send a read request to one of the shards that belong to an index.
 * Supports retrying another shard in case of failure.
 */
public abstract class TransportSingleCustomOperationAction<Request extends SingleCustomOperationRequest, Response extends ActionResponse> extends HandledTransportAction<Request, Response> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;

    final String transportShardAction;
    final String executor;

    protected TransportSingleCustomOperationAction(Settings settings, String actionName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters,
                                                   Class<Request> request, String executor) {
        super(settings, actionName, threadPool, transportService, actionFilters, request);
        this.clusterService = clusterService;
        this.transportService = transportService;

        this.transportShardAction = actionName + "[s]";
        this.executor = executor;

        transportService.registerRequestHandler(transportShardAction, request, executor, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    /**
     * Can return null to execute on this local node.
     */
    protected abstract ShardsIterator shards(ClusterState state, InternalRequest request);

    /**
     * Operation to be executed at the shard level. Can be called with shardId set to null, meaning that there is no
     * shard involved and the operation just needs to be executed on the local node.
     */
    protected abstract Response shardOperation(Request request, ShardId shardId) throws ElasticsearchException;

    protected abstract Response newResponse();

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.concreteIndex());
    }

    protected abstract boolean resolveIndex(Request request);

    private class AsyncSingleAction {

        private final ActionListener<Response> listener;

        private final ShardsIterator shardsIt;

        private final InternalRequest internalRequest;

        private final DiscoveryNodes nodes;

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState);
            if (blockException != null) {
                throw blockException;
            }

            String concreteSingleIndex;
            if (resolveIndex(request)) {
                concreteSingleIndex = clusterState.metaData().concreteSingleIndex(request.index(), request.indicesOptions());
            } else {
                concreteSingleIndex = request.index();
            }
            this.internalRequest = new InternalRequest(request, concreteSingleIndex);

            blockException = checkRequestBlock(clusterState, internalRequest);
            if (blockException != null) {
                throw blockException;
            }
            this.shardsIt = shards(clusterState, internalRequest);
        }

        public void start() {
            performFirst();
        }

        private void onFailure(ShardRouting shardRouting, Throwable e) {
            if (logger.isTraceEnabled() && e != null) {
                logger.trace(shardRouting.shortSummary() + ": Failed to execute [" + internalRequest.request() + "]", e);
            }
            perform(e);
        }

        /**
         * First get should try and use a shard that exists on a local node for better performance
         */
        private void performFirst() {
            if (shardsIt == null) {
                // just execute it on the local node
                if (internalRequest.request().operationThreaded()) {
                    internalRequest.request().beforeLocalFork();
                    threadPool.executor(executor).execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Response response = shardOperation(internalRequest.request(), null);
                                listener.onResponse(response);
                            } catch (Throwable e) {
                                onFailure(null, e);
                            }
                        }
                    });
                    return;
                } else {
                    try {
                        final Response response = shardOperation(internalRequest.request(), null);
                        listener.onResponse(response);
                        return;
                    } catch (Throwable e) {
                        onFailure(null, e);
                    }
                }
                return;
            }

            if (internalRequest.request().preferLocalShard()) {
                boolean foundLocal = false;
                ShardRouting shardX;
                while ((shardX = shardsIt.nextOrNull()) != null) {
                    final ShardRouting shard = shardX;
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        foundLocal = true;
                        if (internalRequest.request().operationThreaded()) {
                            internalRequest.request().beforeLocalFork();
                            threadPool.executor(executor).execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        Response response = shardOperation(internalRequest.request(), shard.shardId());
                                        listener.onResponse(response);
                                    } catch (Throwable e) {
                                        shardsIt.reset();
                                        onFailure(shard, e);
                                    }
                                }
                            });
                            return;
                        } else {
                            try {
                                final Response response = shardOperation(internalRequest.request(), shard.shardId());
                                listener.onResponse(response);
                                return;
                            } catch (Throwable e) {
                                shardsIt.reset();
                                onFailure(shard, e);
                            }
                        }
                    }
                }
                if (!foundLocal) {
                    // no local node get, go remote
                    shardsIt.reset();
                    perform(null);
                }
            } else {
                perform(null);
            }
        }

        private void perform(final Throwable lastException) {
            final ShardRouting shard = shardsIt == null ? null : shardsIt.nextOrNull();
            if (shard == null) {
                Throwable failure = lastException;
                if (failure == null) {
                    failure = new NoShardAvailableActionException(null, "No shard available for [" + internalRequest.request() + "]");
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to execute [" + internalRequest.request() + "]", failure);
                    }
                }
                listener.onFailure(failure);
            } else {
                if (shard.currentNodeId().equals(nodes.localNodeId())) {
                    // we don't prefer local shard, so try and do it here
                    if (!internalRequest.request().preferLocalShard()) {
                        try {
                            if (internalRequest.request().operationThreaded()) {
                                internalRequest.request().beforeLocalFork();
                                threadPool.executor(executor).execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            Response response = shardOperation(internalRequest.request(), shard.shardId());
                                            listener.onResponse(response);
                                        } catch (Throwable e) {
                                            onFailure(shard, e);
                                        }
                                    }
                                });
                            } else {
                                final Response response = shardOperation(internalRequest.request(), shard.shardId());
                                listener.onResponse(response);
                            }
                        } catch (Throwable e) {
                            onFailure(shard, e);
                        }
                    } else {
                        perform(lastException);
                    }
                } else {
                    DiscoveryNode node = nodes.get(shard.currentNodeId());
                    internalRequest.request().internalShardId = shard.shardId();
                    transportService.sendRequest(node, transportShardAction, internalRequest.request(), new BaseTransportResponseHandler<Response>() {
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
                            onFailure(shard, exp);
                        }
                    });
                }
            }
        }
    }

    private class ShardTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
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
