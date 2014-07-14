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
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;

/**
 *
 */
public abstract class TransportSingleCustomOperationAction<Request extends SingleCustomOperationRequest, Response extends ActionResponse> extends TransportAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    final String transportShardAction;
    final String executor;

    protected TransportSingleCustomOperationAction(Settings settings, String actionName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService) {
        super(settings, actionName, threadPool);
        this.clusterService = clusterService;
        this.transportService = transportService;

        this.transportShardAction = actionName + "/s";
        this.executor = executor();

        transportService.registerHandler(actionName, new TransportHandler());
        transportService.registerHandler(transportShardAction, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected abstract String executor();

    /**
     * Can return null to execute on this local node.
     */
    protected abstract ShardsIterator shards(ClusterState state, Request request);

    protected abstract Response shardOperation(Request request, int shardId) throws ElasticsearchException;

    protected abstract Request newRequest();

    protected abstract Response newResponse();

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request);

    private class AsyncSingleAction {

        private final ActionListener<Response> listener;

        private final ShardsIterator shardsIt;

        private final Request request;

        private final DiscoveryNodes nodes;

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }
            blockException = checkRequestBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }
            this.shardsIt = shards(clusterState, request);
        }

        public void start() {
            performFirst();
        }

        private void onFailure(ShardRouting shardRouting, Throwable e) {
            if (logger.isTraceEnabled() && e != null) {
                logger.trace(shardRouting.shortSummary() + ": Failed to execute [" + request + "]", e);
            }
            perform(e);
        }

        /**
         * First get should try and use a shard that exists on a local node for better performance
         */
        private void performFirst() {
            if (shardsIt == null) {
                // just execute it on the local node
                if (request.operationThreaded()) {
                    request.beforeLocalFork();
                    threadPool.executor(executor()).execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Response response = shardOperation(request, -1);
                                listener.onResponse(response);
                            } catch (Throwable e) {
                                onFailure(null, e);
                            }
                        }
                    });
                    return;
                } else {
                    try {
                        final Response response = shardOperation(request, -1);
                        listener.onResponse(response);
                        return;
                    } catch (Throwable e) {
                        onFailure(null, e);
                    }
                }
                return;
            }

            if (request.preferLocalShard()) {
                boolean foundLocal = false;
                ShardRouting shardX;
                while ((shardX = shardsIt.nextOrNull()) != null) {
                    final ShardRouting shard = shardX;
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        foundLocal = true;
                        if (request.operationThreaded()) {
                            request.beforeLocalFork();
                            threadPool.executor(executor()).execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        Response response = shardOperation(request, shard.id());
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
                                final Response response = shardOperation(request, shard.id());
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
                    failure = new NoShardAvailableActionException(null, "No shard available for [" + request + "]");
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to execute [" + request + "]", failure);
                    }
                }
                listener.onFailure(failure);
            } else {
                if (shard.currentNodeId().equals(nodes.localNodeId())) {
                    // we don't prefer local shard, so try and do it here
                    if (!request.preferLocalShard()) {
                        try {
                            if (request.operationThreaded()) {
                                request.beforeLocalFork();
                                threadPool.executor(executor).execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            Response response = shardOperation(request, shard.id());
                                            listener.onResponse(response);
                                        } catch (Throwable e) {
                                            onFailure(shard, e);
                                        }
                                    }
                                });
                            } else {
                                final Response response = shardOperation(request, shard.id());
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
                    transportService.sendRequest(node, transportShardAction, new ShardSingleOperationRequest(request, shard.id()), new BaseTransportResponseHandler<Response>() {
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

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override
        public Request newInstance() {
            return newRequest();
        }

        @Override
        public void messageReceived(Request request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            // if we have a local operation, execute it on a thread since we don't spawn
            request.operationThreaded(true);
            execute(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for get", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    private class ShardTransportHandler extends BaseTransportRequestHandler<ShardSingleOperationRequest> {

        @Override
        public ShardSingleOperationRequest newInstance() {
            return new ShardSingleOperationRequest();
        }

        @Override
        public String executor() {
            return executor;
        }

        @Override
        public void messageReceived(final ShardSingleOperationRequest request, final TransportChannel channel) throws Exception {
            Response response = shardOperation(request.request(), request.shardId());
            channel.sendResponse(response);
        }
    }

    protected class ShardSingleOperationRequest extends TransportRequest {

        private Request request;
        private int shardId;

        ShardSingleOperationRequest() {
        }

        public ShardSingleOperationRequest(Request request, int shardId) {
            super(request);
            this.request = request;
            this.shardId = shardId;
        }

        public Request request() {
            return request;
        }

        public int shardId() {
            return shardId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = newRequest();
            request.readFrom(in);
            shardId = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
            out.writeVInt(shardId);
        }
    }
}
