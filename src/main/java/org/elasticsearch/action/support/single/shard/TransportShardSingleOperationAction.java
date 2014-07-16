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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;

/**
 * A base class for single shard read operations.
 */
public abstract class TransportShardSingleOperationAction<Request extends SingleShardOperationRequest, Response extends ActionResponse> extends TransportAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    final String transportShardAction;
    final String executor;

    protected TransportShardSingleOperationAction(Settings settings, String actionName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService) {
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

    protected abstract Response shardOperation(Request request, int shardId) throws ElasticsearchException;

    protected abstract Request newRequest();

    protected abstract Response newResponse();

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request);

    protected void resolveRequest(ClusterState state, Request request) {
        request.index(state.metaData().concreteSingleIndex(request.index()));
    }

    protected abstract ShardIterator shards(ClusterState state, Request request) throws ElasticsearchException;

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final ShardIterator shardIt;
        private final Request request;
        private final DiscoveryNodes nodes;
        private volatile Throwable lastFailure;

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] based on cluster state version [{}]", request, clusterState.version());
            }
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }
            resolveRequest(clusterState, request);
            blockException = checkRequestBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }

            this.shardIt = shards(clusterState, request);
        }

        public void start() {
            perform(null);
        }

        private void onFailure(ShardRouting shardRouting, Throwable e) {
            if (logger.isTraceEnabled() && e != null) {
                logger.trace("{}: failed to execute [{}]", e, shardRouting, request);
            }
            perform(e);
        }

        private void perform(@Nullable final Throwable currentFailure) {
            Throwable lastFailure = this.lastFailure;
            if (lastFailure == null || TransportActions.isReadOverrideException(currentFailure)) {
                lastFailure = currentFailure;
                this.lastFailure = currentFailure;
            }
            final ShardRouting shardRouting = shardIt.nextOrNull();
            if (shardRouting == null) {
                Throwable failure = lastFailure;
                if (failure == null || isShardNotAvailableException(failure)) {
                    failure = new NoShardAvailableActionException(shardIt.shardId());
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{}: failed to execute [{}]", failure, shardIt.shardId(), request);
                    }
                }
                listener.onFailure(failure);
                return;
            }
            if (shardRouting.currentNodeId().equals(nodes.localNodeId())) {
                if (logger.isTraceEnabled()) {
                    logger.trace("executing [{}] on shard [{}]", request, shardRouting.shardId());
                }
                try {
                    if (request.operationThreaded()) {
                        request.beforeLocalFork();
                        threadPool.executor(executor).execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Response response = shardOperation(request, shardRouting.id());
                                    listener.onResponse(response);
                                } catch (Throwable e) {
                                    onFailure(shardRouting, e);
                                }
                            }
                        });
                    } else {
                        final Response response = shardOperation(request, shardRouting.id());
                        listener.onResponse(response);
                    }
                } catch (Throwable e) {
                    onFailure(shardRouting, e);
                }
            } else {
                DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
                if (node == null) {
                    onFailure(shardRouting, new NoShardAvailableActionException(shardIt.shardId()));
                } else {
                    transportService.sendRequest(node, transportShardAction, new ShardSingleOperationRequest(request, shardRouting.id()), new BaseTransportResponseHandler<Response>() {

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
    }

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override
        public Request newInstance() {
            return newRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
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
                        logger.warn("failed to send response for get", e1);
                    }
                }
            });
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
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] on shard [{}]", request.request(), request.shardId());
            }
            Response response = shardOperation(request.request(), request.shardId());
            channel.sendResponse(response);
        }
    }

    class ShardSingleOperationRequest extends TransportRequest {

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
