/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.support.single;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.cluster.node.Nodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class TransportSingleOperationAction<Request extends SingleOperationRequest, Response extends ActionResponse> extends BaseAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    protected final IndicesService indicesService;

    protected final ThreadPool threadPool;

    protected TransportSingleOperationAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.indicesService = indicesService;

        transportService.registerHandler(transportAction(), new TransportHandler());
        transportService.registerHandler(transportShardAction(), new ShardTransportHandler());
    }

    @Override protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected abstract String transportAction();

    protected abstract String transportShardAction();

    protected abstract Response shardOperation(Request request, int shardId) throws ElasticSearchException;

    protected abstract Request newRequest();

    protected abstract Response newResponse();

    private class AsyncSingleAction {

        private final ActionListener<Response> listener;

        private final ShardsIterator shards;

        private Iterator<ShardRouting> shardsIt;

        private final Request request;

        private final Nodes nodes;

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();

            nodes = clusterState.nodes();

            this.shards = indicesService.indexServiceSafe(request.index).operationRouting()
                    .getShards(clusterState, request.type(), request.id());
            this.shardsIt = shards.iterator();
        }

        public void start() {
            performFirst();
        }

        public void onFailure(ShardRouting shardRouting, Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(shardRouting.shortSummary() + ": Failed to get [" + request.type() + "#" + request.id() + "]", e);
            }
            perform(e);
        }

        /**
         * First get should try and use a shard that exists on a local node for better performance
         */
        private void performFirst() {
            while (shardsIt.hasNext()) {
                final ShardRouting shard = shardsIt.next();
                if (!shard.active()) {
                    continue;
                }
                if (shard.currentNodeId().equals(nodes.localNodeId())) {
                    if (request.threadedOperation()) {
                        threadPool.execute(new Runnable() {
                            @Override public void run() {
                                try {
                                    Response response = shardOperation(request, shard.id());
                                    listener.onResponse(response);
                                } catch (Exception e) {
                                    onFailure(shard, e);
                                }
                            }
                        });
                        return;
                    } else {
                        try {
                            final Response response = shardOperation(request, shard.id());
                            if (request.listenerThreaded()) {
                                threadPool.execute(new Runnable() {
                                    @Override public void run() {
                                        listener.onResponse(response);
                                    }
                                });
                            } else {
                                listener.onResponse(response);
                            }
                            return;
                        } catch (Exception e) {
                            onFailure(shard, e);
                        }
                    }
                }
            }
            if (!shardsIt.hasNext()) {
                // no local node get, go remote
                shardsIt = shards.reset().iterator();
                perform(null);
            }
        }

        private void perform(final Exception lastException) {
            while (shardsIt.hasNext()) {
                final ShardRouting shard = shardsIt.next();
                if (!shard.active()) {
                    continue;
                }
                // no need to check for local nodes, we tried them already in performFirstGet
                if (!shard.currentNodeId().equals(nodes.localNodeId())) {
                    Node node = nodes.get(shard.currentNodeId());
                    transportService.sendRequest(node, transportShardAction(), new ShardSingleOperationRequest(request, shard.id()), new BaseTransportResponseHandler<Response>() {
                        @Override public Response newInstance() {
                            return newResponse();
                        }

                        @Override public void handleResponse(final Response response) {
                            if (request.listenerThreaded()) {
                                threadPool.execute(new Runnable() {
                                    @Override public void run() {
                                        listener.onResponse(response);
                                    }
                                });
                            } else {
                                listener.onResponse(response);
                            }
                        }

                        @Override public void handleException(RemoteTransportException exp) {
                            onFailure(shard, exp);
                        }

                        @Override public boolean spawn() {
                            // no need to spawn, we will execute the listener on a different thread if needed in handleResponse
                            return false;
                        }
                    });
                    return;
                }
            }
            if (!shardsIt.hasNext()) {
                final NoShardAvailableActionException failure = new NoShardAvailableActionException(shards.shardId(), "No shard available for [" + request.type() + "#" + request.id() + "]", lastException);
                if (request.listenerThreaded()) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            listener.onFailure(failure);
                        }
                    });
                } else {
                    listener.onFailure(failure);
                }
            }
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequest();
        }

        @Override public void messageReceived(Request request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            // if we have a local operation, execute it on a thread since we don't spawn
            request.threadedOperation(true);
            execute(request, new ActionListener<Response>() {
                @Override public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for get", e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }

    private class ShardTransportHandler extends BaseTransportRequestHandler<ShardSingleOperationRequest> {

        @Override public ShardSingleOperationRequest newInstance() {
            return new ShardSingleOperationRequest();
        }

        @Override public void messageReceived(ShardSingleOperationRequest request, TransportChannel channel) throws Exception {
            Response response = shardOperation(request.request(), request.shardId());
            channel.sendResponse(response);
        }
    }

    protected class ShardSingleOperationRequest implements Streamable {

        private Request request;

        private int shardId;

        ShardSingleOperationRequest() {
        }

        public ShardSingleOperationRequest(Request request, int shardId) {
            this.request = request;
            this.shardId = shardId;
        }

        public Request request() {
            return request;
        }

        public int shardId() {
            return shardId;
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            request = newRequest();
            request.readFrom(in);
            shardId = in.readInt();
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            request.writeTo(out);
            out.writeInt(shardId);
        }
    }
}
