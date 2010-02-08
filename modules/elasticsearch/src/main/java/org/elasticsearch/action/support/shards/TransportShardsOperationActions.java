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

package org.elasticsearch.action.support.shards;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardNotActiveException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.cluster.node.Nodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.settings.Settings;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.action.Actions.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class TransportShardsOperationActions<Request extends ShardsOperationRequest, Response extends ShardsOperationResponse, ShardRequest extends ShardOperationRequest, ShardResponse extends ShardOperationResponse> extends BaseAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    protected final IndicesService indicesService;

    protected final ThreadPool threadPool;

    protected TransportShardsOperationActions(Settings settings, ClusterService clusterService, TransportService transportService,
                                              IndicesService indicesService, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.threadPool = threadPool;

        transportService.registerHandler(transportAction(), new TransportHandler());
        transportService.registerHandler(transportShardAction(), new ShardTransportHandler());
    }

    @Override protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncBroadcastAction(request, listener).start();
    }

    protected abstract String transportAction();

    protected abstract String transportShardAction();

    protected abstract Request newRequest();

    protected abstract Response newResponse(Request request, ClusterState clusterState, AtomicReferenceArray<Object> shardsResponses);

    protected abstract ShardRequest newShardRequest();

    protected abstract ShardRequest newShardRequest(ShardRouting shard, Request request);

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardResponse shardOperation(ShardRequest request) throws ElasticSearchException;

    protected abstract boolean accumulateExceptions();

    private class AsyncBroadcastAction {

        private final Request request;

        private final ActionListener<Response> listener;

        private final ClusterState clusterState;

        private final Nodes nodes;

        private final List<ShardRouting> shards;

        private final AtomicInteger opsCounter = new AtomicInteger();

        private final AtomicInteger indexCounter = new AtomicInteger();

        private final AtomicReferenceArray<Object> shardsResponses;

        private AsyncBroadcastAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            clusterState = clusterService.state();
            nodes = clusterState.nodes();
            shards = clusterState.routingTable().allShards(processIndices(clusterState, request.indices()));

            shardsResponses = new AtomicReferenceArray<Object>(shards.size());
        }

        public void start() {
            // count the local operations, and perform the non local ones
            int localOperations = 0;
            for (final ShardRouting shard : shards) {
                if (shard.active()) {
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        localOperations++;
                    } else {
                        // do the remote operation here, the localAsync flag is not relevant
                        performOperation(shard, true);
                    }
                } else {
                    // as if we have a "problem", so we iterate to the next one and maintain counts
                    onFailure(shard, new ShardNotActiveException(shard.shardId()), false);
                }
            }
            // we have local operations, perform them now
            if (localOperations > 0) {
                if (request.operationThreading() == ShardsOperationThreading.SINGLE_THREAD) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            for (final ShardRouting shard : shards) {
                                if (shard.active()) {
                                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                        performOperation(shard, false);
                                    }
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == ShardsOperationThreading.THREAD_PER_SHARD;
                    for (final ShardRouting shard : shards) {
                        if (shard.active()) {
                            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                performOperation(shard, localAsync);
                            }
                        }
                    }
                }
            }
        }

        private void performOperation(final ShardRouting shard, boolean localAsync) {
            final ShardRequest shardRequest = newShardRequest(shard, request);
            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                if (localAsync) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            try {
                                onOperation(shard, shardOperation(shardRequest), true);
                            } catch (Exception e) {
                                onFailure(shard, e, true);
                            }
                        }
                    });
                } else {
                    try {
                        onOperation(shard, shardOperation(shardRequest), false);
                    } catch (Exception e) {
                        onFailure(shard, e, false);
                    }
                }
            } else {
                Node node = nodes.get(shard.currentNodeId());
                transportService.sendRequest(node, transportShardAction(), shardRequest, new BaseTransportResponseHandler<ShardResponse>() {
                    @Override public ShardResponse newInstance() {
                        return newShardResponse();
                    }

                    @Override public void handleResponse(ShardResponse response) {
                        onOperation(shard, response, false);
                    }

                    @Override public void handleException(RemoteTransportException exp) {
                        onFailure(shard, exp, false);
                    }

                    @Override public boolean spawn() {
                        // we never spawn here, we will spawn if needed in onOperation
                        return false;
                    }
                });
            }
        }

        private void onOperation(ShardRouting shardRouting, ShardResponse shardResponse, boolean alreadyThreaded) {
            // need two counters to avoid race conditions
            shardsResponses.set(indexCounter.getAndIncrement(), shardResponse);
            if (opsCounter.incrementAndGet() == shardsResponses.length()) {
                finishHim(alreadyThreaded);
            }
        }

        private void onFailure(ShardRouting shardRouting, Throwable t, boolean alreadyThreaded) {
            int idx = indexCounter.getAndIncrement();
            if (accumulateExceptions()) {
                ShardOperationFailedException failedException;
                if (t instanceof ShardOperationFailedException) {
                    failedException = (ShardOperationFailedException) t;
                } else {
                    failedException = new ShardOperationFailedException(shardRouting.shardId(), t);
                }
                shardsResponses.set(idx, failedException);
            }
            if (opsCounter.incrementAndGet() == shardsResponses.length()) {
                finishHim(alreadyThreaded);
            }
        }

        private void finishHim(boolean alreadyThreaded) {
            // if we need to execute the listener on a thread, and we are not threaded already
            // then do it
            if (request.listenerThreaded() && !alreadyThreaded) {
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        listener.onResponse(newResponse(request, clusterState, shardsResponses));
                    }
                });
            } else {
                listener.onResponse(newResponse(request, clusterState, shardsResponses));
            }
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequest();
        }

        @Override public void messageReceived(Request request, final TransportChannel channel) throws Exception {
            // we just send back a response, no need to fork a listener
            request.listenerThreaded(false);
            // we don't spawn, so if we get a request with no threading, change it to single threaded
            if (request.operationThreading() == ShardsOperationThreading.NO_THREADS) {
                request.operationThreading(ShardsOperationThreading.SINGLE_THREAD);
            }
            execute(request, new ActionListener<Response>() {
                @Override public void onResponse(Response response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response", e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }

    private class ShardTransportHandler extends BaseTransportRequestHandler<ShardRequest> {

        @Override public ShardRequest newInstance() {
            return newShardRequest();
        }

        @Override public void messageReceived(ShardRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(shardOperation(request));
        }
    }
}
