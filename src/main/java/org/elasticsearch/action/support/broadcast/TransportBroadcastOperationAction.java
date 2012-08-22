/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public abstract class TransportBroadcastOperationAction<Request extends BroadcastOperationRequest, Response extends BroadcastOperationResponse, ShardRequest extends BroadcastShardOperationRequest, ShardResponse extends BroadcastShardOperationResponse>
        extends TransportAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    protected final ThreadPool threadPool;

    final String transportAction;
    final String transportShardAction;
    final String executor;

    protected TransportBroadcastOperationAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;

        this.transportAction = transportAction();
        this.transportShardAction = transportAction() + "/s";
        this.executor = executor();

        transportService.registerHandler(transportAction, new TransportHandler());
        transportService.registerHandler(transportShardAction, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncBroadcastAction(request, listener).start();
    }

    protected abstract String transportAction();

    protected abstract String executor();

    protected abstract Request newRequest();

    protected abstract Response newResponse(Request request, AtomicReferenceArray shardsResponses, ClusterState clusterState);

    protected abstract ShardRequest newShardRequest();

    protected abstract ShardRequest newShardRequest(ShardRouting shard, Request request);

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardResponse shardOperation(ShardRequest request) throws ElasticSearchException;

    protected abstract GroupShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices);

    protected boolean accumulateExceptions() {
        return true;
    }

    protected boolean ignoreException(Throwable t) {
        return false;
    }

    protected boolean ignoreNonActiveExceptions() {
        return false;
    }

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices);

    class AsyncBroadcastAction {

        private final Request request;

        private final ActionListener<Response> listener;

        private final ClusterState clusterState;

        private final DiscoveryNodes nodes;

        private final GroupShardsIterator shardsIts;

        private final int expectedOps;

        private final AtomicInteger counterOps = new AtomicInteger();

        private final AtomicInteger indexCounter = new AtomicInteger();

        private final AtomicReferenceArray shardsResponses;

        AsyncBroadcastAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            clusterState = clusterService.state();

            ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }
            // update to concrete indices
            String[] concreteIndices = clusterState.metaData().concreteIndices(request.indices(), request.ignoreIndices(), true);
            blockException = checkRequestBlock(clusterState, request, concreteIndices);
            if (blockException != null) {
                throw blockException;
            }

            nodes = clusterState.nodes();
            shardsIts = shards(clusterState, request, concreteIndices);
            expectedOps = shardsIts.size();

            shardsResponses = new AtomicReferenceArray<Object>(expectedOps);
        }

        public void start() {
            if (shardsIts.size() == 0) {
                // no shards
                listener.onResponse(newResponse(request, new AtomicReferenceArray(0), clusterState));
            }
            request.beforeStart();
            // count the local operations, and perform the non local ones
            int localOperations = 0;
            for (final ShardIterator shardIt : shardsIts) {
                final ShardRouting shard = shardIt.firstOrNull();
                if (shard != null) {
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        localOperations++;
                    } else {
                        // do the remote operation here, the localAsync flag is not relevant
                        performOperation(shardIt, true);
                    }
                } else {
                    // really, no shards active in this group
                    onOperation(null, shardIt, null);
                }
            }
            // we have local operations, perform them now
            if (localOperations > 0) {
                if (request.operationThreading() == BroadcastOperationThreading.SINGLE_THREAD) {
                    request.beforeLocalFork();
                    threadPool.executor(executor).execute(new Runnable() {
                        @Override
                        public void run() {
                            for (final ShardIterator shardIt : shardsIts) {
                                final ShardRouting shard = shardIt.firstOrNull();
                                if (shard != null) {
                                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                        performOperation(shardIt, false);
                                    }
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == BroadcastOperationThreading.THREAD_PER_SHARD;
                    if (localAsync) {
                        request.beforeLocalFork();
                    }
                    for (final ShardIterator shardIt : shardsIts) {
                        final ShardRouting shard = shardIt.firstOrNull();
                        if (shard != null) {
                            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                performOperation(shardIt, localAsync);
                            }
                        }
                    }
                }
            }
        }

        void performOperation(final ShardIterator shardIt, boolean localAsync) {
            performOperation(shardIt, shardIt.nextOrNull(), localAsync);
        }

        void performOperation(final ShardIterator shardIt, final ShardRouting shard, boolean localAsync) {
            if (shard == null) {
                // no more active shards... (we should not really get here, just safety)
                onOperation(null, shardIt, null);
            } else {
                final ShardRequest shardRequest = newShardRequest(shard, request);
                if (shard.currentNodeId().equals(nodes.localNodeId())) {
                    if (localAsync) {
                        threadPool.executor(executor).execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    onOperation(shard, shardOperation(shardRequest));
                                } catch (Exception e) {
                                    onOperation(shard, shardIt, e);
                                }
                            }
                        });
                    } else {
                        try {
                            onOperation(shard, shardOperation(shardRequest));
                        } catch (Exception e) {
                            onOperation(shard, shardIt, e);
                        }
                    }
                } else {
                    DiscoveryNode node = nodes.get(shard.currentNodeId());
                    if (node == null) {
                        // no node connected, act as failure
                        onOperation(shard, shardIt, null);
                    } else {
                        transportService.sendRequest(node, transportShardAction, shardRequest, new BaseTransportResponseHandler<ShardResponse>() {
                            @Override
                            public ShardResponse newInstance() {
                                return newShardResponse();
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }

                            @Override
                            public void handleResponse(ShardResponse response) {
                                onOperation(shard, response);
                            }

                            @Override
                            public void handleException(TransportException e) {
                                onOperation(shard, shardIt, e);
                            }
                        });
                    }
                }
            }
        }

        @SuppressWarnings({"unchecked"})
        void onOperation(ShardRouting shard, ShardResponse response) {
            shardsResponses.set(indexCounter.getAndIncrement(), response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim();
            }
        }

        @SuppressWarnings({"unchecked"})
        void onOperation(@Nullable ShardRouting shard, final ShardIterator shardIt, Throwable t) {
            ShardRouting nextShard = shardIt.nextOrNull();
            if (nextShard != null) {
                if (t != null) {
                    // trace log this exception
                    if (logger.isTraceEnabled()) {
                        if (!ignoreException(t)) {
                            if (shard != null) {
                                logger.trace(shard.shortSummary() + ": Failed to execute [" + request + "]", t);
                            } else {
                                logger.trace(shardIt.shardId() + ": Failed to execute [" + request + "]", t);
                            }
                        }
                    }
                }
                // we are not threaded here if we got here from the transport
                // or we possibly threaded if we got from a local threaded one,
                // in which case, the next shard in the partition will not be local one
                // so there is no meaning to this flag
                performOperation(shardIt, nextShard, true);
            } else {
                // e is null when there is no next active....
                if (logger.isDebugEnabled()) {
                    if (t != null) {
                        if (!ignoreException(t)) {
                            if (shard != null) {
                                logger.debug(shard.shortSummary() + ": Failed to execute [" + request + "]", t);
                            } else {
                                logger.debug(shardIt.shardId() + ": Failed to execute [" + request + "]", t);
                            }
                        }
                    }
                }
                // no more shards in this group
                int index = indexCounter.getAndIncrement();
                if (accumulateExceptions()) {
                    if (t == null) {
                        if (!ignoreNonActiveExceptions()) {
                            t = new BroadcastShardOperationFailedException(shardIt.shardId(), "No active shard(s)");
                        }
                    } else {
                        if (ignoreException(t)) {
                            t = null;
                        } else {
                            if (!(t instanceof BroadcastShardOperationFailedException)) {
                                t = new BroadcastShardOperationFailedException(shardIt.shardId(), t);
                            }
                        }
                    }
                    shardsResponses.set(index, t);
                }
                if (expectedOps == counterOps.incrementAndGet()) {
                    finishHim();
                }
            }
        }

        void finishHim() {
            listener.onResponse(newResponse(request, shardsResponses, clusterState));
        }
    }

    class TransportHandler extends BaseTransportRequestHandler<Request> {

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
            // we just send back a response, no need to fork a listener
            request.listenerThreaded(false);
            // we don't spawn, so if we get a request with no threading, change it to single threaded
            if (request.operationThreading() == BroadcastOperationThreading.NO_THREADS) {
                request.operationThreading(BroadcastOperationThreading.SINGLE_THREAD);
            }
            execute(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response", e1);
                    }
                }
            });
        }
    }

    class ShardTransportHandler extends BaseTransportRequestHandler<ShardRequest> {

        @Override
        public ShardRequest newInstance() {
            return newShardRequest();
        }

        @Override
        public String executor() {
            return executor;
        }

        @Override
        public void messageReceived(final ShardRequest request, final TransportChannel channel) throws Exception {
            channel.sendResponse(shardOperation(request));
        }
    }
}
