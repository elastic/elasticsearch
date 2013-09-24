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

package org.elasticsearch.action.support.single.instance;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public abstract class TransportInstanceSingleOperationAction<Request extends InstanceShardOperationRequest, Response extends ActionResponse> extends TransportAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    final String transportAction;
    final String executor;

    protected TransportInstanceSingleOperationAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.transportService = transportService;

        this.transportAction = transportAction();
        this.executor = executor();

        transportService.registerHandler(transportAction, new TransportHandler());
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected abstract String executor();

    protected abstract String transportAction();

    protected abstract void shardOperation(Request request, ActionListener<Response> listener) throws ElasticSearchException;

    protected abstract Request newRequest();

    protected abstract Response newResponse();

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request);

    /**
     * Resolves the request, by default, simply setting the concrete index (if its aliased one). If the resolve
     * means a different execution, then return false here to indicate not to continue and execute this request.
     */
    protected boolean resolveRequest(ClusterState state, Request request, ActionListener<Response> listener) {
        request.index(state.metaData().concreteIndex(request.index()));
        return true;
    }

    protected boolean retryOnFailure(Throwable e) {
        return false;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    /**
     * Should return an iterator with a single shard!
     */
    protected abstract ShardIterator shards(ClusterState clusterState, Request request) throws ElasticSearchException;

    class AsyncSingleAction {

        private final ActionListener<Response> listener;

        private final Request request;

        private ShardIterator shardIt;

        private DiscoveryNodes nodes;

        private final AtomicBoolean operationStarted = new AtomicBoolean();

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            start(false);
        }

        public boolean start(final boolean fromClusterEvent) throws ElasticSearchException {
            final ClusterState clusterState = clusterService.state();
            nodes = clusterState.nodes();
            try {
                ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(fromClusterEvent, blockException);
                        return false;
                    } else {
                        throw blockException;
                    }
                }
                // check if we need to execute, and if not, return
                if (!resolveRequest(clusterState, request, listener)) {
                    return true;
                }
                blockException = checkRequestBlock(clusterState, request);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(fromClusterEvent, blockException);
                        return false;
                    } else {
                        throw blockException;
                    }
                }
                shardIt = shards(clusterState, request);
            } catch (Throwable e) {
                listener.onFailure(e);
                return true;
            }

            // no shardIt, might be in the case between index gateway recovery and shardIt initialization
            if (shardIt.size() == 0) {
                retry(fromClusterEvent, null);
                return false;
            }

            // this transport only make sense with an iterator that returns a single shard routing (like primary)
            assert shardIt.size() == 1;

            ShardRouting shard = shardIt.nextOrNull();
            assert shard != null;

            if (!shard.active()) {
                retry(fromClusterEvent, null);
                return false;
            }

            if (!operationStarted.compareAndSet(false, true)) {
                return true;
            }

            request.shardId = shardIt.shardId().id();
            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                request.beforeLocalFork();
                try {
                    threadPool.executor(executor).execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                shardOperation(request, listener);
                            } catch (Throwable e) {
                                if (retryOnFailure(e)) {
                                    operationStarted.set(false);
                                    // we already marked it as started when we executed it (removed the listener) so pass false
                                    // to re-add to the cluster listener
                                    retry(false, null);
                                } else {
                                    listener.onFailure(e);
                                }
                            }
                        }
                    });
                } catch (Throwable e) {
                    if (retryOnFailure(e)) {
                        retry(fromClusterEvent, null);
                    } else {
                        listener.onFailure(e);
                    }
                }
            } else {
                DiscoveryNode node = nodes.get(shard.currentNodeId());
                transportService.sendRequest(node, transportAction, request, transportOptions(), new BaseTransportResponseHandler<Response>() {

                    @Override
                    public Response newInstance() {
                        return newResponse();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(Response response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                        if (exp.unwrapCause() instanceof ConnectTransportException || exp.unwrapCause() instanceof NodeClosedException ||
                                retryOnFailure(exp)) {
                            operationStarted.set(false);
                            // we already marked it as started when we executed it (removed the listener) so pass false
                            // to re-add to the cluster listener
                            retry(false, null);
                        } else {
                            listener.onFailure(exp);
                        }
                    }
                });
            }
            return true;
        }

        void retry(final boolean fromClusterEvent, final @Nullable Throwable failure) {
            if (!fromClusterEvent) {
                // make it threaded operation so we fork on the discovery listener thread
                request.beforeLocalFork();
                clusterService.add(request.timeout(), new TimeoutClusterStateListener() {
                    @Override
                    public void postAdded() {
                        if (start(true)) {
                            // if we managed to start and perform the operation on the primary, we can remove this listener
                            clusterService.remove(this);
                        }
                    }

                    @Override
                    public void onClose() {
                        clusterService.remove(this);
                        listener.onFailure(new NodeClosedException(nodes.localNode()));
                    }

                    @Override
                    public void clusterChanged(ClusterChangedEvent event) {
                        if (start(true)) {
                            // if we managed to start and perform the operation on the primary, we can remove this listener
                            clusterService.remove(this);
                        }
                    }

                    @Override
                    public void onTimeout(TimeValue timeValue) {
                        // just to be on the safe side, see if we can start it now?
                        if (start(true)) {
                            clusterService.remove(this);
                            return;
                        }
                        clusterService.remove(this);
                        Throwable listenFailure = failure;
                        if (listenFailure == null) {
                            if (shardIt == null) {
                                listenFailure = new UnavailableShardsException(new ShardId(request.index(), -1), "Timeout waiting for [" + timeValue + "], request: " + request.toString());
                            } else {
                                listenFailure = new UnavailableShardsException(shardIt.shardId(), "[" + shardIt.size() + "] shardIt, [" + shardIt.sizeActive() + "] active : Timeout waiting for [" + timeValue + "], request: " + request.toString());
                            }
                        }
                        listener.onFailure(listenFailure);
                    }
                });
            }
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
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
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
    }
}
