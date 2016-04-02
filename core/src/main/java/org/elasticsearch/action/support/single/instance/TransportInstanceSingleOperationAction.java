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

package org.elasticsearch.action.support.single.instance;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

/**
 *
 */
public abstract class TransportInstanceSingleOperationAction<Request extends InstanceShardOperationRequest<Request>, Response extends ActionResponse>
        extends HandledTransportAction<Request, Response> {
    protected final ClusterService clusterService;
    protected final TransportService transportService;

    final String executor;
    final String shardActionName;

    protected TransportInstanceSingleOperationAction(Settings settings, String actionName, ThreadPool threadPool,
                                                     ClusterService clusterService, TransportService transportService,
                                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        super(settings, actionName, threadPool, transportService, actionFilters, indexNameExpressionResolver, request);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.executor = executor();
        this.shardActionName = actionName + "[s]";
        transportService.registerRequestHandler(shardActionName, request, executor, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected abstract String executor();

    protected abstract void shardOperation(Request request, ActionListener<Response> listener);

    protected abstract Response newResponse();

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, Request request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.concreteIndex());
    }

    /**
     * Resolves the request. Throws an exception if the request cannot be resolved.
     */
    protected abstract void resolveRequest(ClusterState state, Request request);

    protected boolean retryOnFailure(Throwable e) {
        return false;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    /**
     * Should return an iterator with a single shard!
     */
    protected abstract ShardIterator shards(ClusterState clusterState, Request request);

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final Request request;
        private volatile ClusterStateObserver observer;
        private ShardIterator shardIt;
        private DiscoveryNodes nodes;

        AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            this.observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
            doStart();
        }

        protected void doStart() {
            nodes = observer.observedState().nodes();
            try {
                ClusterBlockException blockException = checkGlobalBlock(observer.observedState());
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                request.concreteIndex(indexNameExpressionResolver.concreteSingleIndex(observer.observedState(), request).getName());
                resolveRequest(observer.observedState(), request);
                blockException = checkRequestBlock(observer.observedState(), request);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                shardIt = shards(observer.observedState(), request);
            } catch (Throwable e) {
                listener.onFailure(e);
                return;
            }

            // no shardIt, might be in the case between index gateway recovery and shardIt initialization
            if (shardIt.size() == 0) {
                retry(null);
                return;
            }

            // this transport only make sense with an iterator that returns a single shard routing (like primary)
            assert shardIt.size() == 1;

            ShardRouting shard = shardIt.nextOrNull();
            assert shard != null;

            if (!shard.active()) {
                retry(null);
                return;
            }

            request.shardId = shardIt.shardId();
            DiscoveryNode node = nodes.get(shard.currentNodeId());
            transportService.sendRequest(node, shardActionName, request, transportOptions(), new BaseTransportResponseHandler<Response>() {

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
                    Throwable cause = exp.unwrapCause();
                    // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                    if (cause instanceof ConnectTransportException || cause instanceof NodeClosedException ||
                            retryOnFailure(exp)) {
                        retry(cause);
                    } else {
                        listener.onFailure(exp);
                    }
                }
            });
        }

        void retry(final @Nullable Throwable failure) {
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                Throwable listenFailure = failure;
                if (listenFailure == null) {
                    if (shardIt == null) {
                        listenFailure = new UnavailableShardsException(request.concreteIndex(), -1, "Timeout waiting for [{}], request: {}", request.timeout(), actionName);
                    } else {
                        listenFailure = new UnavailableShardsException(shardIt.shardId(), "[{}] shardIt, [{}] active : Timeout waiting for [{}], request: {}", shardIt.size(), shardIt.sizeActive(), request.timeout(), actionName);
                    }
                }
                listener.onFailure(listenFailure);
                return;
            }

            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    doStart();
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(nodes.getLocalNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // just to be on the safe side, see if we can start it now?
                    doStart();
                }
            }, request.timeout());
        }
    }

    private class ShardTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            shardOperation(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        channel.sendResponse(response);
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
}
