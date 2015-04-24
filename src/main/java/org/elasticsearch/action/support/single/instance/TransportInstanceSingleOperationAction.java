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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
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
public abstract class TransportInstanceSingleOperationAction<Request extends InstanceShardOperationRequest, Response extends ActionResponse> extends HandledTransportAction<Request, Response> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;

    final String executor;

    protected TransportInstanceSingleOperationAction(Settings settings, String actionName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, Class<Request> request) {
        super(settings, actionName, threadPool, transportService, actionFilters, request);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.executor = executor();
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected abstract String executor();

    protected abstract void shardOperation(InternalRequest request, ActionListener<Response> listener) throws ElasticsearchException;

    protected abstract Response newResponse();

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.concreteIndex());
    }
    /**
     * Resolves the request. If the resolve means a different execution, then return false
     * here to indicate not to continue and execute this request.
     */
    protected abstract boolean resolveRequest(ClusterState state, InternalRequest request, ActionListener<Response> listener);

    protected boolean retryOnFailure(Throwable e) {
        return false;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    /**
     * Should return an iterator with a single shard!
     */
    protected abstract ShardIterator shards(ClusterState clusterState, InternalRequest request) throws ElasticsearchException;

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final InternalRequest internalRequest;
        private volatile ClusterStateObserver observer;
        private ShardIterator shardIt;
        private DiscoveryNodes nodes;
        private final AtomicBoolean operationStarted = new AtomicBoolean();

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.internalRequest = new InternalRequest(request);
            this.listener = listener;
        }

        public void start() {
            this.observer = new ClusterStateObserver(clusterService, internalRequest.request().timeout(), logger);
            doStart();
        }

        protected boolean doStart() throws ElasticsearchException {
            nodes = observer.observedState().nodes();
            try {
                ClusterBlockException blockException = checkGlobalBlock(observer.observedState());
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return false;
                    } else {
                        throw blockException;
                    }
                }
                internalRequest.concreteIndex(observer.observedState().metaData().concreteSingleIndex(internalRequest.request().index(), internalRequest.request().indicesOptions()));
                // check if we need to execute, and if not, return
                if (!resolveRequest(observer.observedState(), internalRequest, listener)) {
                    return true;
                }
                blockException = checkRequestBlock(observer.observedState(), internalRequest);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return false;
                    } else {
                        throw blockException;
                    }
                }
                shardIt = shards(observer.observedState(), internalRequest);
            } catch (Throwable e) {
                listener.onFailure(e);
                return true;
            }

            // no shardIt, might be in the case between index gateway recovery and shardIt initialization
            if (shardIt.size() == 0) {
                retry(null);
                return false;
            }

            // this transport only make sense with an iterator that returns a single shard routing (like primary)
            assert shardIt.size() == 1;

            ShardRouting shard = shardIt.nextOrNull();
            assert shard != null;

            if (!shard.active()) {
                retry(null);
                return false;
            }

            if (!operationStarted.compareAndSet(false, true)) {
                return true;
            }

            internalRequest.request().shardId = shardIt.shardId().id();
            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                internalRequest.request().beforeLocalFork();
                try {
                    threadPool.executor(executor).execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                shardOperation(internalRequest, listener);
                            } catch (Throwable e) {
                                if (retryOnFailure(e)) {
                                    operationStarted.set(false);
                                    // we already marked it as started when we executed it (removed the listener) so pass false
                                    // to re-add to the cluster listener
                                    retry(null);
                                } else {
                                    listener.onFailure(e);
                                }
                            }
                        }
                    });
                } catch (Throwable e) {
                    if (retryOnFailure(e)) {
                        retry(null);
                    } else {
                        listener.onFailure(e);
                    }
                }
            } else {
                DiscoveryNode node = nodes.get(shard.currentNodeId());
                transportService.sendRequest(node, actionName, internalRequest.request(), transportOptions(), new BaseTransportResponseHandler<Response>() {

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
                            retry(null);
                        } else {
                            listener.onFailure(exp);
                        }
                    }
                });
            }
            return true;
        }

        void retry(final @Nullable Throwable failure) {
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                return;
            }

            // make it threaded operation so we fork on the discovery listener thread
            internalRequest.request().beforeLocalFork();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    doStart();
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(nodes.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // just to be on the safe side, see if we can start it now?
                    if (!doStart()) {
                        Throwable listenFailure = failure;
                        if (listenFailure == null) {
                            if (shardIt == null) {
                                listenFailure = new UnavailableShardsException(new ShardId(internalRequest.concreteIndex(), -1), "Timeout waiting for [" + timeout + "], request: " + internalRequest.request().toString());
                            } else {
                                listenFailure = new UnavailableShardsException(shardIt.shardId(), "[" + shardIt.size() + "] shardIt, [" + shardIt.sizeActive() + "] active : Timeout waiting for [" + timeout + "], request: " + internalRequest.request().toString());
                            }
                        }
                        listener.onFailure(listenFailure);
                    }
                }
            }, internalRequest.request().timeout());
        }
    }

    /**
     * Internal request class that gets built on each node. Holds the original request plus additional info.
     */
    protected class InternalRequest {
        final Request request;
        String concreteIndex;

        InternalRequest(Request request) {
            this.request = request;
        }

        public Request request() {
            return request;
        }

        void concreteIndex(String concreteIndex) {
            this.concreteIndex = concreteIndex;
        }

        public String concreteIndex() {
            return concreteIndex;
        }
    }
}
