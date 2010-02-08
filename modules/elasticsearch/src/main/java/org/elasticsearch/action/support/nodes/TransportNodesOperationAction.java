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

package org.elasticsearch.action.support.nodes;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class TransportNodesOperationAction<Request extends NodesOperationRequest, Response extends NodesOperationResponse, NodeRequest extends NodeOperationRequest, NodeResponse extends NodeOperationResponse> extends BaseAction<Request, Response> {

    protected final ClusterName clusterName;

    protected final ThreadPool threadPool;

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    @Inject public TransportNodesOperationAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                                 ClusterService clusterService, TransportService transportService) {
        super(settings);
        this.clusterName = clusterName;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;

        transportService.registerHandler(transportAction(), new TransportHandler());
        transportService.registerHandler(transportNodeAction(), new NodeTransportHandler());
    }

    @Override protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncAction(request, listener).start();
    }

    protected abstract String transportAction();

    protected abstract String transportNodeAction();

    protected abstract Request newRequest();

    protected abstract Response newResponse(Request request, AtomicReferenceArray nodesResponses);

    protected abstract NodeRequest newNodeRequest();

    protected abstract NodeRequest newNodeRequest(String nodeId, Request request);

    protected abstract NodeResponse newNodeResponse();

    protected abstract NodeResponse nodeOperation(NodeRequest request) throws ElasticSearchException;

    protected abstract boolean accumulateExceptions();


    private class AsyncAction {

        private final Request request;

        private final String[] nodesIds;

        private final ActionListener<Response> listener;

        private final ClusterState clusterState;

        private final AtomicReferenceArray<Object> responses;

        private final AtomicInteger index = new AtomicInteger();

        private final AtomicInteger counter = new AtomicInteger();

        private AsyncAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
            clusterState = clusterService.state();
            String[] nodesIds = request.nodesIds();
            if (nodesIds == null || nodesIds.length == 0) {
                int index = 0;
                nodesIds = new String[clusterState.nodes().size()];
                for (Node node : clusterState.nodes()) {
                    nodesIds[index++] = node.id();
                }
            }
            this.nodesIds = nodesIds;
            this.responses = new AtomicReferenceArray<Object>(nodesIds.length);
        }

        private void start() {
            for (final String nodeId : nodesIds) {
                final Node node = clusterState.nodes().nodes().get(nodeId);
                if (nodeId.equals("_local") || nodeId.equals(clusterState.nodes().localNodeId())) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            try {
                                onOperation(nodeOperation(newNodeRequest(clusterState.nodes().localNodeId(), request)));
                            } catch (Exception e) {
                                onFailure(clusterState.nodes().localNodeId(), e);
                            }
                        }
                    });
                } else if (nodeId.equals("_master")) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            try {
                                onOperation(nodeOperation(newNodeRequest(clusterState.nodes().masterNodeId(), request)));
                            } catch (Exception e) {
                                onFailure(clusterState.nodes().masterNodeId(), e);
                            }
                        }
                    });
                } else {
                    if (node == null) {
                        onFailure(nodeId, new NoSuchNodeException(nodeId));
                    } else {
                        NodeRequest nodeRequest = newNodeRequest(nodeId, request);
                        transportService.sendRequest(node, transportNodeAction(), nodeRequest, new BaseTransportResponseHandler<NodeResponse>() {
                            @Override public NodeResponse newInstance() {
                                return newNodeResponse();
                            }

                            @Override public void handleResponse(NodeResponse response) {
                                onOperation(response);
                            }

                            @Override public void handleException(RemoteTransportException exp) {
                                onFailure(node.id(), exp);
                            }

                            @Override public boolean spawn() {
                                return false;
                            }
                        });
                    }
                }
            }
        }

        private void onOperation(NodeResponse nodeResponse) {
            // need two counters to avoid race conditions
            responses.set(index.getAndIncrement(), nodeResponse);
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void onFailure(String nodeId, Throwable t) {
            int idx = index.getAndIncrement();
            if (accumulateExceptions()) {
                responses.set(idx, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t));
            }
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void finishHim() {
            if (request.listenerThreaded()) {
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        listener.onResponse(newResponse(request, responses));
                    }
                });
            } else {
                listener.onResponse(newResponse(request, responses));
            }
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequest();
        }

        @Override public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            request.listenerThreaded(false);
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
                        logger.warn("Failed to send response", e);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }

    private class NodeTransportHandler extends BaseTransportRequestHandler<NodeRequest> {

        @Override public NodeRequest newInstance() {
            return newNodeRequest();
        }

        @Override public void messageReceived(NodeRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(nodeOperation(request));
        }
    }
}
