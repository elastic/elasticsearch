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

package org.elasticsearch.action.support.nodes;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeShouldNotConnectException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

public abstract class TransportNodesAction<NodesRequest extends BaseNodesRequest<NodesRequest>,
                                           NodesResponse extends BaseNodesResponse,
                                           NodeRequest extends BaseNodeRequest,
                                           NodeResponse extends BaseNodeResponse>
    extends HandledTransportAction<NodesRequest, NodesResponse> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final Class<NodeResponse> nodeResponseClass;

    final String transportNodeAction;

    protected TransportNodesAction(Settings settings, String actionName, ThreadPool threadPool,
                                   ClusterService clusterService, TransportService transportService, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                   Supplier<NodesRequest> request, Supplier<NodeRequest> nodeRequest,
                                   String nodeExecutor,
                                   Class<NodeResponse> nodeResponseClass) {
        super(settings, actionName, threadPool, transportService, actionFilters, indexNameExpressionResolver, request);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.transportService = Objects.requireNonNull(transportService);
        this.nodeResponseClass = Objects.requireNonNull(nodeResponseClass);

        this.transportNodeAction = actionName + "[n]";

        transportService.registerRequestHandler(
            transportNodeAction, nodeRequest, nodeExecutor, new NodeTransportHandler());
    }

    @Override
    protected final void doExecute(NodesRequest request, ActionListener<NodesResponse> listener) {
        logger.warn("attempt to execute a transport nodes operation without a task");
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void doExecute(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
        new AsyncAction(task, request, listener).start();
    }

    protected boolean transportCompress() {
        return false;
    }

    /**
     * Map the responses into {@code nodeResponseClass} responses and {@link FailedNodeException}s.
     *
     * @param request The associated request.
     * @param nodesResponses All node-level responses
     * @return Never {@code null}.
     * @throws NullPointerException if {@code nodesResponses} is {@code null}
     * @see #newResponse(BaseNodesRequest, List, List)
     */
    protected NodesResponse newResponse(NodesRequest request, AtomicReferenceArray nodesResponses) {
        final List<NodeResponse> responses = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();

        for (int i = 0; i < nodesResponses.length(); ++i) {
            Object response = nodesResponses.get(i);

            if (response instanceof FailedNodeException) {
                failures.add((FailedNodeException)response);
            } else {
                responses.add(nodeResponseClass.cast(response));
            }
        }

        return newResponse(request, responses, failures);
    }

    /**
     * Create a new {@link NodesResponse} (multi-node response).
     *
     * @param request The associated request.
     * @param responses All successful node-level responses.
     * @param failures All node-level failures.
     * @return Never {@code null}.
     * @throws NullPointerException if any parameter is {@code null}.
     */
    protected abstract NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures);

    protected abstract NodeRequest newNodeRequest(String nodeId, NodesRequest request);

    protected abstract NodeResponse newNodeResponse();

    protected abstract NodeResponse nodeOperation(NodeRequest request);

    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        return nodeOperation(request);
    }

    /**
     * resolve node ids to concrete nodes of the incoming request
     **/
    protected void resolveRequest(NodesRequest request, ClusterState clusterState) {
        assert request.concreteNodes() == null : "request concreteNodes shouldn't be set";
        String[] nodesIds = clusterState.nodes().resolveNodes(request.nodesIds());
        request.setConcreteNodes(Arrays.stream(nodesIds).map(clusterState.nodes()::get).toArray(DiscoveryNode[]::new));
    }


    class AsyncAction {

        private final NodesRequest request;
        private final ActionListener<NodesResponse> listener;
        private final AtomicReferenceArray<Object> responses;
        private final AtomicInteger counter = new AtomicInteger();
        private final Task task;

        AsyncAction(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;
            if (request.concreteNodes() == null) {
                resolveRequest(request, clusterService.state());
                assert request.concreteNodes() != null;
            }
            this.responses = new AtomicReferenceArray<>(request.concreteNodes().length);
        }

        void start() {
            final DiscoveryNode[] nodes = request.concreteNodes();
            if (nodes.length == 0) {
                // nothing to notify
                threadPool.generic().execute(() -> listener.onResponse(newResponse(request, responses)));
                return;
            }
            TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
            if (request.timeout() != null) {
                builder.withTimeout(request.timeout());
            }
            builder.withCompress(transportCompress());
            for (int i = 0; i < nodes.length; i++) {
                final int idx = i;
                final DiscoveryNode node = nodes[i];
                final String nodeId = node.getId();
                try {
                    if (node == null) {
                        onFailure(idx, nodeId, new NoSuchNodeException(nodeId));
                    } else {
                        TransportRequest nodeRequest = newNodeRequest(nodeId, request);
                        if (task != null) {
                            nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                        }

                        transportService.sendRequest(node, transportNodeAction, nodeRequest, builder.build(),
                                                     new TransportResponseHandler<NodeResponse>() {
                            @Override
                            public NodeResponse newInstance() {
                                return newNodeResponse();
                            }

                            @Override
                            public void handleResponse(NodeResponse response) {
                                onOperation(idx, response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                onFailure(idx, node.getId(), exp);
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }
                        });
                    }
                } catch (Exception e) {
                    onFailure(idx, nodeId, e);
                }
            }
        }

        private void onOperation(int idx, NodeResponse nodeResponse) {
            responses.set(idx, nodeResponse);
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void onFailure(int idx, String nodeId, Throwable t) {
            if (logger.isDebugEnabled() && !(t instanceof NodeShouldNotConnectException)) {
                logger.debug(
                    (org.apache.logging.log4j.util.Supplier<?>)
                        () -> new ParameterizedMessage("failed to execute on node [{}]", nodeId), t);
            }
            responses.set(idx, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t));
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        private void finishHim() {
            NodesResponse finalResponse;
            try {
                finalResponse = newResponse(request, responses);
            } catch (Exception e) {
                logger.debug("failed to combine responses from nodes", e);
                listener.onFailure(e);
                return;
            }
            listener.onResponse(finalResponse);
        }
    }

    class NodeTransportHandler implements TransportRequestHandler<NodeRequest> {

        @Override
        public void messageReceived(NodeRequest request, TransportChannel channel, Task task) throws Exception {
            channel.sendResponse(nodeOperation(request, task));
        }

        @Override
        public void messageReceived(NodeRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(nodeOperation(request));
        }

    }

}
