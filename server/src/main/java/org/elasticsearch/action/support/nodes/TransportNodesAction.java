/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.nodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.CancellableFanOut;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;

public abstract class TransportNodesAction<
    NodesRequest extends BaseNodesRequest<NodesRequest>,
    NodesResponse extends BaseNodesResponse<?>,
    NodeRequest extends TransportRequest,
    NodeResponse extends BaseNodeResponse> extends HandledTransportAction<NodesRequest, NodesResponse> {

    private static final Logger logger = LogManager.getLogger(TransportNodesAction.class);

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final String transportNodeAction;

    private final Executor finalExecutor;

    /**
     * @param actionName        action name
     * @param threadPool        thread-pool
     * @param clusterService    cluster service
     * @param transportService  transport service
     * @param actionFilters     action filters
     * @param request           node request writer
     * @param nodeRequest       node request reader
     * @param executor          executor to execute node action and final collection
     */
    protected TransportNodesAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<NodesRequest> request,
        Writeable.Reader<NodeRequest> nodeRequest,
        String executor
    ) {
        super(actionName, transportService, actionFilters, request);
        assert executor.equals(ThreadPool.Names.SAME) == false : "TransportNodesAction must always fork off the transport thread";
        this.clusterService = Objects.requireNonNull(clusterService);
        this.transportService = Objects.requireNonNull(transportService);
        this.finalExecutor = threadPool.executor(executor);
        this.transportNodeAction = actionName + "[n]";
        transportService.registerRequestHandler(transportNodeAction, executor, nodeRequest, new NodeTransportHandler());
    }

    @Override
    protected void doExecute(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
        if (request.concreteNodes() == null) {
            resolveRequest(request, clusterService.state());
            assert request.concreteNodes() != null;
        }

        new CancellableFanOut<DiscoveryNode, NodeResponse, CheckedConsumer<ActionListener<NodesResponse>, Exception>>() {

            final ArrayList<NodeResponse> responses = new ArrayList<>(request.concreteNodes().length);
            final ArrayList<FailedNodeException> exceptions = new ArrayList<>(0);

            final TransportRequestOptions transportRequestOptions = TransportRequestOptions.timeout(request.timeout());

            @Override
            protected void sendItemRequest(DiscoveryNode discoveryNode, ActionListener<NodeResponse> listener) {
                final var nodeRequest = newNodeRequest(request);
                if (task != null) {
                    nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                }

                transportService.sendRequest(
                    discoveryNode,
                    transportNodeAction,
                    nodeRequest,
                    transportRequestOptions,
                    new ActionListenerResponseHandler<>(listener, nodeResponseReader(discoveryNode))
                );
            }

            @Override
            protected void onItemResponse(DiscoveryNode discoveryNode, NodeResponse nodeResponse) {
                synchronized (responses) {
                    responses.add(nodeResponse);
                }
            }

            @Override
            protected void onItemFailure(DiscoveryNode discoveryNode, Exception e) {
                logger.debug(() -> format("failed to execute [%s] on node [%s]", actionName, discoveryNode), e);
                synchronized (exceptions) {
                    exceptions.add(new FailedNodeException(discoveryNode.getId(), "Failed node [" + discoveryNode.getId() + "]", e));
                }
            }

            @Override
            protected CheckedConsumer<ActionListener<NodesResponse>, Exception> onCompletion() {
                // ref releases all happen-before here so no need to be synchronized
                return l -> newResponseAsync(task, request, responses, exceptions, l);
            }

            @Override
            public String toString() {
                return actionName;
            }
        }.run(
            task,
            Iterators.forArray(request.concreteNodes()),
            new ThreadedActionListener<>(finalExecutor, listener.delegateFailureAndWrap((l, c) -> c.accept(l)))
        );
    }

    private Writeable.Reader<NodeResponse> nodeResponseReader(DiscoveryNode discoveryNode) {
        // not an inline lambda to avoid capturing CancellableFanOut.this.
        return in -> TransportNodesAction.this.newNodeResponse(in, discoveryNode);
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

    /**
     * Create a new {@link NodesResponse}, possibly asynchronously. The default implementation is synchronous and calls
     * {@link #newResponse(BaseNodesRequest, List, List)}
     */
    protected void newResponseAsync(
        Task task,
        NodesRequest request,
        List<NodeResponse> responses,
        List<FailedNodeException> failures,
        ActionListener<NodesResponse> listener
    ) {
        ActionListener.completeWith(listener, () -> newResponse(request, responses, failures));
    }

    protected abstract NodeRequest newNodeRequest(NodesRequest request);

    protected abstract NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException;

    protected abstract NodeResponse nodeOperation(NodeRequest request, Task task);

    /**
     * resolve node ids to concrete nodes of the incoming request
     **/
    protected void resolveRequest(NodesRequest request, ClusterState clusterState) {
        assert request.concreteNodes() == null : "request concreteNodes shouldn't be set";
        String[] nodesIds = clusterState.nodes().resolveNodes(request.nodesIds());
        request.setConcreteNodes(Arrays.stream(nodesIds).map(clusterState.nodes()::get).toArray(DiscoveryNode[]::new));
    }

    class NodeTransportHandler implements TransportRequestHandler<NodeRequest> {
        @Override
        public void messageReceived(NodeRequest request, TransportChannel channel, Task task) throws Exception {
            channel.sendResponse(nodeOperation(request, task));
        }
    }

}
