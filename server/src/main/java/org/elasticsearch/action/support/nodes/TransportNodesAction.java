/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.nodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.CancellableFanOut;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;

public abstract class TransportNodesAction<
    NodesRequest extends BaseNodesRequest,
    NodesResponse extends BaseNodesResponse<?>,
    NodeRequest extends TransportRequest,
    NodeResponse extends BaseNodeResponse,
    ActionContext> extends TransportAction<NodesRequest, NodesResponse> {

    private static final Logger logger = LogManager.getLogger(TransportNodesAction.class);

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final String transportNodeAction;

    private final Executor finalExecutor;

    /**
     * @param actionName        action name
     * @param clusterService    cluster service
     * @param transportService  transport service
     * @param actionFilters     action filters
     * @param nodeRequest       node request reader
     * @param executor          executor to execute node action and final collection
     */
    protected TransportNodesAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<NodeRequest> nodeRequest,
        Executor executor
    ) {
        // Only part of this action execution needs to be forked off - coordination can run on SAME because it's only O(#nodes) work.
        // Hence the separate "finalExecutor", and why we run the whole TransportAction.execute on SAME.
        super(actionName, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        assert executor.equals(EsExecutors.DIRECT_EXECUTOR_SERVICE) == false
            : "TransportNodesAction must always fork off the transport thread";
        this.clusterService = Objects.requireNonNull(clusterService);
        this.transportService = Objects.requireNonNull(transportService);
        this.finalExecutor = executor;
        this.transportNodeAction = actionName + "[n]";
        transportService.registerRequestHandler(transportNodeAction, finalExecutor, nodeRequest, new NodeTransportHandler());
    }

    @Override
    protected void doExecute(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
        // coordination can run on SAME because it's only O(#nodes) work

        final var concreteNodes = Objects.requireNonNull(resolveRequest(request, clusterService.state()));

        new CancellableFanOut<DiscoveryNode, NodeResponse, CheckedConsumer<ActionListener<NodesResponse>, Exception>>() {

            final ActionContext actionContext = createActionContext(task, request);
            final ArrayList<NodeResponse> responses = new ArrayList<>(concreteNodes.length);
            final ArrayList<FailedNodeException> exceptions = new ArrayList<>(0);

            final TransportRequestOptions transportRequestOptions = TransportRequestOptions.timeout(request.timeout());

            {
                addReleaseOnCancellationListener();
            }

            private void addReleaseOnCancellationListener() {
                if (task instanceof CancellableTask cancellableTask) {
                    cancellableTask.addListener(() -> {
                        final List<NodeResponse> drainedResponses;
                        synchronized (responses) {
                            drainedResponses = List.copyOf(responses);
                            responses.clear();
                        }
                        Releasables.wrap(Iterators.map(drainedResponses.iterator(), r -> r::decRef)).close();
                    });
                }
            }

            @Override
            protected void sendItemRequest(DiscoveryNode discoveryNode, ActionListener<NodeResponse> listener) {
                final var nodeRequest = newNodeRequest(request);
                if (task != null) {
                    nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                }

                try {
                    transportService.sendRequest(
                        discoveryNode,
                        transportNodeAction,
                        nodeRequest,
                        transportRequestOptions,
                        new ActionListenerResponseHandler<>(listener, nodeResponseReader(discoveryNode), finalExecutor)
                    );
                } finally {
                    nodeRequest.decRef();
                }
            }

            @Override
            protected void onItemResponse(DiscoveryNode discoveryNode, NodeResponse nodeResponse) {
                nodeResponse.mustIncRef();
                synchronized (responses) {
                    if ((task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) == false) {
                        responses.add(nodeResponse);
                        return;
                    }
                }
                nodeResponse.decRef();
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
                return l -> {
                    try (var ignored = Releasables.wrap(Iterators.map(responses.iterator(), r -> r::decRef))) {
                        newResponseAsync(task, request, actionContext, responses, exceptions, l);
                    }
                };
            }

            @Override
            public String toString() {
                return actionName;
            }
        }.run(
            task,
            Iterators.forArray(concreteNodes),
            new ThreadedActionListener<>(finalExecutor, listener.delegateFailureAndWrap((l, c) -> c.accept(l)))
        );
    }

    private Writeable.Reader<NodeResponse> nodeResponseReader(DiscoveryNode discoveryNode) {
        // not an inline lambda to avoid capturing CancellableFanOut.this.
        return in -> TransportNodesAction.this.newNodeResponse(in, discoveryNode);
    }

    /**
     * Create an (optional) {@link ActionContext}: called when starting to execute this action, and the result passed to
     * {@link #newResponseAsync} on completion. NB runs on the transport worker thread, must not do anything expensive without dispatching
     * to a different executor.
     */
    @Nullable
    protected ActionContext createActionContext(Task task, NodesRequest request) {
        return null;
    }

    /**
     * Create a new {@link NodesResponse}. This method is executed on {@link #finalExecutor}.
     *
     * @param request The request whose response we are constructing. {@link TransportNodesAction} may have already released all its
     *                references to this object before calling this method, so it's up to individual implementations to retain their own
     *                reference to the request if still needed here.
     * @param responses All successful node-level responses.
     * @param failures All node-level failures.
     * @return Never {@code null}.
     * @throws NullPointerException if any parameter is {@code null}.
     */
    protected abstract NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures);

    /**
     * Create a new {@link NodesResponse}, possibly asynchronously. The default implementation is synchronous and calls
     * {@link #newResponse(BaseNodesRequest, List, List)}. This method is executed on {@link #finalExecutor}.
     *
     * @param request The request whose response we are constructing. {@link TransportNodesAction} may have already released all its
     *                references to this object before calling this method, so it's up to individual implementations to retain their own
     *                reference to the request if still needed here.
     */
    protected void newResponseAsync(
        Task task,
        NodesRequest request,
        ActionContext actionContext,
        List<NodeResponse> responses,
        List<FailedNodeException> failures,
        ActionListener<NodesResponse> listener
    ) {
        ActionListener.run(listener, l -> ActionListener.respondAndRelease(l, newResponse(request, responses, failures)));
    }

    protected abstract NodeRequest newNodeRequest(NodesRequest request);

    protected abstract NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException;

    /**
     * Implements the request recipient logic.
     * If access to the request listener is needed, override {@link #nodeOperationAsync(TransportRequest, Task, ActionListener)}.
     */
    protected abstract NodeResponse nodeOperation(NodeRequest request, Task task);

    /**
     * This method can be overridden if a subclass needs to access to a listener in order to asynchronously respond to the node request.
     * The default implementation is to fall through to {@link #nodeOperation}.
     */
    protected void nodeOperationAsync(NodeRequest request, Task task, ActionListener<NodeResponse> listener) {
        ActionListener.respondAndRelease(listener, nodeOperation(request, task));
    }

    /**
     * Resolves node ids to concrete nodes of the incoming request.
     * NB: if the request's nodeIds() returns nothing, then the request will be sent to ALL known nodes in the cluster.
     */
    protected DiscoveryNode[] resolveRequest(NodesRequest request, ClusterState clusterState) {
        return request.resolveNodes(clusterState);
    }

    class NodeTransportHandler implements TransportRequestHandler<NodeRequest> {
        @Override
        public void messageReceived(NodeRequest request, TransportChannel channel, Task task) throws Exception {
            ActionListener.run(
                new ChannelActionListener<NodeResponse>(channel),
                channelListener -> nodeOperationAsync(request, task, channelListener)
            );
        }
    }
}
