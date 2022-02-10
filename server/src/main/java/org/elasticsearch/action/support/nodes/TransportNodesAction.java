/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.nodes;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.NodeResponseTracker;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class TransportNodesAction<
    NodesRequest extends BaseNodesRequest<NodesRequest>,
    NodesResponse extends BaseNodesResponse<?>,
    NodeRequest extends BaseNodeRequest,
    NodeResponse extends BaseNodeResponse> extends HandledTransportAction<NodesRequest, NodesResponse> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final Class<NodeResponse> nodeResponseClass;
    protected final String transportNodeAction;

    private final String finalExecutor;

    /**
     * @param actionName        action name
     * @param threadPool        thread-pool
     * @param clusterService    cluster service
     * @param transportService  transport service
     * @param actionFilters     action filters
     * @param request           node request writer
     * @param nodeRequest       node request reader
     * @param nodeExecutor      executor to execute node action on
     * @param finalExecutor     executor to execute final collection of all responses on
     * @param nodeResponseClass class of the node responses
     */
    protected TransportNodesAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<NodesRequest> request,
        Writeable.Reader<NodeRequest> nodeRequest,
        String nodeExecutor,
        String finalExecutor,
        Class<NodeResponse> nodeResponseClass
    ) {
        super(actionName, transportService, actionFilters, request);
        this.threadPool = threadPool;
        this.clusterService = Objects.requireNonNull(clusterService);
        this.transportService = Objects.requireNonNull(transportService);
        this.nodeResponseClass = Objects.requireNonNull(nodeResponseClass);

        this.transportNodeAction = actionName + "[n]";
        this.finalExecutor = finalExecutor;
        transportService.registerRequestHandler(transportNodeAction, nodeExecutor, nodeRequest, new NodeTransportHandler());
    }

    /**
     * Same as {@link #TransportNodesAction(String, ThreadPool, ClusterService, TransportService, ActionFilters, Writeable.Reader,
     * Writeable.Reader, String, String, Class)} but executes final response collection on the transport thread except for when the final
     * node response is received from the local node, in which case {@code nodeExecutor} is used.
     * This constructor should only be used for actions for which the creation of the final response is fast enough to be safely executed
     * on a transport thread.
     */
    protected TransportNodesAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<NodesRequest> request,
        Writeable.Reader<NodeRequest> nodeRequest,
        String nodeExecutor,
        Class<NodeResponse> nodeResponseClass
    ) {
        this(
            actionName,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            request,
            nodeRequest,
            nodeExecutor,
            ThreadPool.Names.SAME,
            nodeResponseClass
        );
    }

    @Override
    protected void doExecute(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
        new AsyncAction(task, request, listener).start();
    }

    /**
     * Map the responses into {@code nodeResponseClass} responses and {@link FailedNodeException}s, convert to a {@link NodesResponse} and
     * pass it to the listener. Fails the listener with a {@link NullPointerException} if {@code nodesResponses} is null.
     *
     * @param request The associated request.
     * @param nodeResponseTracker All node-level responses collected so far
     * @throws NodeResponseTracker.DiscardedResponsesException if {@code nodeResponseTracker} has already discarded the intermediate results
     * @see #newResponseAsync(Task, BaseNodesRequest, List, List, ActionListener)
     */
    // exposed for tests
    void newResponse(Task task, NodesRequest request, NodeResponseTracker nodeResponseTracker, ActionListener<NodesResponse> listener)
        throws NodeResponseTracker.DiscardedResponsesException {

        if (nodeResponseTracker == null) {
            listener.onFailure(new NullPointerException("nodesResponses"));
            return;
        }

        final List<NodeResponse> responses = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();

        for (int i = 0; i < nodeResponseTracker.getExpectedResponseCount(); ++i) {
            Object response = nodeResponseTracker.getResponse(i);
            if (nodeResponseTracker.getResponse(i) instanceof FailedNodeException) {
                FailedNodeException failedNodeException = (FailedNodeException) nodeResponseTracker.getResponse(i);
                failures.add(failedNodeException);
            } else {
                responses.add(nodeResponseClass.cast(response));
            }
        }

        newResponseAsync(task, request, responses, failures, listener);
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

    /**
     * Get a backwards compatible transport action name
     */
    protected String getTransportNodeAction(DiscoveryNode node) {
        return transportNodeAction;
    }

    class AsyncAction implements CancellableTask.CancellationListener {

        private final NodesRequest request;
        private final ActionListener<NodesResponse> listener;
        private final NodeResponseTracker nodeResponseTracker;
        private final Task task;

        AsyncAction(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;
            if (request.concreteNodes() == null) {
                resolveRequest(request, clusterService.state());
                assert request.concreteNodes() != null;
            }
            this.nodeResponseTracker = new NodeResponseTracker(request.concreteNodes().length);
        }

        void start() {
            if (task instanceof CancellableTask) {
                CancellableTask cancellableTask = (CancellableTask) task;
                cancellableTask.addListener(this);
            }
            final DiscoveryNode[] nodes = request.concreteNodes();
            if (nodes.length == 0) {
                finishHim();
                return;
            }
            final TransportRequestOptions transportRequestOptions = TransportRequestOptions.timeout(request.timeout());
            for (int i = 0; i < nodes.length; i++) {
                final int idx = i;
                final DiscoveryNode node = nodes[i];
                final String nodeId = node.getId();
                try {
                    TransportRequest nodeRequest = newNodeRequest(request);
                    if (task != null) {
                        nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                    }

                    transportService.sendRequest(
                        node,
                        getTransportNodeAction(node),
                        nodeRequest,
                        transportRequestOptions,
                        new TransportResponseHandler<NodeResponse>() {
                            @Override
                            public NodeResponse read(StreamInput in) throws IOException {
                                return newNodeResponse(in, node);
                            }

                            @Override
                            public void handleResponse(NodeResponse response) {
                                onOperation(idx, response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                onFailure(idx, node.getId(), exp);
                            }
                        }
                    );
                } catch (Exception e) {
                    onFailure(idx, nodeId, e);
                }
            }
        }

        // For testing purposes
        NodeResponseTracker getNodeResponseTracker() {
            return nodeResponseTracker;
        }

        private void onOperation(int idx, NodeResponse nodeResponse) {
            if (nodeResponseTracker.trackResponseAndCheckIfLast(idx, nodeResponse)) {
                finishHim();
            }
        }

        private void onFailure(int idx, String nodeId, Throwable t) {
            logger.debug(new ParameterizedMessage("failed to execute on node [{}]", nodeId), t);
            if (nodeResponseTracker.trackResponseAndCheckIfLast(idx, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t))) {
                finishHim();
            }
        }

        private void finishHim() {
            if (task instanceof CancellableTask) {
                CancellableTask cancellableTask = (CancellableTask) task;
                if (cancellableTask.notifyIfCancelled(listener)) {
                    return;
                }
            }

            final String executor = finalExecutor.equals(ThreadPool.Names.SAME) ? ThreadPool.Names.GENERIC : finalExecutor;
            threadPool.executor(executor).execute(() -> {
                try {
                    newResponse(task, request, nodeResponseTracker, listener);
                } catch (NodeResponseTracker.DiscardedResponsesException e) {
                    // We propagate the reason that the results, in this case the task cancellation, in case the listener needs to take
                    // follow-up actions
                    listener.onFailure((Exception) e.getCause());
                }
            });
        }

        @Override
        public void onCancelled() {
            assert task instanceof CancellableTask : "task must be cancellable";
            try {
                ((CancellableTask) task).ensureNotCancelled();
            } catch (TaskCancelledException e) {
                nodeResponseTracker.discardIntermediateResponses(e);
            }
        }
    }

    class NodeTransportHandler implements TransportRequestHandler<NodeRequest> {
        @Override
        public void messageReceived(NodeRequest request, TransportChannel channel, Task task) throws Exception {
            if (task instanceof CancellableTask) {
                ((CancellableTask) task).ensureNotCancelled();
            }
            channel.sendResponse(nodeOperation(request, task));
        }
    }

}
