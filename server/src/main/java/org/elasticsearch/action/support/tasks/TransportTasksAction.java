/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.tasks;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.search.ReservingResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.CancellableFanOut;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The base class for transport actions that are interacting with currently running tasks.
 */
public abstract class TransportTasksAction<
    OperationTask extends Task,
    TasksRequest extends BaseTasksRequest<TasksRequest>,
    TasksResponse extends BaseTasksResponse,
    TaskResponse extends Writeable> extends HandledTransportAction<TasksRequest, TasksResponse> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final Writeable.Reader<TasksRequest> requestReader;
    protected final Writeable.Reader<TaskResponse> responseReader;

    protected final String transportNodeAction;

    private static final String NODE_RESPONSE_BREAKER_LABEL = "<node_tasks_response>";

    @SuppressWarnings("this-escape") // registers a NodeTransportHandler bound to this action; the handler is only invoked after
                                     // construction.
    protected TransportTasksAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<TasksRequest> requestReader,
        Writeable.Reader<TaskResponse> responseReader,
        Executor nodeExecutor
    ) {
        // coordination can run on SAME because it's only O(#nodes) work
        super(actionName, transportService, actionFilters, requestReader, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.transportNodeAction = actionName + "[n]";
        this.requestReader = requestReader;
        this.responseReader = responseReader;

        transportService.registerRequestHandler(transportNodeAction, nodeExecutor, NodeTaskRequest::new, new NodeTransportHandler());
    }

    /**
     * Whether the replies of the other nodes are charged to the in-flight requests breaker, and so can fail a node when there is no room
     * for them. Off by default. An action whose replies can be large, such as listing tasks, opts in.
     */
    protected boolean nodeRepliesCanTripCircuitBreaker() {
        return false;
    }

    @Override
    protected void doExecute(Task task, TasksRequest request, ActionListener<TasksResponse> listener) {
        final NodeReplyCharge charge = newNodeReplyCharge(task);
        executeCharged(task, request, charge, ActionListener.runAfter(listener, charge::close));
    }

    /**
     * Creates the charge for the replies of the other nodes, released when its owner closes it, which a cancellation of the task does too,
     * unless somebody retained it. An action that lists more than once creates one charge for each pass.
     */
    protected final NodeReplyCharge newNodeReplyCharge(Task task) {
        final NodeReplyCharge charge = new NodeReplyCharge(nodeReplyBreaker());
        if (task instanceof CancellableTask cancellableTask) {
            cancellableTask.addListener(charge::close);
        }
        return charge;
    }

    /**
     * Fans out to the nodes and charges their replies to {@code charge}, which the caller closes.
     */
    protected final void executeCharged(Task task, TasksRequest request, NodeReplyCharge charge, ActionListener<TasksResponse> listener) {
        final var discoveryNodes = clusterService.state().nodes();
        final String[] nodeIds = resolveNodes(request, discoveryNodes);
        final CircuitBreaker breaker = charge.breaker;

        new CancellableFanOut<String, NodeTasksResponse, TasksResponse>() {
            final ArrayList<TaskResponse> taskResponses = new ArrayList<>();
            final ArrayList<TaskOperationFailure> taskOperationFailures = new ArrayList<>();
            final ArrayList<FailedNodeException> failedNodeExceptions = new ArrayList<>();
            final TransportRequestOptions transportRequestOptions = TransportRequestOptions.timeout(request.getTimeout());

            @Override
            protected void sendItemRequest(String nodeId, ActionListener<NodeTasksResponse> listener) {
                final var discoveryNode = discoveryNodes.get(nodeId);
                if (discoveryNode == null) {
                    listener.onFailure(new NoSuchNodeException(nodeId));
                    return;
                }

                final NodeTaskRequest nodeTaskRequest = new NodeTaskRequest(request);
                try {
                    transportService.sendChildRequest(
                        discoveryNode,
                        transportNodeAction,
                        nodeTaskRequest,
                        task,
                        transportRequestOptions,
                        newNodeResponseHandler(listener, breaker, task)
                    );
                } finally {
                    nodeTaskRequest.decRef();
                }
            }

            @Override
            protected void onItemResponse(String nodeId, NodeTasksResponse nodeTasksResponse) {
                try {
                    charge.charge(nodeTasksResponse.arrivedBytes);
                } catch (CircuitBreakingException e) {
                    onItemFailure(nodeId, e);
                    return;
                }
                addAllSynchronized(taskResponses, nodeTasksResponse.results);
                addAllSynchronized(taskOperationFailures, nodeTasksResponse.exceptions);
            }

            @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
            private static <T> void addAllSynchronized(List<T> allResults, Collection<T> response) {
                if (response.isEmpty() == false) {
                    synchronized (allResults) {
                        allResults.addAll(response);
                    }
                }
            }

            @Override
            protected void onItemFailure(String nodeId, Exception e) {
                logger.debug(() -> Strings.format("failed to execute on node [%s]", nodeId), e);
                synchronized (failedNodeExceptions) {
                    failedNodeExceptions.add(new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", e));
                }
            }

            @Override
            protected TasksResponse onCompletion() {
                // ref releases all happen-before here so no need to be synchronized
                return newResponse(request, taskResponses, taskOperationFailures, failedNodeExceptions);
            }

            @Override
            public String toString() {
                return actionName;
            }
        }.run(task, Iterators.forArray(nodeIds), listener);
    }

    private CircuitBreaker nodeReplyBreaker() {
        final CircuitBreaker inFlightBreaker = nodeRepliesCanTripCircuitBreaker() ? transportService.getInFlightRequestsBreaker() : null;
        return inFlightBreaker != null ? inFlightBreaker : new NoopCircuitBreaker(NODE_RESPONSE_BREAKER_LABEL);
    }

    private TransportResponseHandler<NodeTasksResponse> newNodeResponseHandler(
        ActionListener<NodeTasksResponse> listener,
        CircuitBreaker breaker,
        Task task
    ) {
        final CancellableTask cancellableTask = task instanceof CancellableTask ct ? ct : null;
        return new ReservingResponseHandler<>(
            listener,
            nodeResponseReader,
            TransportResponseHandler.TRANSPORT_WORKER,
            breaker,
            NODE_RESPONSE_BREAKER_LABEL
        ) {
            @Override
            public Releasable reserveResponseBytes(int bytes) {
                if (cancellableTask != null) {
                    cancellableTask.ensureNotCancelled();
                }
                return super.reserveResponseBytes(bytes);
            }

            @Override
            public void handleResponse(NodeTasksResponse response) {
                response.arrivedBytes = reservation.reservedTotal();
                super.handleResponse(response);
            }
        };
    }

    protected static final class NodeReplyCharge implements Releasable {
        private final CircuitBreaker breaker;
        private static final long RELEASED = Long.MIN_VALUE;

        private final AbstractRefCounted refs = AbstractRefCounted.of(this::release);
        private final AtomicBoolean ownerReleased = new AtomicBoolean();
        private final AtomicLong retained = new AtomicLong();

        NodeReplyCharge(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        void charge(long bytes) {
            if (bytes == 0 || retained.get() == RELEASED) {
                return;
            }
            breaker.addEstimateBytesAndMaybeBreak(bytes, NODE_RESPONSE_BREAKER_LABEL);
            adopt(bytes);
        }

        /**
         * Moves up to {@code bytesToMove} of what this charge holds to {@code other}, which releases them from then on, for instance
         * because the data they account for is kept by the response that {@code other} goes with. The rest is released.
         */
        public void transferTo(NodeReplyCharge other, long bytesToMove) {
            assert bytesToMove >= 0 : bytesToMove;
            final long bytes = retained.getAndSet(RELEASED);
            if (bytes != RELEASED) {
                final long moved = Math.min(bytes, bytesToMove);
                if (bytes > moved) {
                    breaker.addWithoutBreaking(moved - bytes);
                }
                other.adopt(moved);
            }
        }

        private void adopt(long bytes) {
            long current;
            do {
                current = retained.get();
                if (current == RELEASED) {
                    breaker.addWithoutBreaking(-bytes);
                    return;
                }
            } while (retained.compareAndSet(current, current + bytes) == false);
        }

        /**
         * Keeps the charge until the returned {@link Releasable} is closed, even after the owner has closed this charge.
         *
         * @return the {@link Releasable}, or {@code null} if the charge has already been released, for instance by a cancellation
         */
        @Nullable
        public Releasable retain() {
            if (refs.tryIncRef() == false) {
                return null;
            }
            return Releasables.releaseOnce(refs::decRef);
        }

        /**
         * Releases the reference of the owner, which releases the charge unless somebody retained it.
         */
        @Override
        public void close() {
            if (ownerReleased.compareAndSet(false, true)) {
                refs.decRef();
            }
        }

        void release() {
            final long bytes = retained.getAndSet(RELEASED);
            if (bytes != RELEASED) {
                breaker.addWithoutBreaking(-bytes);
            }
        }
    }

    // not an inline method reference to avoid capturing CancellableFanOut.this.
    private final Writeable.Reader<NodeTasksResponse> nodeResponseReader = NodeTasksResponse::new;

    private void nodeOperation(
        CancellableTask nodeTask,
        ActionListener<NodeTasksResponse> listener,
        TasksRequest request,
        List<OperationTask> operationTasks
    ) {
        new CancellableFanOut<OperationTask, TaskResponse, NodeTasksResponse>() {

            final ArrayList<TaskResponse> results = new ArrayList<>(operationTasks.size());
            final ArrayList<TaskOperationFailure> exceptions = new ArrayList<>();

            @Override
            protected void sendItemRequest(OperationTask operationTask, ActionListener<TaskResponse> listener) {
                ActionListener.run(listener, l -> taskOperation(nodeTask, request, operationTask, l));
            }

            @Override
            protected void onItemResponse(OperationTask operationTask, TaskResponse taskResponse) {
                synchronized (results) {
                    results.add(taskResponse);
                }
            }

            @Override
            protected void onItemFailure(OperationTask operationTask, Exception e) {
                synchronized (exceptions) {
                    exceptions.add(new TaskOperationFailure(clusterService.localNode().getId(), operationTask.getId(), e));
                }
            }

            @Override
            protected NodeTasksResponse onCompletion() {
                // ref releases all happen-before here so no need to be synchronized
                return new NodeTasksResponse(clusterService.localNode().getId(), results, exceptions);
            }

            @Override
            public String toString() {
                return transportNodeAction;
            }
        }.run(nodeTask, operationTasks.iterator(), listener);
    }

    protected String[] resolveNodes(TasksRequest request, DiscoveryNodes discoveryNodes) {
        if (request.getTargetTaskId().isSet()) {
            return new String[] { request.getTargetTaskId().getNodeId() };
        } else {
            return discoveryNodes.resolveNodes(request.getNodes());
        }
    }

    protected void processTasks(CancellableTask nodeTask, TasksRequest request, ActionListener<List<OperationTask>> nodeOperation) {
        nodeOperation.onResponse(processTasks(request));
    }

    @SuppressWarnings("unchecked")
    protected List<OperationTask> processTasks(TasksRequest request) {
        if (request.getTargetTaskId().isSet()) {
            // we are only checking one task, we can optimize it
            Task task = taskManager.getTask(request.getTargetTaskId().getId());
            if (task != null && match(task)) {
                if (request.match(task)) {
                    return List.of((OperationTask) task);
                } else {
                    throw new ResourceNotFoundException("task [{}] doesn't support this operation", request.getTargetTaskId());
                }
            } else {
                throw new ResourceNotFoundException("task [{}] is missing", request.getTargetTaskId());
            }
        } else {
            final var tasks = new ArrayList<OperationTask>();
            for (Task task : taskManager.getTasks().values()) {
                if (match(task) && request.match(task)) {
                    tasks.add((OperationTask) task);
                }
            }
            return tasks;
        }
    }

    // Determine if a task should be processed, allows subclass to extend filtering of tasks
    protected boolean match(Task task) {
        return true;
    }

    protected abstract TasksResponse newResponse(
        TasksRequest request,
        List<TaskResponse> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    );

    /**
     * Perform the required operation on the task. It is OK start an asynchronous operation or to throw an exception but not both.
     *
     * @param actionTask The related transport action task. Can be used to create a task ID to handle upstream transport cancellations.
     * @param request    the original transport request
     * @param task       the task on which the operation is taking place
     * @param listener   the listener to signal.
     */
    protected abstract void taskOperation(
        CancellableTask actionTask,
        TasksRequest request,
        OperationTask task,
        ActionListener<TaskResponse> listener
    );

    class NodeTransportHandler implements TransportRequestHandler<NodeTaskRequest> {

        @Override
        public void messageReceived(final NodeTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            assert task instanceof CancellableTask;
            TasksRequest tasksRequest = request.tasksRequest;
            processTasks(
                (CancellableTask) task,
                tasksRequest,
                new ChannelActionListener<NodeTasksResponse>(channel).delegateFailure(
                    (l, tasks) -> nodeOperation((CancellableTask) task, l, tasksRequest, tasks)
                )
            );
        }
    }

    private class NodeTaskRequest extends AbstractTransportRequest {
        private final TasksRequest tasksRequest;

        protected NodeTaskRequest(StreamInput in) throws IOException {
            super(in);
            this.tasksRequest = requestReader.read(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            assert tasksRequest.hasReferences();
            tasksRequest.writeTo(out);
        }

        protected NodeTaskRequest(TasksRequest tasksRequest) {
            super();
            tasksRequest.mustIncRef();
            this.tasksRequest = tasksRequest;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public void incRef() {
            tasksRequest.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return tasksRequest.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return tasksRequest.decRef();
        }

        @Override
        public boolean hasReferences() {
            return tasksRequest.hasReferences();
        }

        @Override
        public String toString() {
            return "[" + transportNodeAction + "][" + tasksRequest + "]";
        }
    }

    private class NodeTasksResponse extends TransportResponse {
        protected String nodeId;
        protected List<TaskOperationFailure> exceptions;
        protected List<TaskResponse> results;
        long arrivedBytes;

        NodeTasksResponse(StreamInput in) throws IOException {
            nodeId = in.readString();
            int resultsSize = in.readVInt();
            results = new ArrayList<>(resultsSize);
            for (; resultsSize > 0; resultsSize--) {
                final TaskResponse result = in.readBoolean() ? responseReader.read(in) : null;
                results.add(result);
            }
            if (in.readBoolean()) {
                int taskFailures = in.readVInt();
                exceptions = new ArrayList<>(taskFailures);
                for (int i = 0; i < taskFailures; i++) {
                    exceptions.add(new TaskOperationFailure(in));
                }
            } else {
                exceptions = null;
            }
        }

        NodeTasksResponse(String nodeId, List<TaskResponse> results, List<TaskOperationFailure> exceptions) {
            this.nodeId = nodeId;
            this.results = results;
            this.exceptions = exceptions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeCollection(results, StreamOutput::writeOptionalWriteable);
            out.writeBoolean(exceptions != null);
            if (exceptions != null) {
                out.writeCollection(exceptions);
            }
        }
    }
}
