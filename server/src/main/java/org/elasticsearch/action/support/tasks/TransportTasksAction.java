/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.tasks;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    protected final Writeable.Reader<TasksResponse> responsesReader;
    protected final Writeable.Reader<TaskResponse> responseReader;

    protected final String transportNodeAction;

    protected TransportTasksAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<TasksRequest> requestReader,
        Writeable.Reader<TasksResponse> responsesReader,
        Writeable.Reader<TaskResponse> responseReader,
        String nodeExecutor
    ) {
        super(actionName, transportService, actionFilters, requestReader);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.transportNodeAction = actionName + "[n]";
        this.requestReader = requestReader;
        this.responsesReader = responsesReader;
        this.responseReader = responseReader;

        transportService.registerRequestHandler(transportNodeAction, nodeExecutor, NodeTaskRequest::new, new NodeTransportHandler());
    }

    @Override
    protected void doExecute(Task task, TasksRequest request, ActionListener<TasksResponse> listener) {

        final var taskResponses = new ArrayList<TaskResponse>();
        final var taskOperationFailures = new ArrayList<TaskOperationFailure>();
        final var failedNodeExceptions = new ArrayList<FailedNodeException>();

        final var resultListener = new SubscribableListener<TasksResponse>();
        final var resultListenerCompleter = new RunOnce(() -> {
            if (task instanceof CancellableTask cancellableTask && cancellableTask.notifyIfCancelled(resultListener)) {
                return;
            }
            // ref releases all happen-before here so no need to be synchronized
            ActionListener.completeWith(
                resultListener,
                () -> newResponse(request, taskResponses, taskOperationFailures, failedNodeExceptions)
            );
        });

        // collects node listeners & completes them if cancelled
        final var nodeCancellationListener = new SubscribableListener<NodeTasksResponse>();
        if (task instanceof CancellableTask cancellableTask) {
            cancellableTask.addListener(() -> {
                assert cancellableTask.isCancelled();
                resultListenerCompleter.run();
                cancellableTask.notifyIfCancelled(nodeCancellationListener);
            });
        }

        try (var refs = new RefCountingRunnable(() -> {
            resultListener.addListener(listener);
            resultListenerCompleter.run();
        })) {
            final var discoveryNodes = clusterService.state().nodes();
            final String[] nodeIds = resolveNodes(request, discoveryNodes);

            final var transportRequestOptions = TransportRequestOptions.timeout(request.getTimeout());

            for (final var nodeId : nodeIds) {
                final ActionListener<NodeTasksResponse> nodeResponseListener = ActionListener.notifyOnce(new ActionListener<>() {
                    @Override
                    public void onResponse(NodeTasksResponse nodeResponse) {
                        synchronized (taskResponses) {
                            taskResponses.addAll(nodeResponse.results);
                        }
                        synchronized (taskOperationFailures) {
                            taskOperationFailures.addAll(nodeResponse.exceptions);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
                            return;
                        }

                        logger.debug(() -> Strings.format("failed to execute on node [{}]", nodeId), e);

                        synchronized (failedNodeExceptions) {
                            failedNodeExceptions.add(new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", e));
                        }
                    }
                });

                if (task instanceof CancellableTask) {
                    nodeCancellationListener.addListener(nodeResponseListener);
                }

                final var discoveryNode = discoveryNodes.get(nodeId);
                if (discoveryNode == null) {
                    nodeResponseListener.onFailure(new NoSuchNodeException(nodeId));
                    continue;
                }

                transportService.sendChildRequest(
                    discoveryNode,
                    transportNodeAction,
                    new NodeTaskRequest(request),
                    task,
                    transportRequestOptions,
                    new ActionListenerResponseHandler<>(
                        ActionListener.releaseAfter(nodeResponseListener, refs.acquire()),
                        NodeTasksResponse::new
                    )
                );
            }
        } catch (Exception e) {
            // NB the listener may have been completed already (by exiting this try block) so this exception may not be sent to the caller,
            // but we cannot do anything else with it; an exception here is a bug anyway.
            logger.error("failure while broadcasting requests to nodes", e);
            assert false : e;
            throw e;
        }
    }

    private void nodeOperation(
        CancellableTask nodeTask,
        ActionListener<NodeTasksResponse> listener,
        TasksRequest request,
        List<OperationTask> operationTasks
    ) {
        final var results = new ArrayList<TaskResponse>(operationTasks.size());
        final var exceptions = new ArrayList<TaskOperationFailure>();

        final var resultListener = new SubscribableListener<NodeTasksResponse>();
        final var resultListenerCompleter = new RunOnce(() -> {
            if (nodeTask.notifyIfCancelled(resultListener)) {
                return;
            }
            // ref releases all happen-before here so no need to be synchronized
            resultListener.onResponse(new NodeTasksResponse(clusterService.localNode().getId(), results, exceptions));
        });

        // collects task listeners & completes them if cancelled
        final var taskCancellationListener = new SubscribableListener<TaskResponse>();
        nodeTask.addListener(() -> {
            assert nodeTask.isCancelled();
            resultListenerCompleter.run();
            nodeTask.notifyIfCancelled(taskCancellationListener);
        });

        try (var refs = new RefCountingRunnable(() -> {
            resultListener.addListener(listener);
            resultListenerCompleter.run();
        })) {
            for (final var operationTask : operationTasks) {
                if (nodeTask.isCancelled()) {
                    return;
                }

                final var operationTaskListener = ActionListener.notifyOnce(new ActionListener<TaskResponse>() {
                    @Override
                    public void onResponse(TaskResponse taskResponse) {
                        synchronized (results) {
                            results.add(taskResponse);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        synchronized (exceptions) {
                            exceptions.add(new TaskOperationFailure(clusterService.localNode().getId(), operationTask.getId(), e));
                        }
                    }
                });

                taskCancellationListener.addListener(operationTaskListener);

                ActionListener.run(
                    ActionListener.releaseAfter(operationTaskListener, refs.acquire()),
                    l -> taskOperation(nodeTask, request, operationTask, l)
                );
            }
        } catch (Exception e) {
            logger.error("failure processing tasks request", e);
            assert false : e;
            // This should be impossible, but just in case we can try to pass the exception back to the listener (waiting for subsidiary
            // requests too). NB the listener may already be completed by the exit from the try block, in which case there's nothing more
            // we can do.
            resultListener.onFailure(e);
        }
    }

    protected String[] resolveNodes(TasksRequest request, DiscoveryNodes discoveryNodes) {
        if (request.getTargetTaskId().isSet()) {
            return new String[] { request.getTargetTaskId().getNodeId() };
        } else {
            return discoveryNodes.resolveNodes(request.getNodes());
        }
    }

    protected void processTasks(TasksRequest request, ActionListener<List<OperationTask>> nodeOperation) {
        nodeOperation.onResponse(processTasks(request));
    }

    @SuppressWarnings("unchecked")
    protected List<OperationTask> processTasks(TasksRequest request) {
        if (request.getTargetTaskId().isSet()) {
            // we are only checking one task, we can optimize it
            Task task = taskManager.getTask(request.getTargetTaskId().getId());
            if (task != null) {
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
                if (request.match(task)) {
                    tasks.add((OperationTask) task);
                }
            }
            return tasks;
        }
    }

    protected abstract TasksResponse newResponse(
        TasksRequest request,
        List<TaskResponse> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    );

    /**
     * Perform the required operation on the task. It is OK start an asynchronous operation or to throw an exception but not both.
     * @param actionTask The related transport action task. Can be used to create a task ID to handle upstream transport cancellations.
     * @param request the original transport request
     * @param task the task on which the operation is taking place
     * @param listener the listener to signal.
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
                tasksRequest,
                new ChannelActionListener<NodeTasksResponse>(channel).delegateFailure(
                    (l, tasks) -> nodeOperation((CancellableTask) task, l, tasksRequest, tasks)
                )
            );
        }
    }

    private class NodeTaskRequest extends TransportRequest {
        private final TasksRequest tasksRequest;

        protected NodeTaskRequest(StreamInput in) throws IOException {
            super(in);
            this.tasksRequest = requestReader.read(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            tasksRequest.writeTo(out);
        }

        protected NodeTaskRequest(TasksRequest tasksRequest) {
            super();
            this.tasksRequest = tasksRequest;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

    }

    private class NodeTasksResponse extends TransportResponse {
        protected String nodeId;
        protected List<TaskOperationFailure> exceptions;
        protected List<TaskResponse> results;

        NodeTasksResponse(StreamInput in) throws IOException {
            super(in);
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

        public String getNodeId() {
            return nodeId;
        }

        public List<TaskOperationFailure> getExceptions() {
            return exceptions;
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
