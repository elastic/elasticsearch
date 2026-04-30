/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.TaskRelocatedException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNullElse;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

/**
 * ActionType to get a single task. If the task isn't running then it'll try to request the status from request index.
 *
 * The general flow is:
 * <ul>
 * <li>If this isn't being executed on the node to which the requested TaskId belongs then move to that node.
 * <li>Look up the task and return it if it exists
 * <li>If it doesn't then look up the task from the results index
 * </ul>
 */
public class TransportGetTaskAction extends HandledTransportAction<GetTaskRequest, GetTaskResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetTaskAction.class);

    public static final String TASKS_ORIGIN = "tasks";
    public static final ActionType<GetTaskResponse> TYPE = new ActionType<>("cluster:monitor/task/get");
    private static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = timeValueSeconds(30);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Client client;
    // Used for follow-up requests when we need to follow a relocated request to preserve the user origin
    private final Client rawClient;
    private final NamedXContentRegistry xContentRegistry;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportGetTaskAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ProjectResolver projectResolver
    ) {
        super(TYPE.name(), transportService, actionFilters, GetTaskRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = new OriginSettingClient(client, TASKS_ORIGIN);
        this.rawClient = client;
        this.xContentRegistry = xContentRegistry;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        ActionListener<GetTaskResponse> relocationAwareListener = request.getFollowRelocations()
            ? listener.delegateFailureAndWrap((l, response) -> followReindexRelocationIfNeeded(request, response, l))
            : listener;
        if (clusterService.localNode().getId().equals(request.getTaskId().getNodeId())) {
            getRunningTaskFromNode(thisTask, request, relocationAwareListener);
        } else {
            runOnNodeWithTaskIfPossible(thisTask, request, relocationAwareListener);
        }
    }

    /**
     * Executed on the coordinating node to forward execution of the remaining work to the node that matches that requested
     * {@link TaskId#getNodeId()}. If the node isn't in the cluster then this will just proceed to
     * {@link #getFinishedTaskFromIndex(Task, GetTaskRequest, ActionListener)} on this node.
     */
    private void runOnNodeWithTaskIfPossible(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        DiscoveryNode node = clusterService.state().nodes().get(request.getTaskId().getNodeId());
        ActionListener<GetTaskResponse> finishedTaskListener = ActionListener.wrap(listener::onResponse, e -> {
            if (e instanceof ResourceNotFoundException) {
                e = new ResourceNotFoundException(
                    "task ["
                        + request.getTaskId()
                        + "] belongs to the node ["
                        + request.getTaskId().getNodeId()
                        + "] which isn't part of the cluster and there is no record of the task",
                    e
                );
            }
            listener.onFailure(e);
        });
        if (node == null) {
            // Node is no longer part of the cluster! Try and look the task up from the results index.
            getFinishedTaskFromIndex(thisTask, request, finishedTaskListener);
            return;
        }
        GetTaskRequest nodeRequest = request.nodeRequest(clusterService.localNode().getId(), thisTask.getId());
        ActionListener<GetTaskResponse> getTaskListener = ActionListener.wrap(listener::onResponse, e -> {
            if (ExceptionsHelper.unwrap(e, ConnectTransportException.class) != null) {
                // The node is still in the cluster state but disconnected (e.g. shutting down during relocation).
                // Fall back to the .tasks index where the completed task result should be stored.
                logger.debug("failed to contact node [{}] for task [{}], falling back to .tasks index", node.getId(), request.getTaskId());
                getFinishedTaskFromIndex(thisTask, request, finishedTaskListener);
            } else {
                listener.onFailure(e);
            }
        });
        transportService.sendRequest(
            node,
            TYPE.name(),
            nodeRequest,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                ActionListener.addTimeout(
                    request.getTimeout(),
                    threadPool,
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    getTaskListener,
                    () -> { /* TODO cancel the remote tasks? */}
                ),
                GetTaskResponse::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            )
        );
    }

    /**
     * Executed on the node that should be running the task to find and return the running task. Falls back to
     * {@link #getFinishedTaskFromIndex(Task, GetTaskRequest, ActionListener)} if the task isn't still running.
     */
    void getRunningTaskFromNode(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        Task runningTask = taskManager.getTask(request.getTaskId().getId());
        if (runningTask == null) {
            // Task isn't running, go look in the task index
            getFinishedTaskFromIndex(thisTask, request, listener);
        } else {
            if (projectResolver.supportsMultipleProjects()) {
                var requestProjectId = projectResolver.getProjectId();
                assert requestProjectId != null : "project ID cannot be null";
                if (requestProjectId == null) {
                    listener.onFailure(new IllegalStateException("No Project ID specified"));
                    return;
                }
                if (requestProjectId.id().equals(runningTask.getProjectId()) == false) {
                    listener.onFailure(
                        new ResourceNotFoundException("task [{}] isn't running and hasn't stored its results", request.getTaskId())
                    );
                    return;
                }
            }
            if (request.getWaitForCompletion()) {
                final ListenableActionFuture<Void> future = new ListenableActionFuture<>();
                RemovedTaskListener removedTaskListener = new RemovedTaskListener() {
                    @Override
                    public void onRemoved(Task task) {
                        if (task.equals(runningTask)) {
                            future.onResponse(null);
                        }
                    }

                    @Override
                    public String toString() {
                        return "Waiting for task completion " + runningTask;
                    }
                };
                taskManager.registerRemovedTaskListener(removedTaskListener);
                // Check if the task had finished before we registered the listener, so we wouldn't wait
                // for an event that would never come
                if (taskManager.getTask(request.getTaskId().getId()) == null) {
                    future.onResponse(null);
                }
                final ActionListener<Void> waitedForCompletionListener = ActionListener.runBefore(
                    listener.delegateFailureAndWrap(
                        (l, v) -> waitedForCompletion(thisTask, request, runningTask.taskInfo(clusterService.localNode().getId(), true), l)
                    ),
                    () -> taskManager.unregisterRemovedTaskListener(removedTaskListener)
                );

                future.addListener(
                    new ContextPreservingActionListener<>(
                        threadPool.getThreadContext().newRestorableContext(false),
                        waitedForCompletionListener
                    )
                );
                future.addTimeout(
                    requireNonNullElse(request.getTimeout(), DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT),
                    threadPool,
                    EsExecutors.DIRECT_EXECUTOR_SERVICE
                );
            } else {
                TaskInfo info = runningTask.taskInfo(clusterService.localNode().getId(), true);
                listener.onResponse(new GetTaskResponse(new TaskResult(false, info)));
            }
        }
    }

    /**
     * Called after waiting for the task to complete. Attempts to load the results of the task from the tasks index. If it isn't in the
     * index then returns a snapshot of the task taken shortly after completion.
     */
    void waitedForCompletion(
        Task thisTask,
        GetTaskRequest request,
        TaskInfo snapshotOfRunningTask,
        ActionListener<GetTaskResponse> listener
    ) {
        getFinishedTaskFromIndex(thisTask, request, listener.delegateResponse((delegatedListener, e) -> {
            /*
             * We couldn't load the task from the task index. Instead of 404 we should use the snapshot we took after it finished. If
             * the error isn't a 404 then we'll just throw it back to the user.
             */
            if (ExceptionsHelper.unwrap(e, ResourceNotFoundException.class) != null) {
                delegatedListener.onResponse(new GetTaskResponse(new TaskResult(true, snapshotOfRunningTask)));
            } else {
                delegatedListener.onFailure(e);
            }
        }));
    }

    /**
     * Send a {@link GetRequest} to the tasks index looking for a persisted copy of the task completed task. It'll only be found only if the
     * task's result was stored. Called on the node that once had the task if that node is still part of the cluster or on the
     * coordinating node if the node is no longer part of the cluster.
     */
    void getFinishedTaskFromIndex(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        GetRequest get = new GetRequest(TaskResultsService.TASK_INDEX, request.getTaskId().toString());
        get.setParentTask(clusterService.localNode().getId(), thisTask.getId());

        client.get(get, ActionListener.wrap(r -> onGetFinishedTaskFromIndex(r, listener), e -> {
            if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) != null) {
                // We haven't yet created the index for the task results, so it can't be found.
                listener.onFailure(
                    new ResourceNotFoundException("task [{}] isn't running and hasn't stored its results", e, request.getTaskId())
                );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Called with the {@linkplain GetResponse} from loading the task from the results index. Called on the node that once had the task if
     * that node is part of the cluster or on the coordinating node if the node wasn't part of the cluster.
     */
    void onGetFinishedTaskFromIndex(GetResponse response, ActionListener<GetTaskResponse> listener) throws IOException {
        if (false == response.isExists()) {
            listener.onFailure(new ResourceNotFoundException("task [{}] isn't running and hasn't stored its results", response.getId()));
            return;
        }
        if (response.isSourceEmpty()) {
            listener.onFailure(new ElasticsearchException("Stored task status for [{}] didn't contain any source!", response.getId()));
            return;
        }
        try (
            XContentParser parser = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                response.getSourceAsBytesRef()
            )
        ) {
            TaskResult result = TaskResult.PARSER.apply(parser, null);
            listener.onResponse(new GetTaskResponse(result));
        }
    }

    /**
     * This adds relocation awareness for reindex tasks. This is needed to maintain backward compatibility so that
     * reindex task can still be found after relocation.
     * <p>
     * If the response represents a completed reindex task that was relocated to another node, issues a new
     * {@code GetTask} request for the relocated task ID. The same step would be repeated if the relocated task was also relocated,
     * effectively following the relocation chain until the current location of the task is found.
     * <p>
     * The relocated task's response is merged with the original task's timing: the start time is taken from
     * the original task and the running time is adjusted to cover the full duration including the relocation gap.
     * For multi-hop chains each merge step correctly chains the adjustments.
     * <p>
     * Non-reindex tasks and reindex tasks without relocations pass through unchanged.
     */
    private void followReindexRelocationIfNeeded(
        GetTaskRequest originalRequest,
        GetTaskResponse response,
        ActionListener<GetTaskResponse> listener
    ) {
        TaskId relocatedTaskId = extractRelocatedReindexTaskId(response.getTask());
        if (relocatedTaskId == null) {
            listener.onResponse(response);
            return;
        }
        logger.debug("task [{}] was relocated to [{}], following relocation chain", originalRequest.getTaskId(), relocatedTaskId);
        GetTaskRequest relocatedRequest = new GetTaskRequest().setTaskId(relocatedTaskId)
            .setWaitForCompletion(originalRequest.getWaitForCompletion())
            .setTimeout(originalRequest.getTimeout());

        rawClient.admin().cluster().getTask(relocatedRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetTaskResponse relocatedResponse) {
                listener.onResponse(mergeRelocatedTask(response.getTask(), relocatedResponse));
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    "failed to follow task [{}] to its relocated task [{}], returning original response",
                    originalRequest.getTaskId(),
                    relocatedTaskId,
                    e
                );
                listener.onResponse(response);
            }
        });
    }

    /**
     * Merges the original (pre-relocation) task's timing with the relocated task's current state.
     */
    static GetTaskResponse mergeRelocatedTask(TaskResult original, GetTaskResponse relocatedResponse) {
        TaskResult relocated = relocatedResponse.getTask();
        TaskInfo originalInfo = original.getTask();
        TaskInfo relocatedInfo = relocated.getTask();

        long adjustedRunningTimeNanos = relocatedInfo.runningTimeNanos() + TimeUnit.MILLISECONDS.toNanos(
            relocatedInfo.startTime() - originalInfo.startTime()
        );

        TaskInfo mergedInfo = new TaskInfo(
            relocatedInfo.taskId(),
            relocatedInfo.type(),
            relocatedInfo.node(),
            relocatedInfo.action(),
            relocatedInfo.description(),
            relocatedInfo.status(),
            // startTime and runningTime reflect the full duration of the original task, so that the task appears to have been running
            // continuously since it was originally started
            originalInfo.startTime(),
            adjustedRunningTimeNanos,
            relocatedInfo.cancellable(),
            relocatedInfo.cancelled(),
            relocatedInfo.parentTaskId(),
            relocatedInfo.headers(),
            relocatedInfo.originalTaskId(),
            relocatedInfo.originalStartTimeMillis()
        );

        return new GetTaskResponse(new TaskResult(relocated.isCompleted(), mergedInfo, relocated.getError(), relocated.getResponse()));
    }

    /**
     * Extracts the relocated task ID from a completed reindex task result. Returns {@code null} if the result
     * is not a completed reindex task with a {@code task_relocated_exception} error containing a {@code relocated_task_id}.
     */
    static TaskId extractRelocatedReindexTaskId(TaskResult result) {
        if (result.isCompleted() == false) {
            return null;
        }
        if (ReindexAction.NAME.equals(result.getTask().action()) == false) {
            return null;
        }
        return TaskRelocatedException.relocatedTaskIdFromErrorMap(result.getErrorAsMap()).orElse(null);
    }
}
