/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.LeaderBulkByScrollTaskState;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;

public class TransportRethrottleAction extends TransportTasksAction<BulkByScrollTask, RethrottleRequest, ListTasksResponse, TaskInfo> {
    private final Client client;
    private final Client tasksClient;

    @Inject
    public TransportRethrottleAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            ReindexPlugin.RETHROTTLE_ACTION.name(),
            clusterService,
            transportService,
            actionFilters,
            RethrottleRequest::new,
            TaskInfo::from,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.client = Objects.requireNonNull(client);
        this.tasksClient = new OriginSettingClient(client, TASKS_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, RethrottleRequest request, ActionListener<ListTasksResponse> listener) {
        super.doExecute(task, request, listener.delegateFailureAndWrap((l, response) -> {
            final ListTasksResponse responseWithOriginalIdentityTasks = new ListTasksResponse(
                response.getTasks().stream().map(TaskInfo::withOriginalRelocationIdentity).toList(),
                response.getTaskFailures(),
                response.getNodeFailures()
            );
            // follow relocation chain even if there is a node failure, node might be gone, but the task is still running elsewhere.
            if (request.followRelocations()
                && responseWithOriginalIdentityTasks.getTasks().isEmpty()
                && responseWithOriginalIdentityTasks.getTaskFailures().isEmpty()) {
                followRelocationAndRethrottle(task, request, responseWithOriginalIdentityTasks, l);
            } else {
                l.onResponse(responseWithOriginalIdentityTasks);
            }
        }));
    }

    /// Potentially follow the relocation chain for a rethrottle that found no running task. The task either:
    /// - Doesn't exist
    /// - Has completed
    /// - Has completed and relocated
    ///
    /// We use `GET _tasks/{task_id}` (AKA get-by-id, which follows relocations) to resolve the task's current location,
    /// then re-issue the rethrottle against the resolved task ID. We largely stick to existing functionality. Scenarios and handling:
    /// - If the task doesn't exist, return the initial response that doesn't have the task listed and may or may not have a node failure
    /// - If get-by-id fails to follow relocation, it'll return the last completed task in the chain with a TaskRelocatedException,
    /// return the initial response that doesn't have the task listed and may or may not have a node failure.
    ///
    /// Visible for testing.
    void followRelocationAndRethrottle(
        final Task actionTask,
        final RethrottleRequest request,
        final ListTasksResponse emptyResponse,
        final ActionListener<ListTasksResponse> listener
    ) {
        assert request.followRelocations();
        final GetTaskRequest getTask = new GetTaskRequest();
        getTask.setTaskId(request.getTargetTaskId());

        tasksClient.admin().cluster().getTask(getTask, ActionListener.wrap(getResponse -> {
            final TaskResult result = getResponse.getTask();
            if (result.isCompleted()) {
                listener.onResponse(emptyResponse);
            } else {  // re-issue rethrottle to relocated task
                final TaskId currentTaskId = result.getTask().taskId();
                final RethrottleRequest retry = new RethrottleRequest();
                retry.setTargetTaskId(currentTaskId);
                retry.setRequestsPerSecond(request.getRequestsPerSecond());
                retry.setFollowRelocations(true);
                doExecute(actionTask, retry, listener);
            }
        }, e -> {
            if (ExceptionsHelper.unwrap(e, ResourceNotFoundException.class) != null) {
                listener.onResponse(emptyResponse);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        RethrottleRequest request,
        BulkByScrollTask task,
        ActionListener<TaskInfo> listener
    ) {
        rethrottle(logger, clusterService.localNode().getId(), client, task, request.getRequestsPerSecond(), listener);
    }

    static void rethrottle(
        Logger logger,
        String localNodeId,
        Client client,
        BulkByScrollTask task,
        float newRequestsPerSecond,
        ActionListener<TaskInfo> listener
    ) {

        if (task.isWorker()) {
            rethrottleChildTask(logger, localNodeId, task, newRequestsPerSecond, listener);
            return;
        }

        if (task.isLeader()) {
            rethrottleParentTask(logger, localNodeId, client, task, newRequestsPerSecond, listener);
            return;
        }

        throw new IllegalArgumentException(
            "task [" + task.getId() + "] has not yet been initialized to the point where it knows how to " + "rethrottle itself"
        );
    }

    private static void rethrottleParentTask(
        Logger logger,
        String localNodeId,
        Client client,
        BulkByScrollTask task,
        float newRequestsPerSecond,
        ActionListener<TaskInfo> listener
    ) {
        final LeaderBulkByScrollTaskState leaderState = task.getLeaderState();

        try {
            leaderState.setRequestsPerSecondWithRelocationGuard(newRequestsPerSecond);
        } catch (ElasticsearchStatusException e) {
            listener.onFailure(e);
            return;
        }

        final int runningSubtasks = leaderState.runningSliceSubTasks();

        if (runningSubtasks > 0) {
            RethrottleRequest subRequest = new RethrottleRequest();
            subRequest.setRequestsPerSecond(newRequestsPerSecond / runningSubtasks);
            subRequest.setTargetParentTaskId(new TaskId(localNodeId, task.getId()));
            logger.debug("rethrottling children of task [{}] to [{}] requests per second", task.getId(), subRequest.getRequestsPerSecond());
            client.execute(ReindexPlugin.RETHROTTLE_ACTION, subRequest, listener.delegateFailureAndWrap((l, r) -> {
                r.rethrowFailures("Rethrottle");
                l.onResponse(task.taskInfoGivenSubtaskInfo(localNodeId, r.getTasks()));
            }));
        } else {
            logger.debug("children of task [{}] are already finished, nothing to rethrottle", task.getId());
            listener.onResponse(task.taskInfo(localNodeId, true));
        }
    }

    private static void rethrottleChildTask(
        Logger logger,
        String localNodeId,
        BulkByScrollTask task,
        float newRequestsPerSecond,
        ActionListener<TaskInfo> listener
    ) {
        logger.debug("rethrottling local task [{}] to [{}] requests per second", task.getId(), newRequestsPerSecond);
        try {
            task.getWorkerState().rethrottleWithRelocationGuard(newRequestsPerSecond);
        } catch (ElasticsearchStatusException e) {
            listener.onFailure(e);
            return;
        }
        listener.onResponse(task.taskInfo(localNodeId, true));
    }

    @Override
    protected ListTasksResponse newResponse(
        RethrottleRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

}
