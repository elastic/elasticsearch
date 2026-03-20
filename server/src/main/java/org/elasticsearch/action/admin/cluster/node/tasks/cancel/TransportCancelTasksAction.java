/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexTaskManagementFeatures;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.action.admin.cluster.RestGetTaskAction;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Transport action that can be used to cancel currently running cancellable tasks.
 * <p>
 * For a task to be cancellable it has to return an instance of
 * {@link CancellableTask} from {@link TransportRequest#createTask}
 */
public class TransportCancelTasksAction extends TransportTasksAction<CancellableTask, CancelTasksRequest, ListTasksResponse, TaskInfo> {

    public static final String NAME = "cluster:admin/tasks/cancel";

    public static final ActionType<ListTasksResponse> TYPE = new ActionType<>(NAME);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetTaskAction.class);

    private final FeatureService featureService;

    @Inject
    public TransportCancelTasksAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        FeatureService featureService
    ) {
        super(
            NAME,
            clusterService,
            transportService,
            actionFilters,
            CancelTasksRequest::new,
            TaskInfo::from,
            // Cancellation is usually lightweight, and runs on the transport thread if the task didn't even start yet, but some
            // implementations of CancellableTask#onCancelled() are nontrivial so we use GENERIC here. TODO could it be SAME?
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.featureService = featureService;
    }

    @Override
    protected ListTasksResponse newResponse(
        CancelTasksRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    protected List<CancellableTask> processTasks(CancelTasksRequest request) {
        if (request.getTargetTaskId().isSet()) {
            // we are only checking one task, we can optimize it
            CancellableTask task = taskManager.getCancellableTask(request.getTargetTaskId().getId());
            // Log a deprecation message if this is a reindex task
            softDeprecateReindexingTasks(task);
            if (task != null) {
                if (request.match(task)) {
                    return List.of(task);
                } else {
                    throw new IllegalArgumentException("task [" + request.getTargetTaskId() + "] doesn't support this operation");
                }
            } else {
                if (taskManager.getTask(request.getTargetTaskId().getId()) != null) {
                    // The task exists, but doesn't support cancellation
                    throw new IllegalArgumentException("task [" + request.getTargetTaskId() + "] doesn't support cancellation");
                } else {
                    throw new ResourceNotFoundException("task [{}] is not found", request.getTargetTaskId());
                }
            }
        } else {
            boolean softDeprecatedWarningLogged = false;
            final var tasks = new ArrayList<CancellableTask>();
            for (CancellableTask task : taskManager.getCancellableTasks().values()) {
                // Log a deprecation message if this is a reindex task, and we haven't already logged one
                softDeprecatedWarningLogged = softDeprecateReindexingTasks(task, softDeprecatedWarningLogged);
                if (request.match(task)) {
                    tasks.add(task);
                }
            }
            return tasks;
        }
    }

    /**
     * The Get and Cancel APIs have been soft deprecated for reindexing tasks in favour of the dedicated reindexing APIs,
     * when the cluster supports {@link org.elasticsearch.index.reindex.ReindexTaskManagementFeatures#RELOCATE_ON_SHUTDOWN_NODE_FEATURE}.
     * This method logs a deprecation warning if we're processing a reindexing task
     *
     * @param task The task
     */
    private void softDeprecateReindexingTasks(Task task) {
        softDeprecateReindexingTasks(task, false);
    }

    /**
     * The Get and Cancel APIs have been soft deprecated for reindexing tasks in favour of the dedicated reindexing APIs,
     * when the cluster supports {@link org.elasticsearch.index.reindex.ReindexTaskManagementFeatures#RELOCATE_ON_SHUTDOWN_NODE_FEATURE}.
     * This method logs a deprecation warning if we're processing a reindexing task, and we haven't already logged
     * a deprecation message.
     * @param task The task
     */
    private boolean softDeprecateReindexingTasks(Task task, boolean softDeprecatedWarningLogged) {
        if (softDeprecatedWarningLogged == false
            && task != null
            && task.getAction().equals(ReindexAction.NAME)
            && featureService.clusterHasFeature(clusterService.state(), ReindexTaskManagementFeatures.RELOCATE_ON_SHUTDOWN_NODE_FEATURE)) {
            deprecationLogger.warn(
                DeprecationCategory.API,
                "cancel-api-deprecated-for-reindexing-tasks",
                "Using the task management APIs to cancel reindex tasks is deprecated. "
                    + "Use the dedicated reindex API instead, POST /_reindex/<task_id>/_cancel."
            );
            return true;
        }
        return false;
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        CancelTasksRequest request,
        CancellableTask cancellableTask,
        ActionListener<TaskInfo> listener
    ) {
        String nodeId = clusterService.localNode().getId();
        taskManager.cancelTaskAndDescendants(
            cancellableTask,
            request.getReason(),
            request.waitForCompletion(),
            listener.map(r -> cancellableTask.taskInfo(nodeId, false))
        );
    }
}
