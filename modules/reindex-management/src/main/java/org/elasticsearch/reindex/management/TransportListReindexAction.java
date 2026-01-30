/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksProjectAction;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Transport action for listing all running reindex tasks.
 * This action filters for reindex tasks and only returns parent tasks (not subtasks).
 * Project scoping is handled by TransportTasksProjectAction, which only returns
 * tasks for the current project.
 */
public class TransportListReindexAction extends TransportTasksProjectAction<Task, ListReindexRequest, ListReindexResponse, TaskInfo> {

    public static final ActionType<ListReindexResponse> TYPE = new ActionType<>("cluster:monitor/reindex/list");

    @Inject
    public TransportListReindexAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            ListReindexRequest::new,
            TaskInfo::from,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
    }

    @Override
    protected ListReindexResponse newResponse(
        ListReindexRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new ListReindexResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(CancellableTask actionTask, ListReindexRequest request, Task task, ActionListener<TaskInfo> listener) {
        listener.onResponse(task.taskInfo(clusterService.localNode().getId(), request.getDetailed()));
    }
}
