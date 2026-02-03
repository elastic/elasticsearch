/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.tasks;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Base class for task actions that target tasks for a specific project.
 */
public abstract class TransportTasksProjectAction<
    OperationTask extends Task,
    TasksRequest extends BaseTasksRequest<TasksRequest>,
    TasksResponse extends BaseTasksResponse,
    TaskResponse extends Writeable> extends TransportTasksAction<OperationTask, TasksRequest, TasksResponse, TaskResponse> {

    private final ProjectResolver projectResolver;

    public TransportTasksProjectAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<TasksRequest> requestReader,
        Writeable.Reader<TaskResponse> responseReader,
        Executor nodeExecutor,
        ProjectResolver projectResolver
    ) {
        super(actionName, clusterService, transportService, actionFilters, requestReader, responseReader, nodeExecutor);
        this.projectResolver = Objects.requireNonNull(projectResolver, "projectResolver cannot be null");
    }

    @Override
    protected boolean match(Task task) {
        String taskProjectId = task.getProjectId();
        ProjectId projectId = projectResolver.getProjectId();
        if (taskProjectId == null) {
            // assumes tasks with no project ID belong to the default project
            return ProjectId.DEFAULT.equals(projectId);
        } else {
            return Objects.equals(projectId.id(), taskProjectId);
        }
    }
}
