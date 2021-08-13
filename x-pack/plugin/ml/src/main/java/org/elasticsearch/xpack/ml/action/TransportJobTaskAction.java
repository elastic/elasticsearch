/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.JobTaskRequest;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.task.JobTask;

import java.util.List;

/**
 * Base class that redirects a request to a node where the job task is running.
 */
// TODO: Hacking around here with TransportTasksAction. Ideally we should have another base class in core that
// redirects to a single node only
public abstract class TransportJobTaskAction<Request extends JobTaskRequest<Request>,
        Response extends BaseTasksResponse & Writeable>
        extends TransportTasksAction<JobTask, Request, Response, Response> {

    protected final AutodetectProcessManager processManager;

    TransportJobTaskAction(String actionName, ClusterService clusterService,
                           TransportService transportService, ActionFilters actionFilters,
                           Writeable.Reader<Request> requestReader, Writeable.Reader<Response> responseReader,
                           String nodeExecutor, AutodetectProcessManager processManager) {
        super(actionName, clusterService, transportService, actionFilters,
            requestReader, responseReader, responseReader, nodeExecutor);
        this.processManager = processManager;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        String jobId = request.getJobId();
        // We need to check whether there is at least an assigned task here, otherwise we cannot redirect to the
        // node running the job task.
        PersistentTasksCustomMetadata tasks = clusterService.state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
        if (jobTask == null || jobTask.isAssigned() == false) {
            String message = "Cannot perform requested action because job [" + jobId + "] is not open";
            listener.onFailure(ExceptionsHelper.conflictStatusException(message));
        } else {
            request.setNodes(jobTask.getExecutorNode());
            super.doExecute(task, request, listener);
        }
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
                                   List<FailedNodeException> failedNodeExceptions) {
        return selectFirst(tasks, taskOperationFailures, failedNodeExceptions);

    }

    static <Response extends BaseTasksResponse> Response selectFirst(List<Response> tasks,
                                                                     List<TaskOperationFailure> taskOperationFailures,
                                                                     List<FailedNodeException> failedNodeExceptions) {
        // no need to accumulate sub responses, since we only perform an operation on one task only
        // not ideal, but throwing exceptions here works, because higher up the stack there is a try-catch block delegating to
        // the actionlistener's onFailure
        if (tasks.isEmpty()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
            } else {
                throw new IllegalStateException("No errors or response");
            }
        } else {
            if (tasks.size() > 1) {
                throw new IllegalStateException(
                        "Expected one node level response, but got [" + tasks.size() + "]");
            }
            return tasks.get(0);
        }
    }

}
