/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class TransportStopRollupAction extends TransportTasksAction<RollupJobTask, StopRollupJobAction.Request,
        StopRollupJobAction.Response, StopRollupJobAction.Response> {


    @Inject
    public TransportStopRollupAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                           ActionFilters actionFilters, ClusterService clusterService) {
        super(settings, StopRollupJobAction.NAME, threadPool, clusterService, transportService, actionFilters,
            StopRollupJobAction.Request::new, StopRollupJobAction.Response::new, ThreadPool.Names.SAME);
    }

    @Override
    protected void processTasks(StopRollupJobAction.Request request, Consumer<RollupJobTask> operation) {
       TransportTaskHelper.doProcessTasks(request.getId(), operation, taskManager);
    }

    @Override
    protected void doExecute(Task task, StopRollupJobAction.Request request, ActionListener<StopRollupJobAction.Response> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(StopRollupJobAction.Request request,
                                 RollupJobTask jobTask,
                                 ActionListener<StopRollupJobAction.Response> listener) {
        if (jobTask.getConfig().getId().equals(request.getId())) {
            jobTask.stop(listener);
        } else {
            listener.onFailure(new RuntimeException("ID of rollup task [" + jobTask.getConfig().getId()
                    + "] does not match request's ID [" + request.getId() + "]"));
        }
    }

    @Override
    protected StopRollupJobAction.Response newResponse(StopRollupJobAction.Request request, List<StopRollupJobAction.Response> tasks,
                                                       List<TaskOperationFailure> taskOperationFailures,
                                                       List<FailedNodeException> failedNodeExceptions) {

        if (taskOperationFailures.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper
                    .convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper
                    .convertToElastic(failedNodeExceptions.get(0));
        }

        // Either the job doesn't exist (the user didn't create it yet) or was deleted after the Stop API executed.
        // In either case, let the user know
        if (tasks.size() == 0) {
            throw new ResourceNotFoundException("Task for Rollup Job [" + request.getId() + "] not found");
        }

        assert tasks.size() == 1;

        boolean allStopped = tasks.stream().allMatch(StopRollupJobAction.Response::isStopped);
        return new StopRollupJobAction.Response(allStopped);
    }

    @Override
    protected StopRollupJobAction.Response readTaskResponse(StreamInput in) throws IOException {
        return new StopRollupJobAction.Response(in);
    }

}