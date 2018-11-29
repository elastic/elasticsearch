/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.dataframe.job.DataFrameJobTask;

import java.io.IOException;
import java.util.List;

public class TransportStopDataFrameJobAction extends
        TransportTasksAction<DataFrameJobTask, StopDataFrameJobAction.Request,
        StopDataFrameJobAction.Response, StopDataFrameJobAction.Response> {

    @Inject
    public TransportStopDataFrameJobAction(TransportService transportService, ActionFilters actionFilters,
            ClusterService clusterService) {
        super(StopDataFrameJobAction.NAME, clusterService, transportService, actionFilters,
                StopDataFrameJobAction.Request::new, StopDataFrameJobAction.Response::new, ThreadPool.Names.SAME);
    }

    @Override
    protected void doExecute(Task task, StopDataFrameJobAction.Request request,
            ActionListener<StopDataFrameJobAction.Response> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(StopDataFrameJobAction.Request request, DataFrameJobTask jobTask,
            ActionListener<StopDataFrameJobAction.Response> listener) {
        if (jobTask.getConfig().getId().equals(request.getId())) {
            jobTask.stop(listener);
        } else {
            listener.onFailure(new RuntimeException("ID of data frame indexer task [" + jobTask.getConfig().getId()
                    + "] does not match request's ID [" + request.getId() + "]"));
        }
    }

    @Override
    protected StopDataFrameJobAction.Response newResponse(StopDataFrameJobAction.Request request,
            List<StopDataFrameJobAction.Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {

        if (taskOperationFailures.isEmpty() == false) {
            throw ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
        }

        // Either the job doesn't exist (the user didn't create it yet) or was deleted
        // after the Stop API executed.
        // In either case, let the user know
        if (tasks.size() == 0) {
            throw new ResourceNotFoundException("Task for Data Frame Job [" + request.getId() + "] not found");
        }

        assert tasks.size() == 1;

        boolean allStopped = tasks.stream().allMatch(StopDataFrameJobAction.Response::isStopped);
        return new StopDataFrameJobAction.Response(allStopped);
    }

    @Override
    protected StopDataFrameJobAction.Response readTaskResponse(StreamInput in) throws IOException {
        return new StopDataFrameJobAction.Response(in);
    }
}