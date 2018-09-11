/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.featureindexbuilder.action;

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
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobTask;

import java.io.IOException;
import java.util.List;

public class TransportStopFeatureIndexBuilderJobAction extends TransportTasksAction<FeatureIndexBuilderJobTask, StopFeatureIndexBuilderJobAction.Request,
StopFeatureIndexBuilderJobAction.Response, StopFeatureIndexBuilderJobAction.Response> {


    @Inject
    public TransportStopFeatureIndexBuilderJobAction(Settings settings, TransportService transportService,
                           ActionFilters actionFilters, ClusterService clusterService) {
        super(settings, StopFeatureIndexBuilderJobAction.NAME, clusterService, transportService, actionFilters,
                StopFeatureIndexBuilderJobAction.Request::new, StopFeatureIndexBuilderJobAction.Response::new, ThreadPool.Names.SAME);
    }

    @Override
    protected void doExecute(Task task, StopFeatureIndexBuilderJobAction.Request request, ActionListener<StopFeatureIndexBuilderJobAction.Response> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(StopFeatureIndexBuilderJobAction.Request request,
                                 FeatureIndexBuilderJobTask jobTask,
                                 ActionListener<StopFeatureIndexBuilderJobAction.Response> listener) {
        if (jobTask.getConfig().getId().equals(request.getId())) {
            jobTask.stop(listener);
        } else {
            listener.onFailure(new RuntimeException("ID of feature index builder task [" + jobTask.getConfig().getId()
                    + "] does not match request's ID [" + request.getId() + "]"));
        }
    }

    @Override
    protected StopFeatureIndexBuilderJobAction.Response newResponse(StopFeatureIndexBuilderJobAction.Request request, List<StopFeatureIndexBuilderJobAction.Response> tasks,
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
            throw new ResourceNotFoundException("Task for Feature Index Builder Job [" + request.getId() + "] not found");
        }

        assert tasks.size() == 1;

        boolean allStopped = tasks.stream().allMatch(StopFeatureIndexBuilderJobAction.Response::isStopped);
        return new StopFeatureIndexBuilderJobAction.Response(allStopped);
    }

    @Override
    protected StopFeatureIndexBuilderJobAction.Response readTaskResponse(StreamInput in) throws IOException {
        return new StopFeatureIndexBuilderJobAction.Response(in);
    }

}