/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.dataframe.job.DataFrameJobTask;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class TransportStartDataFrameJobAction extends
        TransportTasksAction<DataFrameJobTask, StartDataFrameJobAction.Request, 
        StartDataFrameJobAction.Response, StartDataFrameJobAction.Response> {

    private final XPackLicenseState licenseState;

    @Inject
    public TransportStartDataFrameJobAction(TransportService transportService, ActionFilters actionFilters,
            ClusterService clusterService, XPackLicenseState licenseState) {
        super(StartDataFrameJobAction.NAME, clusterService, transportService, actionFilters,
                StartDataFrameJobAction.Request::new, StartDataFrameJobAction.Response::new, ThreadPool.Names.SAME);
        this.licenseState = licenseState;
    }

    @Override
    protected void processTasks(StartDataFrameJobAction.Request request, Consumer<DataFrameJobTask> operation) {
        DataFrameJobTask matchingTask = null;

        // todo: re-factor, see rollup TransportTaskHelper
        for (Task task : taskManager.getTasks().values()) {
            if (task instanceof DataFrameJobTask
                    && ((DataFrameJobTask) task).getConfig().getId().equals(request.getId())) {
                if (matchingTask != null) {
                    throw new IllegalArgumentException("Found more than one matching task for data frame job [" + request.getId()
                            + "] when " + "there should only be one.");
                }
                matchingTask = (DataFrameJobTask) task;
            }
        }

        if (matchingTask != null) {
            operation.accept(matchingTask);
        }
    }

    @Override
    protected void doExecute(Task task, StartDataFrameJobAction.Request request,
            ActionListener<StartDataFrameJobAction.Response> listener) {

        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(StartDataFrameJobAction.Request request, DataFrameJobTask jobTask,
            ActionListener<StartDataFrameJobAction.Response> listener) {
        if (jobTask.getConfig().getId().equals(request.getId())) {
            jobTask.start(listener);
        } else {
            listener.onFailure(new RuntimeException("ID of FeatureIndexBuilder task [" + jobTask.getConfig().getId()
                    + "] does not match request's ID [" + request.getId() + "]"));
        }
    }

    @Override
    protected StartDataFrameJobAction.Response newResponse(StartDataFrameJobAction.Request request,
            List<StartDataFrameJobAction.Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {

        if (taskOperationFailures.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
        }

        // Either the job doesn't exist (the user didn't create it yet) or was deleted
        // after the StartAPI executed.
        // In either case, let the user know
        if (tasks.size() == 0) {
            throw new ResourceNotFoundException("Task for FeatureIndexBuilder Job [" + request.getId() + "] not found");
        }

        assert tasks.size() == 1;

        boolean allStarted = tasks.stream().allMatch(StartDataFrameJobAction.Response::isStarted);
        return new StartDataFrameJobAction.Response(allStarted);
    }

    @Override
    protected StartDataFrameJobAction.Response readTaskResponse(StreamInput in) throws IOException {
        return new StartDataFrameJobAction.Response(in);
    }

}
