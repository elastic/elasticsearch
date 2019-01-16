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
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.dataframe.transform.DataFrameTransformTask;

import java.util.List;
import java.util.function.Consumer;

public class TransportStartDataFrameTransformAction extends
        TransportTasksAction<DataFrameTransformTask, StartDataFrameTransformAction.Request,
        StartDataFrameTransformAction.Response, StartDataFrameTransformAction.Response> {

    private final XPackLicenseState licenseState;

    @Inject
    public TransportStartDataFrameTransformAction(TransportService transportService, ActionFilters actionFilters,
            ClusterService clusterService, XPackLicenseState licenseState) {
        super(StartDataFrameTransformAction.NAME, clusterService, transportService, actionFilters,
                StartDataFrameTransformAction.Request::new, StartDataFrameTransformAction.Response::new,
                StartDataFrameTransformAction.Response::new, ThreadPool.Names.SAME);
        this.licenseState = licenseState;
    }

    @Override
    protected void processTasks(StartDataFrameTransformAction.Request request, Consumer<DataFrameTransformTask> operation) {
        DataFrameTransformTask matchingTask = null;

        // todo: re-factor, see rollup TransportTaskHelper
        for (Task task : taskManager.getTasks().values()) {
            if (task instanceof DataFrameTransformTask
                    && ((DataFrameTransformTask) task).getTransformId().equals(request.getId())) {
                if (matchingTask != null) {
                    throw new IllegalArgumentException("Found more than one matching task for data frame transform [" + request.getId()
                            + "] when " + "there should only be one.");
                }
                matchingTask = (DataFrameTransformTask) task;
            }
        }

        if (matchingTask != null) {
            operation.accept(matchingTask);
        }
    }

    @Override
    protected void doExecute(Task task, StartDataFrameTransformAction.Request request,
            ActionListener<StartDataFrameTransformAction.Response> listener) {

        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(StartDataFrameTransformAction.Request request, DataFrameTransformTask transformTask,
            ActionListener<StartDataFrameTransformAction.Response> listener) {
        if (transformTask.getTransformId().equals(request.getId())) {
            transformTask.start(listener);
        } else {
            listener.onFailure(new RuntimeException("ID of data frame transform task [" + transformTask.getTransformId()
                    + "] does not match request's ID [" + request.getId() + "]"));
        }
    }

    @Override
    protected StartDataFrameTransformAction.Response newResponse(StartDataFrameTransformAction.Request request,
            List<StartDataFrameTransformAction.Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {

        if (taskOperationFailures.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
        }

        // Either the transform doesn't exist (the user didn't create it yet) or was deleted
        // after the StartAPI executed.
        // In either case, let the user know
        if (tasks.size() == 0) {
            throw new ResourceNotFoundException("Task for data frame transform [" + request.getId() + "] not found");
        }

        assert tasks.size() == 1;

        boolean allStarted = tasks.stream().allMatch(StartDataFrameTransformAction.Response::isStarted);
        return new StartDataFrameTransformAction.Response(allStarted);
    }
}
