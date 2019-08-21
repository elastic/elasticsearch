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
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformTaskAction;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.List;

/**
 * Internal only transport class to change an allocated persistent task's state to started
 */
public class TransportStartDataFrameTransformTaskAction extends
    TransportTasksAction<DataFrameTransformTask, StartDataFrameTransformTaskAction.Request,
        StartDataFrameTransformTaskAction.Response, StartDataFrameTransformTaskAction.Response> {

    private final XPackLicenseState licenseState;

    @Inject
    public TransportStartDataFrameTransformTaskAction(TransportService transportService, ActionFilters actionFilters,
                                                  ClusterService clusterService, XPackLicenseState licenseState) {
        super(StartDataFrameTransformTaskAction.NAME, clusterService, transportService, actionFilters,
            StartDataFrameTransformTaskAction.Request::new, StartDataFrameTransformTaskAction.Response::new,
            StartDataFrameTransformTaskAction.Response::new, ThreadPool.Names.SAME);
        this.licenseState = licenseState;
    }

    @Override
    protected void doExecute(Task task, StartDataFrameTransformTaskAction.Request request,
                             ActionListener<StartDataFrameTransformTaskAction.Response> listener) {

        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(StartDataFrameTransformTaskAction.Request request, DataFrameTransformTask transformTask,
                                 ActionListener<StartDataFrameTransformTaskAction.Response> listener) {
        if (transformTask.getTransformId().equals(request.getId())) {
            transformTask.start(null, request.isForce(), listener);
        } else {
            listener.onFailure(new RuntimeException("ID of data frame transform task [" + transformTask.getTransformId()
                + "] does not match request's ID [" + request.getId() + "]"));
        }
    }

    @Override
    protected StartDataFrameTransformTaskAction.Response newResponse(StartDataFrameTransformTaskAction.Request request,
                                                                     List<StartDataFrameTransformTaskAction.Response> tasks,
                                                                     List<TaskOperationFailure> taskOperationFailures,
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

        boolean allStarted = tasks.stream().allMatch(StartDataFrameTransformTaskAction.Response::isStarted);
        return new StartDataFrameTransformTaskAction.Response(allStarted);
    }
}
