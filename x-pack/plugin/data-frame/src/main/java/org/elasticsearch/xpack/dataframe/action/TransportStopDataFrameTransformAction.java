/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransportStopDataFrameTransformAction extends
        TransportTasksAction<DataFrameTransformTask, StopDataFrameTransformAction.Request,
        StopDataFrameTransformAction.Response, StopDataFrameTransformAction.Response> {

    private final ThreadPool threadPool;
    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportStopDataFrameTransformAction(TransportService transportService, ActionFilters actionFilters,
                                                 ClusterService clusterService, ThreadPool threadPool,
                                                 PersistentTasksService persistentTasksService,
                                                 DataFrameTransformsConfigManager dataFrameTransformsConfigManager) {
        super(StopDataFrameTransformAction.NAME, clusterService, transportService, actionFilters, StopDataFrameTransformAction.Request::new,
                StopDataFrameTransformAction.Response::new, StopDataFrameTransformAction.Response::new, ThreadPool.Names.SAME);
        this.threadPool = threadPool;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void doExecute(Task task, StopDataFrameTransformAction.Request request,
            ActionListener<StopDataFrameTransformAction.Response> listener) {

        final ActionListener<StopDataFrameTransformAction.Response> finalListener;
        if (request.waitForCompletion()) {
            finalListener = waitForStopListener(request, listener);
        } else {
            finalListener = listener;
        }

        dataFrameTransformsConfigManager.expandTransformIds(request.getId(), new PageParams(0, 10_000), ActionListener.wrap(
                expandedIds -> {
                    request.setExpandedIds(new HashSet<>(expandedIds));
                    request.setNodes(DataFrameNodes.dataFrameTaskNodes(expandedIds, clusterService.state()));
                    super.doExecute(task, request, finalListener);
                },
                listener::onFailure
        ));
    }

    @Override
    protected void taskOperation(StopDataFrameTransformAction.Request request, DataFrameTransformTask transformTask,
            ActionListener<StopDataFrameTransformAction.Response> listener) {

        Set<String> ids = request.getExpandedIds();
        if (ids == null) {
            listener.onFailure(new IllegalStateException("Request does not have expandedIds set"));
            return;
        }

        if (ids.contains(transformTask.getTransformId())) {
            if (transformTask.getState().getTaskState() == DataFrameTransformTaskState.FAILED && request.isForce() == false) {
                listener.onFailure(
                    new ElasticsearchStatusException("Unable to stop data frame transform [" + request.getId()
                        + "] as it is in a failed state with reason: [" + transformTask.getState().getReason() +
                        "]. Use force stop to stop the data frame transform.",
                        RestStatus.CONFLICT));
                return;
            }

            transformTask.stop();
        } else {
            listener.onFailure(new RuntimeException("ID of data frame indexer task [" + transformTask.getTransformId()
                    + "] does not match request's ID [" + request.getId() + "]"));
        }
    }

    @Override
    protected StopDataFrameTransformAction.Response newResponse(StopDataFrameTransformAction.Request request,
            List<StopDataFrameTransformAction.Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {

        if (taskOperationFailures.isEmpty() == false || failedNodeExceptions.isEmpty() == false) {
            return new StopDataFrameTransformAction.Response(taskOperationFailures, failedNodeExceptions, false);
        }

        // if tasks is empty allMatch is 'vacuously satisfied'
        boolean allStopped = tasks.stream().allMatch(StopDataFrameTransformAction.Response::isStopped);
        return new StopDataFrameTransformAction.Response(allStopped);
    }

    private ActionListener<StopDataFrameTransformAction.Response>
    waitForStopListener(StopDataFrameTransformAction.Request request,
                        ActionListener<StopDataFrameTransformAction.Response> listener) {

        return ActionListener.wrap(
                response -> {
                    // Wait until the persistent task is stopped
                    // Switch over to Generic threadpool so we don't block the network thread
                    threadPool.generic().execute(() ->
                        waitForDataFrameStopped(request.getExpandedIds(), request.getTimeout(), listener));
                },
                listener::onFailure
        );
    }

    private void waitForDataFrameStopped(Collection<String> persistentTaskIds, TimeValue timeout,
                                         ActionListener<StopDataFrameTransformAction.Response> listener) {
        persistentTasksService.waitForPersistentTasksCondition(persistentTasksCustomMetaData -> {

            logger.error("PTasks: " + persistentTasksCustomMetaData.toString());

            for (String persistentTaskId: persistentTaskIds) {
                if (persistentTasksCustomMetaData.getTask(persistentTaskId) != null) {
                    return false;
                }
            }
                logger.error("task gone");
                return true;
        }, timeout, new ActionListener<>() {
            @Override
            public void onResponse(Boolean result) {
                listener.onResponse(new StopDataFrameTransformAction.Response(Boolean.TRUE));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
