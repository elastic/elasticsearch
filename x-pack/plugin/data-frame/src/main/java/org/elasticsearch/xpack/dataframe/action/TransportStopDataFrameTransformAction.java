/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.dataframe.transform.DataFrameTransformTask;

import java.util.List;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class TransportStopDataFrameTransformAction extends
        TransportTasksAction<DataFrameTransformTask, StopDataFrameTransformAction.Request,
        StopDataFrameTransformAction.Response, StopDataFrameTransformAction.Response> {

    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(100);
    private final ThreadPool threadPool;

    @Inject
    public TransportStopDataFrameTransformAction(TransportService transportService, ActionFilters actionFilters,
            ClusterService clusterService, ThreadPool threadPool) {
        super(StopDataFrameTransformAction.NAME, clusterService, transportService, actionFilters, StopDataFrameTransformAction.Request::new,
                StopDataFrameTransformAction.Response::new, StopDataFrameTransformAction.Response::new, ThreadPool.Names.SAME);
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, StopDataFrameTransformAction.Request request,
            ActionListener<StopDataFrameTransformAction.Response> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(StopDataFrameTransformAction.Request request, DataFrameTransformTask transformTask,
            ActionListener<StopDataFrameTransformAction.Response> listener) {
        if (transformTask.getTransformId().equals(request.getId())) {
            if (request.waitForCompletion() == false) {
                transformTask.stop(listener);
            } else {
                ActionListener<StopDataFrameTransformAction.Response> blockingListener = ActionListener.wrap(response -> {
                    if (response.isStopped()) {
                        // The Task acknowledged that it is stopped/stopping... wait until the status actually
                        // changes over before returning. Switch over to Generic threadpool so
                        // we don't block the network thread
                        threadPool.generic().execute(() -> {
                            try {
                                long untilInNanos = System.nanoTime() + request.getTimeout().getNanos();

                                while (System.nanoTime() - untilInNanos < 0) {
                                    if (transformTask.isStopped()) {
                                        listener.onResponse(response);
                                        return;
                                    }
                                    Thread.sleep(WAIT_FOR_COMPLETION_POLL.millis());
                                }
                                // ran out of time
                                listener.onFailure(new ElasticsearchTimeoutException(
                                        DataFrameMessages.getMessage(DataFrameMessages.REST_STOP_TRANSFORM_WAIT_FOR_COMPLETION_TIMEOUT,
                                                request.getTimeout().getStringRep(), request.getId())));
                            } catch (InterruptedException e) {
                                listener.onFailure(new ElasticsearchException(DataFrameMessages.getMessage(
                                        DataFrameMessages.REST_STOP_TRANSFORM_WAIT_FOR_COMPLETION_INTERRUPT, request.getId()), e));
                            }
                        });
                    } else {
                        // Did not acknowledge stop, just return the response
                        listener.onResponse(response);
                    }
                }, listener::onFailure);

                transformTask.stop(blockingListener);
            }
        } else {
            listener.onFailure(new RuntimeException("ID of data frame indexer task [" + transformTask.getTransformId()
                    + "] does not match request's ID [" + request.getId() + "]"));
        }
    }

    @Override
    protected StopDataFrameTransformAction.Response newResponse(StopDataFrameTransformAction.Request request,
            List<StopDataFrameTransformAction.Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {

        if (taskOperationFailures.isEmpty() == false) {
            throw ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
        }

        // Either the transform doesn't exist (the user didn't create it yet) or was deleted
        // after the Stop API executed.
        // In either case, let the user know
        if (tasks.size() == 0) {
            throw new ResourceNotFoundException("Task for Data Frame transform [" + request.getId() + "] not found");
        }

        assert tasks.size() == 1;

        boolean allStopped = tasks.stream().allMatch(StopDataFrameTransformAction.Response::isStopped);
        return new StopDataFrameTransformAction.Response(allStopped);
    }
}