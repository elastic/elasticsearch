/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.ExceptionsHelper.convertToElastic;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class TransportStopDataFrameTransformAction extends
        TransportTasksAction<DataFrameTransformTask, StopDataFrameTransformAction.Request,
        StopDataFrameTransformAction.Response, StopDataFrameTransformAction.Response> {

    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(100);
    private final ThreadPool threadPool;
    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;

    @Inject
    public TransportStopDataFrameTransformAction(TransportService transportService, ActionFilters actionFilters,
                                                 ClusterService clusterService, ThreadPool threadPool,
                                                 DataFrameTransformsConfigManager dataFrameTransformsConfigManager) {
        super(StopDataFrameTransformAction.NAME, clusterService, transportService, actionFilters, StopDataFrameTransformAction.Request::new,
                StopDataFrameTransformAction.Response::new, StopDataFrameTransformAction.Response::new, ThreadPool.Names.SAME);
        this.threadPool = threadPool;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
    }

    @Override
    protected void doExecute(Task task, StopDataFrameTransformAction.Request request,
            ActionListener<StopDataFrameTransformAction.Response> listener) {

        dataFrameTransformsConfigManager.expandTransformIds(request.getId(), new PageParams(0, 10_000), ActionListener.wrap(
                expandedIds -> {
                    request.setExpandedIds(new HashSet<>(expandedIds));
                    request.setNodes(dataframeNodes(expandedIds, clusterService.state()));
                    super.doExecute(task, request, listener);
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
            throw convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw convertToElastic(failedNodeExceptions.get(0));
        }

        // Either the transform doesn't exist (the user didn't create it yet) or was deleted
        // after the Stop API executed.
        // In either case, let the user know
        if (tasks.size() == 0) {
            if (taskOperationFailures.isEmpty() == false) {
                throw convertToElastic(taskOperationFailures.get(0).getCause());
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw convertToElastic(failedNodeExceptions.get(0));
            } else {
                // This can happen we the actual task in the node no longer exists, or was never started
                return new StopDataFrameTransformAction.Response(true);
            }
        }

        boolean allStopped = tasks.stream().allMatch(StopDataFrameTransformAction.Response::isStopped);
        return new StopDataFrameTransformAction.Response(allStopped);
    }

     static String[] dataframeNodes(List<String> dataFrameIds, ClusterState clusterState) {

        Set<String> executorNodes = new HashSet<>();

        PersistentTasksCustomMetaData tasksMetaData =
                PersistentTasksCustomMetaData.getPersistentTasksCustomMetaData(clusterState);

        if (tasksMetaData != null) {
            Set<String> dataFrameIdsSet = new HashSet<>(dataFrameIds);

            Collection<PersistentTasksCustomMetaData.PersistentTask<?>> tasks =
                tasksMetaData.findTasks(DataFrameField.TASK_NAME, t -> dataFrameIdsSet.contains(t.getId()));

            for (PersistentTasksCustomMetaData.PersistentTask<?> task : tasks) {
                executorNodes.add(task.getExecutorNode());
            }
        }

        return executorNodes.toArray(new String[0]);
    }
}
