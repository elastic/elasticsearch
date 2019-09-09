/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.xpack.core.dataframe.DataFrameMessages.DATA_FRAME_CANNOT_STOP_FAILED_TRANSFORM;

public class TransportStopDataFrameTransformAction extends TransportTasksAction<DataFrameTransformTask, Request, Response, Response> {

    private static final Logger logger = LogManager.getLogger(TransportStopDataFrameTransformAction.class);

    private final ThreadPool threadPool;
    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;
    private final PersistentTasksService persistentTasksService;
    private final Client client;

    @Inject
    public TransportStopDataFrameTransformAction(TransportService transportService, ActionFilters actionFilters,
                                                 ClusterService clusterService, ThreadPool threadPool,
                                                 PersistentTasksService persistentTasksService,
                                                 DataFrameTransformsConfigManager dataFrameTransformsConfigManager,
                                                 Client client) {
        super(StopDataFrameTransformAction.NAME, clusterService, transportService, actionFilters, Request::new,
                Response::new, Response::new, ThreadPool.Names.SAME);
        this.threadPool = threadPool;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
        this.persistentTasksService = persistentTasksService;
        this.client = client;
    }

    static void validateTaskState(ClusterState state, List<String> transformIds, boolean isForce) {
        PersistentTasksCustomMetaData tasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (isForce == false && tasks != null) {
            List<String> failedTasks = new ArrayList<>();
            List<String> failedReasons = new ArrayList<>();
            for (String transformId : transformIds) {
                PersistentTasksCustomMetaData.PersistentTask<?> dfTask = tasks.getTask(transformId);
                if (dfTask != null
                    && dfTask.getState() instanceof DataFrameTransformState
                    && ((DataFrameTransformState) dfTask.getState()).getTaskState() == DataFrameTransformTaskState.FAILED) {
                    failedTasks.add(transformId);
                    failedReasons.add(((DataFrameTransformState) dfTask.getState()).getReason());
                }
            }
            if (failedTasks.isEmpty() == false) {
                String msg = failedTasks.size() == 1 ?
                    DataFrameMessages.getMessage(DATA_FRAME_CANNOT_STOP_FAILED_TRANSFORM,
                        failedTasks.get(0),
                        failedReasons.get(0)) :
                    "Unable to stop data frame transforms. The following transforms are in a failed state " +
                        failedTasks + " with reasons " + failedReasons + ". Use force stop to stop the data frame transforms.";
                throw new ElasticsearchStatusException(msg, RestStatus.CONFLICT);
            }
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        if (nodes.isLocalNodeElectedMaster() == false) {
            // Delegates stop data frame to elected master node so it becomes the coordinating node.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException("no known master node"));
            } else {
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                        new ActionListenerResponseHandler<>(listener, Response::new));
            }
        } else {
            final ActionListener<Response> finalListener;
            if (request.waitForCompletion()) {
                finalListener = waitForStopListener(request, listener);
            } else {
                finalListener = listener;
            }

            dataFrameTransformsConfigManager.expandTransformIds(request.getId(),
                new PageParams(0, 10_000),
                request.isAllowNoMatch(),
                ActionListener.wrap(hitsAndIds -> {
                    validateTaskState(state, hitsAndIds.v2(), request.isForce());
                    request.setExpandedIds(new HashSet<>(hitsAndIds.v2()));
                    request.setNodes(DataFrameNodes.dataFrameTaskNodes(hitsAndIds.v2(), state));
                    super.doExecute(task, request, finalListener);
                },
                listener::onFailure
            ));
        }
    }

    @Override
    protected void taskOperation(Request request, DataFrameTransformTask transformTask, ActionListener<Response> listener) {

        Set<String> ids = request.getExpandedIds();
        if (ids == null) {
            listener.onFailure(new IllegalStateException("Request does not have expandedIds set"));
            return;
        }

        if (ids.contains(transformTask.getTransformId())) {
            try {
                transformTask.stop(request.isForce());
            } catch (ElasticsearchException ex) {
                listener.onFailure(ex);
                return;
            }
            listener.onResponse(new Response(Boolean.TRUE));
        } else {
            listener.onFailure(new RuntimeException("ID of data frame indexer task [" + transformTask.getTransformId()
                    + "] does not match request's ID [" + request.getId() + "]"));
        }
    }

    @Override
    protected StopDataFrameTransformAction.Response newResponse(Request request,
                                                                List<Response> tasks,
                                                                List<TaskOperationFailure> taskOperationFailures,
                                                                List<FailedNodeException> failedNodeExceptions) {

        if (taskOperationFailures.isEmpty() == false || failedNodeExceptions.isEmpty() == false) {
            return new Response(taskOperationFailures, failedNodeExceptions, false);
        }

        // if tasks is empty allMatch is 'vacuously satisfied'
        return new Response(tasks.stream().allMatch(Response::isAcknowledged));
    }

    private ActionListener<Response> waitForStopListener(Request request, ActionListener<Response> listener) {

        ActionListener<Response> onStopListener = ActionListener.wrap(
            waitResponse ->
                client.admin()
                    .indices()
                    .prepareRefresh(DataFrameInternalIndex.LATEST_INDEX_NAME)
                    .execute(ActionListener.wrap(
                        r -> listener.onResponse(waitResponse),
                        e -> {
                            logger.info("Failed to refresh internal index after delete", e);
                            listener.onResponse(waitResponse);
                        })
                    ),
            listener::onFailure
        );
        return ActionListener.wrap(
                response -> {
                    // Wait until the persistent task is stopped
                    // Switch over to Generic threadpool so we don't block the network thread
                    threadPool.generic().execute(() ->
                        waitForDataFrameStopped(request.getExpandedIds(), request.getTimeout(), request.isForce(), onStopListener));
                },
                listener::onFailure
        );
    }

    private void waitForDataFrameStopped(Set<String> persistentTaskIds,
                                         TimeValue timeout,
                                         boolean force,
                                         ActionListener<Response> listener) {
        // This map is accessed in the predicate and the listener callbacks
        final Map<String, ElasticsearchException> exceptions = new ConcurrentHashMap<>();
        persistentTasksService.waitForPersistentTasksCondition(persistentTasksCustomMetaData -> {
            if (persistentTasksCustomMetaData == null) {
                return true;
            }
            for (String persistentTaskId : persistentTaskIds) {
                PersistentTasksCustomMetaData.PersistentTask<?> transformsTask = persistentTasksCustomMetaData.getTask(persistentTaskId);
                // Either the task has successfully stopped or we have seen that it has failed
                if (transformsTask == null || exceptions.containsKey(persistentTaskId)) {
                    continue;
                }

                // If force is true, then it should eventually go away, don't add it to the collection of failures.
                DataFrameTransformState taskState = (DataFrameTransformState)transformsTask.getState();
                if (force == false && taskState != null && taskState.getTaskState() == DataFrameTransformTaskState.FAILED) {
                    exceptions.put(persistentTaskId, new ElasticsearchStatusException(
                        DataFrameMessages.getMessage(DATA_FRAME_CANNOT_STOP_FAILED_TRANSFORM,
                            persistentTaskId,
                            taskState.getReason()),
                        RestStatus.CONFLICT));

                    // If all the tasks are now flagged as failed, do not wait for another ClusterState update.
                    // Return to the caller as soon as possible
                    return persistentTasksCustomMetaData.tasks().stream().allMatch(p -> exceptions.containsKey(p.getId()));
                }
                return false;
            }
            return true;
        }, timeout, ActionListener.wrap(
            r -> {
                // No exceptions AND the tasks have gone away
                if (exceptions.isEmpty()) {
                    listener.onResponse(new Response(Boolean.TRUE));
                    return;
                }

                // We are only stopping one task, so if there is a failure, it is the only one
                if (persistentTaskIds.size() == 1) {
                    listener.onFailure(exceptions.get(persistentTaskIds.iterator().next()));
                    return;
                }

                Set<String> stoppedTasks = new HashSet<>(persistentTaskIds);
                stoppedTasks.removeAll(exceptions.keySet());
                String message = stoppedTasks.isEmpty() ?
                    "Could not stop any of the tasks as all were failed. Use force stop to stop the transforms." :
                    LoggerMessageFormat.format("Successfully stopped [{}] transforms. " +
                        "Could not stop the transforms {} as they were failed. Use force stop to stop the transforms.",
                        stoppedTasks.size(),
                        exceptions.keySet());

                listener.onFailure(new ElasticsearchStatusException(message, RestStatus.CONFLICT));
            },
            e -> {
                // waitForPersistentTasksCondition throws a IllegalStateException on timeout
                if (e instanceof IllegalStateException && e.getMessage().startsWith("Timed out")) {
                    PersistentTasksCustomMetaData persistentTasksCustomMetaData = clusterService.state().metaData()
                        .custom(PersistentTasksCustomMetaData.TYPE);

                    if (persistentTasksCustomMetaData == null) {
                        listener.onResponse(new Response(Boolean.TRUE));
                        return;
                    }

                    // collect which tasks are still running
                    Set<String> stillRunningTasks = new HashSet<>();
                    for (String persistentTaskId : persistentTaskIds) {
                        if (persistentTasksCustomMetaData.getTask(persistentTaskId) != null) {
                            stillRunningTasks.add(persistentTaskId);
                        }
                    }

                    if (stillRunningTasks.isEmpty()) {
                        // should not happen
                        listener.onResponse(new Response(Boolean.TRUE));
                        return;
                    } else {
                        StringBuilder message = new StringBuilder();
                        if (persistentTaskIds.size() - stillRunningTasks.size() - exceptions.size() > 0) {
                            message.append("Successfully stopped [");
                            message.append(persistentTaskIds.size() - stillRunningTasks.size() - exceptions.size());
                            message.append("] transforms. ");
                        }

                        if (exceptions.size() > 0) {
                            message.append("Could not stop the transforms ");
                            message.append(exceptions.keySet());
                            message.append(" as they were failed. Use force stop to stop the transforms. ");
                        }

                        if (stillRunningTasks.size() > 0) {
                            message.append("Could not stop the transforms ");
                            message.append(stillRunningTasks);
                            message.append(" as they timed out.");
                        }

                        listener.onFailure(new ElasticsearchStatusException(message.toString(), RestStatus.REQUEST_TIMEOUT));
                        return;
                    }
                }
                listener.onFailure(e);
            }
        ));
    }
}
