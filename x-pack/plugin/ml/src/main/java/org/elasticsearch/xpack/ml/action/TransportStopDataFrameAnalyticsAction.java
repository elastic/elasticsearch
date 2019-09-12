/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Stops the persistent task for running data frame analytics.
 *
 * TODO Add to the upgrade mode action
 */
public class TransportStopDataFrameAnalyticsAction
    extends TransportTasksAction<DataFrameAnalyticsTask, StopDataFrameAnalyticsAction.Request,
        StopDataFrameAnalyticsAction.Response, StopDataFrameAnalyticsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportStopDataFrameAnalyticsAction.class);

    private final ThreadPool threadPool;
    private final PersistentTasksService persistentTasksService;
    private final DataFrameAnalyticsConfigProvider configProvider;

    @Inject
    public TransportStopDataFrameAnalyticsAction(TransportService transportService, ActionFilters actionFilters,
                                                 ClusterService clusterService, ThreadPool threadPool,
                                                 PersistentTasksService persistentTasksService,
                                                 DataFrameAnalyticsConfigProvider configProvider) {
        super(StopDataFrameAnalyticsAction.NAME, clusterService, transportService, actionFilters, StopDataFrameAnalyticsAction.Request::new,
            StopDataFrameAnalyticsAction.Response::new, StopDataFrameAnalyticsAction.Response::new, ThreadPool.Names.SAME);
        this.threadPool = threadPool;
        this.persistentTasksService = persistentTasksService;
        this.configProvider = configProvider;
    }

    @Override
    protected void doExecute(Task task, StopDataFrameAnalyticsAction.Request request,
                             ActionListener<StopDataFrameAnalyticsAction.Response> listener) {
        ClusterState state = clusterService.state();
        DiscoveryNodes nodes = state.nodes();
        if (nodes.isLocalNodeElectedMaster() == false) {
            redirectToMasterNode(nodes.getMasterNode(), request, listener);
            return;
        }

        logger.debug("Received request to stop data frame analytics [{}]", request.getId());

        ActionListener<Set<String>> expandedIdsListener = ActionListener.wrap(
            expandedIds -> {
                logger.debug("Resolved data frame analytics to stop: {}", expandedIds);
                if (expandedIds.isEmpty()) {
                    listener.onResponse(new StopDataFrameAnalyticsAction.Response(true));
                    return;
                }

                PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                Set<String> analyticsToStop = findAnalyticsToStop(tasks, expandedIds, request.isForce());
                request.setExpandedIds(analyticsToStop);
                request.setNodes(findAllocatedNodesAndRemoveUnassignedTasks(analyticsToStop, tasks));

                ActionListener<StopDataFrameAnalyticsAction.Response> finalListener = ActionListener.wrap(
                    r -> waitForTaskRemoved(expandedIds, request, r, listener),
                    listener::onFailure
                );

                super.doExecute(task, request, finalListener);
            },
            listener::onFailure
        );

        expandIds(state, request, expandedIdsListener);
    }

    /** Visible for testing */
    static Set<String> findAnalyticsToStop(PersistentTasksCustomMetaData tasks, Set<String> ids, boolean force) {
        Set<String> startedAnalytics = new HashSet<>();
        Set<String> stoppingAnalytics = new HashSet<>();
        Set<String> failedAnalytics = new HashSet<>();
        sortAnalyticsByTaskState(ids, tasks, startedAnalytics, stoppingAnalytics, failedAnalytics);

        if (force == false && failedAnalytics.isEmpty() == false) {
            ElasticsearchStatusException e = failedAnalytics.size() == 1 ? ExceptionsHelper.conflictStatusException(
                "cannot close data frame analytics [{}] because it failed, use force stop instead", failedAnalytics.iterator().next()) :
                ExceptionsHelper.conflictStatusException("one or more data frame analytics are in failed state, " +
                    "use force stop instead");
            throw e;
        }

        startedAnalytics.addAll(failedAnalytics);
        return startedAnalytics;
    }

    private static void sortAnalyticsByTaskState(Set<String> analyticsIds, PersistentTasksCustomMetaData tasks,
                                                 Set<String> startedAnalytics, Set<String> stoppingAnalytics,
                                                 Set<String> failedAnalytics) {
        for (String analyticsId : analyticsIds) {
            switch (MlTasks.getDataFrameAnalyticsState(analyticsId, tasks)) {
                case STARTED:
                case REINDEXING:
                case ANALYZING:
                    startedAnalytics.add(analyticsId);
                    break;
                case STOPPING:
                    stoppingAnalytics.add(analyticsId);
                    break;
                case STOPPED:
                    break;
                case FAILED:
                    failedAnalytics.add(analyticsId);
                    break;
                default:
                    break;
            }
        }
    }

    private void expandIds(ClusterState clusterState, StopDataFrameAnalyticsAction.Request request,
                           ActionListener<Set<String>> expandedIdsListener) {
        ActionListener<List<DataFrameAnalyticsConfig>> configsListener = ActionListener.wrap(
            configs -> {
                Set<String> matchingIds = configs.stream().map(DataFrameAnalyticsConfig::getId).collect(Collectors.toSet());
                PersistentTasksCustomMetaData tasksMetaData = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                Set<String> startedIds = tasksMetaData == null ? Collections.emptySet() : tasksMetaData.tasks().stream()
                    .filter(t -> t.getId().startsWith(MlTasks.DATA_FRAME_ANALYTICS_TASK_ID_PREFIX))
                    .map(t -> t.getId().replaceFirst(MlTasks.DATA_FRAME_ANALYTICS_TASK_ID_PREFIX, ""))
                    .collect(Collectors.toSet());
                startedIds.retainAll(matchingIds);
                expandedIdsListener.onResponse(startedIds);
            },
            expandedIdsListener::onFailure
        );

        configProvider.getMultiple(request.getId(), request.allowNoMatch(), configsListener);
    }

    private String[] findAllocatedNodesAndRemoveUnassignedTasks(Set<String> analyticsIds, PersistentTasksCustomMetaData tasks) {
        List<String> nodes = new ArrayList<>();
        for (String analyticsId : analyticsIds) {
            PersistentTasksCustomMetaData.PersistentTask<?> task = MlTasks.getDataFrameAnalyticsTask(analyticsId, tasks);
            if (task == null) {
                // This should not be possible; we filtered started analytics thus the task should exist
                String msg = "Requested data frame analytics [" + analyticsId + "] be stopped but the task could not be found";
                assert task != null : msg;
            } else if (task.isAssigned()) {
                nodes.add(task.getExecutorNode());
            } else {
                // This means the task has not been assigned to a node yet so
                // we can stop it by removing its persistent task.
                // The listener is a no-op as we're already going to wait for the task to be removed.
                persistentTasksService.sendRemoveRequest(task.getId(), ActionListener.wrap(r -> {}, e -> {}));
            }
        }
        return nodes.toArray(new String[0]);
    }

    private void redirectToMasterNode(DiscoveryNode masterNode, StopDataFrameAnalyticsAction.Request request,
                                      ActionListener<StopDataFrameAnalyticsAction.Response> listener) {
        if (masterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException("no known master node"));
        } else {
            transportService.sendRequest(masterNode, actionName, request,
                new ActionListenerResponseHandler<>(listener, StopDataFrameAnalyticsAction.Response::new));
        }
    }

    @Override
    protected StopDataFrameAnalyticsAction.Response newResponse(StopDataFrameAnalyticsAction.Request request,
                                                                List<StopDataFrameAnalyticsAction.Response> tasks,
                                                                List<TaskOperationFailure> taskOperationFailures,
                                                                List<FailedNodeException> failedNodeExceptions) {
        if (request.getExpandedIds().size() != tasks.size()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
            } else {
                // This can happen when the actual task in the node no longer exists,
                // which means the data frame analytic(s) have already been closed.
                return new StopDataFrameAnalyticsAction.Response(true);
            }
        }
        return new StopDataFrameAnalyticsAction.Response(tasks.stream().allMatch(StopDataFrameAnalyticsAction.Response::isStopped));
    }

    @Override
    protected void taskOperation(StopDataFrameAnalyticsAction.Request request,
                                 DataFrameAnalyticsTask task,
                                 ActionListener<StopDataFrameAnalyticsAction.Response> listener) {
        DataFrameAnalyticsTaskState stoppingState =
            new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.STOPPING, task.getAllocationId(), null);
        task.updatePersistentTaskState(stoppingState, ActionListener.wrap(pTask -> {
                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    protected void doRun() {
                        logger.info("[{}] Stopping task with force [{}]", task.getParams().getId(), request.isForce());
                        task.stop("stop_data_frame_analytics (api)", request.getTimeout());
                        listener.onResponse(new StopDataFrameAnalyticsAction.Response(true));
                    }
                });
            },
            e -> {
                if (e instanceof ResourceNotFoundException) {
                    // the task has disappeared so must have stopped
                    listener.onResponse(new StopDataFrameAnalyticsAction.Response(true));
                } else {
                    listener.onFailure(e);
                }
            }));
    }

    void waitForTaskRemoved(Set<String> analyticsIds, StopDataFrameAnalyticsAction.Request request,
                                StopDataFrameAnalyticsAction.Response response,
                                ActionListener<StopDataFrameAnalyticsAction.Response> listener) {
        persistentTasksService.waitForPersistentTasksCondition(persistentTasks ->
                filterPersistentTasks(persistentTasks, analyticsIds).isEmpty(),
            request.getTimeout(), ActionListener.wrap(
                booleanResponse -> listener.onResponse(response),
                listener::onFailure
            ));
    }

    private static Collection<PersistentTasksCustomMetaData.PersistentTask<?>> filterPersistentTasks(
            PersistentTasksCustomMetaData persistentTasks, Set<String> analyticsIds) {
        return persistentTasks.findTasks(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            t -> analyticsIds.contains(MlTasks.dataFrameAnalyticsIdFromTaskId(t.getId())));
    }
}
