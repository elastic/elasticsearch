/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction.Response.Stats;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.ml.action.TransportStartDataFrameAnalyticsAction.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportGetDataFrameAnalyticsStatsAction
    extends TransportTasksAction<DataFrameAnalyticsTask, GetDataFrameAnalyticsStatsAction.Request,
        GetDataFrameAnalyticsStatsAction.Response, QueryPage<Stats>> {

    private static final Logger LOGGER = LogManager.getLogger(TransportGetDataFrameAnalyticsStatsAction.class);

    private final Client client;
    private final AnalyticsProcessManager analyticsProcessManager;

    @Inject
    public TransportGetDataFrameAnalyticsStatsAction(TransportService transportService, ClusterService clusterService, Client client,
                                                     ActionFilters actionFilters, AnalyticsProcessManager analyticsProcessManager) {
        super(GetDataFrameAnalyticsStatsAction.NAME, clusterService, transportService, actionFilters,
            GetDataFrameAnalyticsStatsAction.Request::new, GetDataFrameAnalyticsStatsAction.Response::new,
            in -> new QueryPage<>(in, GetDataFrameAnalyticsStatsAction.Response.Stats::new), ThreadPool.Names.MANAGEMENT);
        this.client = client;
        this.analyticsProcessManager = analyticsProcessManager;
    }

    @Override
    protected GetDataFrameAnalyticsStatsAction.Response newResponse(GetDataFrameAnalyticsStatsAction.Request request,
                                                                    List<QueryPage<Stats>> tasks,
                                                                    List<TaskOperationFailure> taskFailures,
                                                                    List<FailedNodeException> nodeFailures) {
        List<Stats> stats = new ArrayList<>();
        for (QueryPage<Stats> task : tasks) {
            stats.addAll(task.results());
        }
        Collections.sort(stats, Comparator.comparing(Stats::getId));
        return new GetDataFrameAnalyticsStatsAction.Response(taskFailures, nodeFailures, new QueryPage<>(stats, stats.size(),
            GetDataFrameAnalyticsAction.Response.RESULTS_FIELD));
    }

    @Override
    protected void taskOperation(GetDataFrameAnalyticsStatsAction.Request request, DataFrameAnalyticsTask task,
                                 ActionListener<QueryPage<Stats>> listener) {
        LOGGER.debug("Get stats for running task [{}]", task.getParams().getId());

        ActionListener<Integer> progressListener = ActionListener.wrap(
            progress -> {
                Stats stats = buildStats(task.getParams().getId(), progress);
                listener.onResponse(new QueryPage<>(Collections.singletonList(stats), 1,
                    GetDataFrameAnalyticsAction.Response.RESULTS_FIELD));
            }, listener::onFailure
        );

        ClusterState clusterState = clusterService.state();
        PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        DataFrameAnalyticsState analyticsState = MlTasks.getDataFrameAnalyticsState(task.getParams().getId(), tasks);

        // For a running task we report the progress associated with its current state
        if (analyticsState == DataFrameAnalyticsState.REINDEXING) {
            getReindexTaskProgress(task, progressListener);
        } else {
            progressListener.onResponse(analyticsProcessManager.getProgressPercent(task.getAllocationId()));
        }
    }

    private void getReindexTaskProgress(DataFrameAnalyticsTask task, ActionListener<Integer> listener) {
        TaskId reindexTaskId = new TaskId(clusterService.localNode().getId(), task.getReindexingTaskId());
        GetTaskRequest getTaskRequest = new GetTaskRequest();
        getTaskRequest.setTaskId(reindexTaskId);
        client.admin().cluster().getTask(getTaskRequest, ActionListener.wrap(
            taskResponse -> {
                TaskResult taskResult = taskResponse.getTask();
                BulkByScrollTask.Status taskStatus = (BulkByScrollTask.Status) taskResult.getTask().getStatus();
                int progress =  taskStatus.getTotal() == 0 ? 100 : (int) (taskStatus.getCreated() * 100.0 / taskStatus.getTotal());
                listener.onResponse(progress);
            },
            error -> {
                if (error instanceof ResourceNotFoundException) {
                    // The task has either not started yet or has finished, thus it is better to respond null and not show progress at all
                    listener.onResponse(null);
                } else {
                    listener.onFailure(error);
                }
            }
        ));
    }

    @Override
    protected void doExecute(Task task, GetDataFrameAnalyticsStatsAction.Request request,
                             ActionListener<GetDataFrameAnalyticsStatsAction.Response> listener) {
        LOGGER.debug("Get stats for data frame analytics [{}]", request.getId());

        ActionListener<GetDataFrameAnalyticsAction.Response> getResponseListener = ActionListener.wrap(
            getResponse -> {
                List<String> expandedIds = getResponse.getResources().results().stream().map(DataFrameAnalyticsConfig::getId)
                    .collect(Collectors.toList());
                request.setExpandedIds(expandedIds);
                ActionListener<GetDataFrameAnalyticsStatsAction.Response> runningTasksStatsListener = ActionListener.wrap(
                    runningTasksStatsResponse -> gatherStatsForStoppedTasks(request.getExpandedIds(), runningTasksStatsResponse,
                        ActionListener.wrap(
                            finalResponse -> {
                                // While finalResponse has all the stats objects we need, we should report the count
                                // from the get response
                                QueryPage<Stats> finalStats = new QueryPage<>(finalResponse.getResponse().results(),
                                    getResponse.getResources().count(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD);
                                listener.onResponse(new GetDataFrameAnalyticsStatsAction.Response(finalStats));
                            },
                            listener::onFailure)),
                    listener::onFailure
                );
                super.doExecute(task, request, runningTasksStatsListener);
            },
            listener::onFailure
        );

        GetDataFrameAnalyticsAction.Request getRequest = new GetDataFrameAnalyticsAction.Request();
        getRequest.setResourceId(request.getId());
        getRequest.setAllowNoResources(request.isAllowNoMatch());
        getRequest.setPageParams(request.getPageParams());
        executeAsyncWithOrigin(client, ML_ORIGIN, GetDataFrameAnalyticsAction.INSTANCE, getRequest, getResponseListener);
    }

    void gatherStatsForStoppedTasks(List<String> expandedIds, GetDataFrameAnalyticsStatsAction.Response runningTasksResponse,
                                    ActionListener<GetDataFrameAnalyticsStatsAction.Response> listener) {
        List<String> stoppedTasksIds = determineStoppedTasksIds(expandedIds, runningTasksResponse.getResponse().results());
        List<Stats> stoppedTasksStats = stoppedTasksIds.stream().map(this::buildStatsForStoppedTask).collect(Collectors.toList());
        List<Stats> allTasksStats = new ArrayList<>(runningTasksResponse.getResponse().results());
        allTasksStats.addAll(stoppedTasksStats);
        Collections.sort(allTasksStats, Comparator.comparing(Stats::getId));
        listener.onResponse(new GetDataFrameAnalyticsStatsAction.Response(new QueryPage<>(
            allTasksStats, allTasksStats.size(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD)));
    }

    static List<String> determineStoppedTasksIds(List<String> expandedIds, List<Stats> runningTasksStats) {
        Set<String> startedTasksIds = runningTasksStats.stream().map(Stats::getId).collect(Collectors.toSet());
        return expandedIds.stream().filter(id -> startedTasksIds.contains(id) == false).collect(Collectors.toList());
    }

    private GetDataFrameAnalyticsStatsAction.Response.Stats buildStatsForStoppedTask(String concreteAnalyticsId) {
        return buildStats(concreteAnalyticsId, null);
    }

    private GetDataFrameAnalyticsStatsAction.Response.Stats buildStats(String concreteAnalyticsId, @Nullable Integer progressPercent) {
        ClusterState clusterState = clusterService.state();
        PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData.PersistentTask<?> analyticsTask = MlTasks.getDataFrameAnalyticsTask(concreteAnalyticsId, tasks);
        DataFrameAnalyticsState analyticsState = MlTasks.getDataFrameAnalyticsState(concreteAnalyticsId, tasks);
        String failureReason = null;
        if (analyticsState == DataFrameAnalyticsState.FAILED) {
            DataFrameAnalyticsTaskState taskState = (DataFrameAnalyticsTaskState) analyticsTask.getState();
            failureReason = taskState.getReason();
        }
        DiscoveryNode node = null;
        String assignmentExplanation = null;
        if (analyticsTask != null) {
            node = clusterState.nodes().get(analyticsTask.getExecutorNode());
            assignmentExplanation = analyticsTask.getAssignment().getExplanation();
        }
        return new GetDataFrameAnalyticsStatsAction.Response.Stats(
            concreteAnalyticsId, analyticsState, failureReason, progressPercent, node, assignmentExplanation);
    }
}
