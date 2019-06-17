/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportGetDataFrameTransformsStatsAction extends
        TransportTasksAction<DataFrameTransformTask,
        GetDataFrameTransformsStatsAction.Request,
        GetDataFrameTransformsStatsAction.Response,
        GetDataFrameTransformsStatsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetDataFrameTransformsStatsAction.class);

    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;
    private final DataFrameTransformsCheckpointService transformsCheckpointService;

    @Inject
    public TransportGetDataFrameTransformsStatsAction(TransportService transportService, ActionFilters actionFilters,
                                                      ClusterService clusterService,
                                                      DataFrameTransformsConfigManager dataFrameTransformsConfigManager,
                                                      DataFrameTransformsCheckpointService transformsCheckpointService) {
        super(GetDataFrameTransformsStatsAction.NAME, clusterService, transportService, actionFilters, Request::new, Response::new,
            Response::new, ThreadPool.Names.SAME);
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
        this.transformsCheckpointService = transformsCheckpointService;
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {
        List<DataFrameTransformStateAndStats> responses = tasks.stream()
            .flatMap(r -> r.getTransformsStateAndStats().stream())
            .sorted(Comparator.comparing(DataFrameTransformStateAndStats::getId))
            .collect(Collectors.toList());
        List<ElasticsearchException> allFailedNodeExceptions = new ArrayList<>(failedNodeExceptions);
        allFailedNodeExceptions.addAll(tasks.stream().flatMap(r -> r.getNodeFailures().stream()).collect(Collectors.toList()));
        return new Response(responses, taskOperationFailures, allFailedNodeExceptions);
    }

    @Override
    protected void taskOperation(Request request, DataFrameTransformTask task, ActionListener<Response> listener) {
        // Little extra insurance, make sure we only return transforms that aren't cancelled
        ClusterState state = clusterService.state();
        String nodeId = state.nodes().getLocalNode().getId();
        if (task.isCancelled() == false) {
            transformsCheckpointService.getCheckpointStats(task.getTransformId(), task.getCheckpoint(), task.getInProgressCheckpoint(),
                    ActionListener.wrap(checkpointStats -> {
                        listener.onResponse(new Response(Collections.singletonList(
                        new DataFrameTransformStateAndStats(task.getTransformId(), task.getState(), task.getStats(), checkpointStats))));
            }, e -> {
                    listener.onResponse(new Response(
                        Collections.singletonList(new DataFrameTransformStateAndStats(task.getTransformId(), task.getState(),
                                task.getStats(), DataFrameTransformCheckpointingInfo.EMPTY)),
                        Collections.emptyList(),
                        Collections.singletonList(new FailedNodeException(nodeId, "Failed to retrieve checkpointing info", e))));
            }));
        } else {
            listener.onResponse(new Response(Collections.emptyList()));
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> finalListener) {
        dataFrameTransformsConfigManager.expandTransformIds(request.getId(), request.getPageParams(), ActionListener.wrap(
            ids -> {
                request.setExpandedIds(ids);
                request.setNodes(DataFrameNodes.dataFrameTaskNodes(ids, clusterService.state()));
                super.doExecute(task, request, ActionListener.wrap(
                    response -> collectStatsForTransformsWithoutTasks(request, response, finalListener),
                    finalListener::onFailure
                ));
            },
            e -> {
                // If the index to search, or the individual config is not there, just return empty
                if (e instanceof ResourceNotFoundException) {
                    finalListener.onResponse(new Response(Collections.emptyList()));
                } else {
                    finalListener.onFailure(e);
                }
            }
        ));
    }

    private void collectStatsForTransformsWithoutTasks(Request request,
                                                       Response response,
                                                       ActionListener<Response> listener) {
        // We gathered all there is, no need to continue
        if (request.getExpandedIds().size() == response.getTransformsStateAndStats().size()) {
            listener.onResponse(response);
            return;
        }

        Set<String> transformsWithoutTasks = new HashSet<>(request.getExpandedIds());
        transformsWithoutTasks.removeAll(response.getTransformsStateAndStats().stream().map(DataFrameTransformStateAndStats::getId)
            .collect(Collectors.toList()));

        // Small assurance that we are at least below the max. Terms search has a hard limit of 10k, we should at least be below that.
        assert transformsWithoutTasks.size() <= Request.MAX_SIZE_RETURN;

        ActionListener<List<DataFrameTransformStateAndStats>> searchStatsListener = ActionListener.wrap(
            stats -> {
                List<DataFrameTransformStateAndStats> allStateAndStats = response.getTransformsStateAndStats();
                // If the persistent task does NOT exist, it is STOPPED
                // There is a potential race condition where the saved document does not actually have a STOPPED state
                //    as the task is cancelled before we persist state.
                stats.forEach(stat ->
                    allStateAndStats.add(new DataFrameTransformStateAndStats(
                        stat.getId(),
                        new DataFrameTransformState(DataFrameTransformTaskState.STOPPED,
                            IndexerState.STOPPED,
                            stat.getTransformState().getPosition(),
                            stat.getTransformState().getCheckpoint(),
                            stat.getTransformState().getReason(),
                            stat.getTransformState().getProgress()),
                        stat.getTransformStats(),
                        stat.getCheckpointingInfo()))
                );
                transformsWithoutTasks.removeAll(
                        stats.stream().map(DataFrameTransformStateAndStats::getId).collect(Collectors.toSet()));

                // Transforms that have not been started and have no state or stats.
                transformsWithoutTasks.forEach(transformId ->
                    allStateAndStats.add(DataFrameTransformStateAndStats.initialStateAndStats(transformId)));

                // Any transform in collection could NOT have a task, so, even though the list is initially sorted
                // it can easily become arbitrarily ordered based on which transforms don't have a task or stats docs
                allStateAndStats.sort(Comparator.comparing(DataFrameTransformStateAndStats::getId));

                listener.onResponse(new Response(allStateAndStats, response.getTaskFailures(), response.getNodeFailures()));
            },
            e -> {
                if (e instanceof IndexNotFoundException) {
                    listener.onResponse(response);
                } else {
                    listener.onFailure(e);
                }
            }
        );

        dataFrameTransformsConfigManager.getTransformStats(transformsWithoutTasks, searchStatsListener);
    }
}
