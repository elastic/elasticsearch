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
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStoredDoc;
import org.elasticsearch.xpack.core.dataframe.transforms.NodeAttributes;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
        List<DataFrameTransformStats> responses = tasks.stream()
            .flatMap(r -> r.getTransformsStats().stream())
            .sorted(Comparator.comparing(DataFrameTransformStats::getId))
            .collect(Collectors.toList());
        List<ElasticsearchException> allFailedNodeExceptions = new ArrayList<>(failedNodeExceptions);
        allFailedNodeExceptions.addAll(tasks.stream().flatMap(r -> r.getNodeFailures().stream()).collect(Collectors.toList()));
        return new Response(responses, responses.size(), taskOperationFailures, allFailedNodeExceptions);
    }

    @Override
    protected void taskOperation(Request request, DataFrameTransformTask task, ActionListener<Response> listener) {
        // Little extra insurance, make sure we only return transforms that aren't cancelled
        ClusterState state = clusterService.state();
        String nodeId = state.nodes().getLocalNode().getId();
        if (task.isCancelled() == false) {
            DataFrameTransformState transformState = task.getState();
            task.getCheckpointingInfo(transformsCheckpointService, ActionListener.wrap(
                checkpointingInfo -> listener.onResponse(new Response(
                    Collections.singletonList(new DataFrameTransformStats(task.getTransformId(),
                        DataFrameTransformStats.State.fromComponents(transformState.getTaskState(), transformState.getIndexerState()),
                        transformState.getReason(),
                        null,
                        task.getStats(),
                        checkpointingInfo)),
                    1L)),
                e -> {
                    logger.warn("Failed to retrieve checkpointing info for transform [" + task.getTransformId() + "]", e);
                    listener.onResponse(new Response(
                    Collections.singletonList(new DataFrameTransformStats(task.getTransformId(),
                        DataFrameTransformStats.State.fromComponents(transformState.getTaskState(), transformState.getIndexerState()),
                        transformState.getReason(),
                        null,
                        task.getStats(),
                        DataFrameTransformCheckpointingInfo.EMPTY)),
                    1L,
                    Collections.emptyList(),
                    Collections.singletonList(new FailedNodeException(nodeId, "Failed to retrieve checkpointing info", e))));
                }
                ));
        } else {
            listener.onResponse(new Response(Collections.emptyList(), 0L));
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> finalListener) {
        dataFrameTransformsConfigManager.expandTransformIds(request.getId(),
            request.getPageParams(),
            request.isAllowNoMatch(),
            ActionListener.wrap(hitsAndIds -> {
                request.setExpandedIds(hitsAndIds.v2());
                final ClusterState state = clusterService.state();
                request.setNodes(DataFrameNodes.dataFrameTaskNodes(hitsAndIds.v2(), state));
                super.doExecute(task, request, ActionListener.wrap(
                    response -> {
                        PersistentTasksCustomMetaData tasksInProgress = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                        if (tasksInProgress != null) {
                            // Mutates underlying state object with the assigned node attributes
                            response.getTransformsStats().forEach(dtsasi -> setNodeAttributes(dtsasi, tasksInProgress, state));
                        }
                        collectStatsForTransformsWithoutTasks(request, response, ActionListener.wrap(
                            finalResponse -> finalListener.onResponse(new Response(finalResponse.getTransformsStats(),
                                hitsAndIds.v1(),
                                finalResponse.getTaskFailures(),
                                finalResponse.getNodeFailures())),
                            finalListener::onFailure
                        ));
                    },
                    finalListener::onFailure
                ));
            },
            e -> {
                // If the index to search, or the individual config is not there, just return empty
                if (e instanceof ResourceNotFoundException) {
                    finalListener.onResponse(new Response(Collections.emptyList(), 0L));
                } else {
                    finalListener.onFailure(e);
                }
            }
        ));
    }

    private static void setNodeAttributes(DataFrameTransformStats dataFrameTransformStats,
                                          PersistentTasksCustomMetaData persistentTasksCustomMetaData, ClusterState state) {
        var pTask = persistentTasksCustomMetaData.getTask(dataFrameTransformStats.getId());
        if (pTask != null) {
            dataFrameTransformStats.setNode(NodeAttributes.fromDiscoveryNode(state.nodes().get(pTask.getExecutorNode())));
        }
    }

    private void collectStatsForTransformsWithoutTasks(Request request,
                                                       Response response,
                                                       ActionListener<Response> listener) {
        // We gathered all there is, no need to continue
        if (request.getExpandedIds().size() == response.getTransformsStats().size()) {
            listener.onResponse(response);
            return;
        }

        Set<String> transformsWithoutTasks = new HashSet<>(request.getExpandedIds());
        transformsWithoutTasks.removeAll(response.getTransformsStats().stream().map(DataFrameTransformStats::getId)
            .collect(Collectors.toList()));

        // Small assurance that we are at least below the max. Terms search has a hard limit of 10k, we should at least be below that.
        assert transformsWithoutTasks.size() <= Request.MAX_SIZE_RETURN;

        // If the persistent task does NOT exist, it is STOPPED
        // There is a potential race condition where the saved document does not actually have a STOPPED state
        // as the task is cancelled before we persist state.
        ActionListener<List<DataFrameTransformStoredDoc>> searchStatsListener = ActionListener.wrap(
            statsForTransformsWithoutTasks -> {
                List<DataFrameTransformStats> allStateAndStats = response.getTransformsStats();
                addCheckpointingInfoForTransformsWithoutTasks(allStateAndStats, statsForTransformsWithoutTasks,
                    ActionListener.wrap(
                        aVoid -> {
                            transformsWithoutTasks.removeAll(statsForTransformsWithoutTasks.stream()
                                .map(DataFrameTransformStoredDoc::getId).collect(Collectors.toSet()));

                            // Transforms that have not been started and have no state or stats.
                            transformsWithoutTasks.forEach(
                                transformId -> allStateAndStats.add(DataFrameTransformStats.initialStats(transformId)));

                            // Any transform in collection could NOT have a task, so, even though the list is initially sorted
                            // it can easily become arbitrarily ordered based on which transforms don't have a task or stats docs
                            allStateAndStats.sort(Comparator.comparing(DataFrameTransformStats::getId));

                            listener.onResponse(new Response(allStateAndStats,
                                allStateAndStats.size(),
                                response.getTaskFailures(),
                                response.getNodeFailures()));
                        },
                        listener::onFailure));
            },
            e -> {
                if (e instanceof IndexNotFoundException) {
                    listener.onResponse(response);
                } else {
                    listener.onFailure(e);
                }
            }
        );

        dataFrameTransformsConfigManager.getTransformStoredDoc(transformsWithoutTasks, searchStatsListener);
    }

    private void populateSingleStoppedTransformStat(DataFrameTransformStoredDoc transform,
                                                    ActionListener<DataFrameTransformCheckpointingInfo> listener) {
        transformsCheckpointService.getCheckpointingInfo(
            transform.getId(),
            transform.getTransformState().getCheckpoint(),
            transform.getTransformState().getPosition(),
            transform.getTransformState().getProgress(),
            ActionListener.wrap(
                listener::onResponse,
                e -> {
                    logger.warn("Failed to retrieve checkpointing info for transform [" + transform.getId() + "]", e);
                    listener.onResponse(DataFrameTransformCheckpointingInfo.EMPTY);
                }
            ));
    }

    private void addCheckpointingInfoForTransformsWithoutTasks(List<DataFrameTransformStats> allStateAndStats,
                                                               List<DataFrameTransformStoredDoc> statsForTransformsWithoutTasks,
                                                               ActionListener<Void> listener) {

        if (statsForTransformsWithoutTasks.isEmpty()) {
            // No work to do, but we must respond to the listener
            listener.onResponse(null);
            return;
        }

        AtomicInteger numberRemaining = new AtomicInteger(statsForTransformsWithoutTasks.size());
        AtomicBoolean isExceptionReported = new AtomicBoolean(false);

        statsForTransformsWithoutTasks.forEach(stat -> populateSingleStoppedTransformStat(stat,
            ActionListener.wrap(
                checkpointingInfo -> {
                    synchronized (allStateAndStats) {
                        allStateAndStats.add(new DataFrameTransformStats(
                            stat.getId(),
                            DataFrameTransformStats.State.STOPPED,
                            null,
                            null,
                            stat.getTransformStats(),
                            checkpointingInfo));
                    }
                    if (numberRemaining.decrementAndGet() == 0) {
                        listener.onResponse(null);
                    }
                },
                e -> {
                    if (isExceptionReported.compareAndSet(false, true)) {
                        listener.onFailure(e);
                    }
                }
            )));
    }
}
