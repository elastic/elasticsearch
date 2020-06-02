/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.NodeAttributes;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TransportGetTransformStatsAction extends TransportTasksAction<TransformTask, Request, Response, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetTransformStatsAction.class);

    private final TransformConfigManager transformConfigManager;
    private final TransformCheckpointService transformCheckpointService;
    private final Client client;

    @Inject
    public TransportGetTransformStatsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        TransformServices transformServices,
        Client client
    ) {
        this(GetTransformStatsAction.NAME, transportService, actionFilters, clusterService, transformServices, client);
    }

    protected TransportGetTransformStatsAction(
        String name,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        TransformServices transformServices,
        Client client
    ) {
        super(name, clusterService, transportService, actionFilters, Request::new, Response::new, Response::new, ThreadPool.Names.SAME);
        this.transformConfigManager = transformServices.getConfigManager();
        this.transformCheckpointService = transformServices.getCheckpointService();
        this.client = client;
    }

    @Override
    protected Response newResponse(
        Request request,
        List<Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        List<TransformStats> responses = tasks.stream()
            .flatMap(r -> r.getTransformsStats().stream())
            .sorted(Comparator.comparing(TransformStats::getId))
            .collect(Collectors.toList());
        List<ElasticsearchException> allFailedNodeExceptions = new ArrayList<>(failedNodeExceptions);
        allFailedNodeExceptions.addAll(tasks.stream().flatMap(r -> r.getNodeFailures().stream()).collect(Collectors.toList()));
        return new Response(responses, responses.size(), taskOperationFailures, allFailedNodeExceptions);
    }

    @Override
    protected void taskOperation(Request request, TransformTask task, ActionListener<Response> listener) {
        // Little extra insurance, make sure we only return transforms that aren't cancelled
        ClusterState state = clusterService.state();
        String nodeId = state.nodes().getLocalNode().getId();
        if (task.isCancelled() == false) {
            task.getCheckpointingInfo(
                transformCheckpointService,
                ActionListener.wrap(
                    checkpointingInfo -> listener.onResponse(
                        new Response(Collections.singletonList(deriveStats(task, checkpointingInfo)), 1L)
                    ),
                    e -> {
                        logger.warn("Failed to retrieve checkpointing info for transform [" + task.getTransformId() + "]", e);
                        listener.onResponse(
                            new Response(
                                Collections.singletonList(deriveStats(task, null)),
                                1L,
                                Collections.emptyList(),
                                Collections.singletonList(new FailedNodeException(nodeId, "Failed to retrieve checkpointing info", e))
                            )
                        );
                    }
                )
            );
        } else {
            listener.onResponse(new Response(Collections.emptyList(), 0L));
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> finalListener) {
        transformConfigManager.expandTransformIds(
            request.getId(),
            request.getPageParams(),
            request.isAllowNoMatch(),
            ActionListener.wrap(hitsAndIds -> {
                request.setExpandedIds(hitsAndIds.v2());
                final ClusterState state = clusterService.state();
                request.setNodes(TransformNodes.transformTaskNodes(hitsAndIds.v2(), state));
                super.doExecute(task, request, ActionListener.wrap(response -> {
                    PersistentTasksCustomMetadata tasksInProgress = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
                    if (tasksInProgress != null) {
                        // Mutates underlying state object with the assigned node attributes
                        response.getTransformsStats().forEach(dtsasi -> setNodeAttributes(dtsasi, tasksInProgress, state));
                    }
                    collectStatsForTransformsWithoutTasks(
                        request,
                        response,
                        ActionListener.wrap(
                            finalResponse -> finalListener.onResponse(
                                new Response(
                                    finalResponse.getTransformsStats(),
                                    hitsAndIds.v1(),
                                    finalResponse.getTaskFailures(),
                                    finalResponse.getNodeFailures()
                                )
                            ),
                            finalListener::onFailure
                        )
                    );
                }, finalListener::onFailure));
            },
                e -> {
                    // If the index to search, or the individual config is not there, just return empty
                    if (e instanceof ResourceNotFoundException) {
                        finalListener.onResponse(new Response(Collections.emptyList(), 0L));
                    } else {
                        finalListener.onFailure(e);
                    }
                }
            )
        );
    }

    private static void setNodeAttributes(
        TransformStats transformStats,
        PersistentTasksCustomMetadata persistentTasksCustomMetadata,
        ClusterState state
    ) {
        var pTask = persistentTasksCustomMetadata.getTask(transformStats.getId());
        if (pTask != null) {
            transformStats.setNode(NodeAttributes.fromDiscoveryNode(state.nodes().get(pTask.getExecutorNode())));
        }
    }

    static TransformStats deriveStats(TransformTask task, @Nullable TransformCheckpointingInfo checkpointingInfo) {
        TransformState transformState = task.getState();
        TransformStats.State derivedState = TransformStats.State.fromComponents(
            transformState.getTaskState(),
            transformState.getIndexerState()
        );
        String reason = transformState.getReason();
        if (transformState.shouldStopAtNextCheckpoint()
            && derivedState.equals(TransformStats.State.STOPPED) == false
            && derivedState.equals(TransformStats.State.FAILED) == false) {
            derivedState = TransformStats.State.STOPPING;
            reason = Strings.isNullOrEmpty(reason) ? "transform is set to stop at the next checkpoint" : reason;
        }
        return new TransformStats(
            task.getTransformId(),
            derivedState,
            reason,
            null,
            task.getStats(),
            checkpointingInfo == null ? TransformCheckpointingInfo.EMPTY : checkpointingInfo
        );
    }

    private void collectStatsForTransformsWithoutTasks(Request request, Response response, ActionListener<Response> listener) {
        // We gathered all there is, no need to continue
        if (request.getExpandedIds().size() == response.getTransformsStats().size()) {
            listener.onResponse(response);
            return;
        }
        Set<String> transformsWithoutTasks = new HashSet<>(request.getExpandedIds());
        transformsWithoutTasks.removeAll(response.getTransformsStats().stream().map(TransformStats::getId).collect(Collectors.toList()));

        // Small assurance that we are at least below the max. Terms search has a hard limit of 10k, we should at least be below that.
        assert transformsWithoutTasks.size() <= Request.MAX_SIZE_RETURN;

        // If the persistent task does NOT exist, it is STOPPED
        // There is a potential race condition where the saved document does not actually have a STOPPED state
        // as the task is cancelled before we persist state.
        ActionListener<List<TransformStoredDoc>> searchStatsListener = ActionListener.wrap(statsForTransformsWithoutTasks -> {
            List<TransformStats> allStateAndStats = response.getTransformsStats();
            addCheckpointingInfoForTransformsWithoutTasks(allStateAndStats, statsForTransformsWithoutTasks, ActionListener.wrap(aVoid -> {
                transformsWithoutTasks.removeAll(
                    statsForTransformsWithoutTasks.stream().map(TransformStoredDoc::getId).collect(Collectors.toSet())
                );

                // Transforms that have not been started and have no state or stats.
                transformsWithoutTasks.forEach(transformId -> allStateAndStats.add(TransformStats.initialStats(transformId)));

                // Any transform in collection could NOT have a task, so, even though the list is initially sorted
                // it can easily become arbitrarily ordered based on which transforms don't have a task or stats docs
                allStateAndStats.sort(Comparator.comparing(TransformStats::getId));

                listener.onResponse(
                    new Response(allStateAndStats, allStateAndStats.size(), response.getTaskFailures(), response.getNodeFailures())
                );
            }, listener::onFailure));
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                listener.onResponse(response);
            } else {
                listener.onFailure(e);
            }
        });

        transformConfigManager.getTransformStoredDocs(transformsWithoutTasks, searchStatsListener);
    }

    private void populateSingleStoppedTransformStat(TransformStoredDoc transform, ActionListener<TransformCheckpointingInfo> listener) {
        transformCheckpointService.getCheckpointingInfo(
            client,
            transform.getId(),
            transform.getTransformState().getCheckpoint(),
            transform.getTransformState().getPosition(),
            transform.getTransformState().getProgress(),
            ActionListener.wrap(infoBuilder -> listener.onResponse(infoBuilder.build()), e -> {
                logger.warn("Failed to retrieve checkpointing info for transform [" + transform.getId() + "]", e);
                listener.onResponse(TransformCheckpointingInfo.EMPTY);
            })
        );
    }

    private void addCheckpointingInfoForTransformsWithoutTasks(
        List<TransformStats> allStateAndStats,
        List<TransformStoredDoc> statsForTransformsWithoutTasks,
        ActionListener<Void> listener
    ) {

        if (statsForTransformsWithoutTasks.isEmpty()) {
            // No work to do, but we must respond to the listener
            listener.onResponse(null);
            return;
        }

        AtomicInteger numberRemaining = new AtomicInteger(statsForTransformsWithoutTasks.size());
        AtomicBoolean isExceptionReported = new AtomicBoolean(false);

        statsForTransformsWithoutTasks.forEach(stat -> populateSingleStoppedTransformStat(stat, ActionListener.wrap(checkpointingInfo -> {
            synchronized (allStateAndStats) {
                allStateAndStats.add(
                    new TransformStats(stat.getId(), TransformStats.State.STOPPED, null, null, stat.getTransformStats(), checkpointingInfo)
                );
            }
            if (numberRemaining.decrementAndGet() == 0) {
                listener.onResponse(null);
            }
        }, e -> {
            if (isExceptionReported.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        })));
    }
}
