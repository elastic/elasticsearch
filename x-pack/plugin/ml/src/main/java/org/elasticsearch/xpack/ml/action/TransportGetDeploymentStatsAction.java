/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationMetadata;
import org.elasticsearch.xpack.ml.inference.deployment.ModelStats;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TransportGetDeploymentStatsAction extends TransportTasksAction<TrainedModelDeploymentTask,
    GetDeploymentStatsAction.Request, GetDeploymentStatsAction.Response, GetDeploymentStatsAction.Response.AllocationStats> {

    @Inject
    public TransportGetDeploymentStatsAction(TransportService transportService,
                                             ActionFilters actionFilters,
                                             ClusterService clusterService) {
        super(GetDeploymentStatsAction.NAME, clusterService, transportService, actionFilters, GetDeploymentStatsAction.Request::new,
            GetDeploymentStatsAction.Response::new, GetDeploymentStatsAction.Response.AllocationStats::new, ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected GetDeploymentStatsAction.Response newResponse(GetDeploymentStatsAction.Request request,
                                                            List<GetDeploymentStatsAction.Response.AllocationStats> taskResponse,
                                                            List<TaskOperationFailure> taskOperationFailures,
                                                            List<FailedNodeException> failedNodeExceptions) {
        // group the stats by model and merge individual node stats
        var mergedNodeStatsByModel =
            taskResponse.stream().collect(Collectors.toMap(GetDeploymentStatsAction.Response.AllocationStats::getModelId,
                Function.identity(),
                (l, r) -> {
                    l.getNodeStats().addAll(r.getNodeStats());
                    return l;
                },
                TreeMap::new));

        List<GetDeploymentStatsAction.Response.AllocationStats> bunchedAndSorted = new ArrayList<>(mergedNodeStatsByModel.values());

        return new GetDeploymentStatsAction.Response(taskOperationFailures,
            failedNodeExceptions,
            bunchedAndSorted,
            bunchedAndSorted.size());
    }

    @Override
    protected void doExecute(Task task, GetDeploymentStatsAction.Request request,
                             ActionListener<GetDeploymentStatsAction.Response> listener) {

        String[] tokenizedRequestIds = Strings.tokenizeToStringArray(request.getDeploymentId(), ",");
        ExpandedIdsMatcher.SimpleIdsMatcher idsMatcher =
            new ExpandedIdsMatcher.SimpleIdsMatcher(tokenizedRequestIds);

        TrainedModelAllocationMetadata allocation = TrainedModelAllocationMetadata.fromState(clusterService.state());
        List<String> matchedDeploymentIds = new ArrayList<>();
        Set<String> taskNodes = new HashSet<>();
        Map<String, Map<String, RoutingStateAndReason>> nonStartedAllocationsForModel = new HashMap<>();
        for (var allocationEntry : allocation.modelAllocations().entrySet()) {
            String modelId = allocationEntry.getKey();
            if (idsMatcher.idMatches(modelId)) {
                matchedDeploymentIds.add(modelId);

                taskNodes.addAll(Arrays.asList(allocationEntry.getValue().getStartedNodes()));

                Map<String, RoutingStateAndReason> routings = allocationEntry.getValue().getNodeRoutingTable().entrySet()
                    .stream()
                    .filter(routingEntry -> RoutingState.STARTED.equals(routingEntry.getValue().getState()) == false)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                nonStartedAllocationsForModel.put(modelId, routings);
            }
        }

        // check request has been satisfied
        ExpandedIdsMatcher requiredIdsMatcher = new ExpandedIdsMatcher(tokenizedRequestIds, request.isAllowNoMatch());
        requiredIdsMatcher.filterMatchedIds(matchedDeploymentIds);
        if (requiredIdsMatcher.hasUnmatchedIds()) {
            listener.onFailure(ExceptionsHelper.missingDeployment(requiredIdsMatcher.unmatchedIdsString()));
            return;
        }
        if (matchedDeploymentIds.isEmpty()) {
            listener.onResponse(new GetDeploymentStatsAction.Response(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), 0L));
            return;
        }

        request.setNodes(taskNodes.toArray(String[]::new));
        request.setExpandedIds(matchedDeploymentIds);

        ActionListener<GetDeploymentStatsAction.Response> addFailedListener = listener.delegateFailure(
            (l, response) -> {
                var updatedResponse= GetDeploymentStatsAction.Response.addFailedRoutes(response,
                    nonStartedAllocationsForModel,
                    clusterService.state().nodes()
                );
                // Set the allocation state and reason if we have it
                for (GetDeploymentStatsAction.Response.AllocationStats stats : updatedResponse.getStats().results()) {
                    Optional<TrainedModelAllocation> modelAllocation = Optional.ofNullable(
                        allocation.getModelAllocation(stats.getModelId())
                    );
                    stats.setState(modelAllocation.map(TrainedModelAllocation::getAllocationState).orElse(null));
                    stats.setReason(modelAllocation.flatMap(TrainedModelAllocation::getReason).orElse(null));
                }
                l.onResponse(updatedResponse);
            }
        );

        super.doExecute(task, request, addFailedListener);
    }

    @Override
    protected void taskOperation(GetDeploymentStatsAction.Request request, TrainedModelDeploymentTask task,
                                 ActionListener<GetDeploymentStatsAction.Response.AllocationStats> listener) {
        Optional<ModelStats> stats = task.modelStats();

        List<GetDeploymentStatsAction.Response.AllocationStats.NodeStats> nodeStats = new ArrayList<>();

        if (stats.isPresent()) {
            nodeStats.add(GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forStartedState(
                clusterService.localNode(),
                stats.get().getTimingStats().getCount(),
                stats.get().getTimingStats().getAverage(),
                stats.get().getLastUsed()));
        } else {
            // if there are no stats the process is missing.
            // Either because it is starting or stopped
            nodeStats.add(GetDeploymentStatsAction.Response.AllocationStats.NodeStats.forNotStartedState(
                clusterService.localNode(),
                RoutingState.STOPPED, ""));
        }

        var modelSize = stats.map(ModelStats::getModelSize).orElse(null);
        listener.onResponse(new GetDeploymentStatsAction.Response.AllocationStats(task.getModelId(), modelSize, nodeStats));
    }
}
