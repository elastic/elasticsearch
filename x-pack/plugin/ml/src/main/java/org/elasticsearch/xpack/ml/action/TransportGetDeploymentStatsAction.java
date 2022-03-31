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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationState;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationStats;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationMetadata;
import org.elasticsearch.xpack.ml.inference.deployment.ModelStats;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TransportGetDeploymentStatsAction extends TransportTasksAction<
    TrainedModelDeploymentTask,
    GetDeploymentStatsAction.Request,
    GetDeploymentStatsAction.Response,
    AllocationStats> {

    @Inject
    public TransportGetDeploymentStatsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(
            GetDeploymentStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            GetDeploymentStatsAction.Request::new,
            GetDeploymentStatsAction.Response::new,
            AllocationStats::new,
            ThreadPool.Names.MANAGEMENT
        );
    }

    @Override
    protected GetDeploymentStatsAction.Response newResponse(
        GetDeploymentStatsAction.Request request,
        List<AllocationStats> taskResponse,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        // group the stats by model and merge individual node stats
        var mergedNodeStatsByModel = taskResponse.stream()
            .collect(Collectors.toMap(AllocationStats::getModelId, Function.identity(), (l, r) -> {
                l.getNodeStats().addAll(r.getNodeStats());
                return l;
            }, TreeMap::new));

        List<AllocationStats> bunchedAndSorted = new ArrayList<>(mergedNodeStatsByModel.values());

        return new GetDeploymentStatsAction.Response(
            taskOperationFailures,
            failedNodeExceptions,
            bunchedAndSorted,
            bunchedAndSorted.size()
        );
    }

    @Override
    protected void doExecute(
        Task task,
        GetDeploymentStatsAction.Request request,
        ActionListener<GetDeploymentStatsAction.Response> listener
    ) {
        final ClusterState clusterState = clusterService.state();
        final TrainedModelAllocationMetadata allocation = TrainedModelAllocationMetadata.fromState(clusterState);

        String[] tokenizedRequestIds = Strings.tokenizeToStringArray(request.getDeploymentId(), ",");
        ExpandedIdsMatcher.SimpleIdsMatcher idsMatcher = new ExpandedIdsMatcher.SimpleIdsMatcher(tokenizedRequestIds);

        List<String> matchedDeploymentIds = new ArrayList<>();
        Set<String> taskNodes = new HashSet<>();
        Map<TrainedModelAllocation, Map<String, RoutingStateAndReason>> allocationNonStartedRoutes = new HashMap<>();
        for (var allocationEntry : allocation.modelAllocations().entrySet()) {
            String modelId = allocationEntry.getKey();
            if (idsMatcher.idMatches(modelId)) {
                matchedDeploymentIds.add(modelId);

                taskNodes.addAll(Arrays.asList(allocationEntry.getValue().getStartedNodes()));

                Map<String, RoutingStateAndReason> routings = allocationEntry.getValue()
                    .getNodeRoutingTable()
                    .entrySet()
                    .stream()
                    .filter(routingEntry -> RoutingState.STARTED.equals(routingEntry.getValue().getState()) == false)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                allocationNonStartedRoutes.put(allocationEntry.getValue(), routings);
            }
        }

        if (matchedDeploymentIds.isEmpty()) {
            listener.onResponse(
                new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), 0L)
            );
            return;
        }

        request.setNodes(taskNodes.toArray(String[]::new));
        request.setExpandedIds(matchedDeploymentIds);

        ActionListener<GetDeploymentStatsAction.Response> addFailedListener = listener.delegateFailure((l, response) -> {
            var updatedResponse = addFailedRoutes(response, allocationNonStartedRoutes, clusterState.nodes());
            ClusterState latestState = clusterService.state();
            Set<String> nodesShuttingDown = TransportStartTrainedModelDeploymentAction.nodesShuttingDown(latestState);
            List<DiscoveryNode> nodes = latestState.getNodes()
                .stream()
                .filter(d -> nodesShuttingDown.contains(d.getId()) == false)
                .filter(StartTrainedModelDeploymentAction.TaskParams::mayAllocateToNode)
                .collect(Collectors.toList());
            // Set the allocation state and reason if we have it
            for (AllocationStats stats : updatedResponse.getStats().results()) {
                TrainedModelAllocation trainedModelAllocation = allocation.getModelAllocation(stats.getModelId());
                if (trainedModelAllocation != null) {
                    stats.setState(trainedModelAllocation.getAllocationState()).setReason(trainedModelAllocation.getReason().orElse(null));
                    if (trainedModelAllocation.getAllocationState().isAnyOf(AllocationState.STARTED, AllocationState.STARTING)) {
                        stats.setAllocationStatus(trainedModelAllocation.calculateAllocationStatus(nodes).orElse(null));
                    }
                }
            }
            l.onResponse(updatedResponse);
        });

        super.doExecute(task, request, addFailedListener);
    }

    /**
     * Update the collected task responses with the non-started
     * allocation information. The result is the task responses
     * merged with the non-started model allocations.
     *
     * Where there is a merge collision for the pair {@code <model_id, node_id>}
     * the non-started allocations are used.
     *
     * @param tasksResponse All the responses from the tasks
     * @param allocationNonStartedRoutes Non-started routes
     * @param nodes current cluster nodes
     * @return The result of merging tasksResponse and the non-started routes
     */
    static GetDeploymentStatsAction.Response addFailedRoutes(
        GetDeploymentStatsAction.Response tasksResponse,
        Map<TrainedModelAllocation, Map<String, RoutingStateAndReason>> allocationNonStartedRoutes,
        DiscoveryNodes nodes
    ) {
        final Map<String, TrainedModelAllocation> modelToAllocationWithNonStartedRoutes = allocationNonStartedRoutes.keySet()
            .stream()
            .collect(Collectors.toMap(TrainedModelAllocation::getModelId, Function.identity()));

        final List<AllocationStats> updatedAllocationStats = new ArrayList<>();

        for (AllocationStats stat : tasksResponse.getStats().results()) {
            if (modelToAllocationWithNonStartedRoutes.containsKey(stat.getModelId())) {
                // there is merging to be done
                Map<String, RoutingStateAndReason> nodeToRoutingStates = allocationNonStartedRoutes.get(
                    modelToAllocationWithNonStartedRoutes.get(stat.getModelId())
                );
                List<AllocationStats.NodeStats> updatedNodeStats = new ArrayList<>();

                Set<String> visitedNodes = new HashSet<>();
                for (var nodeStat : stat.getNodeStats()) {
                    if (nodeToRoutingStates.containsKey(nodeStat.getNode().getId())) {
                        // conflict as there is both a task response for the model/node pair
                        // and we have a non-started routing entry.
                        // Prefer the entry from allocationNonStartedRoutes as we cannot be sure
                        // of the state of the task - it may be starting, started, stopping, or stopped.
                        RoutingStateAndReason stateAndReason = nodeToRoutingStates.get(nodeStat.getNode().getId());
                        updatedNodeStats.add(
                            AllocationStats.NodeStats.forNotStartedState(
                                nodeStat.getNode(),
                                stateAndReason.getState(),
                                stateAndReason.getReason()
                            )
                        );
                    } else {
                        updatedNodeStats.add(nodeStat);
                    }

                    visitedNodes.add(nodeStat.getNode().getId());
                }

                // add nodes from the failures that were not in the task responses
                for (var nodeRoutingState : nodeToRoutingStates.entrySet()) {
                    if (visitedNodes.contains(nodeRoutingState.getKey()) == false) {
                        updatedNodeStats.add(
                            AllocationStats.NodeStats.forNotStartedState(
                                nodes.get(nodeRoutingState.getKey()),
                                nodeRoutingState.getValue().getState(),
                                nodeRoutingState.getValue().getReason()
                            )
                        );
                    }
                }

                updatedNodeStats.sort(Comparator.comparing(n -> n.getNode().getId()));
                updatedAllocationStats.add(
                    new AllocationStats(
                        stat.getModelId(),
                        stat.getInferenceThreads(),
                        stat.getModelThreads(),
                        stat.getQueueCapacity(),
                        stat.getStartTime(),
                        updatedNodeStats
                    )
                );
            } else {
                updatedAllocationStats.add(stat);
            }
        }

        // Merge any models in the non-started that were not in the task responses
        for (var nonStartedEntries : allocationNonStartedRoutes.entrySet()) {
            final TrainedModelAllocation allocation = nonStartedEntries.getKey();
            final String modelId = allocation.getTaskParams().getModelId();
            if (tasksResponse.getStats().results().stream().anyMatch(e -> modelId.equals(e.getModelId())) == false) {

                // no tasks for this model so build the allocation stats from the non-started states
                List<AllocationStats.NodeStats> nodeStats = new ArrayList<>();

                for (var routingEntry : nonStartedEntries.getValue().entrySet()) {
                    nodeStats.add(
                        AllocationStats.NodeStats.forNotStartedState(
                            nodes.get(routingEntry.getKey()),
                            routingEntry.getValue().getState(),
                            routingEntry.getValue().getReason()
                        )
                    );
                }

                nodeStats.sort(Comparator.comparing(n -> n.getNode().getId()));

                updatedAllocationStats.add(new AllocationStats(modelId, null, null, null, allocation.getStartTime(), nodeStats));
            }
        }

        updatedAllocationStats.sort(Comparator.comparing(AllocationStats::getModelId));

        return new GetDeploymentStatsAction.Response(
            tasksResponse.getTaskFailures(),
            tasksResponse.getNodeFailures(),
            updatedAllocationStats,
            updatedAllocationStats.size()
        );
    }

    @Override
    protected void taskOperation(
        GetDeploymentStatsAction.Request request,
        TrainedModelDeploymentTask task,
        ActionListener<AllocationStats> listener
    ) {
        Optional<ModelStats> stats = task.modelStats();

        List<AllocationStats.NodeStats> nodeStats = new ArrayList<>();

        if (stats.isPresent()) {
            var presentValue = stats.get();
            nodeStats.add(
                AllocationStats.NodeStats.forStartedState(
                    clusterService.localNode(),
                    presentValue.timingStats().getCount(),
                    presentValue.timingStats().getAverage(),
                    presentValue.pendingCount(),
                    presentValue.errorCount(),
                    presentValue.rejectedExecutionCount(),
                    presentValue.timeoutCount(),
                    presentValue.lastUsed(),
                    presentValue.startTime(),
                    presentValue.inferenceThreads(),
                    presentValue.modelThreads(),
                    presentValue.peakThroughput(),
                    presentValue.throughputLastPeriod(),
                    presentValue.avgInferenceTimeLastPeriod()
                )
            );
        } else {
            // if there are no stats the process is missing.
            // Either because it is starting or stopped
            nodeStats.add(AllocationStats.NodeStats.forNotStartedState(clusterService.localNode(), RoutingState.STOPPED, ""));
        }

        listener.onResponse(
            new AllocationStats(
                task.getModelId(),
                task.getParams().getInferenceThreads(),
                task.getParams().getModelThreads(),
                task.getParams().getQueueCapacity(),
                TrainedModelAllocationMetadata.fromState(clusterService.state()).getModelAllocation(task.getModelId()).getStartTime(),
                nodeStats
            )
        );
    }
}
