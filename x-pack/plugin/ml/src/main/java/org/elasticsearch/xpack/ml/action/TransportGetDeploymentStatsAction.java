/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationMetadata;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.deployment.ModelStats;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TransportGetDeploymentStatsAction extends TransportTasksAction<TrainedModelDeploymentTask,
    GetDeploymentStatsAction.Request, GetDeploymentStatsAction.Response, GetDeploymentStatsAction.Response.AllocationStats> {

    private final DeploymentManager deploymentManager;
    private static final Logger logger = LogManager.getLogger(TransportGetDeploymentStatsAction.class);

    @Inject
    public TransportGetDeploymentStatsAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService,
                                             DeploymentManager deploymentManager) {
        super(GetDeploymentStatsAction.NAME, clusterService, transportService, actionFilters, GetDeploymentStatsAction.Request::new,
            GetDeploymentStatsAction.Response::new, GetDeploymentStatsAction.Response.AllocationStats::new, ThreadPool.Names.MANAGEMENT);

        this.deploymentManager = deploymentManager;
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
                }));

        List<GetDeploymentStatsAction.Response.AllocationStats> bunchedAndSorted =
            mergedNodeStatsByModel.values().stream()
                .sorted(Comparator.comparing(GetDeploymentStatsAction.Response.AllocationStats::getModelId))
                .collect(Collectors.toList());

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

        for (var routingEntry : allocation.modelAllocations().entrySet()) {
            if (idsMatcher.idMatches(routingEntry.getKey())) {
                matchedDeploymentIds.add(routingEntry.getKey());

                taskNodes.addAll(Arrays.asList(routingEntry.getValue().getStartedNodes()));  // TODO: limited to started nodes
                logger.warn("Routing table " + Strings.toString(routingEntry.getValue()));
                logger.warn("getting stats on nodes: " + taskNodes);
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

        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(GetDeploymentStatsAction.Request request, TrainedModelDeploymentTask task,
                                 ActionListener<GetDeploymentStatsAction.Response.AllocationStats> listener) {
        ModelStats stats = deploymentManager.getStats(task);

        List<GetDeploymentStatsAction.Response.AllocationStats.NodeStats> nodeStats = new ArrayList<>();
        nodeStats.add(new GetDeploymentStatsAction.Response.AllocationStats.NodeStats(this.clusterService.localNode(),
            stats.getTimingStats().getCount(),
            stats.getTimingStats().getAverage(),
            stats.getLastUsed()));

        listener.onResponse(new GetDeploymentStatsAction.Response.AllocationStats(task.getModelId(), stats.getModelSize(), nodeStats));
    }
}
