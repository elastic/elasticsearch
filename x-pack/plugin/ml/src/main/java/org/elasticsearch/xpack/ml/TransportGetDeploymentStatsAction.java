/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.deployment.ModelStats;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportGetDeploymentStatsAction extends TransportTasksAction<TrainedModelDeploymentTask,
    GetDeploymentStatsAction.Request, GetDeploymentStatsAction.Response, GetDeploymentStatsAction.Response.DeploymentStats> {

    private final DeploymentManager deploymentManager;

    @Inject
    public TransportGetDeploymentStatsAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService,
                                             DeploymentManager deploymentManager) {
        super(GetDeploymentStatsAction.NAME, clusterService, transportService, actionFilters, GetDeploymentStatsAction.Request::new,
            GetDeploymentStatsAction.Response::new, GetDeploymentStatsAction.Response.DeploymentStats::new, ThreadPool.Names.MANAGEMENT);

        this.deploymentManager = deploymentManager;
    }

    @Override
    protected GetDeploymentStatsAction.Response newResponse(GetDeploymentStatsAction.Request request,
                                                            List<GetDeploymentStatsAction.Response.DeploymentStats> tasks,
                                                            List<TaskOperationFailure> taskOperationFailures,
                                                            List<FailedNodeException> failedNodeExceptions) {
        tasks.sort(Comparator.comparing(GetDeploymentStatsAction.Response.DeploymentStats::getModelId));
        return new GetDeploymentStatsAction.Response(taskOperationFailures, failedNodeExceptions, tasks, tasks.size());
    }

    @Override
    protected void doExecute(Task task, GetDeploymentStatsAction.Request request,
                             ActionListener<GetDeploymentStatsAction.Response> listener) {

        ClusterState clusterState = clusterService.state();
        PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);

        String[] tokenizedRequestIds = Strings.tokenizeToStringArray(request.getDeploymentId(), ",");

        Collection<PersistentTasksCustomMetadata.PersistentTask<?>> deploymentTasks = MlTasks.trainedModelDeploymentTasks(tasks);

        if (Strings.isAllOrWildcard(request.getDeploymentId())) {
            if (deploymentTasks.isEmpty()) {
                if (request.isAllowNoMatch()) {
                    listener.onResponse(new GetDeploymentStatsAction.Response(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), 0L));
                } else {
                    listener.onFailure(ExceptionsHelper.missingDeployment(request.getDeploymentId()));
                }
                return;
            }

            List<String> activeDeploymentIds = deploymentTasks.stream()
                .map(t -> MlTasks.trainedModelDeploymentId(t.getId()))
                .collect(Collectors.toList());

            String[] nodes = deploymentTasks.stream()
                .map(PersistentTasksCustomMetadata.PersistentTask::getExecutorNode)
                .toArray(String[]::new);

            request.setNodes(nodes);
            request.setExpandedIds(activeDeploymentIds);
        } else {
            ExpandedIdsMatcher idsMatcher = new ExpandedIdsMatcher(tokenizedRequestIds, false);
            List<String> matchedDeploymentIds = new ArrayList<>();
            Set<String> taskNodes = new HashSet<>();

            for (var deployedTask : deploymentTasks) {
                String deploymentTaskId = MlTasks.trainedModelDeploymentId(deployedTask.getId());
                if (idsMatcher.idMatches(deploymentTaskId)) {
                    matchedDeploymentIds.add(deploymentTaskId);
                    taskNodes.add(deployedTask.getExecutorNode());
                }
            }

            if (matchedDeploymentIds.isEmpty()) {
                listener.onFailure(ExceptionsHelper.missingDeployment(request.getDeploymentId()));
                return;
            }

            // check request has been satisfied
            idsMatcher.filterMatchedIds(matchedDeploymentIds);
            if (idsMatcher.hasUnmatchedIds()) {
                listener.onFailure(ExceptionsHelper.missingDeployment(idsMatcher.unmatchedIdsString()));
                return;
            }

            request.setNodes(taskNodes.toArray(String[]::new));
            request.setExpandedIds(matchedDeploymentIds);
        }

        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(GetDeploymentStatsAction.Request request, TrainedModelDeploymentTask task,
                                 ActionListener<GetDeploymentStatsAction.Response.DeploymentStats> listener) {
        ModelStats stats = deploymentManager.getStats(task);
        var response = new GetDeploymentStatsAction.Response.DeploymentStats(task.getModelId(),
            this.clusterService.getNodeName(),
            stats.getTimingStats().getCount(),
            stats.getTimingStats().getAverage(),
            stats.getLastUsed(),
            stats.getModelSize());

        listener.onResponse(response);
    }
}
