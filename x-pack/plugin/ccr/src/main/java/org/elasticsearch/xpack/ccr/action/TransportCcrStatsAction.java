/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class TransportCcrStatsAction extends TransportTasksAction<
        ShardFollowNodeTask,
        CcrStatsAction.TasksRequest,
        CcrStatsAction.TasksResponse, CcrStatsAction.TaskResponse> {

    private final IndexNameExpressionResolver resolver;

    @Inject
    public TransportCcrStatsAction(
            final Settings settings,
            final ClusterService clusterService,
            final TransportService transportService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver resolver) {
        super(
                settings,
                CcrStatsAction.NAME,
                clusterService,
                transportService,
                actionFilters,
                CcrStatsAction.TasksRequest::new,
                CcrStatsAction.TasksResponse::new,
                Ccr.CCR_THREAD_POOL_NAME);
        this.resolver = resolver;
    }

    @Override
    protected CcrStatsAction.TasksResponse newResponse(
            final CcrStatsAction.TasksRequest request,
            final List<CcrStatsAction.TaskResponse> taskResponses,
            final List<TaskOperationFailure> taskOperationFailures,
            final List<FailedNodeException> failedNodeExceptions) {
        return new CcrStatsAction.TasksResponse(taskOperationFailures, failedNodeExceptions, taskResponses);
    }

    @Override
    protected CcrStatsAction.TaskResponse readTaskResponse(final StreamInput in) throws IOException {
        return new CcrStatsAction.TaskResponse(in);
    }

    @Override
    protected void processTasks(final CcrStatsAction.TasksRequest request, final Consumer<ShardFollowNodeTask> operation) {
        final ClusterState state = clusterService.state();
        final Set<String> concreteIndices = new HashSet<>(Arrays.asList(resolver.concreteIndexNames(state, request)));
        for (final Task task : taskManager.getTasks().values()) {
            if (task instanceof ShardFollowNodeTask) {
                final ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
                if (concreteIndices.contains(shardFollowNodeTask.getFollowShardId().getIndexName())) {
                    operation.accept(shardFollowNodeTask);
                }
            }
        }
    }

    @Override
    protected void taskOperation(
            final CcrStatsAction.TasksRequest request,
            final ShardFollowNodeTask task,
            final ActionListener<CcrStatsAction.TaskResponse> listener) {
        listener.onResponse(new CcrStatsAction.TaskResponse(task.getFollowShardId(), task.getStatus()));
    }

}
