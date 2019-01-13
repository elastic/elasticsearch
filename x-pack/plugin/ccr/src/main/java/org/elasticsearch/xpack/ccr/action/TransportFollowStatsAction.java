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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TransportFollowStatsAction extends TransportTasksAction<
        ShardFollowNodeTask,
        FollowStatsAction.StatsRequest,
        FollowStatsAction.StatsResponses, FollowStatsAction.StatsResponse> {

    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportFollowStatsAction(
            final ClusterService clusterService,
            final TransportService transportService,
            final ActionFilters actionFilters,
            final CcrLicenseChecker ccrLicenseChecker) {
        super(
                FollowStatsAction.NAME,
                clusterService,
                transportService,
                actionFilters,
                FollowStatsAction.StatsRequest::new,
                FollowStatsAction.StatsResponses::new,
                FollowStatsAction.StatsResponse::new,
                Ccr.CCR_THREAD_POOL_NAME);
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    protected void doExecute(
            final Task task,
            final FollowStatsAction.StatsRequest request,
            final ActionListener<FollowStatsAction.StatsResponses> listener) {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected FollowStatsAction.StatsResponses newResponse(
            final FollowStatsAction.StatsRequest request,
            final List<FollowStatsAction.StatsResponse> statsRespons,
            final List<TaskOperationFailure> taskOperationFailures,
            final List<FailedNodeException> failedNodeExceptions) {
        return new FollowStatsAction.StatsResponses(taskOperationFailures, failedNodeExceptions, statsRespons);
    }

    @Override
    protected void processTasks(final FollowStatsAction.StatsRequest request, final Consumer<ShardFollowNodeTask> operation) {
        final ClusterState state = clusterService.state();
        final PersistentTasksCustomMetaData persistentTasksMetaData = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (persistentTasksMetaData == null) {
            return;
        }

        final Set<String> requestedFollowerIndices = request.indices() != null ?
            new HashSet<>(Arrays.asList(request.indices())) : Collections.emptySet();
        final Set<String> followerIndices = persistentTasksMetaData.tasks().stream()
            .filter(persistentTask -> persistentTask.getTaskName().equals(ShardFollowTask.NAME))
            .map(persistentTask -> {
                ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
                return shardFollowTask.getFollowShardId().getIndexName();
            })
            .filter(followerIndex -> requestedFollowerIndices.isEmpty() || requestedFollowerIndices.contains(followerIndex))
            .collect(Collectors.toSet());

        for (final Task task : taskManager.getTasks().values()) {
            if (task instanceof ShardFollowNodeTask) {
                final ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
                if (followerIndices.contains(shardFollowNodeTask.getFollowShardId().getIndexName())) {
                    operation.accept(shardFollowNodeTask);
                }
            }
        }
    }

    @Override
    protected void taskOperation(
            final FollowStatsAction.StatsRequest request,
            final ShardFollowNodeTask task,
            final ActionListener<FollowStatsAction.StatsResponse> listener) {
        listener.onResponse(new FollowStatsAction.StatsResponse(task.getStatus()));
    }

}
