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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexNotFoundException;
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
    private final IndexNameExpressionResolver resolver;

    @Inject
    public TransportFollowStatsAction(
            final ClusterService clusterService,
            final TransportService transportService,
            final ActionFilters actionFilters,
            final CcrLicenseChecker ccrLicenseChecker,
            final IndexNameExpressionResolver resolver) {
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
        this.resolver = resolver;
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

        final ClusterState state = clusterService.state();
        try {
            String[] concreteIndices = resolver.concreteIndexNames(state, IndicesOptions.strictExpand(), request.indices());

            // Also include matching shard follow task's follower indices:
            // A follower index may be removed and therefor concreteIndexNames(...) does not include it.
            // Shard follow tasks for these removed follower indices do exist (with a fatal error set).
            Set<String> shardFollowTaskFollowerIndices = new HashSet<>(Arrays.asList(concreteIndices));
            shardFollowTaskFollowerIndices.addAll(findFollowerIndicesFromShardFollowTasks(state, request.indices()));

            request.setIndices(shardFollowTaskFollowerIndices.toArray(new String[0]));
        } catch (IndexNotFoundException e) {
            // It is possible that there are failed shard follow tasks (with fatal error set) for a none existing index
            // (in case the follower index has been removed)
            // we should also include the stats for these shard follow tasks:
            final Set<String> followerIndices = findFollowerIndicesFromShardFollowTasks(state, request.indices());
            if (followerIndices.size() != 0) {
                request.setIndices(followerIndices.toArray(new String[0]));
            } else {
                throw e;
            }
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
        final Set<String> followerIndices = findFollowerIndicesFromShardFollowTasks(state, request.indices());

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

    static Set<String> findFollowerIndicesFromShardFollowTasks(ClusterState state, String[] indices) {
        final PersistentTasksCustomMetaData persistentTasksMetaData = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (persistentTasksMetaData == null) {
            return Collections.emptySet();
        }

        final Set<String> requestedFollowerIndices = indices != null ?
            new HashSet<>(Arrays.asList(indices)) : Collections.emptySet();
        return persistentTasksMetaData.tasks().stream()
            .filter(persistentTask -> persistentTask.getTaskName().equals(ShardFollowTask.NAME))
            .map(persistentTask -> {
                ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
                return shardFollowTask.getFollowShardId().getIndexName();
            })
            .filter(followerIndex -> Strings.isAllOrWildcard(indices) || requestedFollowerIndices.contains(followerIndex))
            .collect(Collectors.toSet());
    }

}
