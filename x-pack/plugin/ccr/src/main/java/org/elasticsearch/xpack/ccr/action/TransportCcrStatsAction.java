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
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public class TransportCcrStatsAction extends TransportTasksAction<
        ShardFollowNodeTask,
        CcrStatsAction.StatsRequest,
        CcrStatsAction.StatsResponses, CcrStatsAction.StatsResponse> {

    private final IndexNameExpressionResolver resolver;
    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportCcrStatsAction(
            final Settings settings,
            final ClusterService clusterService,
            final TransportService transportService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver resolver,
            final CcrLicenseChecker ccrLicenseChecker) {
        super(
                settings,
                CcrStatsAction.NAME,
                clusterService,
                transportService,
                actionFilters,
                CcrStatsAction.StatsRequest::new,
                CcrStatsAction.StatsResponses::new,
                Ccr.CCR_THREAD_POOL_NAME);
        this.resolver = Objects.requireNonNull(resolver);
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    protected void doExecute(
            final Task task,
            final CcrStatsAction.StatsRequest request,
            final ActionListener<CcrStatsAction.StatsResponses> listener) {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected CcrStatsAction.StatsResponses newResponse(
            final CcrStatsAction.StatsRequest request,
            final List<CcrStatsAction.StatsResponse> statsRespons,
            final List<TaskOperationFailure> taskOperationFailures,
            final List<FailedNodeException> failedNodeExceptions) {
        return new CcrStatsAction.StatsResponses(taskOperationFailures, failedNodeExceptions, statsRespons);
    }

    @Override
    protected CcrStatsAction.StatsResponse readTaskResponse(final StreamInput in) throws IOException {
        return new CcrStatsAction.StatsResponse(in);
    }

    @Override
    protected void processTasks(final CcrStatsAction.StatsRequest request, final Consumer<ShardFollowNodeTask> operation) {
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
            final CcrStatsAction.StatsRequest request,
            final ShardFollowNodeTask task,
            final ActionListener<CcrStatsAction.StatsResponse> listener) {
        listener.onResponse(new CcrStatsAction.StatsResponse(task.getFollowShardId(), task.getStatus()));
    }

}
