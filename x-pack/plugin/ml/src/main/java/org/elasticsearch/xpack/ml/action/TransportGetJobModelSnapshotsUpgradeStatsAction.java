/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetJobModelSnapshotsUpgradeStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobModelSnapshotsUpgradeStatsAction.Request;
import org.elasticsearch.xpack.core.ml.action.GetJobModelSnapshotsUpgradeStatsAction.Response;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class TransportGetJobModelSnapshotsUpgradeStatsAction extends TransportMasterNodeReadAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetJobModelSnapshotsUpgradeStatsAction.class);

    private final JobConfigProvider jobConfigProvider;

    @Inject
    public TransportGetJobModelSnapshotsUpgradeStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        JobConfigProvider jobConfigProvider
    ) {
        super(
            GetJobModelSnapshotsUpgradeStatsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
        this.jobConfigProvider = jobConfigProvider;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        logger.debug(() -> format("[%s] get stats for model snapshot [%s] upgrades", request.getJobId(), request.getSnapshotId()));
        final PersistentTasksCustomMetadata tasksInProgress = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        final Collection<PersistentTasksCustomMetadata.PersistentTask<?>> snapshotUpgrades = MlTasks.snapshotUpgradeTasks(tasksInProgress);
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());

        // 2. Now that we have the job IDs, find the relevant model snapshot upgrades
        ActionListener<List<Job.Builder>> expandIdsListener = ActionListener.wrap(jobs -> {
            ExpandedIdsMatcher requiredSnapshotIdMatches = new ExpandedIdsMatcher(request.getSnapshotId(), request.allowNoMatch());
            Set<String> jobIds = jobs.stream().map(Job.Builder::getId).collect(Collectors.toSet());
            List<Response.JobModelSnapshotUpgradeStats> statsList = snapshotUpgrades.stream()
                .filter(t -> jobIds.contains(((SnapshotUpgradeTaskParams) t.getParams()).getJobId()))
                .filter(t -> requiredSnapshotIdMatches.idMatches(((SnapshotUpgradeTaskParams) t.getParams()).getSnapshotId()))
                .map(t -> {
                    SnapshotUpgradeTaskParams params = (SnapshotUpgradeTaskParams) t.getParams();
                    Response.JobModelSnapshotUpgradeStats.Builder statsBuilder = Response.JobModelSnapshotUpgradeStats.builder(
                        params.getJobId(),
                        params.getSnapshotId()
                    );
                    if (t.getExecutorNode() != null) {
                        statsBuilder.setNode(state.getNodes().get(t.getExecutorNode()));
                    }
                    return statsBuilder.setUpgradeState(MlTasks.getSnapshotUpgradeState(t))
                        .setAssignmentExplanation(t.getAssignment().getExplanation())
                        .build();
                })
                .sorted(
                    Comparator.comparing(Response.JobModelSnapshotUpgradeStats::getJobId)
                        .thenComparing(Response.JobModelSnapshotUpgradeStats::getSnapshotId)
                )
                .collect(Collectors.toList());
            requiredSnapshotIdMatches.filterMatchedIds(
                statsList.stream().map(Response.JobModelSnapshotUpgradeStats::getSnapshotId).collect(Collectors.toList())
            );
            if (requiredSnapshotIdMatches.hasUnmatchedIds()) {
                listener.onFailure(
                    new ResourceNotFoundException(
                        "no snapshot upgrade is running for snapshot_id [{}]",
                        requiredSnapshotIdMatches.unmatchedIdsString()
                    )
                );
            } else {
                listener.onResponse(
                    new Response(new QueryPage<>(statsList, statsList.size(), GetJobModelSnapshotsUpgradeStatsAction.RESULTS_FIELD))
                );
            }
        }, listener::onFailure);

        // 1. Expand jobs - this will throw if a required job ID match isn't made
        jobConfigProvider.expandJobs(request.getJobId(), request.allowNoMatch(), true, parentTaskId, expandIdsListener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
