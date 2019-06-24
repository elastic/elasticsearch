/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransportGetDatafeedsStatsAction extends TransportMasterNodeReadAction<GetDatafeedsStatsAction.Request,
        GetDatafeedsStatsAction.Response> {

    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobResultsProvider jobResultsProvider;

    @Inject
    public TransportGetDatafeedsStatsAction(TransportService transportService, ClusterService clusterService,
                                            ThreadPool threadPool, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver,
                                            DatafeedConfigProvider datafeedConfigProvider, JobResultsProvider jobResultsProvider) {
        super(GetDatafeedsStatsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetDatafeedsStatsAction.Request::new, indexNameExpressionResolver);
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.jobResultsProvider = jobResultsProvider;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetDatafeedsStatsAction.Response newResponse() {
        return new GetDatafeedsStatsAction.Response();
    }

    @Override
    protected void masterOperation(Task task, GetDatafeedsStatsAction.Request request, ClusterState state,
                                   ActionListener<GetDatafeedsStatsAction.Response> listener) throws Exception {
        logger.debug("Get stats for datafeed '{}'", request.getDatafeedId());

        datafeedConfigProvider.expandDatafeedConfigs(
            request.getDatafeedId(),
            request.allowNoDatafeeds(),
            ActionListener.wrap(
                datafeedBuilders -> {
                    Map<String, String> jobIdByDatafeedId = datafeedBuilders.stream()
                        .map(DatafeedConfig.Builder::build)
                        .collect(Collectors.toUnmodifiableMap(DatafeedConfig::getId, DatafeedConfig::getJobId));
                    jobResultsProvider.datafeedTimingStats(
                        jobIdByDatafeedId.values(),
                        timingStatsByJobId -> {
                            PersistentTasksCustomMetaData tasksInProgress = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                            List<GetDatafeedsStatsAction.Response.DatafeedStats> results = datafeedBuilders.stream()
                                .map(
                                    datafeedBuilder -> getDatafeedStats(
                                        datafeedBuilder.getId(),
                                        state,
                                        tasksInProgress,
                                        jobIdByDatafeedId.get(datafeedBuilder.getId()),
                                        timingStatsByJobId))
                                .collect(Collectors.toList());
                            QueryPage<GetDatafeedsStatsAction.Response.DatafeedStats> statsPage =
                                new QueryPage<>(results, results.size(), DatafeedConfig.RESULTS_FIELD);
                            listener.onResponse(new GetDatafeedsStatsAction.Response(statsPage));
                        },
                        listener::onFailure);
                },
                listener::onFailure)
        );
    }

    private static GetDatafeedsStatsAction.Response.DatafeedStats getDatafeedStats(String datafeedId,
                                                                                   ClusterState state,
                                                                                   PersistentTasksCustomMetaData tasks,
                                                                                   String jobId,
                                                                                   Map<String, DatafeedTimingStats> timingStatsByJobId) {
        PersistentTasksCustomMetaData.PersistentTask<?> task = MlTasks.getDatafeedTask(datafeedId, tasks);
        DatafeedState datafeedState = MlTasks.getDatafeedState(datafeedId, tasks);
        DiscoveryNode node = null;
        String explanation = null;
        if (task != null) {
            node = state.nodes().get(task.getExecutorNode());
            explanation = task.getAssignment().getExplanation();
        }
        DatafeedTimingStats timingStats = null;
        if (jobId != null) {
            timingStats = timingStatsByJobId.get(jobId);
            if (timingStats == null) {
                timingStats = new DatafeedTimingStats(jobId);
            }
        }
        return new GetDatafeedsStatsAction.Response.DatafeedStats(datafeedId, datafeedState, node, explanation, timingStats);
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatafeedsStatsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
