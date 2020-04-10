/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TransportGetDatafeedsStatsAction extends TransportMasterNodeReadAction<GetDatafeedsStatsAction.Request,
        GetDatafeedsStatsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetDatafeedsStatsAction.class);

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
    protected GetDatafeedsStatsAction.Response read(StreamInput in) throws IOException {
        return new GetDatafeedsStatsAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, GetDatafeedsStatsAction.Request request,
                                   ClusterState state,
                                   ActionListener<GetDatafeedsStatsAction.Response> listener) {
        logger.debug("Get stats for datafeed '{}'", request.getDatafeedId());
        final PersistentTasksCustomMetadata tasksInProgress = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        ActionListener<SortedSet<String>> expandIdsListener = ActionListener.wrap(
            expandedIds -> {
                datafeedConfigProvider.expandDatafeedConfigs(
                    request.getDatafeedId(),
                    // Already took into account the request parameter when we expanded the IDs with the tasks earlier
                    // Should allow for no datafeeds in case the config is gone
                    true,
                    ActionListener.wrap(
                        datafeedBuilders -> {
                            Map<String, DatafeedConfig> existingConfigs = datafeedBuilders.stream()
                                .map(DatafeedConfig.Builder::build)
                                .collect(Collectors.toMap(DatafeedConfig::getId, Function.identity()));

                            List<String> jobIds = existingConfigs.values()
                                .stream()
                                .map(DatafeedConfig::getJobId)
                                .collect(Collectors.toList());
                            jobResultsProvider.datafeedTimingStats(
                                jobIds,
                                ActionListener.wrap(timingStatsByJobId -> {
                                    List<GetDatafeedsStatsAction.Response.DatafeedStats> results = expandedIds.stream()
                                        .map(datafeedId -> {
                                            DatafeedConfig config = existingConfigs.get(datafeedId);
                                            String jobId = config == null ? null : config.getJobId();
                                            DatafeedTimingStats timingStats = jobId == null ? null : timingStatsByJobId.get(jobId);
                                            return buildDatafeedStats(
                                                datafeedId,
                                                state,
                                                tasksInProgress,
                                                jobId,
                                                timingStats
                                            );
                                        })
                                        .collect(Collectors.toList());
                                    QueryPage<GetDatafeedsStatsAction.Response.DatafeedStats> statsPage =
                                        new QueryPage<>(results, results.size(), DatafeedConfig.RESULTS_FIELD);
                                    listener.onResponse(new GetDatafeedsStatsAction.Response(statsPage));
                                },
                                listener::onFailure));
                        },
                        listener::onFailure)
                );
            },
            listener::onFailure
        );

        // This might also include datafeed tasks that exist but no longer have a config
        datafeedConfigProvider.expandDatafeedIds(request.getDatafeedId(),
            request.allowNoDatafeeds(),
            tasksInProgress,
            true,
            expandIdsListener);
    }

    private static GetDatafeedsStatsAction.Response.DatafeedStats buildDatafeedStats(String datafeedId,
                                                                                     ClusterState state,
                                                                                     PersistentTasksCustomMetadata tasks,
                                                                                     String jobId,
                                                                                     DatafeedTimingStats timingStats) {
        PersistentTasksCustomMetadata.PersistentTask<?> task = MlTasks.getDatafeedTask(datafeedId, tasks);
        DatafeedState datafeedState = MlTasks.getDatafeedState(datafeedId, tasks);
        DiscoveryNode node = null;
        String explanation = null;
        if (task != null) {
            node = state.nodes().get(task.getExecutorNode());
            explanation = task.getAssignment().getExplanation();
        }
        if (timingStats == null && jobId != null) {
            timingStats = new DatafeedTimingStats(jobId);
        }
        return new GetDatafeedsStatsAction.Response.DatafeedStats(datafeedId, datafeedState, node, explanation, timingStats);
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatafeedsStatsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
