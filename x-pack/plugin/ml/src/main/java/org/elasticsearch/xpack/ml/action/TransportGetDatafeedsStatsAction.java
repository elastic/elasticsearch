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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Request;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportGetDatafeedsStatsAction extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetDatafeedsStatsAction.class);

    private final ClusterService clusterService;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobResultsProvider jobResultsProvider;
    private final OriginSettingClient client;

    @Inject
    public TransportGetDatafeedsStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        DatafeedConfigProvider datafeedConfigProvider,
        JobResultsProvider jobResultsProvider,
        Client client
    ) {
        super(GetDatafeedsStatsAction.NAME, transportService, actionFilters, Request::new);
        this.clusterService = clusterService;
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.jobResultsProvider = jobResultsProvider;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        logger.trace(() -> "[" + request.getDatafeedId() + "] get stats for datafeed");
        ClusterState state = clusterService.state();
        final PersistentTasksCustomMetadata tasksInProgress = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        final Response.Builder responseBuilder = new Response.Builder();
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());

        // 5. Build response
        ActionListener<GetDatafeedRunningStateAction.Response> runtimeStateListener = listener.delegateFailureAndWrap(
            (l, runtimeStateResponse) -> l.onResponse(
                responseBuilder.setDatafeedRuntimeState(runtimeStateResponse).build(tasksInProgress, state)
            )
        );

        // 4. Grab runtime state
        ActionListener<Map<String, DatafeedTimingStats>> datafeedTimingStatsListener = runtimeStateListener.delegateFailureAndWrap(
            (delegate, timingStatsByJobId) -> {
                responseBuilder.setTimingStatsMap(timingStatsByJobId);
                GetDatafeedRunningStateAction.Request datafeedRunningStateAction = new GetDatafeedRunningStateAction.Request(
                    responseBuilder.getDatafeedIds()
                );
                datafeedRunningStateAction.setParentTask(parentTaskId);
                client.execute(
                    GetDatafeedRunningStateAction.INSTANCE,
                    new GetDatafeedRunningStateAction.Request(responseBuilder.getDatafeedIds()),
                    delegate
                );
            }
        );

        // 3. Grab timing stats
        ActionListener<List<DatafeedConfig.Builder>> expandedConfigsListener = datafeedTimingStatsListener.delegateFailureAndWrap(
            (delegate, datafeedBuilders) -> {
                Map<String, String> datafeedIdsToJobIds = datafeedBuilders.stream()
                    .collect(Collectors.toMap(DatafeedConfig.Builder::getId, DatafeedConfig.Builder::getJobId));
                responseBuilder.setDatafeedToJobId(datafeedIdsToJobIds);
                jobResultsProvider.datafeedTimingStats(new ArrayList<>(datafeedIdsToJobIds.values()), parentTaskId, delegate);
            }
        );

        // 2. Now that we have the ids, grab the datafeed configs
        ActionListener<SortedSet<String>> expandIdsListener = expandedConfigsListener.delegateFailureAndWrap((delegate, expandedIds) -> {
            responseBuilder.setDatafeedIds(expandedIds);
            datafeedConfigProvider.expandDatafeedConfigs(
                request.getDatafeedId(),
                // Already took into account the request parameter when we expanded the IDs with the tasks earlier
                // Should allow for no datafeeds in case the config is gone
                true,
                parentTaskId,
                delegate
            );
        });

        // 1. This might also include datafeed tasks that exist but no longer have a config
        datafeedConfigProvider.expandDatafeedIds(
            request.getDatafeedId(),
            request.allowNoMatch(),
            tasksInProgress,
            true,
            parentTaskId,
            expandIdsListener
        );
    }
}
