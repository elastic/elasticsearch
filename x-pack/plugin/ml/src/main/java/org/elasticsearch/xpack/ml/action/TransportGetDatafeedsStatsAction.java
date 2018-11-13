/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigReader;

import java.util.List;
import java.util.stream.Collectors;

public class TransportGetDatafeedsStatsAction extends TransportMasterNodeReadAction<GetDatafeedsStatsAction.Request,
        GetDatafeedsStatsAction.Response> {

    private final DatafeedConfigReader datafeedConfigReader;

    @Inject
    public TransportGetDatafeedsStatsAction(Settings settings, TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool,
                                            ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver,
                                            Client client, NamedXContentRegistry xContentRegistry) {
        super(settings, GetDatafeedsStatsAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, GetDatafeedsStatsAction.Request::new);
        this.datafeedConfigReader = new DatafeedConfigReader(client, xContentRegistry);
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
    protected void masterOperation(GetDatafeedsStatsAction.Request request, ClusterState state,
                                   ActionListener<GetDatafeedsStatsAction.Response> listener) {
        logger.debug("Get stats for datafeed '{}'", request.getDatafeedId());

        datafeedConfigReader.expandDatafeedIds(request.getDatafeedId(), request.allowNoDatafeeds(), state, ActionListener.wrap(
                expandedDatafeedIds -> {
                    PersistentTasksCustomMetaData tasksInProgress = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                    List<GetDatafeedsStatsAction.Response.DatafeedStats> results = expandedDatafeedIds.stream()
                            .map(datafeedId -> getDatafeedStats(datafeedId, state, tasksInProgress))
                            .collect(Collectors.toList());
                    QueryPage<GetDatafeedsStatsAction.Response.DatafeedStats> statsPage = new QueryPage<>(results, results.size(),
                            DatafeedConfig.RESULTS_FIELD);
                    listener.onResponse(new GetDatafeedsStatsAction.Response(statsPage));
                },
                listener::onFailure
        ));
    }

    private static GetDatafeedsStatsAction.Response.DatafeedStats getDatafeedStats(
            String datafeedId, ClusterState state, PersistentTasksCustomMetaData tasks) {
        PersistentTasksCustomMetaData.PersistentTask<?> task = MlTasks.getDatafeedTask(datafeedId, tasks);
        DatafeedState datafeedState = MlTasks.getDatafeedState(datafeedId, tasks);
        DiscoveryNode node = null;
        String explanation = null;
        if (task != null) {
            node = state.nodes().get(task.getExecutorNode());
            explanation = task.getAssignment().getExplanation();
        }
        return new GetDatafeedsStatsAction.Response.DatafeedStats(datafeedId, datafeedState, node, explanation);
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatafeedsStatsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
