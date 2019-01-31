/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportGetDataFrameAnalyticsStatsAction
    extends TransportMasterNodeAction<GetDataFrameAnalyticsStatsAction.Request, GetDataFrameAnalyticsStatsAction.Response> {

    private final Client client;

    @Inject
    public TransportGetDataFrameAnalyticsStatsAction(TransportService transportService, ClusterService clusterService, Client client,
                                                     ThreadPool threadPool, ActionFilters actionFilters,
                                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetDataFrameAnalyticsStatsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, GetDataFrameAnalyticsStatsAction.Request::new);
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetDataFrameAnalyticsStatsAction.Response newResponse() {
        return new GetDataFrameAnalyticsStatsAction.Response();
    }

    @Override
    protected void masterOperation(GetDataFrameAnalyticsStatsAction.Request request, ClusterState state,
                                   ActionListener<GetDataFrameAnalyticsStatsAction.Response> listener) throws Exception {
        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        ActionListener<GetDataFrameAnalyticsAction.Response> getResponseListener = ActionListener.wrap(
            response -> {
                List<GetDataFrameAnalyticsStatsAction.Response.Stats> stats = new ArrayList(response.getResources().results().size());
                response.getResources().results().forEach(c -> stats.add(buildStats(c.getId(), tasks, state)));
                listener.onResponse(new GetDataFrameAnalyticsStatsAction.Response(new QueryPage<>(stats, stats.size(),
                    GetDataFrameAnalyticsAction.Response.RESULTS_FIELD)));
            },
            listener::onFailure
        );

        GetDataFrameAnalyticsAction.Request getRequest = new GetDataFrameAnalyticsAction.Request();
        getRequest.setResourceId(request.getId());
        getRequest.setPageParams(request.getPageParams());
        executeAsyncWithOrigin(client, ML_ORIGIN, GetDataFrameAnalyticsAction.INSTANCE, getRequest, getResponseListener);
    }

    private GetDataFrameAnalyticsStatsAction.Response.Stats buildStats(String concreteAnalyticsId, PersistentTasksCustomMetaData tasks,
                                                                       ClusterState clusterState) {
        PersistentTasksCustomMetaData.PersistentTask<?> analyticsTask = MlTasks.getDataFrameAnalyticsTask(concreteAnalyticsId, tasks);
        DataFrameAnalyticsState analyticsState = MlTasks.getDataFrameAnalyticsState(concreteAnalyticsId, tasks);
        DiscoveryNode node = null;
        String assignmentExplanation = null;
        if (analyticsTask != null) {
            node = clusterState.nodes().get(analyticsTask.getExecutorNode());
            assignmentExplanation = analyticsTask.getAssignment().getExplanation();
        }
        return new GetDataFrameAnalyticsStatsAction.Response.Stats(
            concreteAnalyticsId, analyticsState, node, assignmentExplanation);
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataFrameAnalyticsStatsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
