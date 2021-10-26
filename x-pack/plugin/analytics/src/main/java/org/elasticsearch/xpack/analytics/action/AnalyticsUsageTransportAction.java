/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

public class AnalyticsUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final Client client;

    @Inject
    public AnalyticsUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            XPackUsageFeatureAction.ANALYTICS.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        AnalyticsStatsAction.Request statsRequest = new AnalyticsStatsAction.Request();
        statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.execute(
            AnalyticsStatsAction.INSTANCE,
            statsRequest,
            ActionListener.wrap(
                r -> listener.onResponse(new XPackUsageFeatureResponse(new AnalyticsFeatureSetUsage(true, true, r))),
                listener::onFailure
            )
        );
    }
}
