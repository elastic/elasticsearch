/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

import java.util.Collections;

public class AnalyticsUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final XPackLicenseState licenseState;
    private final Client client;

    @Inject
    public AnalyticsUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         XPackLicenseState licenseState, Client client) {
        super(XPackUsageFeatureAction.ANALYTICS.name(), transportService, clusterService,
            threadPool, actionFilters, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        boolean available = licenseState.isAllowed(XPackLicenseState.Feature.ANALYTICS);
        if (available) {
            AnalyticsStatsAction.Request statsRequest = new AnalyticsStatsAction.Request();
            statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
            client.execute(AnalyticsStatsAction.INSTANCE, statsRequest, ActionListener.wrap(r ->
                    listener.onResponse(new XPackUsageFeatureResponse(new AnalyticsFeatureSetUsage(true, true, r))),
                listener::onFailure));
        } else {
            AnalyticsFeatureSetUsage usage = new AnalyticsFeatureSetUsage(false, true,
                new AnalyticsStatsAction.Response(state.getClusterName(), Collections.emptyList(), Collections.emptyList()));
            listener.onResponse(new XPackUsageFeatureResponse(usage));
        }
    }
}
