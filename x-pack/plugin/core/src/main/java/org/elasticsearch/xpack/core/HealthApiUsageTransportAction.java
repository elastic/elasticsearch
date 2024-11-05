/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.health.stats.HealthApiStatsAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;

/**
 * This action provides telemetry of the cluster's health api usage.
 */
public class HealthApiUsageTransportAction extends XPackUsageFeatureTransportAction {

    static final NodeFeature SUPPORTS_HEALTH_STATS = new NodeFeature("health.supports_health_stats");

    private final Client client;
    private final FeatureService featureService;

    @Inject
    public HealthApiUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        FeatureService featureService
    ) {
        super(
            XPackUsageFeatureAction.HEALTH.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.client = client;
        this.featureService = featureService;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {

        ActionListener<XPackUsageFeatureResponse> preservingListener = ContextPreservingActionListener.wrapPreservingContext(
            listener,
            client.threadPool().getThreadContext()
        );

        if (state.clusterRecovered() && featureService.clusterHasFeature(state, SUPPORTS_HEALTH_STATS)) {
            HealthApiStatsAction.Request statsRequest = new HealthApiStatsAction.Request();
            statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
            client.execute(HealthApiStatsAction.INSTANCE, statsRequest, preservingListener.delegateFailureAndWrap((l, r) -> {
                HealthApiFeatureSetUsage usage = new HealthApiFeatureSetUsage(true, true, r.getStats());
                l.onResponse(new XPackUsageFeatureResponse(usage));
            }));
        } else {
            HealthApiFeatureSetUsage usage = new HealthApiFeatureSetUsage(false, true, null);
            preservingListener.onResponse(new XPackUsageFeatureResponse(usage));
        }
    }
}
