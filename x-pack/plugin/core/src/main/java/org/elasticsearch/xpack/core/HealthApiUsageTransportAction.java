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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.health.stats.HealthApiStatsAction;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;

/**
 * This action provides the health api stats of the cluster.
 */
public class HealthApiUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final Client client;

    @Inject
    public HealthApiUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
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

        HealthApiStatsAction.Request statsRequest = new HealthApiStatsAction.Request();
        statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.execute(HealthApiStatsAction.INSTANCE, statsRequest, ActionListener.wrap(r -> {
            HealthApiFeatureSetUsage usage = new HealthApiFeatureSetUsage(
                true,
                true,
                r.getStats(),
                r.getStatuses(),
                r.getIndicators(),
                r.getDiagnoses()
            );
            preservingListener.onResponse(new XPackUsageFeatureResponse(usage));
        }, preservingListener::onFailure));
    }
}
