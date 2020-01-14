/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.enrich.EnrichFeatureSetUsage;
import org.elasticsearch.xpack.core.enrich.EnrichFeatureSetUsage.CoordinatorSummaryStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;

public class EnrichUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final Client client;

    @Inject
    public EnrichUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        Settings settings,
        XPackLicenseState licenseState
    ) {
        super(
            XPackUsageFeatureAction.ENRICH.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.enabled = XPackSettings.ENRICH_ENABLED_SETTING.get(settings);
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) throws Exception {
        client.execute(
            EnrichStatsAction.INSTANCE,
            new EnrichStatsAction.Request(),
            ActionListener.wrap(
                statsResponse -> listener.onResponse(
                    new XPackUsageFeatureResponse(
                        new EnrichFeatureSetUsage(
                            licenseState.isEnrichAllowed(),
                            enabled,
                            statsResponse.getExecutionStats(),
                            CoordinatorSummaryStats.aggregate(statsResponse.getCoordinatorStats())
                        )
                    )
                ),
                listener::onFailure
            )
        );
    }
}
