/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollectionResolver;
import org.elasticsearch.xpack.application.search.action.ListSearchApplicationAction;
import org.elasticsearch.xpack.application.utils.LicenseUtils;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class EnterpriseSearchUsageTransportAction extends XPackUsageFeatureTransportAction {
    private static final Logger logger = LogManager.getLogger(EnterpriseSearchUsageTransportAction.class);
    private final XPackLicenseState licenseState;
    private final OriginSettingClient clientWithOrigin;

    private final boolean enabled;

    @Inject
    public EnterpriseSearchUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        XPackLicenseState licenseState,
        Client client
    ) {
        super(
            XPackUsageFeatureAction.ENTERPRISE_SEARCH.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.licenseState = licenseState;
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.enabled = XPackSettings.ENTERPRISE_SEARCH_ENABLED.get(settings);
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        if (enabled == false) {
            EnterpriseSearchFeatureSetUsage usage = new EnterpriseSearchFeatureSetUsage(
                LicenseUtils.LICENSED_ENT_SEARCH_FEATURE.checkWithoutTracking(licenseState),
                enabled,
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            listener.onResponse(new XPackUsageFeatureResponse(usage));
            return;
        }

        Map<String, Object> searchApplicationsUsage = new HashMap<>();
        Map<String, Object> analyticsCollectionsUsage = new HashMap<>();

        addAnalyticsCollectionsUsage(state, analyticsCollectionsUsage);

        ActionListener<ListSearchApplicationAction.Response> searchApplicationsCountListener = ActionListener.wrap(response -> {
            addSearchApplicationsUsage(response, searchApplicationsUsage);
            listener.onResponse(new XPackUsageFeatureResponse(
                new EnterpriseSearchFeatureSetUsage(
                    LicenseUtils.LICENSED_ENT_SEARCH_FEATURE.checkWithoutTracking(licenseState),
                    enabled,
                    searchApplicationsUsage,
                    analyticsCollectionsUsage
                )
            ));
        }, e -> {
            listener.onResponse(new XPackUsageFeatureResponse(
                new EnterpriseSearchFeatureSetUsage(
                    LicenseUtils.LICENSED_ENT_SEARCH_FEATURE.checkWithoutTracking(licenseState),
                    enabled,
                    Collections.emptyMap(),
                    analyticsCollectionsUsage
                )
            ));
        });

        ListSearchApplicationAction.Request searchApplicationsCountRequest =
            new ListSearchApplicationAction.Request("*", new PageParams(0, 0));

        clientWithOrigin.execute(ListSearchApplicationAction.INSTANCE, searchApplicationsCountRequest, searchApplicationsCountListener);
    }

    private void addSearchApplicationsUsage(ListSearchApplicationAction.Response response, Map<String, Object> searchApplicationsUsage) {
        searchApplicationsUsage.put(EnterpriseSearchFeatureSetUsage.COUNT, response.queryPage().count());
    }

    private void addAnalyticsCollectionsUsage(ClusterState clusterState, Map<String, Object> analyticsCollectionsUsage) {
        AnalyticsCollectionResolver analyticsCollectionResolver =
            new AnalyticsCollectionResolver(indexNameExpressionResolver, clusterService);
        List<AnalyticsCollection> analyticsCollections = analyticsCollectionResolver.collections(clusterState, "*");

        analyticsCollectionsUsage.put(EnterpriseSearchFeatureSetUsage.COUNT, analyticsCollections.size());
    }
}
