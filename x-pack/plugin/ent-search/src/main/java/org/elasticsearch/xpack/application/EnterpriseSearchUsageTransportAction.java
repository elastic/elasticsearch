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
import org.elasticsearch.xpack.application.analytics.action.GetAnalyticsCollectionAction;
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

        // Step 2: Fetch search applications count and return usage
        ListSearchApplicationAction.Request searchApplicationsCountRequest = new ListSearchApplicationAction.Request(
            "*",
            new PageParams(0, 0)
        );
        ActionListener<ListSearchApplicationAction.Response> searchApplicationsCountListener = ActionListener.wrap(response -> {
            addSearchApplicationsUsage(response, searchApplicationsUsage);
            listener.onResponse(
                new XPackUsageFeatureResponse(
                    new EnterpriseSearchFeatureSetUsage(
                        LicenseUtils.LICENSED_ENT_SEARCH_FEATURE.checkWithoutTracking(licenseState),
                        enabled,
                        searchApplicationsUsage,
                        analyticsCollectionsUsage
                    )
                )
            );
        }, e -> {
            listener.onResponse(
                new XPackUsageFeatureResponse(
                    new EnterpriseSearchFeatureSetUsage(
                        LicenseUtils.LICENSED_ENT_SEARCH_FEATURE.checkWithoutTracking(licenseState),
                        enabled,
                        Collections.emptyMap(),
                        analyticsCollectionsUsage
                    )
                )
            );
        });

        // Step 1: Fetch analytics collections count
        GetAnalyticsCollectionAction.Request analyticsCollectionsCountRequest = new GetAnalyticsCollectionAction.Request(
            new String[] { "*" }
        );
        ActionListener<GetAnalyticsCollectionAction.Response> analyticsCollectionsCountListener = ActionListener.wrap(response -> {
            addAnalyticsCollectionsUsage(response, analyticsCollectionsUsage);
            clientWithOrigin.execute(ListSearchApplicationAction.INSTANCE, searchApplicationsCountRequest, searchApplicationsCountListener);
        },
            e -> {
                clientWithOrigin.execute(
                    ListSearchApplicationAction.INSTANCE,
                    searchApplicationsCountRequest,
                    searchApplicationsCountListener
                );
            }
        );

        // Step 0: Kick off requests
        clientWithOrigin.execute(
            GetAnalyticsCollectionAction.INSTANCE,
            analyticsCollectionsCountRequest,
            analyticsCollectionsCountListener
        );
    }

    private void addSearchApplicationsUsage(ListSearchApplicationAction.Response response, Map<String, Object> searchApplicationsUsage) {
        long count = response.queryPage().count();

        searchApplicationsUsage.put(EnterpriseSearchFeatureSetUsage.COUNT, count);
    }

    private void addAnalyticsCollectionsUsage(
        GetAnalyticsCollectionAction.Response response,
        Map<String, Object> analyticsCollectionsUsage
    ) {
        long count = response.getAnalyticsCollections().size();

        analyticsCollectionsUsage.put(EnterpriseSearchFeatureSetUsage.COUNT, count);
    }
}
