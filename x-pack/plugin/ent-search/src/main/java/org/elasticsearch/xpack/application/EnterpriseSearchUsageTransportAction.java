/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
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
import org.elasticsearch.xpack.application.search.action.ListSearchApplicationAction;
import org.elasticsearch.xpack.application.utils.LicenseUtils;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage;

import java.util.Map;

public class EnterpriseSearchUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final Settings settings;
    private final XPackLicenseState licenseState;
    private final NodeClient client;

    @Inject
    public EnterpriseSearchUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        XPackLicenseState licenseState,
        NodeClient client
    ) {
        super(
            XPackUsageFeatureAction.ENTERPRISE_SEARCH.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.settings = settings;
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        try {
            ListSearchApplicationAction.Response resp = client.execute(
                ListSearchApplicationAction.INSTANCE,
                new ListSearchApplicationAction.Request(null, new PageParams(0, 0))
            ).get();
            listener.onResponse(
                new XPackUsageFeatureResponse(
                    new EnterpriseSearchFeatureSetUsage(
                        XPackSettings.ENTERPRISE_SEARCH_ENABLED.get(settings),
                        LicenseUtils.LICENSED_ENT_SEARCH_FEATURE.checkWithoutTracking(licenseState),
                        Map.of("count", resp.queryPage().count())
                    )
                )
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
