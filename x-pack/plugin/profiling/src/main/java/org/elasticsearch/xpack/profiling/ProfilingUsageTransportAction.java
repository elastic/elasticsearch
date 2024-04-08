/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.application.ProfilingUsage;

public class ProfilingUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final ProfilingLicenseChecker licenseChecker;

    private final boolean enabled;

    @Inject
    public ProfilingUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProfilingLicenseChecker licenseChecker,
        Settings settings
    ) {
        super(
            XPackUsageFeatureAction.UNIVERSAL_PROFILING.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.licenseChecker = licenseChecker;
        this.enabled = XPackSettings.PROFILING_ENABLED.get(settings);
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        ProfilingUsage profilingUsage = new ProfilingUsage(licenseChecker.isSupportedLicense(), enabled);
        listener.onResponse(new XPackUsageFeatureResponse(profilingUsage));
    }
}
