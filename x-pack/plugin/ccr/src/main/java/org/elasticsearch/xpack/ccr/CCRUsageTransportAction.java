/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
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
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.CcrConstants;

import java.time.Instant;

public class CCRUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Settings settings;
    private final XPackLicenseState licenseState;

    @Inject
    public CCRUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        XPackLicenseState licenseState
    ) {
        super(XPackUsageFeatureAction.CCR.name(), transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);
        this.settings = settings;
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        Metadata metadata = state.metadata();

        int numberOfFollowerIndices = 0;
        long lastFollowerIndexCreationDate = 0L;
        for (IndexMetadata imd : metadata) {
            if (imd.getCustomData("ccr") != null) {
                numberOfFollowerIndices++;
                if (lastFollowerIndexCreationDate < imd.getCreationDate()) {
                    lastFollowerIndexCreationDate = imd.getCreationDate();
                }
            }
        }
        AutoFollowMetadata autoFollowMetadata = metadata.custom(AutoFollowMetadata.TYPE);
        int numberOfAutoFollowPatterns = autoFollowMetadata != null ? autoFollowMetadata.getPatterns().size() : 0;

        Long lastFollowTimeInMillis;
        if (numberOfFollowerIndices == 0) {
            // Otherwise we would return a value that makes no sense.
            lastFollowTimeInMillis = null;
        } else {
            lastFollowTimeInMillis = Math.max(0, Instant.now().toEpochMilli() - lastFollowerIndexCreationDate);
        }

        CCRInfoTransportAction.Usage usage = new CCRInfoTransportAction.Usage(
            CcrConstants.CCR_FEATURE.checkWithoutTracking(licenseState),
            XPackSettings.CCR_ENABLED_SETTING.get(settings),
            numberOfFollowerIndices,
            numberOfAutoFollowPatterns,
            lastFollowTimeInMillis
        );
        listener.onResponse(new XPackUsageFeatureResponse(usage));
    }
}
