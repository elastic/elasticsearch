/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.vectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.vectors.VectorsFeatureSetUsage;

public class VectorsFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public VectorsFeatureSet(Settings settings, XPackLicenseState licenseState) {
        this.enabled = XPackSettings.VECTORS_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.VECTORS;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isVectorsAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    public static class UsageTransportAction extends XPackUsageFeatureTransportAction {

        private final Settings settings;
        private final XPackLicenseState licenseState;

        @Inject
        public UsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                    Settings settings, XPackLicenseState licenseState) {
            super(XPackUsageFeatureAction.VECTORS.name(), transportService, clusterService,
                threadPool, actionFilters, indexNameExpressionResolver);
            this.settings = settings;
            this.licenseState = licenseState;
        }

        @Override
        protected void masterOperation(XPackUsageRequest request, ClusterState state, ActionListener<XPackUsageFeatureResponse> listener) {
            VectorsFeatureSetUsage usage =
                new VectorsFeatureSetUsage(licenseState.isVectorsAllowed(), XPackSettings.VECTORS_ENABLED.get(settings));
            listener.onResponse(new XPackUsageFeatureResponse(usage));
        }
    }
}
