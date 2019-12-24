/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.core.action.TransportXPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.Monitoring;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class LocalStateSecurity extends LocalStateCompositeXPackPlugin {

    public static class SecurityTransportXPackUsageAction extends TransportXPackUsageAction {
        @Inject
        public SecurityTransportXPackUsageAction(ThreadPool threadPool, TransportService transportService,
                                                   ClusterService clusterService, ActionFilters actionFilters,
                                                   IndexNameExpressionResolver indexNameExpressionResolver, NodeClient client) {
            super(threadPool, transportService, clusterService, actionFilters, indexNameExpressionResolver, client);
        }
        @Override
        protected List<XPackUsageFeatureAction> usageActions() {
            return Collections.singletonList(XPackUsageFeatureAction.SECURITY);
        }
    }

    public static class SecurityTransportXPackInfoAction extends TransportXPackInfoAction {
        @Inject
        public SecurityTransportXPackInfoAction(TransportService transportService, ActionFilters actionFilters,
                                                 LicenseService licenseService, NodeClient client) {
            super(transportService, actionFilters, licenseService, client);
        }

        @Override
        protected List<XPackInfoFeatureAction> infoActions() {
            return Collections.singletonList(XPackInfoFeatureAction.SECURITY);
        }
    }

    public LocalStateSecurity(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateSecurity thisVar = this;
        plugins.add(new Monitoring(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected LicenseService getLicenseService() {
                return thisVar.getLicenseService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        });
        plugins.add(new Security(settings, configPath) {
            @Override
            protected SSLService getSslService() { return thisVar.getSslService(); }

            @Override
            protected XPackLicenseState getLicenseState() { return thisVar.getLicenseState(); }
        });
    }

    @Override
    protected Class<? extends TransportAction<XPackUsageRequest, XPackUsageResponse>> getUsageAction() {
        return SecurityTransportXPackUsageAction.class;
    }

    @Override
    protected Class<? extends TransportAction<XPackInfoRequest, XPackInfoResponse>> getInfoAction() {
        return SecurityTransportXPackInfoAction.class;
    }
}
