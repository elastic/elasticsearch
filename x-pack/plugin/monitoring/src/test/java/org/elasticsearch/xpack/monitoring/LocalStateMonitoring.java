/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.action.TransportXPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.watcher.Watcher;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class LocalStateMonitoring extends LocalStateCompositeXPackPlugin {

    public static class MonitoringTransportXPackUsageAction extends TransportXPackUsageAction {
        @Inject
        public MonitoringTransportXPackUsageAction(ThreadPool threadPool, TransportService transportService,
                                         ClusterService clusterService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver, NodeClient client) {
            super(threadPool, transportService, clusterService, actionFilters, indexNameExpressionResolver, client);
        }
        @Override
        protected List<XPackUsageFeatureAction> usageActions() {
            return Collections.singletonList(XPackUsageFeatureAction.MONITORING);
        }
    }

    final Monitoring monitoring;

    public LocalStateMonitoring(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateMonitoring thisVar = this;

        monitoring = new Monitoring(settings) {
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
        };
        plugins.add(monitoring);
        plugins.add(new Watcher(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        });
        plugins.add(new IndexLifecycle(settings));
    }

    public Monitoring getMonitoring() {
        return monitoring;
    }

    @Override
    protected Class<? extends TransportAction<XPackUsageRequest, XPackUsageResponse>> getUsageAction() {
        return MonitoringTransportXPackUsageAction.class;
    }
}
