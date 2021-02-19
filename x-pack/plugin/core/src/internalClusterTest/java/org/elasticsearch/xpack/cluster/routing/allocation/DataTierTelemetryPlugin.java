/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
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

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * This plugin extends {@link LocalStateCompositeXPackPlugin} to only make the data tier telemetry
 * available. This allows telemetry to be retrieved in integration tests where it would otherwise
 * throw errors trying to retrieve all of the different telemetry types.
 */
public class DataTierTelemetryPlugin extends LocalStateCompositeXPackPlugin {

    public static class DataTiersTransportXPackUsageAction extends TransportXPackUsageAction {
        @Inject
        public DataTiersTransportXPackUsageAction(ThreadPool threadPool, TransportService transportService,
                                                  ClusterService clusterService, ActionFilters actionFilters,
                                                  IndexNameExpressionResolver indexNameExpressionResolver, NodeClient client) {
            super(threadPool, transportService, clusterService, actionFilters, indexNameExpressionResolver, client);
        }
        @Override
        protected List<XPackUsageFeatureAction> usageActions() {
            return Collections.singletonList(XPackUsageFeatureAction.DATA_TIERS);
        }
    }

    public static class DataTiersTransportXPackInfoAction extends TransportXPackInfoAction {
        @Inject
        public DataTiersTransportXPackInfoAction(TransportService transportService, ActionFilters actionFilters,
                                                 LicenseService licenseService, NodeClient client) {
            super(transportService, actionFilters, licenseService, client);
        }

        @Override
        protected List<XPackInfoFeatureAction> infoActions() {
            return Collections.singletonList(XPackInfoFeatureAction.DATA_TIERS);
        }
    }

    public DataTierTelemetryPlugin(final Settings settings, final Path configPath) {
        super(settings, configPath);
    }

    @Override
    protected Class<? extends TransportAction<XPackUsageRequest, XPackUsageResponse>> getUsageAction() {
        return DataTiersTransportXPackUsageAction.class;
    }

    @Override
    protected Class<? extends TransportAction<XPackInfoRequest, XPackInfoResponse>> getInfoAction() {
        return DataTiersTransportXPackInfoAction.class;
    }
}
