/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

/**
 * This plugin extends {@link LocalStateCompositeXPackPlugin} to only make the data tier telemetry
 * available. This allows telemetry to be retrieved in integration tests where it would otherwise
 * throw errors trying to retrieve all of the different telemetry types.
 */
public class DataTierTelemetryPlugin extends LocalStateCompositeXPackPlugin {
    public DataTierTelemetryPlugin(Settings settings, Path configPath) {
        super(settings, configPath);
    }

//    public static class DataTiersTransportXPackUsageAction extends TransportXPackUsageAction {
//        @Inject
//        public DataTiersTransportXPackUsageAction(ThreadPool threadPool, TransportService transportService,
//                                                  ClusterService clusterService, ActionFilters actionFilters,
//                                                  IndexNameExpressionResolver indexNameExpressionResolver, NodeClient client) {
//            super(threadPool, transportService, clusterService, actionFilters, indexNameExpressionResolver, client);
//        }
//        @Override
//        protected List<XPackUsageFeatureAction> usageActions() {
//            return Collections.singletonList(XPackUsageFeatureAction.DATA_TIERS);
//        }
//    }
//
//    public static class DataTiersTransportXPackInfoAction extends TransportXPackInfoAction {
//        @Inject
//        public DataTiersTransportXPackInfoAction(TransportService transportService, ActionFilters actionFilters,
//                                                 LicenseService licenseService, NodeClient client) {
//            super(transportService, actionFilters, licenseService, client);
//        }
//
//        @Override
//        protected List<XPackInfoFeatureAction> infoActions() {
//            return Collections.singletonList(XPackInfoFeatureAction.DATA_TIERS);
//        }
//    }
//
//    public DataTierTelemetryPlugin(final Settings settings, final Path configPath) {
//        super(settings, configPath);
//    }
//
//    @Override
//    protected Class<? extends TransportAction<XPackUsageRequest, XPackUsageResponse>> getUsageAction() {
//        return DataTiersTransportXPackUsageAction.class;
//    }
//
//    @Override
//    protected Class<? extends TransportAction<XPackInfoRequest, XPackInfoResponse>> getInfoAction() {
//        return DataTiersTransportXPackInfoAction.class;
//    }
}
