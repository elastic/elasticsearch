/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureResponse;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class LocalStateEnrich extends LocalStateCompositeXPackPlugin {

    public LocalStateEnrich(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);

        plugins.add(new EnrichPlugin(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return LocalStateEnrich.this.getLicenseState();
            }
        });
    }

    public static class EnrichTransportXPackInfoAction extends TransportXPackInfoAction {
        @Inject
        public EnrichTransportXPackInfoAction(
            TransportService transportService,
            ActionFilters actionFilters,
            LicenseService licenseService,
            NodeClient client
        ) {
            super(transportService, actionFilters, licenseService, client);
        }

        @Override
        protected List<ActionType<XPackInfoFeatureResponse>> infoActions() {
            return Collections.singletonList(XPackInfoFeatureAction.ENRICH);
        }
    }

    @Override
    protected Class<? extends TransportAction<XPackInfoRequest, XPackInfoResponse>> getInfoAction() {
        return EnrichTransportXPackInfoAction.class;
    }
}
