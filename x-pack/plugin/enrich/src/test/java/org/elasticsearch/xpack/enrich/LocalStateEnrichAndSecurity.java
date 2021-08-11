/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;

import java.nio.file.Path;
import java.util.List;

public class LocalStateEnrichAndSecurity extends LocalStateEnrich {

    public LocalStateEnrichAndSecurity(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);

        plugins.add(new Security(settings, configPath) {
            @Override
            protected SSLService getSslService() {
                return LocalStateEnrichAndSecurity.this.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return LocalStateEnrichAndSecurity.this.getLicenseState();
            }
        });
    }

    public static class EnrichAndSecurityTransportXPackInfoAction extends TransportXPackInfoAction {
        @Inject
        public EnrichAndSecurityTransportXPackInfoAction(
            TransportService transportService,
            ActionFilters actionFilters,
            LicenseService licenseService,
            NodeClient client
        ) {
            super(transportService, actionFilters, licenseService, client);
        }

        @Override
        protected List<XPackInfoFeatureAction> infoActions() {
            return List.of(XPackInfoFeatureAction.ENRICH, XPackInfoFeatureAction.SECURITY);
        }
    }

    @Override
    protected Class<? extends TransportAction<XPackInfoRequest, XPackInfoResponse>> getInfoAction() {
        return EnrichAndSecurityTransportXPackInfoAction.class;
    }
}
