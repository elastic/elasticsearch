/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.oidc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

public abstract class OpenIdConnectBaseRestHandler extends SecurityBaseRestHandler {
    private static final Logger logger = LogManager.getLogger(OpenIdConnectBaseRestHandler.class);

    private static final String OIDC_REALM_TYPE = OpenIdConnectRealmSettings.TYPE;

    /**
     * @param settings     the node's settings
     * @param licenseState the license state that will be used to determine if security is licensed
     */
    protected OpenIdConnectBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception innerCheckFeatureAvailable(RestRequest request) {
        if (Realms.isRealmTypeAvailable(licenseState, OIDC_REALM_TYPE)) {
            return null;
        } else {
            logger.info("The '{}' realm is not available under the current license", OIDC_REALM_TYPE);
            return LicenseUtils.newComplianceException(OIDC_REALM_TYPE);
        }
    }
}
