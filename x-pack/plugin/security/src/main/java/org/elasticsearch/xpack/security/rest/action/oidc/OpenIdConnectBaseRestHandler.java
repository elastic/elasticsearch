/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.oidc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

public abstract class OpenIdConnectBaseRestHandler extends SecurityBaseRestHandler {

    private static final String OIDC_REALM_TYPE = OpenIdConnectRealmSettings.TYPE;

    /**
     * @param settings     the node's settings
     * @param licenseState the license state that will be used to determine if security is licensed
     */
    protected OpenIdConnectBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception checkFeatureAvailable(RestRequest request) {
        Exception failedFeature = super.checkFeatureAvailable(request);
        if (failedFeature != null) {
            return failedFeature;
        } else if (Realms.isRealmTypeAvailable(licenseState.allowedRealmType(), OIDC_REALM_TYPE)) {
            return null;
        } else {
            logger.info("The '{}' realm is not available under the current license", OIDC_REALM_TYPE);
            return LicenseUtils.newComplianceException(OIDC_REALM_TYPE);
        }
    }
}
