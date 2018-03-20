/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.saml;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

/**
 * An abstract implementation of {@link SecurityBaseRestHandler} that performs a license check for the SAML realm type
 */
public abstract class SamlBaseRestHandler extends SecurityBaseRestHandler {

    private static final String SAML_REALM_TYPE = SamlRealmSettings.TYPE;

    public SamlBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected String checkLicensedFeature(RestRequest request) {
        String feature = super.checkLicensedFeature(request);
        if (feature != null) {
            return feature;
        } else if (Realms.isRealmTypeAvailable(licenseState.allowedRealmType(), SAML_REALM_TYPE)) {
            return null;
        } else {
            logger.info("The '{}' realm is not available under the current license", SAML_REALM_TYPE);
            return SAML_REALM_TYPE;
        }
    }
}
