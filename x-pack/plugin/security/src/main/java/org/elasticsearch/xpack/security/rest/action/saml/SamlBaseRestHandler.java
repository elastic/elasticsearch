/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.saml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

/**
 * An abstract implementation of {@link SecurityBaseRestHandler} that performs a license check for the SAML realm type
 */
public abstract class SamlBaseRestHandler extends SecurityBaseRestHandler {
    private static final Logger logger = LogManager.getLogger(SamlBaseRestHandler.class);

    private static final String SAML_REALM_TYPE = SamlRealmSettings.TYPE;

    public SamlBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception checkFeatureAvailable(RestRequest request) {
        Exception failedFeature = super.checkFeatureAvailable(request);
        if (failedFeature != null) {
            return failedFeature;
        } else if (Realms.isRealmTypeAvailable(licenseState, SAML_REALM_TYPE)) {
            return null;
        } else {
            logger.info("The '{}' realm is not available under the current license", SAML_REALM_TYPE);
            return LicenseUtils.newComplianceException(SAML_REALM_TYPE);
        }
    }
}
