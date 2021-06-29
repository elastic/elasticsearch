/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.oauth2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

/**
 * A base rest handler that handles licensing for Token actions
 */
abstract class TokenBaseRestHandler extends SecurityBaseRestHandler {

    protected Logger logger = LogManager.getLogger(getClass());

    TokenBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception checkFeatureAvailable(RestRequest request) {
        Exception failedFeature = super.checkFeatureAvailable(request);
        if (failedFeature != null) {
            return failedFeature;
        } else if (licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE)) {
            return null;
        } else {
            logger.info("Security tokens are not available under the current [{}] license", licenseState.getOperationMode().description());
            return LicenseUtils.newComplianceException("security tokens");
        }
    }
}
