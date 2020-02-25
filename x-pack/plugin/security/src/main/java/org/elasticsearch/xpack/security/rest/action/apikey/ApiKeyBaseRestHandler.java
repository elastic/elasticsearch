/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

/**
 * A base rest handler that handles licensing for ApiKey actions
 */
abstract class ApiKeyBaseRestHandler extends SecurityBaseRestHandler {
    private static final Logger logger = LogManager.getLogger();

    ApiKeyBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception checkFeatureAvailable(RestRequest request) {
        Exception failedFeature = super.checkFeatureAvailable(request);
        if (failedFeature != null) {
            return failedFeature;
        } else if (licenseState.isApiKeyServiceAllowed()) {
            return null;
        } else {
            logger.info("API Keys are not available under the current [{}] license", licenseState.getOperationMode().description());
            return LicenseUtils.newComplianceException("api keys");
        }
    }
}
