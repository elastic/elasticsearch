/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;
import org.elasticsearch.xpack.security.support.FeatureNotEnabledException;

abstract class ApiKeyBaseRestHandler extends SecurityBaseRestHandler {
    ApiKeyBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception checkFeatureAvailable(RestRequest request) {
        final Exception failedFeature = super.checkFeatureAvailable(request);
        if (failedFeature != null) {
            return failedFeature;
        } else if (XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.get(settings) == false) {
            return new FeatureNotEnabledException(FeatureNotEnabledException.Feature.API_KEY_SERVICE, "api keys are not enabled");
        } else {
            return null;
        }
    }
}
