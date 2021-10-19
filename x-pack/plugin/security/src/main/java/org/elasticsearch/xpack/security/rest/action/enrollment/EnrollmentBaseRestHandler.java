/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.enrollment;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

/**
 * An abstract implementation of {@link SecurityBaseRestHandler} that performs enrollment_enabled setting check
 */
public abstract class EnrollmentBaseRestHandler extends SecurityBaseRestHandler {

    public EnrollmentBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception checkFeatureAvailable(RestRequest request) {
        Exception failedFeature = super.checkFeatureAvailable(request);
        if (failedFeature != null) {
            return failedFeature;
        } else if (XPackSettings.ENROLLMENT_ENABLED.get(settings) == false) {
            return new ElasticsearchSecurityException("Enrollment mode is not enabled. Set [" + XPackSettings.ENROLLMENT_ENABLED.getKey() +
                "] to true, in order to use this API.",
                RestStatus.FORBIDDEN);
        } else {
            return null;
        }
    }
}
