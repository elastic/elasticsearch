/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

public abstract class NativeUserBaseRestHandler extends SecurityBaseRestHandler {

    public static final String NATIVE_USERS_ENABLED = "xpack.security.authc.native_users.enabled";

    public NativeUserBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception checkFeatureAvailable(RestRequest request) {
        Exception failedFeature = super.checkFeatureAvailable(request);
        if (failedFeature != null) {
            return failedFeature;
        }
        final Boolean nativeUserEnabled = settings.getAsBoolean(NATIVE_USERS_ENABLED, true);
        if (nativeUserEnabled == false) {
            return new ElasticsearchStatusException(
                "Native user management is not enabled in this Elasticsearch instance",
                RestStatus.GONE
            );
        } else {
            return null;
        }

    }
}
