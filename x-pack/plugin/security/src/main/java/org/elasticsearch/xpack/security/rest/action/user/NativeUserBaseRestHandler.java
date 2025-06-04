/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.user;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

abstract class NativeUserBaseRestHandler extends SecurityBaseRestHandler {

    private static final Logger logger = LogManager.getLogger(NativeUserBaseRestHandler.class);

    NativeUserBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception innerCheckFeatureAvailable(RestRequest request) {
        final Boolean nativeUserEnabled = settings.getAsBoolean(NativeRealmSettings.NATIVE_USERS_ENABLED, true);
        if (nativeUserEnabled == false) {
            logger.debug(
                "Attempt to call [{} {}] but [{}] is [{}]",
                request.method(),
                request.rawPath(),
                NativeRealmSettings.NATIVE_USERS_ENABLED,
                settings.get(NativeRealmSettings.NATIVE_USERS_ENABLED)
            );
            return new ElasticsearchStatusException(
                "Native user management is not enabled in this Elasticsearch instance",
                RestStatus.GONE
            );
        } else {
            return null;
        }

    }
}
