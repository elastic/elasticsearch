/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.namedcredentials;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

/**
 * Disables all named-credentials endpoints unless {@code xpack.security.named_credentials.enabled}
 * is true (default: on exactly when TLS is enabled on the HTTP layer). Mirrors how the token
 * service gates its endpoints on HTTPS via a node setting rather than per-request checks.
 */
abstract class NamedCredentialsBaseRestHandler extends SecurityBaseRestHandler {

    NamedCredentialsBaseRestHandler(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    protected Exception innerCheckFeatureAvailable(RestRequest request) {
        if (XPackSettings.NAMED_CREDENTIALS_ENABLED.get(settings)) {
            return null;
        }
        return new ElasticsearchStatusException(
            "named credentials are not enabled; they require TLS on the HTTP layer "
                + "(or set ["
                + XPackSettings.NAMED_CREDENTIALS_ENABLED.getKey()
                + "] explicitly)",
            RestStatus.BAD_REQUEST
        );
    }
}
