/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackSettings;

/**
 * Bootstrap check to ensure that if we are starting up with a production license in the local clusterstate TLS is enabled
 */
public final class TLSLicenseBootstrapCheck implements BootstrapCheck {
    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        if (XPackSettings.TRANSPORT_SSL_ENABLED.get(context.settings()) == false) {
            License license = LicenseService.getLicense(context.metadata());
            if (XPackLicenseState.isTransportTlsRequired(license, context.settings())) {
                return BootstrapCheckResult.failure("Transport SSL must be enabled if security is enabled on a [" +
                    license.operationMode().description() + "] license. " +
                    "Please set [xpack.security.transport.ssl.enabled] to [true] or disable security by setting " +
                    "[xpack.security.enabled] to [false]");
            }
        }
        return BootstrapCheckResult.success();
    }
}
