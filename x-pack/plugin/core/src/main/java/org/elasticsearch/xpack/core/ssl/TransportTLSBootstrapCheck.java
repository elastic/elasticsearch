/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.xpack.core.XPackSettings;

/**
 * Bootstrap check to ensure that if we are starting up with security enabled, transport TLS is enabled
 */
public final class TransportTLSBootstrapCheck implements BootstrapCheck {
    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        assert XPackSettings.SECURITY_ENABLED.get(context.settings())
            : "Bootstrap check should not be installed unless security is enabled";
        if (XPackSettings.TRANSPORT_SSL_ENABLED.get(context.settings()) == false) {
            return BootstrapCheckResult.failure(
                "Transport SSL must be enabled if security is enabled. "
                    + "Please set [xpack.security.transport.ssl.enabled] to [true] or disable security by setting "
                    + "[xpack.security.enabled] to [false]"
            );
        }
        return BootstrapCheckResult.success();
    }
}
