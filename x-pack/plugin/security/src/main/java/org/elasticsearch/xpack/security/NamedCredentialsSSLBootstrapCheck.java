/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Locale;

/**
 * Named credentials are returned in plaintext by the _decrypt API, so the HTTP layer must be
 * encrypted before the feature may be enabled. Mirrors {@link TokenSSLBootstrapCheck}.
 */
final class NamedCredentialsSSLBootstrapCheck implements BootstrapCheck {

    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        final Boolean httpsEnabled = XPackSettings.HTTP_SSL_ENABLED.get(context.settings());
        final Boolean namedCredentialsEnabled = XPackSettings.NAMED_CREDENTIALS_ENABLED.get(context.settings());
        if (httpsEnabled == false && namedCredentialsEnabled) {
            final String message = String.format(
                Locale.ROOT,
                "HTTPS is required in order to use the named credentials store; "
                    + "please enable HTTPS using the [%s] setting or disable named credentials using the [%s] setting",
                XPackSettings.HTTP_SSL_ENABLED.getKey(),
                XPackSettings.NAMED_CREDENTIALS_ENABLED.getKey()
            );
            return BootstrapCheckResult.failure(message);
        } else {
            return BootstrapCheckResult.success();
        }
    }

    @Override
    public ReferenceDocs referenceDocs() {
        return ReferenceDocs.BOOTSTRAP_CHECK_NAMED_CREDENTIALS_SSL;
    }
}
