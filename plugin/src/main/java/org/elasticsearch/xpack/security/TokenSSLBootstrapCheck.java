/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackSettings;

/**
 * Bootstrap check to ensure that the user has enabled HTTPS when using the token service
 */
final class TokenSSLBootstrapCheck implements BootstrapCheck {

    private final Settings settings;

    TokenSSLBootstrapCheck(Settings settings) {
        this.settings = settings;
    }

    @Override
    public boolean check() {
        if (NetworkModule.HTTP_ENABLED.get(settings)) {
            return XPackSettings.HTTP_SSL_ENABLED.get(settings) == false && XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.get(settings);
        }
        return false;
    }

    @Override
    public String errorMessage() {
        return "HTTPS is required in order to use the token service. Please enable HTTPS using the [" +
                XPackSettings.HTTP_SSL_ENABLED.getKey() + "] setting or disable the token service using the [" +
                XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey() + "] setting.";
    }
}
