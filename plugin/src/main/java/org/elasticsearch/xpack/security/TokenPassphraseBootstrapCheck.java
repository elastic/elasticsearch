/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.authc.TokenService;

/**
 * Bootstrap check to ensure that the user has set the token passphrase setting and is not using
 * the default value in production
 */
final class TokenPassphraseBootstrapCheck implements BootstrapCheck {

    static final int MINIMUM_PASSPHRASE_LENGTH = 8;

    private final Settings settings;

    TokenPassphraseBootstrapCheck(Settings settings) {
        this.settings = settings;
    }

    @Override
    public boolean check() {
        if (XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.get(settings)) {
            try (SecureString secureString = TokenService.TOKEN_PASSPHRASE.get(settings)) {
                return secureString.length() < MINIMUM_PASSPHRASE_LENGTH || secureString.equals(TokenService.DEFAULT_PASSPHRASE);
            }
        }
        // service is not enabled so no need to check
        return false;
    }

    @Override
    public String errorMessage() {
        return "Please set a passphrase using the elasticsearch-keystore tool for the setting [" + TokenService.TOKEN_PASSPHRASE.getKey() +
                "] that is at least " + MINIMUM_PASSPHRASE_LENGTH + " characters in length and does not match the default passphrase or " +
                "disable the token service using the [" + XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey() + "] setting";
    }
}
