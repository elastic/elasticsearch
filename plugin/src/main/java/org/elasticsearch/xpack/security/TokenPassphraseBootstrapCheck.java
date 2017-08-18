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

    private final boolean tokenServiceEnabled;
    private final SecureString tokenPassphrase;

    TokenPassphraseBootstrapCheck(Settings settings) {
        this.tokenServiceEnabled = XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.get(settings);

        this.tokenPassphrase = TokenService.TOKEN_PASSPHRASE.exists(settings) ? TokenService.TOKEN_PASSPHRASE.get(settings) : null;
    }

    @Override
    public boolean check() {
        if (tokenPassphrase == null) { // that's fine we bootstrap it ourself
            return false;
        }
        try (SecureString ignore = tokenPassphrase) {
            if (tokenServiceEnabled) {
                return tokenPassphrase.length() < MINIMUM_PASSPHRASE_LENGTH;
            }
        }
        // service is not enabled so no need to check
        return false;
    }

    @Override
    public String errorMessage() {
        return "Please set a passphrase using the elasticsearch-keystore tool for the setting [" + TokenService.TOKEN_PASSPHRASE.getKey() +
                "] that is at least " + MINIMUM_PASSPHRASE_LENGTH + " characters in length or " +
                "disable the token service using the [" + XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey() + "] setting";
    }
}
