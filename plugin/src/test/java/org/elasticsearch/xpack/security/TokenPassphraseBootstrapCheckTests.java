/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.TokenPassphraseBootstrapCheck;
import org.elasticsearch.xpack.security.authc.TokenService;

import static org.elasticsearch.xpack.security.TokenPassphraseBootstrapCheck.MINIMUM_PASSPHRASE_LENGTH;

public class TokenPassphraseBootstrapCheckTests extends ESTestCase {

    public void testTokenPassphraseCheck() throws Exception {
        assertFalse(new TokenPassphraseBootstrapCheck(Settings.EMPTY).check());
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "bar"); // leniency in setSecureSettings... if its empty it's skipped
        Settings settings = Settings.builder()
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).setSecureSettings(secureSettings).build();
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check());

        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(MINIMUM_PASSPHRASE_LENGTH, 30));
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check());

        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(1, MINIMUM_PASSPHRASE_LENGTH - 1));
        assertTrue(new TokenPassphraseBootstrapCheck(settings).check());
    }

    public void testTokenPassphraseCheckServiceDisabled() throws Exception {
        Settings settings = Settings.builder().put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), false)
                .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check());
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "bar"); // leniency in setSecureSettings... if its empty it's skipped
        settings = Settings.builder().put(settings).setSecureSettings(secureSettings).build();
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check());

        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(1, 30));
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check());
    }

    public void testTokenPassphraseCheckAfterSecureSettingsClosed() throws Exception {
        Settings settings = Settings.builder().put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "bar"); // leniency in setSecureSettings... if its empty it's skipped
        settings = Settings.builder().put(settings).setSecureSettings(secureSettings).build();
        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(1, MINIMUM_PASSPHRASE_LENGTH - 1));
        final TokenPassphraseBootstrapCheck check = new TokenPassphraseBootstrapCheck(settings);
        secureSettings.close();
        assertTrue(check.check());
    }
}
