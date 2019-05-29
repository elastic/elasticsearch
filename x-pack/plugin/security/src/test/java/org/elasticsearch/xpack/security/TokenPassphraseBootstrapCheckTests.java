/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.authc.TokenService;

import static org.elasticsearch.xpack.security.TokenPassphraseBootstrapCheck.MINIMUM_PASSPHRASE_LENGTH;

public class TokenPassphraseBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testTokenPassphraseCheck() throws Exception {
        assertFalse(new TokenPassphraseBootstrapCheck(Settings.EMPTY).check(createTestContext(Settings.EMPTY, null)).isFailure());
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "bar"); // leniency in setSecureSettings... if its empty it's skipped
        Settings settings = Settings.builder()
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).setSecureSettings(secureSettings).build();
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check(createTestContext(settings, null)).isFailure());

        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(MINIMUM_PASSPHRASE_LENGTH, 30));
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check(createTestContext(settings, null)).isFailure());

        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(1, MINIMUM_PASSPHRASE_LENGTH - 1));
        assertTrue(new TokenPassphraseBootstrapCheck(settings).check(createTestContext(settings, null)).isFailure());
        assertWarnings("[xpack.security.authc.token.passphrase] setting was deprecated in Elasticsearch and will be removed in a future" +
                " release! See the breaking changes documentation for the next major version.");
    }

    public void testTokenPassphraseCheckServiceDisabled() throws Exception {
        Settings settings = Settings.builder().put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), false)
                .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check(createTestContext(settings, null)).isFailure());
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "bar"); // leniency in setSecureSettings... if its empty it's skipped
        settings = Settings.builder().put(settings).setSecureSettings(secureSettings).build();
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check(createTestContext(settings, null)).isFailure());

        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(1, 30));
        assertFalse(new TokenPassphraseBootstrapCheck(settings).check(createTestContext(settings, null)).isFailure());
        assertWarnings("[xpack.security.authc.token.passphrase] setting was deprecated in Elasticsearch and will be removed in a future" +
                " release! See the breaking changes documentation for the next major version.");
    }

    public void testTokenPassphraseCheckAfterSecureSettingsClosed() throws Exception {
        Settings settings = Settings.builder().put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "bar"); // leniency in setSecureSettings... if its empty it's skipped
        settings = Settings.builder().put(settings).setSecureSettings(secureSettings).build();
        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(1, MINIMUM_PASSPHRASE_LENGTH - 1));
        final TokenPassphraseBootstrapCheck check = new TokenPassphraseBootstrapCheck(settings);
        secureSettings.close();
        assertTrue(check.check(createTestContext(settings, null)).isFailure());
        assertWarnings("[xpack.security.authc.token.passphrase] setting was deprecated in Elasticsearch and will be removed in a future" +
                " release! See the breaking changes documentation for the next major version.");
    }
}
