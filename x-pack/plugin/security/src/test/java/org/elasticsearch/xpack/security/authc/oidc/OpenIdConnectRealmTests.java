/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;


import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.hamcrest.Matchers;
import org.junit.Before;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;

public class OpenIdConnectRealmTests extends ESTestCase {

    private final static String REALM_NAME = "oidc1-realm";
    private static final String REALM_SETTINGS_PREFIX = "xpack.security.authc.realms.oidc." + REALM_NAME;
    private Settings globalSettings;
    private Environment env;
    private ThreadContext threadContext;

    @Before
    public void setupEnv() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);
    }

    public void testIncorrectResponseTypeThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "hybrid");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(), Matchers.containsString("Invalid response type provided"));
    }

    public void testMissingAuthorizationEndpointThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT)));
    }

    public void testMissingIssuerThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER)));
    }

    public void testMissingNameTypeThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME)));
    }

    public void testMissingRedirectUriThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI)));
    }

    public void testMissingClientIdThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID)));
    }

    private RealmConfig buildConfig(Settings realmSettings) {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(realmSettings).build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(new RealmConfig.RealmIdentifier("oidc", REALM_NAME), settings, env, threadContext);
    }
}
