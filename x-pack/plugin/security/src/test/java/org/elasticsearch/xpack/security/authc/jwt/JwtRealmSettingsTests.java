/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.junit.Before;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;

public class JwtRealmSettingsTests extends ESTestCase {

    private static final String REALM_NAME = "jwt1-realm";
    private ThreadContext threadContext;

    @Before
    public void setupEnv() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        threadContext = new ThreadContext(globalSettings);
    }

    public void testAllSettings() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.POPULATE_USER_METADATA), randomBoolean())
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.GROUPS_CLAIM.getClaim()), "group1")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_CONNECT_TIMEOUT), "5s")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT), "5s")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_SOCKET_TIMEOUT), "5s")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_MAX_CONNECTIONS), "5")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS), "5")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_PROXY_HOST), "host")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_PROXY_PORT), "8080")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_PROXY_SCHEME), "http")
            .put(getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_CLOCK_SKEW), "10s");
        settingsBuilder.setSecureSettings(this.getSecureSettings());
        final RealmConfig realmConfig = buildConfig(settingsBuilder.build());
    }

    private MockSecureSettings getSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        return secureSettings;
    }

    private RealmConfig buildConfig(Settings realmSettings) {
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("jwt", REALM_NAME);
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(realmSettings)
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(realmIdentifier, settings, env, threadContext);
    }
}
