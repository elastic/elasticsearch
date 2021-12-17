/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.junit.Before;

import java.util.Set;

public class JwtRealmSettingsTests extends ESTestCase {

    private static final String REALM_NAME = "jwt1-realm";
    private ThreadContext threadContext;

    @Before
    public void setupEnv() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        threadContext = new ThreadContext(globalSettings);
    }

    private static final void validateSettings(final RealmConfig realmConfig, final Set<Setting.AffixSetting<?>> settings) {
        for (final Setting.AffixSetting<?> setting : settings) {
            realmConfig.getSetting(setting);
        }
    }

    public void testAllSettings() {
        final Settings.Builder settingsBuilder = Settings.builder()
            // Issuer settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_ISSUER), "https://op.example.com")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS), "RS512")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_CLOCK_SKEW), randomIntBetween(1, 5) + "m")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.JWKSET_PATH), "https://op.example.com/jwks.json")
            // Audience settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_AUDIENCES), "rp_client1")
            // End-user settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.PRINCIPAL_CLAIM.getPattern()), "^([^@]+)@example\\.com$")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.GROUPS_CLAIM.getClaim()), "group")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.GROUPS_CLAIM.getPattern()), "^(.*)$")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.POPULATE_USER_METADATA), randomBoolean())
            // Client settings for incoming connections
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE), "ssl")
            // Delegated authorization settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply("jwt")), "native1,file1")
            // Cache settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CACHE_TTL), randomIntBetween(10, 120) + "m")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CACHE_MAX_USERS), randomIntBetween(1000, 10000))
            // HTTP settings for outgoing connections
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_CONNECT_TIMEOUT), randomIntBetween(1, 5) + "s")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT), randomIntBetween(5, 10) + "s")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_SOCKET_TIMEOUT), randomIntBetween(5, 10) + "s")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_MAX_CONNECTIONS), randomIntBetween(5, 20))
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS), randomIntBetween(5, 20))
            // TLS settings for outgoing connections
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_TYPE.realm("jwt")), "PKCS12")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_PATH.realm("jwt")), "ts2.p12")
            // .put(RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.LEGACY_TRUSTSTORE_PASSWORD.realm("jwt")), "abc")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_ALGORITHM.realm("jwt")), "PKIX")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_TYPE.realm("jwt")), "PKCS12")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.CERT_AUTH_PATH.realm("jwt")), "ca2.pem")
            // Secure settings
            .setSecureSettings(this.getSecureSettings());
        final RealmConfig realmConfig = this.buildConfig(settingsBuilder.build());

        JwtRealmSettingsTests.validateSettings(realmConfig, JwtRealmSettings.getSettings());

        // ClaimParser.forSetting(logger, JwtRealmSettings.PRINCIPAL_CLAIM, realmConfig, randomBoolean());
        // ClaimParser.forSetting(logger, JwtRealmSettings.GROUPS_CLAIM, realmConfig, randomBoolean());
    }

    public void testNullIssuer() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_ISSUER), (String) null);
        final RealmConfig realmConfig = this.buildConfig(settingsBuilder.build());
        JwtRealmSettingsTests.validateSettings(realmConfig, JwtRealmSettings.getSettings());
    }

    private MockSecureSettings getSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_PASSWORD.realm("jwt")),
            "abc"
        );
        return secureSettings;
    }

    private RealmConfig buildConfig(Settings realmSettings) {
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("jwt", REALM_NAME);
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(realmSettings)
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(realmIdentifier, settings, env, threadContext);
    }
}
