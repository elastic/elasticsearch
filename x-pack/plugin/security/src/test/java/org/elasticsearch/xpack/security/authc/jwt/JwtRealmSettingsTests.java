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

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class JwtRealmSettingsTests extends ESTestCase {

    private static final String REALM_NAME = "jwt1-realm";
    private ThreadContext threadContext;

    @Before
    public void setupEnv() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        threadContext = new ThreadContext(globalSettings);
    }

    public void testAllSettings() {
        final RealmConfig realmConfig = getMockWorkingSettings();
        JwtRealmSettingsTests.validateSettings(realmConfig, JwtRealmSettings.getSettings());
        // ClaimParser.forSetting(logger, JwtRealmSettings.PRINCIPAL_CLAIM, realmConfig, randomBoolean());
        // ClaimParser.forSetting(logger, JwtRealmSettings.GROUPS_CLAIM, realmConfig, randomBoolean());
    }

    public void testAllowedIssuer() {
        final String fullSettingKey = RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_ISSUER);
        for (final String unexpectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, unexpectedValue);
                final RealmConfig config = this.buildConfig(settingsBuilder.build());
                final String actualValue = config.getSetting(JwtRealmSettings.ALLOWED_ISSUER);
                fail("No exception. Expected one for " + fullSettingKey + "=" + unexpectedValue);
            });
            assertThat(exception.getMessage(), equalTo("Invalid null or empty value for [" + fullSettingKey + "]."));
        }
        for (final String expectedValue : new String[] { "http://localhost/iss1", "issuer1" }) {
            final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, expectedValue);
            final RealmConfig config = this.buildConfig(settingsBuilder.build());
            final String actualValue = config.getSetting(JwtRealmSettings.ALLOWED_ISSUER);
            assertThat(actualValue, equalTo(expectedValue));
        }
    }

    public void testAllowedSignatureAlgorithms() {
        final String fullSettingKey = RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
        final String allCsv = "HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384, ES512, PS256, PS384, PS512";
        for (final String unexpectedValue : new String[] { "unknown", "HS256,unknown" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, unexpectedValue);
                final RealmConfig config = this.buildConfig(settingsBuilder.build());
                final List<String> actualValue = config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
                fail("No exception. Expected one for " + fullSettingKey + "=" + unexpectedValue);
            });
            assertThat(
                exception.getMessage(),
                equalTo("Invalid value [unknown] for [" + fullSettingKey + "]." + " Allowed values are [" + allCsv + "]}].")
            );
        }
        for (final String expectedValue : List.of(allCsv)) {
            final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, expectedValue);
            final RealmConfig config = this.buildConfig(settingsBuilder.build());
            final List<String> actualValue = config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
            assertThat(actualValue, equalTo(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS));
        }
    }

    public void testJwtPath() {
        final String fullSettingKey = RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.JWKSET_PATH);
        for (final String unexpectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, unexpectedValue);
                final RealmConfig config = this.buildConfig(settingsBuilder.build());
                final String actualValue = config.getSetting(JwtRealmSettings.JWKSET_PATH);
                fail("No exception. Expected one for " + fullSettingKey + "=" + unexpectedValue);
            });
            assertThat(exception.getMessage(), equalTo("Invalid null or empty value for [" + fullSettingKey + "]."));
        }
        for (final String expectedValue : new String[] { "./config/jwkset.json", "http://localhost/jwkset.json" }) {
            final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, expectedValue);
            final RealmConfig config = this.buildConfig(settingsBuilder.build());
            final String actualValue = config.getSetting(JwtRealmSettings.JWKSET_PATH);
            assertThat(actualValue, equalTo(expectedValue));
        }
    }

    public void testAllowedAudiences() {
        final String fullSettingKey = RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_AUDIENCES);
        for (final String unexpectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, unexpectedValue);
                final RealmConfig config = this.buildConfig(settingsBuilder.build());
                final List<String> actualValue = config.getSetting(JwtRealmSettings.ALLOWED_AUDIENCES);
                fail("No exception. Expected one for " + fullSettingKey + "=" + unexpectedValue);
            });
            assertThat(exception.getMessage(), equalTo("Invalid null or empty value for [" + fullSettingKey + "]."));
        }
        for (final String expectedValue : new String[] { "elasticsearch,otherapp" }) {
            final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, expectedValue);
            final RealmConfig config = this.buildConfig(settingsBuilder.build());
            final List<String> setting = config.getSetting(JwtRealmSettings.ALLOWED_AUDIENCES);
            assertThat(setting, equalTo(List.of("elasticsearch", "otherapp")));
        }
    }

    public void testClaimNames() {
        for (final Setting.AffixSetting<String> claimName : List.of(
            JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim(),
            JwtRealmSettings.CLAIMS_GROUPS.getClaim()
        )) {
            final String fullSettingKey = RealmSettings.getFullSettingKey(REALM_NAME, claimName);
            for (final String unexpectedValue : new String[] { null, "" }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, unexpectedValue);
                    final RealmConfig config = this.buildConfig(settingsBuilder.build());
                    final String actualValue = config.getSetting(claimName);
                    fail("No exception. Expected one for " + fullSettingKey + "=" + unexpectedValue);
                });
                assertThat(exception.getMessage(), equalTo("Invalid null or empty claim name for [" + fullSettingKey + "]."));
            }
            for (final String expectedValue : new String[] { "sub", "name", "email", "dn" }) {
                final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, expectedValue);
                final RealmConfig config = this.buildConfig(settingsBuilder.build());
                final String actualValue = config.getSetting(claimName);
                assertThat(actualValue, equalTo(expectedValue));
            }
        }
    }

    public void testClaimPatterns() {
        for (final Setting.AffixSetting<String> claimPattern : List.of(
            JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern(),
            JwtRealmSettings.CLAIMS_GROUPS.getPattern()
        )) {
            final String fullSettingKey = RealmSettings.getFullSettingKey(REALM_NAME, claimPattern);
            for (final String unexpectedValue : new String[] { "[" }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, unexpectedValue);
                    final RealmConfig config = this.buildConfig(settingsBuilder.build());
                    final String actualValue = config.getSetting(claimPattern);
                    fail("No exception. Expected one for " + fullSettingKey + "=" + unexpectedValue);
                });
                assertThat(exception.getMessage(), equalTo("Invalid claim value regex pattern for [" + fullSettingKey + "]."));
            }
            for (final String expectedValue : new String[] { "^([^@]+)@example\\.com$", "^Group-(.+)$" }) {
                final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, expectedValue);
                final RealmConfig config = this.buildConfig(settingsBuilder.build());
                final String actualValue = config.getSetting(claimPattern);
                assertThat(actualValue, equalTo(expectedValue));
            }
        }
    }

    public void testPopulateUserMetadata() {
        final String fullSettingKey = RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.POPULATE_USER_METADATA);
        for (final String unexpectedValue : new String[] { "unknown", "t", "f", "TRUE", "FALSE", "True", "False" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings.Builder settingsBuilder = Settings.builder()
                    .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.POPULATE_USER_METADATA), unexpectedValue);
                final RealmConfig config = this.buildConfig(settingsBuilder.build());
                final Boolean actualValue = config.getSetting(JwtRealmSettings.POPULATE_USER_METADATA);
                fail("No exception. Expected one for " + fullSettingKey + "=" + unexpectedValue);
            });
            assertThat(
                exception.getMessage(),
                equalTo("Failed to parse value [" + unexpectedValue + "] as only [true] or [false] are allowed.")
            );
        }
        for (final String expectedValue : new String[] { "true", "false" }) {
            final Settings.Builder settingsBuilder = Settings.builder().put(fullSettingKey, expectedValue);
            final RealmConfig config = this.buildConfig(settingsBuilder.build());
            final Boolean actualValue = config.getSetting(JwtRealmSettings.POPULATE_USER_METADATA);
            assertThat(actualValue, equalTo(Boolean.valueOf(expectedValue)));
        }
    }

    public void testClientAuthenticationType() {
        final String fullSettingKey = RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE);
        for (final String unexpectedValue : new String[] { "unknown" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings.Builder settingsBuilder = Settings.builder()
                    .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE), unexpectedValue);
                final RealmConfig config = this.buildConfig(settingsBuilder.build());
                final String actualValue = config.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE);
                fail("No exception. Expected one for " + fullSettingKey + "=" + unexpectedValue);
            });
            assertThat(
                exception.getMessage(),
                equalTo(
                    "Invalid value [" + unexpectedValue + "] for [" + fullSettingKey + "]." + " Allowed values are [sharedsecret, none]}]."
                )
            );
        }
        for (final String expectedValue : new String[] { "sharedsecret", "none" }) {
            final Settings.Builder settingsBuilder = Settings.builder()
                .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE), expectedValue);
            final RealmConfig config = this.buildConfig(settingsBuilder.build());
            final String actualValue = config.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE);
            assertThat(actualValue, equalTo(expectedValue));
        }
    }

    // HELPER METHODS

    private static void validateSettings(final RealmConfig realmConfig, final Set<Setting.AffixSetting<?>> settings) {
        for (final Setting.AffixSetting<?> setting : settings) {
            validateSetting(realmConfig, setting);
        }
    }

    private static void validateSetting(final RealmConfig realmConfig, final Setting.AffixSetting<?> setting) {
        realmConfig.getSetting(setting);
    }

    private RealmConfig getMockWorkingSettings() {
        final Settings.Builder settingsBuilder = Settings.builder()
            // Issuer settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_ISSUER), "https://op.example.com")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS), "RS512")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_CLOCK_SKEW), randomIntBetween(1, 5) + "m")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.JWKSET_PATH), "https://op.example.com/jwks.json")
            // Audience settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_AUDIENCES), "rp_client1")
            // End-user settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), "sub")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern()), "^([^@]+)@example\\.com$")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLAIMS_GROUPS.getClaim()), "group")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLAIMS_GROUPS.getPattern()), "^(.*)$")
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
            .setSecureSettings(this.getMockWorkingSecretSettings());
        return this.buildConfig(settingsBuilder.build());
    }

    private MockSecureSettings getMockWorkingSecretSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_PASSWORD.realm("jwt")),
            "abc"
        );
        return secureSettings;
    }

    private RealmConfig buildConfig(Settings realmSettings) {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("jwt", REALM_NAME);
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(realmSettings)
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(realmIdentifier, settings, env, threadContext);
    }
}
