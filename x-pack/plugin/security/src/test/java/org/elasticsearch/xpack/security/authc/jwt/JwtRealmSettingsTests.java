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
import org.elasticsearch.core.TimeValue;
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
    }

    public void testAllowedIssuer() {
        final Setting.AffixSetting<String> setting = JwtRealmSettings.ALLOWED_ISSUER;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final String actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(exception.getMessage(), equalTo("Invalid null or empty value for [" + settingKey + "]."));
        }
        for (final String acceptedValue : new String[] { "http://localhost/iss1", "issuer1", "i" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final String actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(acceptedValue));
        }
    }

    public void testAllowedClockSkew() {
        final Setting.AffixSetting<TimeValue> setting = JwtRealmSettings.ALLOWED_CLOCK_SKEW;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { "", "-2", "10", "1w", "1M", "1y" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final TimeValue actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(
                exception.getMessage(),
                equalTo(
                    "failed to parse setting ["
                        + settingKey
                        + "] with value ["
                        + rejectedValue
                        + "] as a time value: unit is missing or unrecognized"
                )
            );
        }
        for (final String ignoredValue : new String[] { null }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final TimeValue actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(setting.getDefault(settings)));
        }
        for (final String acceptedValue : new String[] { "-1", "0", "0s", "1s", "1m", "1h", "1d" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final TimeValue actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(TimeValue.parseTimeValue(acceptedValue, settingKey)));
        }
    }

    public void testAllowedSignatureAlgorithms() {
        Setting.AffixSetting<List<String>> setting = JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        final String allCsv = "HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384, ES512, PS256, PS384, PS512";
        for (final String rejectedValue : new String[] { "unknown", "HS256,unknown" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(
                exception.getMessage(),
                equalTo("Invalid value [unknown] for [" + settingKey + "]." + " Allowed values are [" + allCsv + "]}].")
            );
        }
        for (final String ignoredValue : new String[] { null, "" }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(setting.getDefault(settings)));
        }
        for (final String acceptedValue : List.of(allCsv)) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS));
        }
    }

    public void testJwtPath() {
        Setting.AffixSetting<String> setting = JwtRealmSettings.JWKSET_PATH;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final String actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(exception.getMessage(), equalTo("Invalid null or empty value for [" + settingKey + "]."));
        }
        for (final String acceptedValue : new String[] { "./config/jwkset.json", "http://localhost/jwkset.json" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final String actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(acceptedValue));
        }
    }

    public void testAllowedAudiences() {
        Setting.AffixSetting<List<String>> setting = JwtRealmSettings.ALLOWED_AUDIENCES;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(exception.getMessage(), equalTo("Invalid null or empty value for [" + settingKey + "]."));
        }
        for (final String acceptedValue : new String[] { "elasticsearch,other" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(List.of("elasticsearch", "other")));
        }
    }

    public void testClaimNames() {
        for (final Setting.AffixSetting<String> setting : List.of(
            JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim(),
            JwtRealmSettings.CLAIMS_GROUPS.getClaim()
        )) {
            final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
            for (final String rejectedValue : new String[] { null, "" }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                    final String actualValue = this.buildConfig(settings).getSetting(setting);
                    fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
                });
                assertThat(exception.getMessage(), equalTo("Invalid null or empty claim name for [" + settingKey + "]."));
            }
            for (final String acceptedValue : new String[] { "sub", "name", "email", "dn" }) {
                final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
                final String actualValue = this.buildConfig(settings).getSetting(setting);
                assertThat(actualValue, equalTo(acceptedValue));
            }
        }
    }

    public void testClaimPatterns() {
        for (final Setting.AffixSetting<String> setting : List.of(
            JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern(),
            JwtRealmSettings.CLAIMS_GROUPS.getPattern()
        )) {
            final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
            for (final String rejectedValue : new String[] { "[" }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                    final String actualValue = this.buildConfig(settings).getSetting(setting);
                    fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
                });
                assertThat(exception.getMessage(), equalTo("Invalid claim value regex pattern for [" + settingKey + "]."));
            }
            for (final String acceptedValue : new String[] { "^([^@]+)@example\\.com$", "^Group-(.+)$" }) {
                final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
                final String actualValue = this.buildConfig(settings).getSetting(setting);
                assertThat(actualValue, equalTo(acceptedValue));
            }
        }
    }

    public void testPopulateUserMetadata() {
        Setting.AffixSetting<Boolean> setting = JwtRealmSettings.POPULATE_USER_METADATA;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { "", "unknown", "t", "f", "TRUE", "FALSE", "True", "False" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final Boolean actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(
                exception.getMessage(),
                equalTo("Failed to parse value [" + rejectedValue + "] as only [true] or [false] are allowed.")
            );
        }
        for (final String ignoredValue : new String[] { null }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final Boolean actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(setting.getDefault(settings)));
        }
        for (final String acceptedValue : new String[] { "true", "false" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final Boolean actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(Boolean.valueOf(acceptedValue)));
        }
    }

    public void testClientAuthenticationType() {
        Setting.AffixSetting<String> setting = JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { "", "unknown" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final String actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(
                exception.getMessage(),
                equalTo("Invalid value [" + rejectedValue + "] for [" + settingKey + "]." + " Allowed values are [sharedsecret, none]}].")
            );
        }
        for (final String ignoredValue : new String[] { null }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final String actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(setting.getDefault(settings)));
        }
        for (final String acceptedValue : new String[] { "sharedsecret", "none" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final String actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(acceptedValue));
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
        final Settings settings = Settings.builder()
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
            .setSecureSettings(this.getMockWorkingSecretSettings())
            .build();
        return this.buildConfig(settings);
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
