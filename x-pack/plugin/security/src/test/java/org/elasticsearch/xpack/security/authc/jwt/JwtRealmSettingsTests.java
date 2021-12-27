/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
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

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * JWT realm settings unit tests. These are low-level tests against ES settings parsers.
 *
 * Test inputs are direct against ES settings parsers. Tests do not assume inputs have
 * been formatted by wrapping code such as YAML parsers or KeyStoreWrapper.
 *
 * For example, YAML parsers accept many boolean-like strings and reduce them down to
 * "true" or "false", but the corresponding ES boolean parser only accepts "true" or "false".
 * @see org.elasticsearch.core.Booleans#parseBoolean(String)
 */
public class JwtRealmSettingsTests extends ESTestCase {

    private static final String REALM_NAME = "jwt1";
    private String pathHome;
    private ThreadContext threadContext;

    @Before
    public void setupEnv() {
        this.pathHome = createTempDir().toString();
        final Settings globalSettings = Settings.builder().put("path.home", this.pathHome).build();
        this.threadContext = new ThreadContext(globalSettings);
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
            assertThat(exception.getMessage(), equalTo("Invalid empty value for [" + settingKey + "]."));
        }
        for (final String acceptedValue : new String[] { "http://localhost/iss1", "issuer1", "i" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final String actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(acceptedValue));
        }
    }

    public void testAllowedSignatureAlgorithms() {
        final Setting.AffixSetting<List<String>> setting = JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { "unknown", "HS256,unknown" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(
                exception.getMessage(),
                equalTo(
                    "Invalid value [unknown] for ["
                        + settingKey
                        + "]."
                        + " Allowed values are "
                        + JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS
                        + "."
                )
            );
        }
        for (final String ignoredValue : new String[] { null, "" }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(setting.getDefault(settings)));
        }
        final String allAcceptedValues = JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.toString().replaceAll("[\\[\\] ]", "");
        for (final String acceptedValue : List.of("HS256", "HS512,RS512", allAcceptedValues)) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(Arrays.asList(acceptedValue.split(",", -1))));
        }
    }

    public void testJwtPath() {
        final Setting.AffixSetting<String> setting = JwtRealmSettings.JWKSET_PATH;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final String actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(exception.getMessage(), equalTo("Invalid empty value for [" + settingKey + "]."));
        }
        for (final String acceptedValue : new String[] { "./config/jwkset.json", "http://localhost/jwkset.json" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final String actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(acceptedValue));
        }
    }

    public void testAllowedAudiences() {
        final Setting.AffixSetting<List<String>> setting = JwtRealmSettings.ALLOWED_AUDIENCES;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(exception.getMessage(), equalTo("Invalid empty list for [" + settingKey + "]."));
        }
        for (final String acceptedValue : new String[] { "elasticsearch", "elasticsearch,other" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(Arrays.asList(acceptedValue.split(",", -1))));
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
        final Setting.AffixSetting<Boolean> setting = JwtRealmSettings.POPULATE_USER_METADATA;
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
        final Setting.AffixSetting<String> setting = JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE;
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String rejectedValue : new String[] { "", "unknown" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final String actualValue = this.buildConfig(settings).getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(
                exception.getMessage(),
                equalTo(
                    "Invalid value ["
                        + rejectedValue
                        + "] for ["
                        + settingKey
                        + "]."
                        + " Allowed values are "
                        + JwtRealmSettings.SUPPORTED_CLIENT_AUTHENTICATION_TYPE
                        + "."
                )
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

    public void testAuthorizationRealms() {
        final Setting.AffixSetting<List<String>> setting = DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE);
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        for (final String ignoredValue : new String[] { null, "" }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(List.of()));
        }
        for (final String acceptedValue : new String[] { "a", "1", "native1,file1,ldap1,ad1" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final List<String> actualValue = this.buildConfig(settings).getSetting(setting);
            assertThat(actualValue, equalTo(Arrays.asList(acceptedValue.split(",", -1))));
        }
    }

    public void testSecureStrings() {
        for (final Setting.AffixSetting<SecureString> setting : List.of(
            JwtRealmSettings.ISSUER_HMAC_SECRET_KEY,
            JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET
        )) {
            final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
            for (final String rejectedValue : new String[] { null }) {
                final Exception exception = expectThrows(NullPointerException.class, () -> {
                    final MockSecureSettings secureSettings = new MockSecureSettings();
                    secureSettings.setString(settingKey, rejectedValue);
                    final SecureString actualValue = secureSettings.getString(settingKey);
                    fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
                });
                assertThat(
                    exception.getMessage(),
                    equalTo("Cannot invoke \"String.getBytes(java.nio.charset.Charset)\" because \"value\" is null")
                );
            }
            for (final String acceptedValue : new String[] { "", "abc123", "a", "1" }) {
                final MockSecureSettings secureSettings = new MockSecureSettings();
                secureSettings.setString(settingKey, acceptedValue);
                final SecureString actualValue = secureSettings.getString(settingKey);
                assertThat(actualValue, equalTo(acceptedValue));
            }
        }
    }

    public void testTimeSettingsWithDefault() {
        for (final Setting.AffixSetting<TimeValue> setting : List.of(
            JwtRealmSettings.ALLOWED_CLOCK_SKEW,
            JwtRealmSettings.CACHE_TTL,
            JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT,
            JwtRealmSettings.HTTP_SOCKET_TIMEOUT
        )) {
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
    }

    public void testIntegerSettingsWithDefault() {
        for (final Setting.AffixSetting<Integer> setting : List.of(
            JwtRealmSettings.CACHE_MAX_USERS,
            JwtRealmSettings.HTTP_MAX_CONNECTIONS,
            JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS
        )) {
            final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
            // If Integer parsing fails, " must be >= 0" is not appended to exception message.
            for (final String rejectedValue : new String[] { "", "100_000", "NaN" }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                    final Integer actualValue = this.buildConfig(settings).getSetting(setting);
                    fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
                });
                assertThat(
                    exception.getMessage(),
                    equalTo("Failed to parse value [" + rejectedValue + "] for setting [" + settingKey + "]")
                );
            }
            // If Integer parsing succeeds, " must be >= 0" is appended to exception message.
            for (final String rejectedValue : new String[] { "-1", Integer.toString(Integer.MIN_VALUE) }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                    final Integer actualValue = this.buildConfig(settings).getSetting(setting);
                    fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
                });
                assertThat(
                    exception.getMessage(),
                    equalTo("Failed to parse value [" + rejectedValue + "] for setting [" + settingKey + "] must be >= 0")
                );
            }
            for (final String ignoredValue : new String[] { null }) {
                final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
                final Integer actualValue = this.buildConfig(settings).getSetting(setting);
                assertThat(actualValue, equalTo(setting.getDefault(settings)));
            }
            for (final String acceptedValue : new String[] { "0", "1", "100000", Integer.toString(Integer.MAX_VALUE) }) {
                final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
                final Integer actualValue = this.buildConfig(settings).getSetting(setting);
                assertThat(actualValue, equalTo(Integer.valueOf(acceptedValue)));
            }
        }
    }

    public void testAllSettings() {
        final Settings settings = this.getAllSettingsWorking();
        final RealmConfig config = this.buildConfig(settings);
        for (final Setting.AffixSetting<?> setting : JwtRealmSettings.getNonSecureSettings()) {
            config.getSetting(setting);
        }
        for (final Setting.AffixSetting<SecureString> setting : JwtRealmSettings.getSecureSettings()) {
            config.getConcreteSetting(setting);
        }
    }

    // HELPER METHODS

    private Settings getAllSettingsWorking() {
        return Settings.builder()
            // Issuer settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_ISSUER), "https://op.example.com")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS), "RS512")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_CLOCK_SKEW), randomIntBetween(1, 5) + "m")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY), "base64mime")
            // Audience settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.ALLOWED_AUDIENCES), "rp_client1")
            // End-user settings
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), "sub")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern()), "^([^@]+)@example\\.com$")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLAIMS_GROUPS.getClaim()), "group")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLAIMS_GROUPS.getPattern()), "^(.*)$")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.POPULATE_USER_METADATA), randomBoolean())
            // Client settings for incoming connections
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE), "sharedsecret")
            .put(RealmSettings.getFullSettingKey(REALM_NAME, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET), "base64url")
            // Delegated authorization settings
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE)),
                "native1,file1"
            )
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
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_TYPE.realm(JwtRealmSettings.TYPE)),
                "PKCS12"
            )
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_PATH.realm(JwtRealmSettings.TYPE)),
                "ts2.p12"
            )
            // .put(RealmSettings.getFullSettingKey(REALM_NAME,
            // SSLConfigurationSettings.LEGACY_TRUSTSTORE_PASSWORD.realm(JwtRealmSettings.TYPE)), "abc")
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_ALGORITHM.realm(JwtRealmSettings.TYPE)),
                "PKIX"
            )
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.TRUSTSTORE_TYPE.realm(JwtRealmSettings.TYPE)),
                "PKCS12"
            )
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, SSLConfigurationSettings.CERT_AUTH_PATH.realm(JwtRealmSettings.TYPE)),
                "ca2.pem"
            )
            // Secure settings
            .setSecureSettings(this.getMockWorkingSecretSettings())
            .build();
    }

    private MockSecureSettings getMockWorkingSecretSettings() {
        final Setting.AffixSetting<SecureString> setting = SSLConfigurationSettings.TRUSTSTORE_PASSWORD.realm(JwtRealmSettings.TYPE);
        final String settingKey = RealmSettings.getFullSettingKey(REALM_NAME, setting);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(settingKey, "abc");
        return secureSettings;
    }

    private RealmConfig buildConfig(final Settings realmSettings) {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(JwtRealmSettings.TYPE, REALM_NAME);
        final Settings settings = Settings.builder()
            .put("path.home", this.pathHome)
            .put(realmSettings)
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(realmIdentifier, settings, env, this.threadContext);
    }
}
