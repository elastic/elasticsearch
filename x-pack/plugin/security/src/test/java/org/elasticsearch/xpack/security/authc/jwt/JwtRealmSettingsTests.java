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
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings.ClientAuthenticationType;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.Strings.capitalize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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
public class JwtRealmSettingsTests extends JwtTestCase {

    public void testAllSettings() throws Exception {
        final String realmName = "jwt" + randomIntBetween(1, 9);
        final Settings settings = generateRandomRealmSettings(realmName).build();
        final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
        for (final Setting.AffixSetting<?> setting : JwtRealmSettings.getSettings()) {
            realmConfig.getConcreteSetting(setting);
        }
    }

    public void testAllowedIssuer() {
        final String realmName = "jwt" + randomIntBetween(1, 9);
        final Setting.AffixSetting<String> setting = JwtRealmSettings.ALLOWED_ISSUER;
        final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
        for (final String rejectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final String actualValue = realmConfig.getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(exception.getMessage(), equalTo("Invalid empty value for [" + settingKey + "]."));
        }
        for (final String acceptedValue : new String[] { "http://localhost/iss1", "issuer1", "i" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final String actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(acceptedValue));
        }
    }

    public void testAllowedSignatureAlgorithms() {
        final String realmName = "jwt" + randomIntBetween(1, 9);
        final Setting.AffixSetting<List<String>> setting = JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS;
        final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
        for (final String rejectedValue : new String[] { "unknown", "HS256,unknown" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final List<String> actualValue = realmConfig.getSetting(setting);
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
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final List<String> actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(setting.getDefault(settings)));
        }
        final String allAcceptedValues = JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.toString().replaceAll("[\\[\\] ]", "");
        for (final String acceptedValue : List.of("HS256", "HS512,RS512", allAcceptedValues)) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final List<String> actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(Arrays.asList(acceptedValue.split(",", -1))));
        }
    }

    public void testJwtPath() {
        final String realmName = "jwt" + randomIntBetween(1, 9);
        final Setting.AffixSetting<String> setting = JwtRealmSettings.PKC_JWKSET_PATH;
        final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
        for (final String ignoredValue : new String[] { null, "" }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final String actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(setting.getDefault(settings)));
        }
        for (final String acceptedValue : new String[] { "./config/jwkset.json", "http://localhost/jwkset.json" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final String actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(acceptedValue));
        }
    }

    public void testAllowedAudiences() {
        final String realmName = "jwt" + randomIntBetween(1, 9);
        final Setting.AffixSetting<List<String>> setting = JwtRealmSettings.ALLOWED_AUDIENCES;
        final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
        for (final String rejectedValue : new String[] { null, "" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final List<String> actualValue = realmConfig.getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(exception.getMessage(), equalTo("Invalid empty list for [" + settingKey + "]."));
        }
        for (final String acceptedValue : new String[] { "elasticsearch", "elasticsearch,other" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final List<String> actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(Arrays.asList(acceptedValue.split(",", -1))));
        }
    }

    public void testClaimNames() {
        for (final Setting.AffixSetting<String> setting : List.of(
            JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim(),
            JwtRealmSettings.CLAIMS_GROUPS.getClaim(),
            JwtRealmSettings.CLAIMS_DN.getClaim(),
            JwtRealmSettings.CLAIMS_MAIL.getClaim(),
            JwtRealmSettings.CLAIMS_NAME.getClaim()
        )) {
            final String realmName = "jwt" + randomIntBetween(1, 9);
            final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
            for (final String rejectedValue : new String[] { null, "" }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                    final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                    final String actualValue = realmConfig.getSetting(setting);
                    fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
                });
                assertThat(exception.getMessage(), equalTo("Invalid null or empty claim name for [" + settingKey + "]."));
            }
            for (final String acceptedValue : new String[] { "sub", "name", "email", "dn" }) {
                final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final String actualValue = realmConfig.getSetting(setting);
                assertThat(actualValue, equalTo(acceptedValue));
            }
        }
    }

    public void testClaimPatterns() {
        for (final Setting.AffixSetting<String> setting : List.of(
            JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern(),
            JwtRealmSettings.CLAIMS_GROUPS.getPattern(),
            JwtRealmSettings.CLAIMS_DN.getPattern(),
            JwtRealmSettings.CLAIMS_MAIL.getPattern(),
            JwtRealmSettings.CLAIMS_NAME.getPattern()
        )) {
            final String realmName = "jwt" + randomIntBetween(1, 9);
            final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
            for (final String rejectedValue : new String[] { "[" }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                    final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                    final String actualValue = realmConfig.getSetting(setting);
                    fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
                });
                assertThat(exception.getMessage(), equalTo("Invalid claim value regex pattern for [" + settingKey + "]."));
            }
            for (final String acceptedValue : new String[] { "^([^@]+)@example\\.com$", "^Group-(.+)$" }) {
                final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final String actualValue = realmConfig.getSetting(setting);
                assertThat(actualValue, equalTo(acceptedValue));
            }
        }
    }

    public void testPopulateUserMetadata() {
        final String realmName = "jwt" + randomIntBetween(1, 9);
        final Setting.AffixSetting<Boolean> setting = JwtRealmSettings.POPULATE_USER_METADATA;
        final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
        for (final String rejectedValue : new String[] { "", "unknown", "t", "f", "TRUE", "FALSE", "True", "False" }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final Boolean actualValue = realmConfig.getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(
                exception.getMessage(),
                equalTo("Failed to parse value [" + rejectedValue + "] as only [true] or [false] are allowed.")
            );
        }
        for (final String ignoredValue : new String[] { null }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final Boolean actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(setting.getDefault(settings)));
        }
        for (final String acceptedValue : new String[] { "true", "false" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final Boolean actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(Boolean.valueOf(acceptedValue)));
        }
    }

    public void testClientAuthenticationType() {
        final String realmName = "jwt" + randomIntBetween(1, 9);
        final Setting.AffixSetting<ClientAuthenticationType> setting = JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE;
        final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
        for (final String rejectedValue : new String[] { "unknown", "", randomAlphaOfLengthBetween(1, 3) }) {
            final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final ClientAuthenticationType actualValue = realmConfig.getSetting(setting);
                fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
            });
            assertThat(
                exception.getMessage(),
                equalTo("Invalid value [" + rejectedValue + "] for [" + settingKey + "]," + " allowed values are " + "[none,shared_secret]")
            );
        }
        for (final String ignoredValue : new String[] { null }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final ClientAuthenticationType actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(setting.getDefault(settings)));
        }
        for (final String acceptedValue : new String[] { "shared_secret", "none" }) {
            for (String inputValue : new String[] { acceptedValue, acceptedValue.toUpperCase(Locale.ROOT), capitalize(acceptedValue) }) {
                final Settings settings = Settings.builder().put(settingKey, inputValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final ClientAuthenticationType actualValue = realmConfig.getSetting(setting);
                assertThat(actualValue.value(), equalTo(acceptedValue));
            }
        }
    }

    public void testAuthenticationRealms() {
        final String realmName = "jwt" + randomIntBetween(1, 9);
        final Setting.AffixSetting<List<String>> setting = DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE);
        final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
        for (final String ignoredValue : new String[] { null, "" }) {
            final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final List<String> actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(List.of()));
        }
        for (final String acceptedValue : new String[] { "a", "1", "native1,file1,ldap1,ad1" }) {
            final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
            final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
            final List<String> actualValue = realmConfig.getSetting(setting);
            assertThat(actualValue, equalTo(Arrays.asList(acceptedValue.split(",", -1))));
        }
    }

    public void testSecureStrings() {
        for (final Setting.AffixSetting<SecureString> setting : List.of(
            JwtRealmSettings.HMAC_JWKSET,
            JwtRealmSettings.HMAC_KEY,
            JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET
        )) {
            final String realmName = "jwt" + randomIntBetween(1, 9);
            final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
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
            JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT,
            JwtRealmSettings.HTTP_SOCKET_TIMEOUT
        )) {
            final String realmName = "jwt" + randomIntBetween(1, 9);
            final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
            for (final String rejectedValue : new String[] { "", "-2", "10", "1w", "1M", "1y" }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                    final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                    final TimeValue actualValue = realmConfig.getSetting(setting);
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
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final TimeValue actualValue = realmConfig.getSetting(setting);
                assertThat(actualValue, equalTo(setting.getDefault(settings)));
            }
            for (final String acceptedValue : new String[] { "-1", "0", "0s", "1s", "1m", "1h", "1d" }) {
                final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final TimeValue actualValue = realmConfig.getSetting(setting);
                assertThat(actualValue, equalTo(TimeValue.parseTimeValue(acceptedValue, settingKey)));
            }
        }
    }

    public void testIntegerSettingsWithDefault() {
        for (final Setting.AffixSetting<Integer> setting : List.of(
            JwtRealmSettings.HTTP_MAX_CONNECTIONS,
            JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS
        )) {
            final String realmName = "jwt" + randomIntBetween(1, 9);
            final String settingKey = RealmSettings.getFullSettingKey(realmName, setting);
            // If Integer parsing fails, " must be >= 0" is not appended to exception message.
            for (final String rejectedValue : new String[] { "", "100_000", "NaN" }) {
                final Exception exception = expectThrows(IllegalArgumentException.class, () -> {
                    final Settings settings = Settings.builder().put(settingKey, rejectedValue).build();
                    final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                    final Integer actualValue = realmConfig.getSetting(setting);
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
                    final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                    final Integer actualValue = realmConfig.getSetting(setting);
                    fail("No exception. Expected one for " + settingKey + "=" + rejectedValue + ". Got " + actualValue + ".");
                });
                assertThat(
                    exception.getMessage(),
                    equalTo("Failed to parse value [" + rejectedValue + "] for setting [" + settingKey + "] must be >= 0")
                );
            }
            for (final String ignoredValue : new String[] { null }) {
                final Settings settings = Settings.builder().put(settingKey, ignoredValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final Integer actualValue = realmConfig.getSetting(setting);
                assertThat(actualValue, equalTo(setting.getDefault(settings)));
            }
            for (final String acceptedValue : new String[] { "0", "1", "100000", Integer.toString(Integer.MAX_VALUE) }) {
                final Settings settings = Settings.builder().put(settingKey, acceptedValue).build();
                final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, 0);
                final Integer actualValue = realmConfig.getSetting(setting);
                assertThat(actualValue, equalTo(Integer.valueOf(acceptedValue)));
            }
        }
    }

    public void testTokenTypeSetting() {
        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final String fullSettingKey = RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.TOKEN_TYPE);

        // Default is id_token
        assertThat(
            buildRealmConfig(JwtRealmSettings.TYPE, realmName, Settings.EMPTY, randomInt()).getSetting(JwtRealmSettings.TOKEN_TYPE),
            is(JwtRealmSettings.TokenType.ID_TOKEN)
        );

        // Valid values
        final JwtRealmSettings.TokenType expectedTokenType = randomFrom(JwtRealmSettings.TokenType.values());
        final Settings settings = Settings.builder()
            .put(fullSettingKey, randomBoolean() ? expectedTokenType.value() : expectedTokenType.value().toUpperCase(Locale.ROOT))
            .build();
        assertThat(
            buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, randomInt()).getSetting(JwtRealmSettings.TOKEN_TYPE),
            is(expectedTokenType)
        );

        // Anything else is invalid
        final Settings invalidSettings = Settings.builder().put(fullSettingKey, randomAlphaOfLengthBetween(3, 20)).build();

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> buildRealmConfig(JwtRealmSettings.TYPE, realmName, invalidSettings, randomInt()).getSetting(JwtRealmSettings.TOKEN_TYPE)
        );
        assertThat(e.getMessage(), containsString("Invalid value"));
    }

    public void testFallbackClaimSettingsNotAllowedForIdTokenType() {
        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            settingsBuilder.put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.TOKEN_TYPE),
                JwtRealmSettings.TokenType.ID_TOKEN.value()
            );
        }
        settingsBuilder.put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_SUB_CLAIM), randomAlphaOfLength(8))
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_AUD_CLAIM), randomAlphaOfLength(8));

        final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settingsBuilder.build(), randomInt());

        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> realmConfig.getSetting(JwtRealmSettings.FALLBACK_SUB_CLAIM)
        );
        assertThat(
            e1.getMessage(),
            containsString(
                "fallback claim setting ["
                    + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_SUB_CLAIM)
                    + "] is not allowed when JWT realm ["
                    + realmName
                    + "] is [id_token] type"
            )
        );

        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> realmConfig.getSetting(JwtRealmSettings.FALLBACK_AUD_CLAIM)
        );
        assertThat(
            e2.getMessage(),
            containsString(
                "fallback claim setting ["
                    + RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_AUD_CLAIM)
                    + "] is not allowed when JWT realm ["
                    + realmName
                    + "] is [id_token] type"
            )
        );
    }

    public void testFallbackSettingsForAccessTokenType() {
        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final String fallbackSub = randomAlphaOfLength(8);
        final String fallbackAud = randomAlphaOfLength(8);
        final Settings settings = Settings.builder()
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.TOKEN_TYPE), JwtRealmSettings.TokenType.ACCESS_TOKEN.value())
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_SUB_CLAIM), fallbackSub)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_AUD_CLAIM), fallbackAud)
            .build();

        final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, randomInt());
        assertThat(realmConfig.getSetting(JwtRealmSettings.FALLBACK_SUB_CLAIM), equalTo(fallbackSub));
        assertThat(realmConfig.getSetting(JwtRealmSettings.FALLBACK_AUD_CLAIM), equalTo(fallbackAud));
    }

    public void testRegisteredClaimsCannotBeUsedForFallbackSettings() {
        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final String fallbackSub = randomValueOtherThan("sub", () -> randomFrom(JwtRealmSettings.REGISTERED_CLAIM_NAMES));
        final String fallbackAud = randomValueOtherThan("aud", () -> randomFrom(JwtRealmSettings.REGISTERED_CLAIM_NAMES));
        final Settings settings = Settings.builder()
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.TOKEN_TYPE), JwtRealmSettings.TokenType.ACCESS_TOKEN.value())
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_SUB_CLAIM), fallbackSub)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_AUD_CLAIM), fallbackAud)
            .build();

        final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, randomInt());

        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> realmConfig.getSetting(JwtRealmSettings.FALLBACK_SUB_CLAIM)
        );
        assertThat(
            e1.getMessage(),
            containsString(
                Strings.format(
                    "Invalid fallback claims setting [%s]. Claim [%s] cannot fallback to a registered claim [%s]",
                    RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_SUB_CLAIM),
                    "sub",
                    fallbackSub
                )
            )
        );

        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> realmConfig.getSetting(JwtRealmSettings.FALLBACK_AUD_CLAIM)
        );
        assertThat(
            e2.getMessage(),
            containsString(
                Strings.format(
                    "Invalid fallback claims setting [%s]. Claim [%s] cannot fallback to a registered claim [%s]",
                    RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.FALLBACK_AUD_CLAIM),
                    "aud",
                    fallbackAud
                )
            )
        );
    }

    public void testRequiredClaims() {
        final String realmName = randomAlphaOfLengthBetween(3, 8);

        // Required claims are optional
        final RealmConfig realmConfig1 = buildRealmConfig(JwtRealmSettings.TYPE, realmName, Settings.EMPTY, randomInt());
        assertThat(realmConfig1.getSetting(JwtRealmSettings.REQUIRED_CLAIMS).names(), emptyIterable());

        // Multiple required claims with different value types
        final String prefix = RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.REQUIRED_CLAIMS);
        final Settings settings = Settings.builder()
            .put(prefix + "extra_1", "foo")
            .put(prefix + "extra_2", "hello,world")
            .put(prefix + "extra_3", 42)
            .build();
        final RealmConfig realmConfig2 = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, randomInt());
        final Settings requireClaimsSettings = realmConfig2.getSetting(JwtRealmSettings.REQUIRED_CLAIMS);
        assertThat(requireClaimsSettings.names(), containsInAnyOrder("extra_1", "extra_2", "extra_3"));
        assertThat(requireClaimsSettings.getAsList("extra_1"), equalTo(List.of("foo")));
        assertThat(requireClaimsSettings.getAsList("extra_2"), equalTo(List.of("hello", "world")));
        assertThat(requireClaimsSettings.getAsList("extra_3"), equalTo(List.of("42")));
    }

    public void testInvalidRequiredClaims() {
        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final String invalidRequiredClaim = randomFrom("iss", "sub", "aud", "exp", "nbf", "iat");
        final String fullSettingKey = RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.REQUIRED_CLAIMS) + invalidRequiredClaim;
        final Settings settings = Settings.builder().put(fullSettingKey, randomAlphaOfLength(8)).build();

        final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, randomInt());
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> realmConfig.getSetting(JwtRealmSettings.REQUIRED_CLAIMS)
        );

        assertThat(e.getMessage(), containsString("required claim [" + fullSettingKey + "] cannot be one of [iss,sub,aud,exp,nbf,iat]"));
    }

    public void testRequiredClaimsCannotBeEmpty() {
        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final String invalidRequiredClaim = randomAlphaOfLengthBetween(4, 8);
        final String fullSettingKey = RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.REQUIRED_CLAIMS) + invalidRequiredClaim;
        final Settings settings = Settings.builder().put(fullSettingKey, "").build();

        final RealmConfig realmConfig = buildRealmConfig(JwtRealmSettings.TYPE, realmName, settings, randomInt());
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> realmConfig.getSetting(JwtRealmSettings.REQUIRED_CLAIMS)
        );

        assertThat(e.getMessage(), containsString("required claim [" + fullSettingKey + "] cannot be empty"));
    }
}
