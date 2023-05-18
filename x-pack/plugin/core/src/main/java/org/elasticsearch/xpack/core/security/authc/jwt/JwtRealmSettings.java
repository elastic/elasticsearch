/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.jwt;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.ClaimSetting;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Settings unique to each JWT realm.
 */
public class JwtRealmSettings {

    private JwtRealmSettings() {}

    public static final String TYPE = "jwt";

    // Signature algorithms
    public static final List<String> SUPPORTED_SIGNATURE_ALGORITHMS_HMAC = List.of("HS256", "HS384", "HS512");
    public static final List<String> SUPPORTED_SIGNATURE_ALGORITHMS_RSA = List.of("RS256", "RS384", "RS512", "PS256", "PS384", "PS512");
    public static final List<String> SUPPORTED_SIGNATURE_ALGORITHMS_EC = List.of("ES256", "ES384", "ES512");
    public static final List<String> SUPPORTED_SIGNATURE_ALGORITHMS_PKC = Stream.of(
        SUPPORTED_SIGNATURE_ALGORITHMS_RSA,
        SUPPORTED_SIGNATURE_ALGORITHMS_EC
    ).flatMap(Collection::stream).toList();
    public static final List<String> SUPPORTED_SIGNATURE_ALGORITHMS = Stream.of(
        SUPPORTED_SIGNATURE_ALGORITHMS_HMAC,
        SUPPORTED_SIGNATURE_ALGORITHMS_PKC
    ).flatMap(Collection::stream).toList();

    public enum ClientAuthenticationType {
        NONE("none"),
        SHARED_SECRET("shared_secret");

        private final String value;

        ClientAuthenticationType(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        public static ClientAuthenticationType parse(String value, String settingKey) {
            for (ClientAuthenticationType type : values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException(
                "Invalid value ["
                    + value
                    + "] for ["
                    + settingKey
                    + "], allowed values are ["
                    + Stream.of(values()).map(ClientAuthenticationType::value).collect(Collectors.joining(","))
                    + "]"
            );
        }
    }

    public enum TokenType {
        ID_TOKEN("id_token"),
        ACCESS_TOKEN("access_token");

        private final String value;

        TokenType(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        public static TokenType parse(String value, String settingKey) {
            return EnumSet.allOf(TokenType.class)
                .stream()
                .filter(type -> type.value.equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(
                    () -> new IllegalArgumentException(
                        Strings.format(
                            "Invalid value [%s] for [%s], allowed values are [%s]",
                            value,
                            settingKey,
                            Stream.of(values()).map(TokenType::value).collect(Collectors.joining(","))
                        )
                    )
                );
        }
    }

    // Default values and min/max constraints

    private static final TimeValue DEFAULT_ALLOWED_CLOCK_SKEW = TimeValue.timeValueSeconds(60);
    private static final List<String> DEFAULT_ALLOWED_SIGNATURE_ALGORITHMS = Collections.singletonList("RS256");
    private static final boolean DEFAULT_POPULATE_USER_METADATA = true;
    private static final TimeValue DEFAULT_JWT_CACHE_TTL = TimeValue.timeValueMinutes(20);
    private static final int DEFAULT_JWT_CACHE_SIZE = 100_000;
    private static final int MIN_JWT_CACHE_SIZE = 0;
    private static final TimeValue DEFAULT_HTTP_CONNECT_TIMEOUT = TimeValue.timeValueSeconds(5);
    private static final TimeValue DEFAULT_HTTP_CONNECTION_READ_TIMEOUT = TimeValue.timeValueSeconds(5);
    private static final TimeValue DEFAULT_HTTP_SOCKET_TIMEOUT = TimeValue.timeValueSeconds(5);
    private static final int DEFAULT_HTTP_MAX_CONNECTIONS = 200;
    private static final int MIN_HTTP_MAX_CONNECTIONS = 0;
    private static final int DEFAULT_HTTP_MAX_ENDPOINT_CONNECTIONS = 200;
    private static final int MIN_HTTP_MAX_ENDPOINT_CONNECTIONS = 0;

    // All settings

    /**
     * Get all secure and non-secure settings.
     * @return All secure and non-secure settings.
     */
    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> set = new HashSet<>();
        set.addAll(JwtRealmSettings.getNonSecureSettings());
        set.addAll(JwtRealmSettings.getSecureSettings());
        return set;
    }

    /**
     * Get all non-secure settings.
     * @return All non-secure settings.
     */
    private static Set<Setting.AffixSetting<?>> getNonSecureSettings() {
        // Standard realm settings: order, enabled
        final Set<Setting.AffixSetting<?>> set = new HashSet<>(RealmSettings.getStandardSettings(TYPE));
        set.add(TOKEN_TYPE);
        // JWT Issuer settings
        set.addAll(List.of(ALLOWED_ISSUER, ALLOWED_SIGNATURE_ALGORITHMS, ALLOWED_CLOCK_SKEW, PKC_JWKSET_PATH));
        // JWT Audience settings
        set.addAll(List.of(ALLOWED_AUDIENCES));
        // JWT End-user settings
        set.addAll(
            List.of(
                ALLOWED_SUBJECTS,
                FALLBACK_SUB_CLAIM,
                FALLBACK_AUD_CLAIM,
                REQUIRED_CLAIMS,
                CLAIMS_PRINCIPAL.getClaim(),
                CLAIMS_PRINCIPAL.getPattern(),
                CLAIMS_GROUPS.getClaim(),
                CLAIMS_GROUPS.getPattern(),
                CLAIMS_DN.getClaim(),
                CLAIMS_DN.getPattern(),
                CLAIMS_MAIL.getClaim(),
                CLAIMS_MAIL.getPattern(),
                CLAIMS_NAME.getClaim(),
                CLAIMS_NAME.getPattern(),
                POPULATE_USER_METADATA
            )
        );
        // JWT Client settings
        set.addAll(List.of(CLIENT_AUTHENTICATION_TYPE));
        // JWT Cache settings
        set.addAll(List.of(JWT_CACHE_TTL, JWT_CACHE_SIZE));
        // Standard HTTP settings for outgoing connections to get JWT issuer jwkset_path
        set.addAll(
            List.of(
                HTTP_CONNECT_TIMEOUT,
                HTTP_CONNECTION_READ_TIMEOUT,
                HTTP_SOCKET_TIMEOUT,
                HTTP_MAX_CONNECTIONS,
                HTTP_MAX_ENDPOINT_CONNECTIONS
            )
        );
        // Standard TLS connection settings for outgoing connections to get JWT issuer jwkset_path
        set.addAll(SSL_CONFIGURATION_SETTINGS);
        // JWT End-user delegated authorization settings: authorization_realms
        set.addAll(DELEGATED_AUTHORIZATION_REALMS_SETTINGS);
        return set;
    }

    /**
     * Get all secure settings.
     * @return All secure settings.
     */
    private static Set<Setting.AffixSetting<SecureString>> getSecureSettings() {
        return new HashSet<>(List.of(HMAC_JWKSET, HMAC_KEY, CLIENT_AUTHENTICATION_SHARED_SECRET));
    }

    public static final Setting.AffixSetting<TokenType> TOKEN_TYPE = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "token_type",
        key -> new Setting<>(key, TokenType.ID_TOKEN.value(), value -> TokenType.parse(value, key), Setting.Property.NodeScope)
    );

    // JWT issuer settings
    public static final Setting.AffixSetting<String> ALLOWED_ISSUER = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_issuer",
        key -> Setting.simpleString(key, value -> verifyNonNullNotEmpty(key, value, null), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> ALLOWED_CLOCK_SKEW = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_clock_skew",
        key -> Setting.timeSetting(key, DEFAULT_ALLOWED_CLOCK_SKEW, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<List<String>> ALLOWED_SIGNATURE_ALGORITHMS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_signature_algorithms",
        key -> Setting.listSetting(
            key,
            DEFAULT_ALLOWED_SIGNATURE_ALGORITHMS,
            Function.identity(),
            values -> verifyNonNullNotEmpty(key, values, SUPPORTED_SIGNATURE_ALGORITHMS),
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<String> PKC_JWKSET_PATH = RealmSettings.simpleString(
        TYPE,
        "pkc_jwkset_path",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<SecureString> HMAC_JWKSET = RealmSettings.secureString(TYPE, "hmac_jwkset");
    public static final Setting.AffixSetting<SecureString> HMAC_KEY = RealmSettings.secureString(TYPE, "hmac_key");

    // JWT audience settings

    public static final Setting.AffixSetting<List<String>> ALLOWED_AUDIENCES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_audiences",
        key -> Setting.stringListSetting(key, values -> verifyNonNullNotEmpty(key, values, null), Setting.Property.NodeScope)
    );

    // JWT end-user settings

    public static final Setting.AffixSetting<List<String>> ALLOWED_SUBJECTS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_subjects",
        key -> Setting.stringListSetting(key, values -> verifyNonNullNotEmpty(key, values, null), Setting.Property.NodeScope)
    );

    // Registered claim names from the JWT spec https://www.rfc-editor.org/rfc/rfc7519#section-4.1.
    // Being registered means they have prescribed meanings when they present in a JWT.
    public static final List<String> REGISTERED_CLAIM_NAMES = List.of("iss", "sub", "aud", "exp", "nbf", "iat", "jti");

    public static final Setting.AffixSetting<String> FALLBACK_SUB_CLAIM = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "fallback_claims.sub",
        key -> Setting.simpleString(key, "sub", new Setting.Validator<>() {
            @Override
            public void validate(String value) {}

            @Override
            public void validate(String value, Map<Setting<?>, Object> settings, boolean isPresent) {
                validateFallbackClaimSetting(FALLBACK_SUB_CLAIM, key, value, settings, isPresent);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final String namespace = FALLBACK_SUB_CLAIM.getNamespace(FALLBACK_SUB_CLAIM.getConcreteSetting(key));
                final List<Setting<?>> settings = List.of(TOKEN_TYPE.getConcreteSettingForNamespace(namespace));
                return settings.iterator();
            }
        }, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<String> FALLBACK_AUD_CLAIM = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "fallback_claims.aud",
        key -> Setting.simpleString(key, "aud", new Setting.Validator<>() {
            @Override
            public void validate(String value) {}

            @Override
            public void validate(String value, Map<Setting<?>, Object> settings, boolean isPresent) {
                validateFallbackClaimSetting(FALLBACK_AUD_CLAIM, key, value, settings, isPresent);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final String namespace = FALLBACK_AUD_CLAIM.getNamespace(FALLBACK_AUD_CLAIM.getConcreteSetting(key));
                final List<Setting<?>> settings = List.of(TOKEN_TYPE.getConcreteSettingForNamespace(namespace));
                return settings.iterator();
            }
        }, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<Settings> REQUIRED_CLAIMS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "required_claims",
        key -> Setting.groupSetting(key + ".", settings -> {
            final List<String> invalidRequiredClaims = List.of("iss", "sub", "aud", "exp", "nbf", "iat");
            for (String name : settings.names()) {
                final String fullName = key + "." + name;
                if (invalidRequiredClaims.contains(name)) {
                    throw new IllegalArgumentException(
                        Strings.format("required claim [%s] cannot be one of [%s]", fullName, String.join(",", invalidRequiredClaims))
                    );
                }
                final List<String> values = settings.getAsList(name);
                if (values.isEmpty()) {
                    throw new IllegalArgumentException(Strings.format("required claim [%s] cannot be empty", fullName));
                }
            }
        }, Setting.Property.NodeScope)
    );

    // Note: ClaimSetting is a wrapper for two individual settings: getClaim(), getPattern()
    public static final ClaimSetting CLAIMS_PRINCIPAL = new ClaimSetting(TYPE, "principal");
    public static final ClaimSetting CLAIMS_GROUPS = new ClaimSetting(TYPE, "groups");
    public static final ClaimSetting CLAIMS_DN = new ClaimSetting(TYPE, "dn");
    public static final ClaimSetting CLAIMS_MAIL = new ClaimSetting(TYPE, "mail");
    public static final ClaimSetting CLAIMS_NAME = new ClaimSetting(TYPE, "name");

    public static final Setting.AffixSetting<Boolean> POPULATE_USER_METADATA = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "populate_user_metadata",
        key -> Setting.boolSetting(key, DEFAULT_POPULATE_USER_METADATA, Setting.Property.NodeScope)
    );

    // Client authentication settings for incoming connections

    public static final Setting.AffixSetting<ClientAuthenticationType> CLIENT_AUTHENTICATION_TYPE = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "client_authentication.type",
        key -> new Setting<>(
            key,
            ClientAuthenticationType.SHARED_SECRET.value,
            value -> ClientAuthenticationType.parse(value, key),
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<SecureString> CLIENT_AUTHENTICATION_SHARED_SECRET = RealmSettings.secureString(
        TYPE,
        "client_authentication.shared_secret"
    );

    // Individual Cache settings

    public static final Setting.AffixSetting<TimeValue> JWT_CACHE_TTL = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "jwt.cache.ttl",
        key -> Setting.timeSetting(key, DEFAULT_JWT_CACHE_TTL, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<Integer> JWT_CACHE_SIZE = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "jwt.cache.size",
        key -> Setting.intSetting(key, DEFAULT_JWT_CACHE_SIZE, MIN_JWT_CACHE_SIZE, Setting.Property.NodeScope)
    );

    // Individual outgoing HTTP settings

    public static final Setting.AffixSetting<TimeValue> HTTP_CONNECT_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.connect_timeout",
        key -> Setting.timeSetting(key, DEFAULT_HTTP_CONNECT_TIMEOUT, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> HTTP_CONNECTION_READ_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.connection_read_timeout",
        key -> Setting.timeSetting(key, DEFAULT_HTTP_CONNECTION_READ_TIMEOUT, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> HTTP_SOCKET_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.socket_timeout",
        key -> Setting.timeSetting(key, DEFAULT_HTTP_SOCKET_TIMEOUT, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Integer> HTTP_MAX_CONNECTIONS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.max_connections",
        key -> Setting.intSetting(key, DEFAULT_HTTP_MAX_CONNECTIONS, MIN_HTTP_MAX_CONNECTIONS, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Integer> HTTP_MAX_ENDPOINT_CONNECTIONS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.max_endpoint_connections",
        key -> Setting.intSetting(key, DEFAULT_HTTP_MAX_ENDPOINT_CONNECTIONS, MIN_HTTP_MAX_ENDPOINT_CONNECTIONS, Setting.Property.NodeScope)
    );

    // SSL Configuration settings

    public static final Collection<Setting.AffixSetting<?>> SSL_CONFIGURATION_SETTINGS = SSLConfigurationSettings.getRealmSettings(TYPE);
    public static final SSLConfigurationSettings ssl = SSLConfigurationSettings.withoutPrefix(true);

    // Delegated Authorization Realms settings

    public static final Collection<Setting.AffixSetting<?>> DELEGATED_AUTHORIZATION_REALMS_SETTINGS = DelegatedAuthorizationSettings
        .getSettings(TYPE);

    private static void verifyNonNullNotEmpty(final String key, final String value, final List<String> allowedValues) {
        assert value != null : "Invalid null value for [" + key + "].";
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Invalid empty value for [" + key + "].");
        }
        if (allowedValues != null) {
            if (allowedValues.contains(value) == false) {
                throw new IllegalArgumentException(
                    "Invalid value [" + value + "] for [" + key + "]. Allowed values are " + allowedValues + "."
                );
            }
        }
    }

    private static void verifyNonNullNotEmpty(final String key, final List<String> values, final List<String> allowedValues) {
        assert values != null : "Invalid null list of values for [" + key + "].";
        if (values.isEmpty()) {
            if (allowedValues == null) {
                throw new IllegalArgumentException("Invalid empty list for [" + key + "].");
            } else {
                throw new IllegalArgumentException("Invalid empty list for [" + key + "]. Allowed values are " + allowedValues + ".");
            }
        }
        for (final String value : values) {
            verifyNonNullNotEmpty(key, value, allowedValues);
        }
    }

    private static void validateFallbackClaimSetting(
        Setting.AffixSetting<String> setting,
        String key,
        String value,
        Map<Setting<?>, Object> settings,
        boolean isPresent
    ) {
        if (false == isPresent) {
            return;
        }
        final String namespace = setting.getNamespace(setting.getConcreteSetting(key));
        final TokenType tokenType = (TokenType) settings.get(TOKEN_TYPE.getConcreteSettingForNamespace(namespace));
        if (tokenType == TokenType.ID_TOKEN) {
            throw new IllegalArgumentException(
                Strings.format(
                    "fallback claim setting [%s] is not allowed when JWT realm [%s] is [%s] type",
                    key,
                    namespace,
                    JwtRealmSettings.TokenType.ID_TOKEN.value()
                )
            );
        }
        verifyFallbackClaimName(key, value);
    }

    private static void verifyFallbackClaimName(String key, String fallbackClaimName) {
        final String claimName = key.substring(key.lastIndexOf(".") + 1);
        verifyNonNullNotEmpty(key, fallbackClaimName, null);
        if (claimName.equals(fallbackClaimName)) {
            return;
        }
        // Registered claims have prescribed meanings and should not be used for something else.
        if (REGISTERED_CLAIM_NAMES.contains(fallbackClaimName)) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Invalid fallback claims setting [%s]. Claim [%s] cannot fallback to a registered claim [%s]",
                    key,
                    claimName,
                    fallbackClaimName
                )
            );
        }
    }

}
