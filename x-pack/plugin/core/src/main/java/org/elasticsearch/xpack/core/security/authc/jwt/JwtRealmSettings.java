/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.jwt;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.ClaimSetting;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Settings for JWT realms.
 */
public class JwtRealmSettings {

    private JwtRealmSettings() {}

    public static final String TYPE = "jwt";

    /**
     * HMAC length NOT by specific SHA-2 lengths.
     * Example: final byte[] hmacKeyBytes = new byte[randomIntBetween(32,100)];
     */
    public static final List<String> SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS = List.of("HS256", "HS384", "HS512");

    /**
     * RSA lengths NOT by specific SHA-2 lengths.
     * Example: final Integer rsaBits = randomFrom(2048, 3072, 4096, 6144);
     */
    public static final List<String> SUPPORTED_PUBLIC_KEY_RSA_SIGNATURE_ALGORITHMS = List.of(
        "RS256",
        "RS384",
        "RS512",
        "PS256",
        "PS384",
        "PS512"
    );

    /**
     * EC curves by specific SHA-2 lengths.
     * Example: final Curve ecCurve = randomFrom(Curve.forJWSAlgorithm(jwsAlgorithm));
     * Note: Excludes ES256K. The library supports it, but Java 11.0.9, 11.0.10, 17 disabled support for it by default.
     */
    public static final List<String> SUPPORTED_PUBLIC_KEY_EC_SIGNATURE_ALGORITHMS = List.of("ES256", "ES384", "ES512");

    public static final List<String> SUPPORTED_PUBLIC_KEY_SIGNATURE_ALGORITHMS = Stream.of(
        SUPPORTED_PUBLIC_KEY_RSA_SIGNATURE_ALGORITHMS,
        SUPPORTED_PUBLIC_KEY_EC_SIGNATURE_ALGORITHMS
    ).flatMap(Collection::stream).toList();

    public static final List<String> SUPPORTED_SIGNATURE_ALGORITHMS = Stream.of(
        SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS,
        SUPPORTED_PUBLIC_KEY_SIGNATURE_ALGORITHMS
    ).flatMap(Collection::stream).toList();

    // Header names
    public static final String HEADER_END_USER_AUTHORIZATION = "Authorization";
    public static final String HEADER_END_USER_AUTHORIZATION_SCHEME = "Bearer";

    public static final String HEADER_CLIENT_AUTHORIZATION = "X-Client-Authorization";

    public static final String SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET = "SharedSecret";
    public static final String SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE = "None";
    public static final List<String> SUPPORTED_CLIENT_AUTHORIZATION_TYPES = List.of(
        SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET,
        SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE
    );

    // Default values and min/max constraints

    private static final TimeValue DEFAULT_ALLOWED_CLOCK_SKEW = TimeValue.timeValueSeconds(60);
    private static final List<String> DEFAULT_ALLOWED_SIGNATURE_ALGORITHMS = Collections.singletonList("RS256");
    private static final boolean DEFAULT_POPULATE_USER_METADATA = true;
    private static final String DEFAULT_CLIENT_AUTHORIZATION_TYPE = SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET;
    private static final TimeValue DEFAULT_CACHE_TTL = TimeValue.timeValueMinutes(20);
    private static final int DEFAULT_CACHE_MAX_USERS = 100_000;
    private static final String DEFAULT_CACHE_HASH_ALGO = "ssha256";
    private static final int MIN_CACHE_MAX_USERS = 0;
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
        final Set<Setting.AffixSetting<?>> set = Sets.newHashSet();
        set.addAll(JwtRealmSettings.getNonSecureSettings());
        set.addAll(JwtRealmSettings.getSecureSettings());
        return set;
    }

    /**
     * Get all non-secure settings.
     * @return All non-secure settings.
     */
    public static Set<Setting.AffixSetting<?>> getNonSecureSettings() {
        final Set<Setting.AffixSetting<?>> set = Sets.newHashSet();
        // Standard realm settings: order, enabled
        set.addAll(RealmSettings.getStandardSettings(TYPE));
        // JWT Issuer settings
        set.addAll(List.of(ALLOWED_ISSUER, ALLOWED_SIGNATURE_ALGORITHMS, ALLOWED_CLOCK_SKEW, JWKSET_PATH));
        // JWT Audience settings
        set.addAll(List.of(ALLOWED_AUDIENCES));
        // JWT End-user settings
        set.addAll(
            List.of(
                CLAIMS_PRINCIPAL.getClaim(),
                CLAIMS_PRINCIPAL.getPattern(),
                CLAIMS_GROUPS.getClaim(),
                CLAIMS_GROUPS.getPattern(),
                POPULATE_USER_METADATA
            )
        );
        // JWT Client settings
        set.addAll(List.of(CLIENT_AUTHORIZATION_TYPE));
        // JWT Cache settings
        set.addAll(List.of(CACHE_TTL, CACHE_MAX_USERS, CACHE_HASH_ALGO));
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
    public static List<Setting.AffixSetting<SecureString>> getSecureSettings() {
        return List.of(ISSUER_HMAC_SECRET_KEY, CLIENT_AUTHORIZATION_SHARED_SECRET);
    }

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

    public static final Setting.AffixSetting<String> JWKSET_PATH = RealmSettings.simpleString(
        TYPE,
        "jwkset_path",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<SecureString> ISSUER_HMAC_SECRET_KEY = RealmSettings.secureString(
        TYPE,
        "issuer_hmac_secret_key"
    );

    // JWT audience settings

    public static final Setting.AffixSetting<List<String>> ALLOWED_AUDIENCES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_audiences",
        key -> Setting.stringListSetting(key, values -> verifyNonNullNotEmpty(key, values, null), Setting.Property.NodeScope)
    );

    // JWT end-user settings

    // Note: ClaimSetting is a wrapper for two individual settings: getClaim(), getPattern()
    public static final ClaimSetting CLAIMS_PRINCIPAL = new ClaimSetting(TYPE, "principal");
    public static final ClaimSetting CLAIMS_GROUPS = new ClaimSetting(TYPE, "groups");

    public static final Setting.AffixSetting<Boolean> POPULATE_USER_METADATA = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "populate_user_metadata",
        key -> Setting.boolSetting(key, DEFAULT_POPULATE_USER_METADATA, Setting.Property.NodeScope)
    );

    // Client authentication settings for incoming connections

    public static final Setting.AffixSetting<String> CLIENT_AUTHORIZATION_TYPE = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "client_authentication.type",
        key -> Setting.simpleString(key, DEFAULT_CLIENT_AUTHORIZATION_TYPE, value -> {
            if (SUPPORTED_CLIENT_AUTHORIZATION_TYPES.contains(value) == false) {
                throw new IllegalArgumentException(
                    "Invalid value [" + value + "] for [" + key + "]. Allowed values are " + SUPPORTED_CLIENT_AUTHORIZATION_TYPES + "."
                );
            }
        }, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<SecureString> CLIENT_AUTHORIZATION_SHARED_SECRET = RealmSettings.secureString(
        TYPE,
        "client_authentication.shared_secret"
    );

    // Individual Cache settings

    public static final Setting.AffixSetting<TimeValue> CACHE_TTL = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "cache.ttl",
        key -> Setting.timeSetting(key, DEFAULT_CACHE_TTL, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<Integer> CACHE_MAX_USERS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "cache.max_users",
        key -> Setting.intSetting(key, DEFAULT_CACHE_MAX_USERS, MIN_CACHE_MAX_USERS, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<String> CACHE_HASH_ALGO = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "cache.hash_algo",
        key -> Setting.simpleString(key, DEFAULT_CACHE_HASH_ALGO, Setting.Property.NodeScope)
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
}
