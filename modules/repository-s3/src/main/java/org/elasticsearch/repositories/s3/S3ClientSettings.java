/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A container for settings used to create an S3 client.
 */
final class S3ClientSettings {

    static {
        // Make sure repository plugin class is loaded before this class is used to trigger static initializer for that class which applies
        // necessary Jackson workaround
        try {
            Class.forName("org.elasticsearch.repositories.s3.S3RepositoryPlugin");
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    // prefix for s3 client settings
    private static final String PREFIX = "s3.client.";

    /** Placeholder client name for normalizing client settings in the repository settings. */
    private static final String PLACEHOLDER_CLIENT = "placeholder";

    /** The access key (ie login id) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> ACCESS_KEY_SETTING = Setting.affixKeySetting(
        PREFIX,
        "access_key",
        key -> SecureSetting.secureString(key, null)
    );

    /** The secret key (ie password) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> SECRET_KEY_SETTING = Setting.affixKeySetting(
        PREFIX,
        "secret_key",
        key -> SecureSetting.secureString(key, null)
    );

    /** The secret key (ie password) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> SESSION_TOKEN_SETTING = Setting.affixKeySetting(
        PREFIX,
        "session_token",
        key -> SecureSetting.secureString(key, null)
    );

    /** An override for the s3 endpoint to connect to. */
    static final Setting.AffixSetting<String> ENDPOINT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "endpoint",
        key -> new Setting<>(key, "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope)
    );

    /** The protocol to use to connect to s3, now only used if {@link #endpoint} is not a proper URI that starts with {@code http://} or
     * {@code https://}. */
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION) // no longer used, should be removed in v10
    static final Setting.AffixSetting<HttpScheme> PROTOCOL_SETTING = Setting.affixKeySetting(
        PREFIX,
        "protocol",
        key -> new Setting<>(key, "https", s -> HttpScheme.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope, Property.Deprecated)
    );

    /** The host name of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.host",
        key -> Setting.simpleString(key, Property.NodeScope)
    );

    /** The port of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.port",
        key -> Setting.intSetting(key, 80, 0, 1 << 16, Property.NodeScope)
    );

    /** The proxy scheme for connecting to S3 through a proxy. */
    static final Setting.AffixSetting<HttpScheme> PROXY_SCHEME_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.scheme",
        key -> new Setting<>(key, "http", s -> HttpScheme.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope)
    );

    /** The username of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<SecureString> PROXY_USERNAME_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.username",
        key -> SecureSetting.secureString(key, null)
    );

    /** The password of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<SecureString> PROXY_PASSWORD_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.password",
        key -> SecureSetting.secureString(key, null)
    );

    /** The socket timeout for connecting to s3. */
    static final Setting.AffixSetting<TimeValue> READ_TIMEOUT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "read_timeout",
        key -> Setting.timeSetting(key, Defaults.READ_TIMEOUT, Property.NodeScope)
    );

    /** The maximum number of concurrent connections to use. */
    static final Setting.AffixSetting<Integer> MAX_CONNECTIONS_SETTING = Setting.affixKeySetting(
        PREFIX,
        "max_connections",
        key -> Setting.intSetting(key, Defaults.MAX_CONNECTIONS, 1, Property.NodeScope)
    );

    /** The number of retries to use when an s3 request fails. */
    static final Setting.AffixSetting<Integer> MAX_RETRIES_SETTING = Setting.affixKeySetting(
        PREFIX,
        "max_retries",
        key -> Setting.intSetting(key, Defaults.RETRY_COUNT, 0, Property.NodeScope)
    );

    /** Formerly whether retries should be throttled (ie use backoff), now unused. V2 AWS SDK always uses throttling. */
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION) // no longer used, should be removed in v10
    static final Setting.AffixSetting<Boolean> UNUSED_USE_THROTTLE_RETRIES_SETTING = Setting.affixKeySetting(
        PREFIX,
        "use_throttle_retries",
        key -> Setting.boolSetting(key, true, Property.NodeScope, Property.Deprecated)
    );

    /** Whether the s3 client should use path style access. */
    static final Setting.AffixSetting<Boolean> USE_PATH_STYLE_ACCESS = Setting.affixKeySetting(
        PREFIX,
        "path_style_access",
        key -> Setting.boolSetting(key, false, Property.NodeScope)
    );

    /** Whether chunked encoding should be disabled or not (Default is false). */
    static final Setting.AffixSetting<Boolean> DISABLE_CHUNKED_ENCODING = Setting.affixKeySetting(
        PREFIX,
        "disable_chunked_encoding",
        key -> Setting.boolSetting(key, false, Property.NodeScope)
    );

    /** An override for the s3 region to use for signing requests. */
    static final Setting.AffixSetting<String> REGION = Setting.affixKeySetting(
        PREFIX,
        "region",
        key -> Setting.simpleString(key, Property.NodeScope)
    );

    /** Formerly an override for the signer to use, now unused. V2 AWS SDK only supports AWS v4 signatures. */
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION) // no longer used, should be removed in v10
    static final Setting.AffixSetting<String> UNUSED_SIGNER_OVERRIDE = Setting.affixKeySetting(
        PREFIX,
        "signer_override",
        key -> Setting.simpleString(key, Property.NodeScope, Property.Deprecated)
    );

    /** Whether to include the {@code x-purpose} custom query parameter in all requests. */
    static final Setting.AffixSetting<Boolean> ADD_PURPOSE_CUSTOM_QUERY_PARAMETER = Setting.affixKeySetting(
        PREFIX,
        "add_purpose_custom_query_parameter",
        key -> Setting.boolSetting(key, false, Property.NodeScope)
    );

    /** Credentials to authenticate with s3. */
    final AwsCredentials credentials;

    /** The scheme (HTTP or HTTPS) for talking to the endpoint, for use only if the endpoint doesn't contain an explicit scheme */
    final HttpScheme protocol;

    /** The s3 endpoint the client should talk to, or empty string to use the default. */
    final String endpoint;

    /** An optional proxy host that requests to s3 should be made through. */
    final String proxyHost;

    /** The port number the proxy host should be connected on. */
    final int proxyPort;

    /** The proxy scheme to use for connecting to s3 through a proxy. */
    final HttpScheme proxyScheme;

    // these should be "secure" yet the api for the s3 client only takes String, so storing them
    // as SecureString here won't really help with anything
    /** An optional username for the proxy host, for basic authentication. */
    final String proxyUsername;

    /** An optional password for the proxy host, for basic authentication. */
    final String proxyPassword;

    /** The read timeout for the s3 client. */
    final int readTimeoutMillis;

    /** The maximum number of concurrent connections to use. */
    final int maxConnections;

    /** The number of retries to use for the s3 client. */
    final int maxRetries;

    /** Whether the s3 client should use path style access. */
    final boolean pathStyleAccess;

    /** Whether chunked encoding should be disabled or not. */
    final boolean disableChunkedEncoding;

    /** Whether to add the {@code x-purpose} custom query parameter to all requests. */
    final boolean addPurposeCustomQueryParameter;

    /** Region to use for signing requests or empty string to use default. */
    final String region;

    private S3ClientSettings(
        AwsCredentials credentials,
        HttpScheme protocol,
        String endpoint,
        String proxyHost,
        int proxyPort,
        HttpScheme proxyScheme,
        String proxyUsername,
        String proxyPassword,
        int readTimeoutMillis,
        int maxConnections,
        int maxRetries,
        boolean pathStyleAccess,
        boolean disableChunkedEncoding,
        boolean addPurposeCustomQueryParameter,
        String region
    ) {
        this.credentials = credentials;
        this.protocol = protocol;
        this.endpoint = endpoint;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyScheme = proxyScheme;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.readTimeoutMillis = readTimeoutMillis;
        this.maxConnections = maxConnections;
        this.maxRetries = maxRetries;
        this.pathStyleAccess = pathStyleAccess;
        this.disableChunkedEncoding = disableChunkedEncoding;
        this.addPurposeCustomQueryParameter = addPurposeCustomQueryParameter;
        this.region = region;
    }

    /**
     * Overrides the settings in this instance with settings found in repository metadata.
     *
     * @param repositorySettings found in repository metadata
     * @return S3ClientSettings
     */
    S3ClientSettings refine(Settings repositorySettings) {
        // Normalize settings to placeholder client settings prefix so that we can use the affix settings directly
        final Settings normalizedSettings = Settings.builder()
            .put(repositorySettings)
            .normalizePrefix(PREFIX + PLACEHOLDER_CLIENT + '.')
            .build();
        final HttpScheme newProtocol = getRepoSettingOrDefault(PROTOCOL_SETTING, normalizedSettings, protocol);
        final String newEndpoint = getRepoSettingOrDefault(ENDPOINT_SETTING, normalizedSettings, endpoint);

        final String newProxyHost = getRepoSettingOrDefault(PROXY_HOST_SETTING, normalizedSettings, proxyHost);
        final int newProxyPort = getRepoSettingOrDefault(PROXY_PORT_SETTING, normalizedSettings, proxyPort);
        final HttpScheme newProxyScheme = getRepoSettingOrDefault(PROXY_SCHEME_SETTING, normalizedSettings, proxyScheme);
        final int newReadTimeoutMillis = Math.toIntExact(
            getRepoSettingOrDefault(READ_TIMEOUT_SETTING, normalizedSettings, TimeValue.timeValueMillis(readTimeoutMillis)).millis()
        );
        final int newMaxConnections = getRepoSettingOrDefault(MAX_CONNECTIONS_SETTING, normalizedSettings, maxConnections);
        final int newMaxRetries = getRepoSettingOrDefault(MAX_RETRIES_SETTING, normalizedSettings, maxRetries);
        final boolean newPathStyleAccess = getRepoSettingOrDefault(USE_PATH_STYLE_ACCESS, normalizedSettings, pathStyleAccess);
        final boolean newDisableChunkedEncoding = getRepoSettingOrDefault(
            DISABLE_CHUNKED_ENCODING,
            normalizedSettings,
            disableChunkedEncoding
        );
        final boolean newAddPurposeCustomQueryParameter = getRepoSettingOrDefault(
            ADD_PURPOSE_CUSTOM_QUERY_PARAMETER,
            normalizedSettings,
            addPurposeCustomQueryParameter
        );
        final AwsCredentials newCredentials;
        if (checkDeprecatedCredentials(repositorySettings)) {
            newCredentials = loadDeprecatedCredentials(repositorySettings);
        } else {
            newCredentials = credentials;
        }
        final String newRegion = getRepoSettingOrDefault(REGION, normalizedSettings, region);
        if (Objects.equals(protocol, newProtocol)
            && Objects.equals(endpoint, newEndpoint)
            && Objects.equals(proxyHost, newProxyHost)
            && proxyPort == newProxyPort
            && proxyScheme == newProxyScheme
            && newReadTimeoutMillis == readTimeoutMillis
            && maxConnections == newMaxConnections
            && maxRetries == newMaxRetries
            && Objects.equals(credentials, newCredentials)
            && newPathStyleAccess == pathStyleAccess
            && newDisableChunkedEncoding == disableChunkedEncoding
            && newAddPurposeCustomQueryParameter == addPurposeCustomQueryParameter
            && Objects.equals(region, newRegion)) {
            return this;
        }
        return new S3ClientSettings(
            newCredentials,
            newProtocol,
            newEndpoint,
            newProxyHost,
            newProxyPort,
            newProxyScheme,
            proxyUsername,
            proxyPassword,
            newReadTimeoutMillis,
            newMaxConnections,
            newMaxRetries,
            newPathStyleAccess,
            newDisableChunkedEncoding,
            newAddPurposeCustomQueryParameter,
            newRegion
        );
    }

    /**
     * Load all client settings from the given settings.
     * <p>
     * Note this will always at least return a client named "default".
     */
    static Map<String, S3ClientSettings> load(Settings settings) {
        final Set<String> clientNames = settings.getGroups(PREFIX).keySet();
        final Map<String, S3ClientSettings> clients = new HashMap<>();
        for (final String clientName : clientNames) {
            clients.put(clientName, getClientSettings(settings, clientName));
        }
        if (clients.containsKey("default") == false) {
            // this won't find any settings under the default client,
            // but it will pull all the fallback static settings
            clients.put("default", getClientSettings(settings, "default"));
        }
        return Collections.unmodifiableMap(clients);
    }

    static boolean checkDeprecatedCredentials(Settings repositorySettings) {
        if (S3Repository.ACCESS_KEY_SETTING.exists(repositorySettings)) {
            if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings) == false) {
                throw new IllegalArgumentException(
                    "Repository setting ["
                        + S3Repository.ACCESS_KEY_SETTING.getKey()
                        + " must be accompanied by setting ["
                        + S3Repository.SECRET_KEY_SETTING.getKey()
                        + "]"
                );
            }
            return true;
        } else if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings)) {
            throw new IllegalArgumentException(
                "Repository setting ["
                    + S3Repository.SECRET_KEY_SETTING.getKey()
                    + " must be accompanied by setting ["
                    + S3Repository.ACCESS_KEY_SETTING.getKey()
                    + "]"
            );
        }
        return false;
    }

    // Ancient BWC for reading creds stored unsafely in plaintext in RepositoryMetadata#settings - really dangerous but still in use
    private static AwsCredentials loadDeprecatedCredentials(Settings repositorySettings) {
        assert checkDeprecatedCredentials(repositorySettings);
        try (
            SecureString key = S3Repository.ACCESS_KEY_SETTING.get(repositorySettings);
            SecureString secret = S3Repository.SECRET_KEY_SETTING.get(repositorySettings)
        ) {
            return AwsBasicCredentials.create(key.toString(), secret.toString());
        }
    }

    private static AwsCredentials loadCredentials(Settings settings, String clientName) {
        try (
            SecureString accessKey = getConfigValue(settings, clientName, ACCESS_KEY_SETTING);
            SecureString secretKey = getConfigValue(settings, clientName, SECRET_KEY_SETTING);
            SecureString sessionToken = getConfigValue(settings, clientName, SESSION_TOKEN_SETTING)
        ) {
            if (accessKey.length() != 0) {
                if (secretKey.length() != 0) {
                    if (sessionToken.length() != 0) {
                        return AwsSessionCredentials.create(accessKey.toString(), secretKey.toString(), sessionToken.toString());
                    } else {
                        return AwsBasicCredentials.create(accessKey.toString(), secretKey.toString());
                    }
                } else {
                    throw new IllegalArgumentException("Missing secret key for s3 client [" + clientName + "]");
                }
            } else {
                if (secretKey.length() != 0) {
                    throw new IllegalArgumentException("Missing access key for s3 client [" + clientName + "]");
                }
                if (sessionToken.length() != 0) {
                    throw new IllegalArgumentException("Missing access key and secret key for s3 client [" + clientName + "]");
                }
                return null;
            }
        }
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    static S3ClientSettings getClientSettings(final Settings settings, final String clientName) {
        try (
            SecureString proxyUsername = getConfigValue(settings, clientName, PROXY_USERNAME_SETTING);
            SecureString proxyPassword = getConfigValue(settings, clientName, PROXY_PASSWORD_SETTING)
        ) {
            return new S3ClientSettings(
                S3ClientSettings.loadCredentials(settings, clientName),
                getConfigValue(settings, clientName, PROTOCOL_SETTING),
                getConfigValue(settings, clientName, ENDPOINT_SETTING),
                getConfigValue(settings, clientName, PROXY_HOST_SETTING),
                getConfigValue(settings, clientName, PROXY_PORT_SETTING),
                getConfigValue(settings, clientName, PROXY_SCHEME_SETTING),
                proxyUsername.toString(),
                proxyPassword.toString(),
                Math.toIntExact(getConfigValue(settings, clientName, READ_TIMEOUT_SETTING).millis()),
                getConfigValue(settings, clientName, MAX_CONNECTIONS_SETTING),
                getConfigValue(settings, clientName, MAX_RETRIES_SETTING),
                getConfigValue(settings, clientName, USE_PATH_STYLE_ACCESS),
                getConfigValue(settings, clientName, DISABLE_CHUNKED_ENCODING),
                getConfigValue(settings, clientName, ADD_PURPOSE_CUSTOM_QUERY_PARAMETER),
                getConfigValue(settings, clientName, REGION)
            );
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final S3ClientSettings that = (S3ClientSettings) o;
        return proxyPort == that.proxyPort
            && readTimeoutMillis == that.readTimeoutMillis
            && maxConnections == that.maxConnections
            && maxRetries == that.maxRetries
            && Objects.equals(credentials, that.credentials)
            && Objects.equals(protocol, that.protocol)
            && Objects.equals(endpoint, that.endpoint)
            && Objects.equals(proxyHost, that.proxyHost)
            && proxyScheme == that.proxyScheme
            && Objects.equals(proxyUsername, that.proxyUsername)
            && Objects.equals(proxyPassword, that.proxyPassword)
            && Objects.equals(disableChunkedEncoding, that.disableChunkedEncoding)
            && Objects.equals(addPurposeCustomQueryParameter, that.addPurposeCustomQueryParameter)
            && Objects.equals(region, that.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            credentials,
            protocol,
            endpoint,
            proxyHost,
            proxyPort,
            proxyScheme,
            proxyUsername,
            proxyPassword,
            readTimeoutMillis,
            maxRetries,
            maxConnections,
            disableChunkedEncoding,
            addPurposeCustomQueryParameter,
            region
        );
    }

    private static <T> T getConfigValue(Settings settings, String clientName, Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }

    private static <T> T getRepoSettingOrDefault(Setting.AffixSetting<T> setting, Settings normalizedSettings, T defaultValue) {
        if (setting.getConcreteSettingForNamespace(PLACEHOLDER_CLIENT).exists(normalizedSettings)) {
            return getConfigValue(normalizedSettings, PLACEHOLDER_CLIENT, setting);
        }
        return defaultValue;
    }

    static final class Defaults {
        static final TimeValue READ_TIMEOUT = TimeValue.timeValueSeconds(50);
        static final int MAX_CONNECTIONS = 50;
        static final int RETRY_COUNT = 3;
    }
}
