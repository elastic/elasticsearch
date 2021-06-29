/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

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
    static final Setting.AffixSetting<SecureString> ACCESS_KEY_SETTING = Setting.affixKeySetting(PREFIX, "access_key",
        key -> SecureSetting.secureString(key, null));

    /** The secret key (ie password) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> SECRET_KEY_SETTING = Setting.affixKeySetting(PREFIX, "secret_key",
        key -> SecureSetting.secureString(key, null));

    /** The secret key (ie password) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> SESSION_TOKEN_SETTING = Setting.affixKeySetting(PREFIX, "session_token",
        key -> SecureSetting.secureString(key, null));

    /** An override for the s3 endpoint to connect to. */
    static final Setting.AffixSetting<String> ENDPOINT_SETTING = Setting.affixKeySetting(PREFIX, "endpoint",
        key -> new Setting<>(key, "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope));

    /** The protocol to use to connect to s3. */
    static final Setting.AffixSetting<Protocol> PROTOCOL_SETTING = Setting.affixKeySetting(PREFIX, "protocol",
        key -> new Setting<>(key, "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope));

    /** The host name of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(PREFIX, "proxy.host",
        key -> Setting.simpleString(key, Property.NodeScope));

    /** The port of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(PREFIX, "proxy.port",
        key -> Setting.intSetting(key, 80, 0, 1<<16, Property.NodeScope));

    /** The username of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<SecureString> PROXY_USERNAME_SETTING = Setting.affixKeySetting(PREFIX, "proxy.username",
        key -> SecureSetting.secureString(key, null));

    /** The password of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<SecureString> PROXY_PASSWORD_SETTING = Setting.affixKeySetting(PREFIX, "proxy.password",
        key -> SecureSetting.secureString(key, null));

    /** The socket timeout for connecting to s3. */
    static final Setting.AffixSetting<TimeValue> READ_TIMEOUT_SETTING = Setting.affixKeySetting(PREFIX, "read_timeout",
        key -> Setting.timeSetting(key, TimeValue.timeValueMillis(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT), Property.NodeScope));

    /** The number of retries to use when an s3 request fails. */
    static final Setting.AffixSetting<Integer> MAX_RETRIES_SETTING = Setting.affixKeySetting(PREFIX, "max_retries",
        key -> Setting.intSetting(key, ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry(), 0, Property.NodeScope));

    /** Whether retries should be throttled (ie use backoff). */
    static final Setting.AffixSetting<Boolean> USE_THROTTLE_RETRIES_SETTING = Setting.affixKeySetting(PREFIX, "use_throttle_retries",
        key -> Setting.boolSetting(key, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, Property.NodeScope));

    /** Whether the s3 client should use path style access. */
    static final Setting.AffixSetting<Boolean> USE_PATH_STYLE_ACCESS = Setting.affixKeySetting(PREFIX, "path_style_access",
        key -> Setting.boolSetting(key, false, Property.NodeScope));

    /** Whether chunked encoding should be disabled or not (Default is false). */
    static final Setting.AffixSetting<Boolean> DISABLE_CHUNKED_ENCODING = Setting.affixKeySetting(PREFIX, "disable_chunked_encoding",
        key -> Setting.boolSetting(key, false, Property.NodeScope));

    /** An override for the s3 region to use for signing requests. */
    static final Setting.AffixSetting<String> REGION = Setting.affixKeySetting(PREFIX, "region",
        key -> new Setting<>(key, "", Function.identity(), Property.NodeScope));

    /** An override for the signer to use. */
    static final Setting.AffixSetting<String> SIGNER_OVERRIDE = Setting.affixKeySetting(PREFIX, "signer_override",
        key -> new Setting<>(key, "", Function.identity(), Property.NodeScope));

    /** Credentials to authenticate with s3. */
    final S3BasicCredentials credentials;

    /** The s3 endpoint the client should talk to, or empty string to use the default. */
    final String endpoint;

    /** The protocol to use to talk to s3. Defaults to https. */
    final Protocol protocol;

    /** An optional proxy host that requests to s3 should be made through. */
    final String proxyHost;

    /** The port number the proxy host should be connected on. */
    final int proxyPort;

    // these should be "secure" yet the api for the s3 client only takes String, so storing them
    // as SecureString here won't really help with anything
    /** An optional username for the proxy host, for basic authentication. */
    final String proxyUsername;

    /** An optional password for the proxy host, for basic authentication. */
    final String proxyPassword;

    /** The read timeout for the s3 client. */
    final int readTimeoutMillis;

    /** The number of retries to use for the s3 client. */
    final int maxRetries;

    /** Whether the s3 client should use an exponential backoff retry policy. */
    final boolean throttleRetries;

    /** Whether the s3 client should use path style access. */
    final boolean pathStyleAccess;

    /** Whether chunked encoding should be disabled or not. */
    final boolean disableChunkedEncoding;

    /** Region to use for signing requests or empty string to use default. */
    final String region;

    /** Signer override to use or empty string to use default. */
    final String signerOverride;

    private S3ClientSettings(S3BasicCredentials credentials, String endpoint, Protocol protocol,
                             String proxyHost, int proxyPort, String proxyUsername, String proxyPassword,
                             int readTimeoutMillis, int maxRetries, boolean throttleRetries,
                             boolean pathStyleAccess, boolean disableChunkedEncoding, String region, String signerOverride) {
        this.credentials = credentials;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.readTimeoutMillis = readTimeoutMillis;
        this.maxRetries = maxRetries;
        this.throttleRetries = throttleRetries;
        this.pathStyleAccess = pathStyleAccess;
        this.disableChunkedEncoding = disableChunkedEncoding;
        this.region = region;
        this.signerOverride = signerOverride;
    }

    /**
     * Overrides the settings in this instance with settings found in repository metadata.
     *
     * @param repositorySettings found in repository metadata
     * @return S3ClientSettings
     */
    S3ClientSettings refine(Settings repositorySettings) {
        // Normalize settings to placeholder client settings prefix so that we can use the affix settings directly
        final Settings normalizedSettings =
            Settings.builder().put(repositorySettings).normalizePrefix(PREFIX + PLACEHOLDER_CLIENT + '.').build();
        final String newEndpoint = getRepoSettingOrDefault(ENDPOINT_SETTING, normalizedSettings, endpoint);

        final Protocol newProtocol = getRepoSettingOrDefault(PROTOCOL_SETTING, normalizedSettings, protocol);
        final String newProxyHost = getRepoSettingOrDefault(PROXY_HOST_SETTING, normalizedSettings, proxyHost);
        final int newProxyPort = getRepoSettingOrDefault(PROXY_PORT_SETTING, normalizedSettings, proxyPort);
        final int newReadTimeoutMillis = Math.toIntExact(
            getRepoSettingOrDefault(READ_TIMEOUT_SETTING, normalizedSettings, TimeValue.timeValueMillis(readTimeoutMillis)).millis());
        final int newMaxRetries = getRepoSettingOrDefault(MAX_RETRIES_SETTING, normalizedSettings, maxRetries);
        final boolean newThrottleRetries = getRepoSettingOrDefault(USE_THROTTLE_RETRIES_SETTING, normalizedSettings, throttleRetries);
        final boolean newPathStyleAccess = getRepoSettingOrDefault(USE_PATH_STYLE_ACCESS, normalizedSettings, pathStyleAccess);
        final boolean newDisableChunkedEncoding = getRepoSettingOrDefault(
            DISABLE_CHUNKED_ENCODING, normalizedSettings, disableChunkedEncoding);
        final String newRegion = getRepoSettingOrDefault(REGION, normalizedSettings, region);
        final String newSignerOverride = getRepoSettingOrDefault(SIGNER_OVERRIDE, normalizedSettings, signerOverride);
        if (Objects.equals(endpoint, newEndpoint) && protocol == newProtocol && Objects.equals(proxyHost, newProxyHost)
            && proxyPort == newProxyPort && newReadTimeoutMillis == readTimeoutMillis && maxRetries == newMaxRetries
            && newThrottleRetries == throttleRetries
            && newPathStyleAccess == pathStyleAccess
            && newDisableChunkedEncoding == disableChunkedEncoding
            && Objects.equals(region, newRegion) && Objects.equals(signerOverride, newSignerOverride)) {
            return this;
        }
        return new S3ClientSettings(
            credentials,
            newEndpoint,
            newProtocol,
            newProxyHost,
            newProxyPort,
            proxyUsername,
            proxyPassword,
            newReadTimeoutMillis,
            newMaxRetries,
            newThrottleRetries,
            newPathStyleAccess,
            newDisableChunkedEncoding,
            newRegion,
            newSignerOverride
        );
    }

    /**
     * Load all client settings from the given settings.
     *
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

    private static S3BasicCredentials loadCredentials(Settings settings, String clientName) {
        try (SecureString accessKey = getConfigValue(settings, clientName, ACCESS_KEY_SETTING);
             SecureString secretKey = getConfigValue(settings, clientName, SECRET_KEY_SETTING);
             SecureString sessionToken = getConfigValue(settings, clientName, SESSION_TOKEN_SETTING)) {
            if (accessKey.length() != 0) {
                if (secretKey.length() != 0) {
                    if (sessionToken.length() != 0) {
                        return new S3BasicSessionCredentials(accessKey.toString(), secretKey.toString(), sessionToken.toString());
                    } else {
                        return new S3BasicCredentials(accessKey.toString(), secretKey.toString());
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
        try (SecureString proxyUsername = getConfigValue(settings, clientName, PROXY_USERNAME_SETTING);
             SecureString proxyPassword = getConfigValue(settings, clientName, PROXY_PASSWORD_SETTING)) {
            return new S3ClientSettings(
                S3ClientSettings.loadCredentials(settings, clientName),
                getConfigValue(settings, clientName, ENDPOINT_SETTING),
                getConfigValue(settings, clientName, PROTOCOL_SETTING),
                getConfigValue(settings, clientName, PROXY_HOST_SETTING),
                getConfigValue(settings, clientName, PROXY_PORT_SETTING),
                proxyUsername.toString(),
                proxyPassword.toString(),
                Math.toIntExact(getConfigValue(settings, clientName, READ_TIMEOUT_SETTING).millis()),
                getConfigValue(settings, clientName, MAX_RETRIES_SETTING),
                getConfigValue(settings, clientName, USE_THROTTLE_RETRIES_SETTING),
                getConfigValue(settings, clientName, USE_PATH_STYLE_ACCESS),
                getConfigValue(settings, clientName, DISABLE_CHUNKED_ENCODING),
                getConfigValue(settings, clientName, REGION),
                getConfigValue(settings, clientName, SIGNER_OVERRIDE)
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
        return proxyPort == that.proxyPort &&
            readTimeoutMillis == that.readTimeoutMillis &&
            maxRetries == that.maxRetries &&
            throttleRetries == that.throttleRetries &&
            Objects.equals(credentials, that.credentials) &&
            Objects.equals(endpoint, that.endpoint) &&
            protocol == that.protocol &&
            Objects.equals(proxyHost, that.proxyHost) &&
            Objects.equals(proxyUsername, that.proxyUsername) &&
            Objects.equals(proxyPassword, that.proxyPassword) &&
            Objects.equals(disableChunkedEncoding, that.disableChunkedEncoding) &&
            Objects.equals(region, that.region) &&
            Objects.equals(signerOverride, that.signerOverride);
    }

    @Override
    public int hashCode() {
        return Objects.hash(credentials, endpoint, protocol, proxyHost, proxyPort, proxyUsername, proxyPassword,
            readTimeoutMillis, maxRetries, throttleRetries, disableChunkedEncoding, region, signerOverride);
    }

    private static <T> T getConfigValue(Settings settings, String clientName,
                                        Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }

    private static <T> T getRepoSettingOrDefault(Setting.AffixSetting<T> setting, Settings normalizedSettings, T defaultValue) {
        if (setting.getConcreteSettingForNamespace(PLACEHOLDER_CLIENT).exists(normalizedSettings)) {
            return getConfigValue(normalizedSettings, PLACEHOLDER_CLIENT, setting);
        }
        return defaultValue;
    }
}
