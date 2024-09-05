/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.AffixSetting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class AzureStorageSettings {

    public static final int DEFAULT_MAX_RETRIES = 3;

    // prefix for azure client settings
    private static final String AZURE_CLIENT_PREFIX_KEY = "azure.client.";

    /** Azure account name */
    public static final AffixSetting<SecureString> ACCOUNT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "account",
        key -> SecureSetting.secureString(key, null)
    );

    /** Azure key */
    public static final AffixSetting<SecureString> KEY_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "key",
        key -> SecureSetting.secureString(key, null)
    );

    /** Azure SAS token */
    public static final AffixSetting<SecureString> SAS_TOKEN_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "sas_token",
        key -> SecureSetting.secureString(key, null)
    );

    /** max_retries: Number of retries in case of Azure errors. Defaults to 3 (RequestRetryOptions). */
    public static final AffixSetting<Integer> MAX_RETRIES_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "max_retries",
        (key) -> Setting.intSetting(key, DEFAULT_MAX_RETRIES, Setting.Property.NodeScope),
        () -> ACCOUNT_SETTING
    );
    /**
     * Azure endpoint suffix. Default to core.windows.net (CloudStorageAccount.DEFAULT_DNS).
     */
    public static final AffixSetting<String> ENDPOINT_SUFFIX_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "endpoint_suffix",
        key -> Setting.simpleString(key, Property.NodeScope),
        () -> ACCOUNT_SETTING
    );

    public static final AffixSetting<String> ENDPOINT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "endpoint",
        key -> Setting.simpleString(key, Property.NodeScope),
        () -> ACCOUNT_SETTING
    );

    public static final AffixSetting<String> SECONDARY_ENDPOINT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "secondary_endpoint",
        key -> Setting.simpleString(key, Property.NodeScope),
        () -> ACCOUNT_SETTING
    );

    public static final AffixSetting<TimeValue> TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueMinutes(-1), Property.NodeScope),
        () -> ACCOUNT_SETTING
    );

    /** The type of the proxy to connect to azure through. Can be direct (no proxy, default), http or socks */
    public static final AffixSetting<Proxy.Type> PROXY_TYPE_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.type",
        (key) -> new Setting<>(key, "direct", s -> Proxy.Type.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope),
        () -> ACCOUNT_SETTING
    );

    /** The host name of a proxy to connect to azure through. */
    public static final AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.host",
        (key) -> Setting.simpleString(key, Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING
    );

    /** The port of a proxy to connect to azure through. */
    public static final Setting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.port",
        (key) -> Setting.intSetting(key, 0, 0, 65535, Setting.Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING
    );

    private final String account;
    private final String connectString;
    private final String endpointSuffix;
    private final TimeValue timeout;
    private final int maxRetries;
    private final Proxy proxy;
    private final boolean hasCredentials;
    private final Set<String> credentialsUsageFeatures;

    private AzureStorageSettings(
        String account,
        String key,
        String sasToken,
        String endpointSuffix,
        TimeValue timeout,
        int maxRetries,
        Proxy.Type proxyType,
        String proxyHost,
        Integer proxyPort,
        String endpoint,
        String secondaryEndpoint
    ) {
        this.account = account;
        this.connectString = buildConnectString(account, key, sasToken, endpointSuffix, endpoint, secondaryEndpoint);
        this.hasCredentials = Strings.hasText(key) || Strings.hasText(sasToken);
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        this.credentialsUsageFeatures = Strings.hasText(key) ? Set.of("uses_key_credentials")
            : Strings.hasText(sasToken) ? Set.of("uses_sas_token")
            : SocketAccess.doPrivilegedException(() -> System.getenv("AZURE_FEDERATED_TOKEN_FILE")) == null
                ? Set.of("uses_default_credentials", "uses_managed_identity")
            : Set.of("uses_default_credentials", "uses_workload_identity");

        // Register the proxy if we have any
        // Validate proxy settings
        if (proxyType.equals(Proxy.Type.DIRECT) && ((proxyPort != 0) || Strings.hasText(proxyHost))) {
            throw new SettingsException("Azure Proxy port or host have been set but proxy type is not defined.");
        }
        if ((proxyType.equals(Proxy.Type.DIRECT) == false) && ((proxyPort == 0) || Strings.isEmpty(proxyHost))) {
            throw new SettingsException("Azure Proxy type has been set but proxy host or port is not defined.");
        }

        if (proxyType.equals(Proxy.Type.DIRECT)) {
            proxy = null;
        } else {
            try {
                proxy = new Proxy(proxyType, new InetSocketAddress(InetAddress.getByName(proxyHost), proxyPort));
            } catch (final UnknownHostException e) {
                throw new SettingsException("Azure proxy host is unknown.", e);
            }
        }
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Proxy getProxy() {
        return proxy;
    }

    public String getConnectString() {
        return connectString;
    }

    private static String buildConnectString(
        String account,
        @Nullable String key,
        @Nullable String sasToken,
        String endpointSuffix,
        @Nullable String endpoint,
        @Nullable String secondaryEndpoint
    ) {
        final boolean hasSasToken = Strings.hasText(sasToken);
        final boolean hasKey = Strings.hasText(key);
        if (hasSasToken && hasKey) {
            throw new SettingsException("Both a secret as well as a shared access token were set for account [" + account + "]");
        }
        final StringBuilder connectionStringBuilder = new StringBuilder();
        connectionStringBuilder.append("DefaultEndpointsProtocol=https").append(";AccountName=").append(account);
        if (hasKey) {
            connectionStringBuilder.append(";AccountKey=").append(key);
        } else if (hasSasToken) {
            connectionStringBuilder.append(";SharedAccessSignature=").append(sasToken);
        } else {
            connectionStringBuilder.append(";AccountKey=none"); // required for validation, but ignored
        }
        final boolean hasEndpointSuffix = Strings.hasText(endpointSuffix);
        final boolean hasEndpoint = Strings.hasText(endpoint);
        final boolean hasSecondaryEndpoint = Strings.hasText(secondaryEndpoint);

        if (hasEndpointSuffix && hasEndpoint) {
            throw new SettingsException("Both an endpoint suffix as well as a primary endpoint were set for account [" + account + "]");
        }

        if (hasEndpointSuffix && hasSecondaryEndpoint) {
            throw new SettingsException("Both an endpoint suffix as well as a secondary endpoint were set for account [" + account + "]");
        }

        if (hasEndpoint == false && hasSecondaryEndpoint) {
            throw new SettingsException("A primary endpoint is required when setting a secondary endpoint for account [" + account + "]");
        }

        if (hasEndpointSuffix) {
            connectionStringBuilder.append(";EndpointSuffix=").append(endpointSuffix);
        }

        if (hasEndpoint) {
            connectionStringBuilder.append(";BlobEndpoint=").append(endpoint);
        }

        if (hasSecondaryEndpoint) {
            connectionStringBuilder.append(";BlobSecondaryEndpoint=").append(secondaryEndpoint);
        }

        return connectionStringBuilder.toString();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AzureStorageSettings{");
        sb.append("account='").append(account).append('\'');
        sb.append(", timeout=").append(timeout);
        sb.append(", endpointSuffix='").append(endpointSuffix).append('\'');
        sb.append(", maxRetries=").append(maxRetries);
        sb.append(", proxy=").append(proxy);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Parse and read all settings available under the azure.client.* namespace
     * @param settings settings to parse
     * @return All the named configurations
     */
    public static Map<String, AzureStorageSettings> load(Settings settings) {
        // Get the list of existing named configurations
        final Map<String, AzureStorageSettings> storageSettings = new HashMap<>();
        for (final String clientName : ACCOUNT_SETTING.getNamespaces(settings)) {
            storageSettings.put(clientName, getClientSettings(settings, clientName));
        }
        if (false == storageSettings.containsKey("default") && false == storageSettings.isEmpty()) {
            // in case no setting named "default" has been set, let's define our "default"
            // as the first named config we get
            final AzureStorageSettings defaultSettings = storageSettings.values().iterator().next();
            storageSettings.put("default", defaultSettings);
        }
        assert storageSettings.containsKey("default") || storageSettings.isEmpty() : "always have 'default' if any";
        return Collections.unmodifiableMap(storageSettings);
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    private static AzureStorageSettings getClientSettings(Settings settings, String clientName) {
        try (
            SecureString account = getConfigValue(settings, clientName, ACCOUNT_SETTING);
            SecureString key = getConfigValue(settings, clientName, KEY_SETTING);
            SecureString sasToken = getConfigValue(settings, clientName, SAS_TOKEN_SETTING)
        ) {
            return new AzureStorageSettings(
                account.toString(),
                key.toString(),
                sasToken.toString(),
                getValue(settings, clientName, ENDPOINT_SUFFIX_SETTING),
                getValue(settings, clientName, TIMEOUT_SETTING),
                getValue(settings, clientName, MAX_RETRIES_SETTING),
                getValue(settings, clientName, PROXY_TYPE_SETTING),
                getValue(settings, clientName, PROXY_HOST_SETTING),
                getValue(settings, clientName, PROXY_PORT_SETTING),
                getValue(settings, clientName, ENDPOINT_SETTING),
                getValue(settings, clientName, SECONDARY_ENDPOINT_SETTING)
            );
        }
    }

    private static <T> T getConfigValue(Settings settings, String clientName, Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }

    private static <T> T getValue(Settings settings, String groupName, Setting<T> setting) {
        final Setting.AffixKey k = (Setting.AffixKey) setting.getRawKey();
        final String fullKey = k.toConcreteKey(groupName).toString();
        return setting.getConcreteSetting(fullKey).get(settings);
    }

    private static final String BLOB_ENDPOINT_NAME = "BlobEndpoint";
    private static final String BLOB_SECONDARY_ENDPOINT_NAME = "BlobSecondaryEndpoint";

    public boolean hasCredentials() {
        return hasCredentials;
    }

    record StorageEndpoint(String primaryURI, @Nullable String secondaryURI) {}

    StorageEndpoint getStorageEndpoint() {
        String primaryURI = getProperty(BLOB_ENDPOINT_NAME);
        String secondaryURI = getProperty(BLOB_SECONDARY_ENDPOINT_NAME);
        if (primaryURI != null) {
            return new StorageEndpoint(primaryURI, secondaryURI);
        }
        return new StorageEndpoint(deriveURIFromSettings(true), deriveURIFromSettings(false));
    }

    /**
     * Returns the value for the given property name, or null if not configured.
     * @throws IllegalArgumentException if the connectionString is malformed
     */
    private String getProperty(String propertyName) {
        final String[] settings = getConnectString().split(";");
        for (int i = 0; i < settings.length; i++) {
            String setting = settings[i].trim();
            if (setting.length() > 0) {
                final int idx = setting.indexOf('=');
                if (idx == -1 || idx == 0 || idx == settings[i].length() - 1) {
                    new IllegalArgumentException("Invalid connection string: " + getConnectString());
                }
                if (propertyName.equals(setting.substring(0, idx))) {
                    return setting.substring(idx + 1);
                }
            }
        }
        return null;
    }

    private static final String DEFAULT_DNS = "core.windows.net";

    /** Derives the primary or secondary endpoint from the settings. */
    private String deriveURIFromSettings(boolean isPrimary) {
        String uriString = new StringBuilder().append("https://")
            .append(account)
            .append(isPrimary ? "" : "-secondary")
            .append(".blob.")
            .append(Strings.isNullOrEmpty(endpointSuffix) ? DEFAULT_DNS : endpointSuffix)
            .toString();
        try {
            return new URI(uriString).toString();  // validates the URI
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public Set<String> credentialsUsageFeatures() {
        return credentialsUsageFeatures;
    }
}
