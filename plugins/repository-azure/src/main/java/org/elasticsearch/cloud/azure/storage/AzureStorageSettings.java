/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cloud.azure.storage;

import com.microsoft.azure.storage.RetryPolicy;
import org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.AffixSetting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage.STORAGE_ACCOUNTS;

public final class AzureStorageSettings {

    // prefix for azure client settings
    private static final String AZURE_CLIENT_PREFIX_KEY = "azure.client.";

    /** Azure account name */
    public static final AffixSetting<SecureString> ACCOUNT_SETTING =
        Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "account", key -> SecureSetting.secureString(key, null));

    /** Azure key */
    public static final AffixSetting<SecureString> KEY_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "key",
        key -> SecureSetting.secureString(key, null));

    /** max_retries: Number of retries in case of Azure errors. Defaults to 3 (RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT). */
    private static final Setting<Integer> MAX_RETRIES_SETTING =
        Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "max_retries",
            (key) -> Setting.intSetting(key, RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT, Setting.Property.NodeScope),
            ACCOUNT_SETTING, KEY_SETTING);
    /**
     * Azure endpoint suffix. Default to core.windows.net (CloudStorageAccount.DEFAULT_DNS).
     */
    public static final Setting<String> ENDPOINT_SUFFIX_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "endpoint_suffix",
        key -> Setting.simpleString(key, Property.NodeScope), ACCOUNT_SETTING, KEY_SETTING);

    public static final AffixSetting<TimeValue> TIMEOUT_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "timeout",
        (key) -> Setting.timeSetting(key, Storage.TIMEOUT_SETTING, Property.NodeScope), ACCOUNT_SETTING, KEY_SETTING);

    /** The type of the proxy to connect to azure through. Can be direct (no proxy, default), http or socks */
    public static final AffixSetting<Proxy.Type> PROXY_TYPE_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "proxy.type",
        (key) -> new Setting<>(key, "direct", s -> Proxy.Type.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope)
        , ACCOUNT_SETTING, KEY_SETTING);

    /** The host name of a proxy to connect to azure through. */
    public static final AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "proxy.host",
        (key) -> Setting.simpleString(key, Property.NodeScope), KEY_SETTING, ACCOUNT_SETTING, PROXY_TYPE_SETTING);

    /** The port of a proxy to connect to azure through. */
    public static final Setting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(AZURE_CLIENT_PREFIX_KEY, "proxy.port",
        (key) -> Setting.intSetting(key, 0, 0, 65535, Setting.Property.NodeScope), ACCOUNT_SETTING, KEY_SETTING, PROXY_TYPE_SETTING,
        PROXY_HOST_SETTING);


    @Deprecated
    public static final Setting<TimeValue> DEPRECATED_TIMEOUT_SETTING = Setting.affixKeySetting(Storage.PREFIX, "timeout",
        (key) -> Setting.timeSetting(key, Storage.TIMEOUT_SETTING, Property.NodeScope, Property.Deprecated));
    @Deprecated
    public static final Setting<String> DEPRECATED_ACCOUNT_SETTING = Setting.affixKeySetting(Storage.PREFIX, "account",
        (key) -> Setting.simpleString(key, Property.NodeScope, Property.Deprecated));
    @Deprecated
    public static final Setting<String> DEPRECATED_KEY_SETTING = Setting.affixKeySetting(Storage.PREFIX, "key",
        (key) -> Setting.simpleString(key, Property.NodeScope, Property.Deprecated));
    @Deprecated
    public static final Setting<Boolean> DEPRECATED_DEFAULT_SETTING = Setting.affixKeySetting(Storage.PREFIX, "default",
        (key) -> Setting.boolSetting(key, false, Property.NodeScope, Property.Deprecated));


    @Deprecated
    private final String name;
    private final String account;
    private final String key;
    private final String endpointSuffix;
    private final TimeValue timeout;
    @Deprecated
    private final boolean activeByDefault;
    private final int maxRetries;
    private final Proxy proxy;


    public AzureStorageSettings(String account, String key, String endpointSuffix, TimeValue timeout, int maxRetries,
                                Proxy.Type proxyType, String proxyHost, Integer proxyPort) {
        this.name = null;
        this.account = account;
        this.key = key;
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.activeByDefault = false;
        this.maxRetries = maxRetries;

        // Register the proxy if we have any
        // Validate proxy settings
        if (proxyType.equals(Proxy.Type.DIRECT) && (proxyPort != 0 || Strings.hasText(proxyHost))) {
            throw new SettingsException("Azure Proxy port or host have been set but proxy type is not defined.");
        }
        if (proxyType.equals(Proxy.Type.DIRECT) == false && (proxyPort == 0 || Strings.isEmpty(proxyHost))) {
            throw new SettingsException("Azure Proxy type has been set but proxy host or port is not defined.");
        }

        if (proxyType.equals(Proxy.Type.DIRECT)) {
            proxy = null;
        } else {
            try {
                proxy = new Proxy(proxyType, new InetSocketAddress(InetAddress.getByName(proxyHost), proxyPort));
            } catch (UnknownHostException e) {
                throw new SettingsException("Azure proxy host is unknown.", e);
            }
        }
    }

    @Deprecated
    public AzureStorageSettings(String name, String account, String key, TimeValue timeout, boolean activeByDefault, int maxRetries) {
        this.name = name;
        this.account = account;
        this.key = key;
        this.endpointSuffix = null;
        this.timeout = timeout;
        this.activeByDefault = activeByDefault;
        this.maxRetries = maxRetries;
        this.proxy = null;
    }

    @Deprecated
    public String getName() {
        return name;
    }

    public String getKey() {
        return key;
    }

    public String getAccount() {
        return account;
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    @Deprecated
    public Boolean isActiveByDefault() {
        return activeByDefault;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Proxy getProxy() {
        return proxy;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AzureStorageSettings{");
        sb.append("name='").append(name).append('\'');
        sb.append(", account='").append(account).append('\'');
        sb.append(", key='").append(key).append('\'');
        sb.append(", activeByDefault='").append(activeByDefault).append('\'');
        sb.append(", timeout=").append(timeout);
        sb.append(", endpointSuffix='").append(endpointSuffix).append('\'');
        sb.append(", maxRetries=").append(maxRetries);
        sb.append(", proxy=").append(proxy);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Parses settings and read all legacy settings available under cloud.azure.storage.*
     * @param settings settings to parse
     * @return A tuple with v1 = primary storage and v2 = secondary storage
     */
    @Deprecated
    public static Tuple<AzureStorageSettings, Map<String, AzureStorageSettings>> loadLegacy(Settings settings) {
        List<AzureStorageSettings> storageSettings = createStorageSettingsDeprecated(settings);
        return Tuple.tuple(getPrimary(storageSettings), getSecondaries(storageSettings));
    }

    /**
     * Parses settings and read all settings available under azure.client.*
     * @param settings settings to parse
     * @return All the named configurations
     */
    public static Map<String, AzureStorageSettings> load(Settings settings) {
        // Get the list of existing named configurations
        Map<String, AzureStorageSettings> storageSettings = new HashMap<>();
        for (String clientName : ACCOUNT_SETTING.getNamespaces(settings)) {
            storageSettings.put(clientName, getClientSettings(settings, clientName));
        }

        if (storageSettings.containsKey("default") == false && storageSettings.isEmpty() == false) {
            // in case no setting named "default" has been set, let's define our "default"
            // as the first named config we get
            AzureStorageSettings defaultSettings = storageSettings.values().iterator().next();
            storageSettings.put("default", defaultSettings);
        }
        return Collections.unmodifiableMap(storageSettings);
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    static AzureStorageSettings getClientSettings(Settings settings, String clientName) {
        try (SecureString account = getConfigValue(settings, clientName, ACCOUNT_SETTING);
             SecureString key = getConfigValue(settings, clientName, KEY_SETTING)) {
            return new AzureStorageSettings(account.toString(), key.toString(),
                getValue(settings, clientName, ENDPOINT_SUFFIX_SETTING),
                getValue(settings, clientName, TIMEOUT_SETTING),
                getValue(settings, clientName, MAX_RETRIES_SETTING),
                getValue(settings, clientName, PROXY_TYPE_SETTING),
                getValue(settings, clientName, PROXY_HOST_SETTING),
                getValue(settings, clientName, PROXY_PORT_SETTING));
        }
    }

    @Deprecated
    private static List<AzureStorageSettings> createStorageSettingsDeprecated(Settings settings) {
        // ignore global timeout which has the same prefix but does not belong to any group
        Settings groups = STORAGE_ACCOUNTS.get(settings.filter((k) -> k.equals(Storage.TIMEOUT_SETTING.getKey()) == false));
        List<AzureStorageSettings> storageSettings = new ArrayList<>();
        for (String groupName : groups.getAsGroups().keySet()) {
            storageSettings.add(
                new AzureStorageSettings(
                    groupName,
                    getValue(settings, groupName, DEPRECATED_ACCOUNT_SETTING),
                    getValue(settings, groupName, DEPRECATED_KEY_SETTING),
                    getValue(settings, groupName, DEPRECATED_TIMEOUT_SETTING),
                    getValue(settings, groupName, DEPRECATED_DEFAULT_SETTING),
                    getValue(settings, groupName, MAX_RETRIES_SETTING))
            );
        }
        return storageSettings;
    }

    private static <T> T getConfigValue(Settings settings, String clientName,
                                        Setting.AffixSetting<T> clientSetting) {
        Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }

    public static <T> T getValue(Settings settings, String groupName, Setting<T> setting) {
        Setting.AffixKey k = (Setting.AffixKey) setting.getRawKey();
        String fullKey = k.toConcreteKey(groupName).toString();
        return setting.getConcreteSetting(fullKey).get(settings);
    }

    @Deprecated
    private static AzureStorageSettings getPrimary(List<AzureStorageSettings> settings) {
        if (settings.isEmpty()) {
            return null;
        } else if (settings.size() == 1) {
            // the only storage settings belong (implicitly) to the default primary storage
            AzureStorageSettings storage = settings.get(0);
            return new AzureStorageSettings(storage.getName(), storage.getAccount(), storage.getKey(), storage.getTimeout(), true,
                storage.getMaxRetries());
        } else {
            AzureStorageSettings primary = null;
            for (AzureStorageSettings setting : settings) {
                if (setting.isActiveByDefault()) {
                    if (primary == null) {
                        primary = setting;
                    } else {
                        throw new SettingsException("Multiple default Azure data stores configured: [" + primary.getName() + "] and [" + setting.getName() + "]");
                    }
                }
            }
            if (primary == null) {
                throw new SettingsException("No default Azure data store configured");
            }
            return primary;
        }
    }

    @Deprecated
    private static Map<String, AzureStorageSettings> getSecondaries(List<AzureStorageSettings> settings) {
        Map<String, AzureStorageSettings> secondaries = new HashMap<>();
        // when only one setting is defined, we don't have secondaries
        if (settings.size() > 1) {
            for (AzureStorageSettings setting : settings) {
                if (setting.isActiveByDefault() == false) {
                    secondaries.put(setting.getName(), setting);
                }
            }
        }
        return secondaries;
    }
}
