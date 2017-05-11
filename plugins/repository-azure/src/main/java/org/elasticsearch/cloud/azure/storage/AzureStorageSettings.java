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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class AzureStorageSettings {
    private static final Setting<TimeValue> TIMEOUT_SETTING = Setting.affixKeySetting(Storage.PREFIX, "timeout",
        (key) -> Setting.timeSetting(key, Storage.TIMEOUT_SETTING, Setting.Property.NodeScope));
    private static final Setting<String> ACCOUNT_SETTING =
        Setting.affixKeySetting(Storage.PREFIX, "account", (key) -> Setting.simpleString(key, Setting.Property.NodeScope));
    private static final Setting<String> KEY_SETTING =
        Setting.affixKeySetting(Storage.PREFIX, "key", (key) -> Setting.simpleString(key, Setting.Property.NodeScope));
    private static final Setting<Boolean> DEFAULT_SETTING =
        Setting.affixKeySetting(Storage.PREFIX, "default", (key) -> Setting.boolSetting(key, false, Setting.Property.NodeScope));
    /** The host name of a proxy to connect to s3 through. */
    private static final Setting<String> PROXY_HOST_SETTING =
        Setting.affixKeySetting(Storage.PREFIX, "proxy.host", (key) -> Setting.simpleString(key, Setting.Property.NodeScope));
    /** The port of a proxy to connect to azure through. */
    private static final Setting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(Storage.PREFIX, "proxy.port",
        key -> Setting.intSetting(key, 80, 0, 1<<16, Setting.Property.NodeScope));
    /** The type of the proxy to connect to azure through. Can be direct (no proxy, default), http or socks */
    private static final Setting<Proxy.Type> PROXY_TYPE_SETTING = Setting.affixKeySetting(Storage.PREFIX, "proxy.type",
            key -> new Setting<>(key, "direct", s -> Proxy.Type.valueOf(s.toUpperCase(Locale.ROOT)), Setting.Property.NodeScope));

    /**
     * max_retries: Number of retries in case of Azure errors. Defaults to 3 (RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT).
     */
    private static final Setting<Integer> MAX_RETRIES_SETTING =
        Setting.affixKeySetting(Storage.PREFIX, "max_retries",
            (key) -> Setting.intSetting(key, RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT, Setting.Property.NodeScope));

    private final String name;
    private final String account;
    private final String key;
    private final TimeValue timeout;
    private final boolean activeByDefault;
    private final int maxRetries;
    private final String proxyHost;
    private final Integer proxyPort;
    private final Proxy.Type proxyType;
    private final Proxy proxy;

    public AzureStorageSettings(String name, String account, String key, TimeValue timeout, boolean activeByDefault, int maxRetries,
                                String proxyHost, Integer proxyPort, Proxy.Type proxyType) throws UnknownHostException {
        this.name = name;
        this.account = account;
        this.key = key;
        this.timeout = timeout;
        this.activeByDefault = activeByDefault;
        this.maxRetries = maxRetries;
        if (proxyType.equals(Proxy.Type.DIRECT)) {
            proxy = null;
        } else {
            proxy = new Proxy(proxyType, new InetSocketAddress(InetAddress.getByName(proxyHost), proxyPort));
        }
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyType = proxyType;
    }

    public String getName() {
        return name;
    }

    public String getKey() {
        return key;
    }

    public String getAccount() {
        return account;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public boolean isActiveByDefault() {
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
        sb.append(", maxRetries=").append(maxRetries);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Parses settings and read all settings available under cloud.azure.storage.*
     * @param settings settings to parse
     * @return A tuple with v1 = primary storage and v2 = secondary storage
     */
    public static Tuple<AzureStorageSettings, Map<String, AzureStorageSettings>> parse(Settings settings) throws UnknownHostException {
        List<AzureStorageSettings> storageSettings = createStorageSettings(settings);
        return Tuple.tuple(getPrimary(storageSettings), getSecondaries(storageSettings));
    }

    private static List<AzureStorageSettings> createStorageSettings(Settings settings) throws UnknownHostException {
        // ignore global timeout which has the same prefix but does not belong to any group
        Settings groups = Storage.STORAGE_ACCOUNTS.get(settings.filter((k) -> k.equals(Storage.TIMEOUT_SETTING.getKey()) == false));
        List<AzureStorageSettings> storageSettings = new ArrayList<>();
        for (String groupName : groups.getAsGroups().keySet()) {
            storageSettings.add(
                new AzureStorageSettings(
                    groupName,
                    getValue(settings, groupName, ACCOUNT_SETTING),
                    getValue(settings, groupName, KEY_SETTING),
                    getValue(settings, groupName, TIMEOUT_SETTING),
                    getValue(settings, groupName, DEFAULT_SETTING),
                    getValue(settings, groupName, MAX_RETRIES_SETTING),
                    getValue(settings, groupName, PROXY_HOST_SETTING),
                    getValue(settings, groupName, PROXY_PORT_SETTING),
                    getValue(settings, groupName, PROXY_TYPE_SETTING))
            );
        }
        return storageSettings;
    }

    private static <T> T getValue(Settings settings, String groupName, Setting<T> setting) {
        Setting.AffixKey k = (Setting.AffixKey) setting.getRawKey();
        String fullKey = k.toConcreteKey(groupName).toString();
        return setting.getConcreteSetting(fullKey).get(settings);
    }

    private static AzureStorageSettings getPrimary(List<AzureStorageSettings> settings) throws UnknownHostException {
        if (settings.isEmpty()) {
            return null;
        } else if (settings.size() == 1) {
            // the only storage settings belong (implicitly) to the default primary storage
            AzureStorageSettings storage = settings.get(0);
            return new AzureStorageSettings(storage.getName(), storage.getAccount(), storage.getKey(), storage.getTimeout(), true,
                storage.getMaxRetries(), storage.proxyHost, storage.proxyPort, storage.proxyType);
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
        return Collections.unmodifiableMap(secondaries);
    }
}
