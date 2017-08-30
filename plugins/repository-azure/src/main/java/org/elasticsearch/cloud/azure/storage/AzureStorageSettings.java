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
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.AffixSetting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage.STORAGE_ACCOUNTS;

public final class AzureStorageSettings {
    // prefix for azure client settings
    private static final String PREFIX = "azure.client.";

    /**
     * Azure account name
     */
    public static final AffixSetting<SecureString> ACCOUNT_SETTING = Setting.affixKeySetting(PREFIX, "account",
        key -> SecureSetting.secureString(key, null));

    /**
     * max_retries: Number of retries in case of Azure errors. Defaults to 3 (RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT).
     */
    private static final Setting<Integer> MAX_RETRIES_SETTING =
        Setting.affixKeySetting(PREFIX, "max_retries",
            (key) -> Setting.intSetting(key, RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT, Setting.Property.NodeScope));

    /**
     * Azure key
     */
    public static final AffixSetting<SecureString> KEY_SETTING = Setting.affixKeySetting(PREFIX, "key",
        key -> SecureSetting.secureString(key, null));

    public static final AffixSetting<TimeValue> TIMEOUT_SETTING = Setting.affixKeySetting(PREFIX, "timeout",
        (key) -> Setting.timeSetting(key, Storage.TIMEOUT_SETTING, Property.NodeScope));


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
    private final TimeValue timeout;
    @Deprecated
    private final boolean activeByDefault;
    private final int maxRetries;

    public AzureStorageSettings(String account, String key, TimeValue timeout, int maxRetries) {
        this.name = null;
        this.account = account;
        this.key = key;
        this.timeout = timeout;
        this.activeByDefault = false;
        this.maxRetries = maxRetries;
    }

    @Deprecated
    public AzureStorageSettings(String name, String account, String key, TimeValue timeout, boolean activeByDefault, int maxRetries) {
        this.name = name;
        this.account = account;
        this.key = key;
        this.timeout = timeout;
        this.activeByDefault = activeByDefault;
        this.maxRetries = maxRetries;
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
        Set<String> clientNames = settings.getGroups(PREFIX).keySet();
        Map<String, AzureStorageSettings> storageSettings = new HashMap<>();
        for (String clientName : clientNames) {
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
                getValue(settings, clientName, TIMEOUT_SETTING),
                getValue(settings, clientName, MAX_RETRIES_SETTING));
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
