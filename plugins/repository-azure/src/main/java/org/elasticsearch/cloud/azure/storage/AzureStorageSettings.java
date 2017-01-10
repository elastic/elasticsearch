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

import org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class AzureStorageSettings {
    private static final Setting<TimeValue> TIMEOUT_SETTING = Setting.affixKeySetting(Storage.PREFIX, "timeout",
        (key) -> Setting.timeSetting(key, Storage.TIMEOUT_SETTING, Setting.Property.NodeScope));
    private static final Setting<String> ACCOUNT_SETTING =
        Setting.affixKeySetting(Storage.PREFIX, "account", (key) -> Setting.simpleString(key, Setting.Property.NodeScope));
    private static final Setting<String> KEY_SETTING =
        Setting.affixKeySetting(Storage.PREFIX, "key", (key) -> Setting.simpleString(key, Setting.Property.NodeScope));
    private static final Setting<Boolean> DEFAULT_SETTING =
        Setting.affixKeySetting(Storage.PREFIX, "default", (key) -> Setting.boolSetting(key, false, Setting.Property.NodeScope));


    private final String name;
    private final String account;
    private final String key;
    private final TimeValue timeout;
    private final boolean activeByDefault;

    public AzureStorageSettings(String name, String account, String key, TimeValue timeout, boolean activeByDefault) {
        this.name = name;
        this.account = account;
        this.key = key;
        this.timeout = timeout;
        this.activeByDefault = activeByDefault;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AzureStorageSettings{");
        sb.append("name='").append(name).append('\'');
        sb.append(", account='").append(account).append('\'');
        sb.append(", key='").append(key).append('\'');
        sb.append(", activeByDefault='").append(activeByDefault).append('\'');
        sb.append(", timeout=").append(timeout);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Parses settings and read all settings available under cloud.azure.storage.*
     * @param settings settings to parse
     * @return A tuple with v1 = primary storage and v2 = secondary storage
     */
    public static Tuple<AzureStorageSettings, Map<String, AzureStorageSettings>> parse(Settings settings) {
        List<AzureStorageSettings> storageSettings = createStorageSettings(settings);
        return Tuple.tuple(getPrimary(storageSettings), getSecondaries(storageSettings));
    }

    private static List<AzureStorageSettings> createStorageSettings(Settings settings) {
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
                    getValue(settings, groupName, DEFAULT_SETTING))
            );
        }
        return storageSettings;
    }

    private static <T> T getValue(Settings settings, String groupName, Setting<T> setting) {
        Setting.AffixKey k = (Setting.AffixKey) setting.getRawKey();
        String fullKey = k.toConcreteKey(groupName).toString();
        return setting.getConcreteSetting(fullKey).get(settings);
    }

    private static AzureStorageSettings getPrimary(List<AzureStorageSettings> settings) {
        if (settings.isEmpty()) {
            return null;
        } else if (settings.size() == 1) {
            // the only storage settings belong (implicitly) to the default primary storage
            AzureStorageSettings storage = settings.get(0);
            return new AzureStorageSettings(storage.getName(), storage.getAccount(), storage.getKey(), storage.getTimeout(), true);
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

    public static <T> T getValue(Settings repositorySettings,
                                 Settings globalSettings,
                                 Setting<T> repositorySetting,
                                 Setting<T> repositoriesSetting) {
        if (repositorySetting.exists(repositorySettings)) {
            return repositorySetting.get(repositorySettings);
        } else {
            return repositoriesSetting.get(globalSettings);
        }
    }

    public static <T> Setting<T> getEffectiveSetting(Settings repositorySettings,
                                              Setting<T> repositorySetting,
                                              Setting<T> repositoriesSetting) {
        if (repositorySetting.exists(repositorySettings)) {
            return repositorySetting;
        } else {
            return repositoriesSetting;
        }
    }
}
