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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.RepositorySettings;

import java.util.HashMap;
import java.util.Map;

public class AzureStorageSettings {
    private static ESLogger logger = ESLoggerFactory.getLogger(AzureStorageSettings.class.getName());

    private String name;
    private String account;
    private String key;
    private TimeValue timeout;

    public AzureStorageSettings(String name, String account, String key, TimeValue timeout) {
        this.name = name;
        this.account = account;
        this.key = key;
        this.timeout = timeout;
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

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("AzureStorageSettings{");
        sb.append("name='").append(name).append('\'');
        sb.append(", account='").append(account).append('\'');
        sb.append(", key='").append(key).append('\'');
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
        AzureStorageSettings primaryStorage = null;
        Map<String, AzureStorageSettings> secondaryStorage = new HashMap<>();

        // We check for deprecated settings
        String account = settings.get(Storage.ACCOUNT_DEPRECATED);
        String key = settings.get(Storage.KEY_DEPRECATED);

        TimeValue globalTimeout = settings.getAsTime(Storage.TIMEOUT, TimeValue.timeValueMinutes(5));

        if (account != null) {
            logger.warn("[{}] and [{}] have been deprecated. Use now [{}xxx.account] and [{}xxx.key] where xxx is any name",
                    Storage.ACCOUNT_DEPRECATED, Storage.KEY_DEPRECATED, Storage.PREFIX, Storage.PREFIX);
            primaryStorage = new AzureStorageSettings(null, account, key, globalTimeout);
        } else {
            Settings storageSettings = settings.getByPrefix(Storage.PREFIX);
            if (storageSettings != null) {
                Map<String, Object> asMap = storageSettings.getAsStructuredMap();
                for (Map.Entry<String, Object> storage : asMap.entrySet()) {
                    if (storage.getValue() instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, String> map = (Map) storage.getValue();
                        TimeValue timeout = TimeValue.parseTimeValue(map.get("timeout"), globalTimeout, Storage.PREFIX + storage.getKey() + ".timeout");
                        AzureStorageSettings current = new AzureStorageSettings(storage.getKey(), map.get("account"), map.get("key"), timeout);
                        boolean activeByDefault = Boolean.parseBoolean(map.getOrDefault("default", "false"));
                        if (activeByDefault) {
                            if (primaryStorage == null) {
                                primaryStorage = current;
                            } else {
                                logger.warn("default storage settings has already been defined. You can not define it to [{}]", storage.getKey());
                                secondaryStorage.put(storage.getKey(), current);
                            }
                        } else {
                            secondaryStorage.put(storage.getKey(), current);
                        }
                    }
                }
                // If we did not set any default storage, we should complain and define it
                if (primaryStorage == null && secondaryStorage.isEmpty() == false) {
                    Map.Entry<String, AzureStorageSettings> fallback = secondaryStorage.entrySet().iterator().next();
                    // We only warn if the number of secondary storage if > to 1
                    // If the user defined only one storage account, that's fine. We know it's the default one.
                    if (secondaryStorage.size() > 1) {
                        logger.warn("no default storage settings has been defined. " +
                                "Add \"default\": true to the settings you want to activate by default. " +
                                "Forcing default to [{}].", fallback.getKey());
                    }
                    primaryStorage = fallback.getValue();
                    secondaryStorage.remove(fallback.getKey());
                }
            }
        }

        return Tuple.tuple(primaryStorage, secondaryStorage);
    }

    public static String getRepositorySettings(RepositorySettings repositorySettings,
                                               String repositorySettingName,
                                               String repositoriesSettingName,
                                               String defaultValue) {
        return repositorySettings.settings().get(repositorySettingName,
            repositorySettings.globalSettings().get(repositoriesSettingName, defaultValue));
    }

    public static ByteSizeValue getRepositorySettingsAsBytesSize(RepositorySettings repositorySettings,
                                               String repositorySettingName,
                                               String repositoriesSettingName,
                                               ByteSizeValue defaultValue) {
        return repositorySettings.settings().getAsBytesSize(repositorySettingName,
            repositorySettings.globalSettings().getAsBytesSize(repositoriesSettingName, defaultValue));
    }

    public static Boolean getRepositorySettingsAsBoolean(RepositorySettings repositorySettings,
                                                                 String repositorySettingName,
                                                                 String repositoriesSettingName,
                                                         Boolean defaultValue) {
        return repositorySettings.settings().getAsBoolean(repositorySettingName,
            repositorySettings.globalSettings().getAsBoolean(repositoriesSettingName, defaultValue));
    }
}
