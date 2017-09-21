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

package org.elasticsearch.plugin.repository.azure;

import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl;
import org.elasticsearch.cloud.azure.storage.AzureStorageSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.azure.AzureRepository;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A plugin to add a repository type that writes to and from the Azure cloud storage service.
 */
public class AzureRepositoryPlugin extends Plugin implements RepositoryPlugin {

    private final Map<String, AzureStorageSettings> clientsSettings;

    // overridable for tests
    protected AzureStorageService createStorageService(Settings settings) {
        return new AzureStorageServiceImpl(settings, clientsSettings);
    }

    public AzureRepositoryPlugin(Settings settings) {
        // eagerly load client settings so that secure settings are read
        clientsSettings = AzureStorageSettings.load(settings);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry) {
        return Collections.singletonMap(AzureRepository.TYPE,
            (metadata) -> new AzureRepository(metadata, env, namedXContentRegistry, createStorageService(env.settings())));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            AzureStorageService.Storage.STORAGE_ACCOUNTS,
            AzureStorageSettings.ACCOUNT_SETTING,
            AzureStorageSettings.KEY_SETTING,
            AzureStorageSettings.ENDPOINT_SUFFIX_SETTING,
            AzureStorageSettings.TIMEOUT_SETTING,
            AzureStorageSettings.PROXY_TYPE_SETTING,
            AzureStorageSettings.PROXY_HOST_SETTING,
            AzureStorageSettings.PROXY_PORT_SETTING
        );
    }

    @Override
    public List<String> getSettingsFilter() {
        // Cloud storage API settings using a pattern needed to be hidden
        return Arrays.asList(AzureStorageService.Storage.PREFIX + "*.account", AzureStorageService.Storage.PREFIX + "*.key");
    }
}
