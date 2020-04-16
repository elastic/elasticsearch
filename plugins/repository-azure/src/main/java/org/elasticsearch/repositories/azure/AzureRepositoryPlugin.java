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

package org.elasticsearch.repositories.azure;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A plugin to add a repository type that writes to and from the Azure cloud storage service.
 */
public class AzureRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    public static final String REPOSITORY_THREAD_POOL_NAME = "repository_azure";

    // protected for testing
    final AzureStorageService azureStoreService;

    public AzureRepositoryPlugin(Settings settings) {
        // eagerly load client settings so that secure settings are read
        this.azureStoreService = createAzureStoreService(settings);
    }

    // non-static, package private for testing
    AzureStorageService createAzureStoreService(final Settings settings) {
        return new AzureStorageService(settings);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                           ClusterService clusterService) {
        return Collections.singletonMap(AzureRepository.TYPE,
                (metadata) -> new AzureRepository(metadata, namedXContentRegistry, azureStoreService, clusterService));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            AzureStorageSettings.ACCOUNT_SETTING,
            AzureStorageSettings.KEY_SETTING,
            AzureStorageSettings.SAS_TOKEN_SETTING,
            AzureStorageSettings.ENDPOINT_SUFFIX_SETTING,
            AzureStorageSettings.TIMEOUT_SETTING,
            AzureStorageSettings.MAX_RETRIES_SETTING,
            AzureStorageSettings.PROXY_TYPE_SETTING,
            AzureStorageSettings.PROXY_HOST_SETTING,
            AzureStorageSettings.PROXY_PORT_SETTING
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(executorBuilder());
    }

    public static ExecutorBuilder<?> executorBuilder() {
        return new ScalingExecutorBuilder(REPOSITORY_THREAD_POOL_NAME, 0, 32, TimeValue.timeValueSeconds(30L));
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        if (clientsSettings.isEmpty()) {
            throw new SettingsException("If you want to use an azure repository, you need to define a client configuration.");
        }
        azureStoreService.refreshAndClearCache(clientsSettings);
    }
}
