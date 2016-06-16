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

import org.elasticsearch.cloud.azure.AzureRepositoryModule;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.azure.AzureRepository;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class AzureRepositoryPlugin extends Plugin {

    private final Settings settings;
    protected final ESLogger logger = Loggers.getLogger(AzureRepositoryPlugin.class);

    public AzureRepositoryPlugin(Settings settings) {
        this.settings = settings;
        logger.trace("starting azure repository plugin...");
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.singletonList((Module) new AzureRepositoryModule(settings));
    }

    public void onModule(RepositoriesModule module) {
        logger.debug("registering repository type [{}]", AzureRepository.TYPE);
        module.registerRepository(AzureRepository.TYPE, AzureRepository.class, BlobStoreIndexShardRepository.class);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(AzureStorageService.Storage.ACCOUNT_SETTING,
        AzureStorageService.Storage.COMPRESS_SETTING,
        AzureStorageService.Storage.CONTAINER_SETTING,
        AzureStorageService.Storage.BASE_PATH_SETTING,
        AzureStorageService.Storage.CHUNK_SIZE_SETTING,
        AzureStorageService.Storage.LOCATION_MODE_SETTING);

    }

    @Override
    public List<String> getSettingsFilter() {
        // Cloud storage API settings using a pattern needed to be hidden
        return Arrays.asList(AzureStorageService.Storage.PREFIX + "*.account", AzureStorageService.Storage.PREFIX + "*.key");
    }
}
