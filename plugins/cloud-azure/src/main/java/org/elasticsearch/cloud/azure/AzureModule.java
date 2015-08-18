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

package org.elasticsearch.cloud.azure;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cloud.azure.management.AzureComputeService;
import org.elasticsearch.cloud.azure.management.AzureComputeService.Management;
import org.elasticsearch.cloud.azure.management.AzureComputeServiceImpl;
import org.elasticsearch.cloud.azure.management.AzureComputeSettingsFilter;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage;
import org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl;
import org.elasticsearch.cloud.azure.storage.AzureStorageSettingsFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.azure.AzureDiscovery;

/**
 * Azure Module
 *
 * <ul>
 * <li>If needed this module will bind azure discovery service by default
 * to AzureComputeServiceImpl.</li>
 * <li>If needed this module will bind azure repository service by default
 * to AzureStorageServiceImpl.</li>
 * </ul>
 *
 * @see org.elasticsearch.cloud.azure.management.AzureComputeServiceImpl
 * @see org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl
 */
public class AzureModule extends AbstractModule {
    protected final ESLogger logger;
    private Settings settings;

    // pkg private so it is settable by tests
    static Class<? extends AzureComputeService> computeServiceImpl = AzureComputeServiceImpl.class;
    static Class<? extends AzureStorageService> storageServiceImpl = AzureStorageServiceImpl.class;

    public static Class<? extends AzureComputeService> getComputeServiceImpl() {
        return computeServiceImpl;
    }

    public static Class<? extends AzureStorageService> getStorageServiceImpl() {
        return storageServiceImpl;
    }

    @Inject
    public AzureModule(Settings settings) {
        this.settings = settings;
        this.logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    protected void configure() {
        logger.debug("starting azure services");
        bind(AzureStorageSettingsFilter.class).asEagerSingleton();
        bind(AzureComputeSettingsFilter.class).asEagerSingleton();

        // If we have set discovery to azure, let's start the azure compute service
        if (isDiscoveryReady(settings, logger)) {
            logger.debug("starting azure discovery service");
            bind(AzureComputeService.class).to(computeServiceImpl).asEagerSingleton();
        }

        // If we have settings for azure repository, let's start the azure storage service
        if (isSnapshotReady(settings, logger)) {
            logger.debug("starting azure repository service");
            bind(AzureStorageService.class).to(storageServiceImpl).asEagerSingleton();
        }
    }

    /**
     * Check if discovery is meant to start
     * @return true if we can start discovery features
     */
    public static boolean isCloudReady(Settings settings) {
        return (settings.getAsBoolean("cloud.enabled", true));
    }

    /**
     * Check if discovery is meant to start
     * @return true if we can start discovery features
     */
    public static boolean isDiscoveryReady(Settings settings, ESLogger logger) {
        // Cloud services are disabled
        if (!isCloudReady(settings)) {
            logger.trace("cloud settings are disabled");
            return false;
        }

        // User set discovery.type: azure
        if (!AzureDiscovery.AZURE.equalsIgnoreCase(settings.get("discovery.type"))) {
            logger.trace("discovery.type not set to {}", AzureDiscovery.AZURE);
            return false;
        }

        if (isPropertyMissing(settings, Management.SUBSCRIPTION_ID) ||
                isPropertyMissing(settings, Management.SERVICE_NAME) ||
                isPropertyMissing(settings, Management.KEYSTORE_PATH) ||
                isPropertyMissing(settings, Management.KEYSTORE_PASSWORD)
            ) {
            logger.debug("one or more azure discovery settings are missing. " +
                            "Check elasticsearch.yml file. Should have [{}], [{}], [{}] and [{}].",
                    Management.SUBSCRIPTION_ID,
                    Management.SERVICE_NAME,
                    Management.KEYSTORE_PATH,
                    Management.KEYSTORE_PASSWORD);
            return false;
        }

        logger.trace("all required properties for azure discovery are set!");

        return true;
    }

    /**
     * Check if we have repository azure settings available
     * @return true if we can use snapshot and restore
     */
    public static boolean isSnapshotReady(Settings settings, ESLogger logger) {
        // Cloud services are disabled
        if (!isCloudReady(settings)) {
            logger.trace("cloud settings are disabled");
            return false;
        }

        if (isPropertyMissing(settings, Storage.ACCOUNT) ||
                isPropertyMissing(settings, Storage.KEY)) {
            logger.debug("azure repository is not set using [{}] and [{}] properties",
                    Storage.ACCOUNT,
                    Storage.KEY);
            return false;
        }

        logger.trace("all required properties for azure repository are set!");

        return true;
   }

    public static boolean isPropertyMissing(Settings settings, String name) throws ElasticsearchException {
        if (!Strings.hasText(settings.get(name))) {
            return true;
        }
        return false;
    }

}
