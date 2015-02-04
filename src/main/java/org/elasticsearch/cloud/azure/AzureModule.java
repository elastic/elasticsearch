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
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl;
import org.elasticsearch.cloud.azure.management.AzureComputeService;
import org.elasticsearch.cloud.azure.management.AzureComputeServiceImpl;
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

    @Inject
    public AzureModule(Settings settings) {
        this.settings = settings;
        this.logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    protected void configure() {
        logger.debug("starting azure services");

        // If we have set discovery to azure, let's start the azure compute service
        if (isDiscoveryReady(settings, logger)) {
            logger.debug("starting azure discovery service");
            bind(AzureComputeService.class)
                    .to(settings.getAsClass("cloud.azure.api.impl", AzureComputeServiceImpl.class))
                    .asEagerSingleton();
        }

        // If we have settings for azure repository, let's start the azure storage service
        if (isSnapshotReady(settings, logger)) {
            logger.debug("starting azure repository service");
            bind(AzureStorageService.class)
                .to(settings.getAsClass("repositories.azure.api.impl", AzureStorageServiceImpl.class))
                .asEagerSingleton();
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

        if (    // We check new parameters
                (isPropertyMissing(settings, "cloud.azure.management." + AzureComputeService.Fields.SUBSCRIPTION_ID) ||
                        isPropertyMissing(settings, "cloud.azure.management." + AzureComputeService.Fields.SERVICE_NAME) ||
                        isPropertyMissing(settings, "cloud.azure.management." + AzureComputeService.Fields.KEYSTORE_PATH) ||
                        isPropertyMissing(settings, "cloud.azure.management." + AzureComputeService.Fields.KEYSTORE_PASSWORD))
                // We check deprecated
                && (isPropertyMissing(settings, "cloud.azure." + AzureComputeService.Fields.SUBSCRIPTION_ID_DEPRECATED) ||
                        isPropertyMissing(settings, "cloud.azure." + AzureComputeService.Fields.SERVICE_NAME_DEPRECATED) ||
                        isPropertyMissing(settings, "cloud.azure." + AzureComputeService.Fields.KEYSTORE_DEPRECATED) ||
                        isPropertyMissing(settings, "cloud.azure." + AzureComputeService.Fields.PASSWORD_DEPRECATED))
                ) {
            logger.debug("one or more azure discovery settings are missing. " +
                            "Check elasticsearch.yml file and `cloud.azure.management`. Should have [{}], [{}], [{}] and [{}].",
                    AzureComputeService.Fields.SUBSCRIPTION_ID,
                    AzureComputeService.Fields.SERVICE_NAME,
                    AzureComputeService.Fields.KEYSTORE_PATH,
                    AzureComputeService.Fields.KEYSTORE_PASSWORD);
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

        if ((isPropertyMissing(settings, "cloud.azure.storage." + AzureStorageService.Fields.ACCOUNT) ||
                isPropertyMissing(settings, "cloud.azure.storage." + AzureStorageService.Fields.KEY)) &&
                (isPropertyMissing(settings, "cloud.azure." + AzureStorageService.Fields.ACCOUNT_DEPRECATED) ||
                isPropertyMissing(settings, "cloud.azure." + AzureStorageService.Fields.KEY_DEPRECATED))) {
            logger.debug("azure repository is not set [using cloud.azure.storage.{}] and [cloud.azure.storage.{}] properties",
                    AzureStorageService.Fields.ACCOUNT,
                    AzureStorageService.Fields.KEY);
            return false;
        }

        logger.trace("all required properties for azure repository are set!");

        return true;
   }

    /**
     * Check if we are using any deprecated settings
     */
    public static void checkDeprecatedSettings(Settings settings, String oldParameter, String newParameter, ESLogger logger) {
        if (!isPropertyMissing(settings, oldParameter)) {
            logger.warn("using deprecated [{}]. Please change it to [{}] property.",
                    oldParameter,
                    newParameter);
        }
    }

    /**
     * Check all deprecated settings
     * @param settings
     * @param logger
     */
    public static void checkDeprecated(Settings settings, ESLogger logger) {
        // Cloud services are disabled
        if (isCloudReady(settings)) {
            checkDeprecatedSettings(settings, "cloud.azure." + AzureStorageService.Fields.ACCOUNT_DEPRECATED,
                    "cloud.azure.storage." + AzureStorageService.Fields.ACCOUNT, logger);
            checkDeprecatedSettings(settings, "cloud.azure." + AzureStorageService.Fields.KEY_DEPRECATED,
                    "cloud.azure.storage." + AzureStorageService.Fields.KEY, logger);

            // TODO Remove in 3.0.0
            checkDeprecatedSettings(settings, "cloud.azure." + AzureComputeService.Fields.KEYSTORE_DEPRECATED,
                    "cloud.azure.management." + AzureComputeService.Fields.KEYSTORE_PATH, logger);
            checkDeprecatedSettings(settings, "cloud.azure." + AzureComputeService.Fields.PASSWORD_DEPRECATED,
                    "cloud.azure.management." + AzureComputeService.Fields.KEYSTORE_PASSWORD, logger);
            checkDeprecatedSettings(settings, "cloud.azure." + AzureComputeService.Fields.SERVICE_NAME_DEPRECATED,
                    "cloud.azure.management." + AzureComputeService.Fields.SERVICE_NAME, logger);
            checkDeprecatedSettings(settings, "cloud.azure." + AzureComputeService.Fields.SUBSCRIPTION_ID_DEPRECATED,
                    "cloud.azure.management." + AzureComputeService.Fields.SUBSCRIPTION_ID, logger);
            checkDeprecatedSettings(settings, "cloud.azure." + AzureComputeService.Fields.HOST_TYPE_DEPRECATED,
                    "discovery.azure." + AzureComputeService.Fields.HOST_TYPE, logger);
            checkDeprecatedSettings(settings, "cloud.azure." + AzureComputeService.Fields.PORT_NAME_DEPRECATED,
                    "discovery.azure." + AzureComputeService.Fields.ENDPOINT_NAME, logger);
        }
    }

    public static boolean isPropertyMissing(Settings settings, String name) throws ElasticsearchException {
        if (!Strings.hasText(settings.get(name))) {
            return true;
        }
        return false;
    }

}
