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

package org.elasticsearch.plugin.discovery.azure.classic;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeServiceImpl;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.azure.classic.AzureUnicastHostsProvider;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

public class AzureDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    public static final String AZURE = "azure";
    protected final Settings settings;
    protected final Logger logger = Loggers.getLogger(AzureDiscoveryPlugin.class);

    public AzureDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
        deprecationLogger.deprecated("azure classic discovery plugin is deprecated. Use azure arm discovery plugin instead");
        logger.trace("starting azure classic discovery plugin...");
    }

    // overrideable for tests
    protected AzureComputeService createComputeService() {
        return new AzureComputeServiceImpl(settings);
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        return Collections.singletonMap(AZURE,
            () -> new AzureUnicastHostsProvider(settings, createComputeService(), transportService, networkService));
    }

    public void onModule(DiscoveryModule discoveryModule) {
        if (isDiscoveryReady(settings, logger)) {
            discoveryModule.addDiscoveryType(AZURE, ZenDiscovery.class);
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(AzureComputeService.Discovery.REFRESH_SETTING,
                            AzureComputeService.Management.KEYSTORE_PASSWORD_SETTING,
                            AzureComputeService.Management.KEYSTORE_PATH_SETTING,
                            AzureComputeService.Management.KEYSTORE_TYPE_SETTING,
                            AzureComputeService.Management.SUBSCRIPTION_ID_SETTING,
                            AzureComputeService.Management.SERVICE_NAME_SETTING,
                            AzureComputeService.Discovery.HOST_TYPE_SETTING,
                            AzureComputeService.Discovery.DEPLOYMENT_NAME_SETTING,
                            AzureComputeService.Discovery.DEPLOYMENT_SLOT_SETTING,
                            AzureComputeService.Discovery.ENDPOINT_NAME_SETTING);
    }

    /**
     * Check if discovery is meant to start
     * @return true if we can start discovery features
     */
    private static boolean isDiscoveryReady(Settings settings, Logger logger) {
        // User set discovery.type: azure
        if (!AzureDiscoveryPlugin.AZURE.equalsIgnoreCase(DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings))) {
            logger.trace("discovery.type not set to {}", AzureDiscoveryPlugin.AZURE);
            return false;
        }

        if (isDefined(settings, AzureComputeService.Management.SUBSCRIPTION_ID_SETTING) &&
            isDefined(settings, AzureComputeService.Management.SERVICE_NAME_SETTING) &&
            isDefined(settings, AzureComputeService.Management.KEYSTORE_PATH_SETTING) &&
            isDefined(settings, AzureComputeService.Management.KEYSTORE_PASSWORD_SETTING)) {
            logger.trace("All required properties for Azure discovery are set!");
            return true;
        } else {
            logger.debug("One or more Azure discovery settings are missing. " +
                    "Check elasticsearch.yml file. Should have [{}], [{}], [{}] and [{}].",
                AzureComputeService.Management.SUBSCRIPTION_ID_SETTING.getKey(),
                AzureComputeService.Management.SERVICE_NAME_SETTING.getKey(),
                AzureComputeService.Management.KEYSTORE_PATH_SETTING.getKey(),
                AzureComputeService.Management.KEYSTORE_PASSWORD_SETTING.getKey());
            return false;
        }
    }

    private static boolean isDefined(Settings settings, Setting<String> property) throws ElasticsearchException {
        return (property.exists(settings) && Strings.hasText(property.get(settings)));
    }

}
