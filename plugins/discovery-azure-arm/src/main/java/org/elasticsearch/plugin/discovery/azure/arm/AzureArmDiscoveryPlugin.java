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

package org.elasticsearch.plugin.discovery.azure.arm;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cloud.azure.arm.AzureManagementService;
import org.elasticsearch.cloud.azure.arm.AzureManagementServiceImpl;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.azure.arm.AzureArmUnicastHostsProvider;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class AzureArmDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    public static final String AZURE_ARM = "azure-arm";
    private final Settings settings;
    private static final Logger logger = Loggers.getLogger(AzureArmDiscoveryPlugin.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public AzureArmDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        logger.trace("starting azure ARM discovery plugin...");
    }

    // overridable for tests
    protected AzureManagementService createManagementService() {
        return new AzureManagementServiceImpl(settings);
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        return Collections.singletonMap(AZURE_ARM,
            () -> new AzureArmUnicastHostsProvider(settings, createManagementService(), transportService));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            AzureManagementService.Management.CLIENT_ID_SETTING,
            AzureManagementService.Management.SECRET_SETTING,
            AzureManagementService.Management.SUBSCRIPTION_ID_SETTING,
            AzureManagementService.Management.TENANT_ID_SETTING,
            AzureManagementService.Discovery.HOST_GROUP_NAME_SETTING,
            AzureManagementService.Discovery.HOST_NAME_SETTING,
            AzureManagementService.Discovery.HOST_TYPE_SETTING,
            AzureManagementService.Discovery.REFRESH_SETTING,
            AzureManagementService.Discovery.REGION_SETTING);
    }
}
