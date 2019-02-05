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

package org.elasticsearch.discovery.azure.arm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
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

    private static final String AZURE_ARM = "azure-arm";
    private final Settings settings;
    private static final Logger logger = LogManager.getLogger(AzureArmDiscoveryPlugin.class);

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
            AzureClientSettings.CLIENT_ID_SETTING,
            AzureClientSettings.SECRET_SETTING,
            AzureClientSettings.SUBSCRIPTION_ID_SETTING,
            AzureClientSettings.TENANT_ID_SETTING,
            AzureClientSettings.HOST_RESOURCE_GROUP_SETTING,
            AzureClientSettings.HOST_NAME_SETTING,
            AzureClientSettings.HOST_TYPE_SETTING,
            AzureClientSettings.REFRESH_SETTING,
            AzureClientSettings.REGION_SETTING);
    }
}
