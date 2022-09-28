/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery.azure.arm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class AzureArmDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    private static final Logger logger = LogManager.getLogger(AzureArmDiscoveryPlugin.class);

    private final Settings settings;

    public AzureArmDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        logger.trace("starting azure ARM discovery plugin...");
    }

    @Override
    public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService, NetworkService networkService) {
        return Collections.singletonMap(
            "azure-arm",
            () -> new AzureArmSeedHostsProvider(settings, createManagementService(), transportService)
        );
    }

    protected AzureManagementService createManagementService() {
        return new AzureManagementService(settings);
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
            AzureClientSettings.REGION_SETTING
        );
    }
}
