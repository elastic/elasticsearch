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

package org.elasticsearch.plugin.discovery.azure;

import org.elasticsearch.cloud.azure.AzureDiscoveryModule;
import org.elasticsearch.cloud.azure.management.AzureComputeService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.azure.AzureUnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class AzureDiscoveryPlugin extends Plugin {

    public static final String AZURE = "azure";
    private final Settings settings;
    protected final ESLogger logger = Loggers.getLogger(AzureDiscoveryPlugin.class);

    public AzureDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        logger.trace("starting azure discovery plugin...");
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.singletonList((Module) new AzureDiscoveryModule(settings));
    }

    public void onModule(DiscoveryModule discoveryModule) {
        if (AzureDiscoveryModule.isDiscoveryReady(settings, logger)) {
            discoveryModule.addDiscoveryType(AZURE, ZenDiscovery.class);
            discoveryModule.addUnicastHostProvider(AZURE, AzureUnicastHostsProvider.class);
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

}
