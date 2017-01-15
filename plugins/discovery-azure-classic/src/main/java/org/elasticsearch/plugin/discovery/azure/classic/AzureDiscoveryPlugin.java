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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.azure.classic.AzureUnicastHostsProvider;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class AzureDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    public static final String AZURE = "azure";
    protected final Settings settings;
    private static final Logger logger = Loggers.getLogger(AzureDiscoveryPlugin.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public AzureDiscoveryPlugin(Settings settings) {
        this.settings = settings;
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

    @Override
    public Map<String, Supplier<Discovery>> getDiscoveryTypes(ThreadPool threadPool, TransportService transportService,
                                                              NamedWriteableRegistry namedWriteableRegistry,
                                                              ClusterService clusterService, UnicastHostsProvider hostsProvider) {
        // this is for backcompat with pre 5.1, where users would set discovery.type to use ec2 hosts provider
        return Collections.singletonMap(AZURE, () ->
            new ZenDiscovery(settings, threadPool, transportService, namedWriteableRegistry, clusterService, hostsProvider));
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

    @Override
    public Settings additionalSettings() {
        // For 5.0, the hosts provider was "zen", but this was before the discovery.zen.hosts_provider
        // setting existed. This check looks for the legacy setting, and sets hosts provider if set
        String discoveryType = DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings);
        if (discoveryType.equals(AZURE)) {
            deprecationLogger.deprecated("Using " + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() +
                " setting to set hosts provider is deprecated. " +
                "Set \"" + DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey() + ": " + AZURE + "\" instead");
            if (DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.exists(settings) == false) {
                return Settings.builder().put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), AZURE).build();
            }
        }
        return Settings.EMPTY;
    }
}
