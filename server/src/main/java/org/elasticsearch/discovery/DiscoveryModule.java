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

package org.elasticsearch.discovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.single.SingleNodeDiscovery;
import org.elasticsearch.discovery.zen.FileBasedUnicastHostsProvider;
import org.elasticsearch.discovery.zen.SettingsBasedHostsProvider;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

/**
 * A module for loading classes for node discovery.
 */
public class DiscoveryModule {
    private static final Logger logger = LogManager.getLogger(DiscoveryModule.class);

    public static final String ZEN_DISCOVERY_TYPE = "zen";
    public static final String ZEN2_DISCOVERY_TYPE = "zen2";

    public static final Setting<String> DISCOVERY_TYPE_SETTING =
        new Setting<>("discovery.type", ZEN_DISCOVERY_TYPE, Function.identity(), Property.NodeScope);
    public static final Setting<List<String>> DISCOVERY_HOSTS_PROVIDER_SETTING =
        Setting.listSetting("discovery.zen.hosts_provider", Collections.emptyList(), Function.identity(), Property.NodeScope);

    private final Discovery discovery;

    public DiscoveryModule(Settings settings, ThreadPool threadPool, TransportService transportService,
                           NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService, MasterService masterService,
                           ClusterApplier clusterApplier, ClusterSettings clusterSettings, List<DiscoveryPlugin> plugins,
                           AllocationService allocationService, Path configFile, GatewayMetaState gatewayMetaState) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators = new ArrayList<>();
        final Map<String, Supplier<UnicastHostsProvider>> hostProviders = new HashMap<>();
        hostProviders.put("settings", () -> new SettingsBasedHostsProvider(settings, transportService));
        hostProviders.put("file", () -> new FileBasedUnicastHostsProvider(configFile));
        for (DiscoveryPlugin plugin : plugins) {
            plugin.getZenHostsProviders(transportService, networkService).forEach((key, value) -> {
                if (hostProviders.put(key, value) != null) {
                    throw new IllegalArgumentException("Cannot register zen hosts provider [" + key + "] twice");
                }
            });
            BiConsumer<DiscoveryNode, ClusterState> joinValidator = plugin.getJoinValidator();
            if (joinValidator != null) {
                joinValidators.add(joinValidator);
            }
        }
        List<String> hostsProviderNames = DISCOVERY_HOSTS_PROVIDER_SETTING.get(settings);
        // for bwc purposes, add settings provider even if not explicitly specified
        if (hostsProviderNames.contains("settings") == false) {
            List<String> extendedHostsProviderNames = new ArrayList<>();
            extendedHostsProviderNames.add("settings");
            extendedHostsProviderNames.addAll(hostsProviderNames);
            hostsProviderNames = extendedHostsProviderNames;
        }

        final Set<String> missingProviderNames = new HashSet<>(hostsProviderNames);
        missingProviderNames.removeAll(hostProviders.keySet());
        if (missingProviderNames.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown zen hosts providers " + missingProviderNames);
        }

        List<UnicastHostsProvider> filteredHostsProviders = hostsProviderNames.stream()
            .map(hostProviders::get).map(Supplier::get).collect(Collectors.toList());

        final UnicastHostsProvider hostsProvider = hostsResolver -> {
            final List<TransportAddress> addresses = new ArrayList<>();
            for (UnicastHostsProvider provider : filteredHostsProviders) {
                addresses.addAll(provider.buildDynamicHosts(hostsResolver));
            }
            return Collections.unmodifiableList(addresses);
        };

        Map<String, Supplier<Discovery>> discoveryTypes = new HashMap<>();
        discoveryTypes.put(ZEN_DISCOVERY_TYPE,
            () -> new ZenDiscovery(settings, threadPool, transportService, namedWriteableRegistry, masterService, clusterApplier,
                clusterSettings, hostsProvider, allocationService, Collections.unmodifiableCollection(joinValidators), gatewayMetaState));
        discoveryTypes.put(ZEN2_DISCOVERY_TYPE, () -> new Coordinator(NODE_NAME_SETTING.get(settings), settings, clusterSettings,
            transportService, namedWriteableRegistry, allocationService, masterService,
            () -> gatewayMetaState.getPersistedState(settings, (ClusterApplierService) clusterApplier), hostsProvider, clusterApplier,
            Randomness.get()));
        discoveryTypes.put("single-node", () -> new SingleNodeDiscovery(settings, transportService, masterService, clusterApplier));
        for (DiscoveryPlugin plugin : plugins) {
            plugin.getDiscoveryTypes(threadPool, transportService, namedWriteableRegistry, masterService, clusterApplier, clusterSettings,
                hostsProvider, allocationService, gatewayMetaState).forEach((key, value) -> {
                if (discoveryTypes.put(key, value) != null) {
                    throw new IllegalArgumentException("Cannot register discovery type [" + key + "] twice");
                }
            });
        }
        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);
        Supplier<Discovery> discoverySupplier = discoveryTypes.get(discoveryType);
        if (discoverySupplier == null) {
            throw new IllegalArgumentException("Unknown discovery type [" + discoveryType + "]");
        }
        logger.info("using discovery type [{}] and host providers {}", discoveryType, hostsProviderNames);
        discovery = Objects.requireNonNull(discoverySupplier.get());
    }

    public Discovery getDiscovery() {
        return discovery;
    }

}
