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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * A module for loading classes for node discovery.
 */
public class DiscoveryModule {

    public static final Setting<String> DISCOVERY_TYPE_SETTING =
        new Setting<>("discovery.type", "zen", Function.identity(), Property.NodeScope);
    public static final Setting<Optional<String>> DISCOVERY_HOSTS_PROVIDER_SETTING =
        new Setting<>("discovery.zen.hosts_provider", (String)null, Optional::ofNullable, Property.NodeScope);

    private final Discovery discovery;

    public DiscoveryModule(Settings settings, ThreadPool threadPool, TransportService transportService,
                           NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService, ClusterService clusterService,
                           List<DiscoveryPlugin> plugins) {
        final UnicastHostsProvider hostsProvider;

        Map<String, Supplier<UnicastHostsProvider>> hostProviders = new HashMap<>();
        for (DiscoveryPlugin plugin : plugins) {
            plugin.getZenHostsProviders(transportService, networkService).entrySet().forEach(entry -> {
                if (hostProviders.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Cannot register zen hosts provider [" + entry.getKey() + "] twice");
                }
            });
        }
        Optional<String> hostsProviderName = DISCOVERY_HOSTS_PROVIDER_SETTING.get(settings);
        if (hostsProviderName.isPresent()) {
            Supplier<UnicastHostsProvider> hostsProviderSupplier = hostProviders.get(hostsProviderName.get());
            if (hostsProviderSupplier == null) {
                throw new IllegalArgumentException("Unknown zen hosts provider [" + hostsProviderName.get() + "]");
            }
            hostsProvider = Objects.requireNonNull(hostsProviderSupplier.get());
        } else {
            hostsProvider = Collections::emptyList;
        }

        Map<String, Supplier<Discovery>> discoveryTypes = new HashMap<>();
        discoveryTypes.put("zen",
            () -> new ZenDiscovery(settings, threadPool, transportService, namedWriteableRegistry, clusterService, hostsProvider));
        discoveryTypes.put("none", () -> new NoneDiscovery(settings, clusterService, clusterService.getClusterSettings()));
        for (DiscoveryPlugin plugin : plugins) {
            plugin.getDiscoveryTypes(threadPool, transportService, namedWriteableRegistry,
                clusterService, hostsProvider).entrySet().forEach(entry -> {
                if (discoveryTypes.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Cannot register discovery type [" + entry.getKey() + "] twice");
                }
            });
        }
        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);
        Supplier<Discovery> discoverySupplier = discoveryTypes.get(discoveryType);
        if (discoverySupplier == null) {
            throw new IllegalArgumentException("Unknown discovery type [" + discoveryType + "]");
        }
        discovery = Objects.requireNonNull(discoverySupplier.get());
    }

    public Discovery getDiscovery() {
        return discovery;
    }
}
