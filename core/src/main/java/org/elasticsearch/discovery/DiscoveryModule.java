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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.discovery.local.LocalDiscovery;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A module for loading classes for node discovery.
 */
public class DiscoveryModule extends AbstractModule {

    public static final Setting<String> DISCOVERY_TYPE_SETTING =
        new Setting<>("discovery.type", settings -> DiscoveryNode.isLocalNode(settings) ? "local" : "zen", Function.identity(),
            Property.NodeScope);
    public static final Setting<String> ZEN_MASTER_SERVICE_TYPE_SETTING =
        new Setting<>("discovery.zen.masterservice.type", "zen", Function.identity(), Property.NodeScope);

    private final Settings settings;
    private final Map<String, List<Class<? extends UnicastHostsProvider>>> unicastHostProviders = new HashMap<>();
    private final ExtensionPoint.ClassSet<ZenPing> zenPings = new ExtensionPoint.ClassSet<>("zen_ping", ZenPing.class);
    private final Map<String, Class<? extends Discovery>> discoveryTypes = new HashMap<>();
    private final Map<String, Class<? extends ElectMasterService>> masterServiceType = new HashMap<>();

    public DiscoveryModule(Settings settings) {
        this.settings = settings;
        addDiscoveryType("local", LocalDiscovery.class);
        addDiscoveryType("zen", ZenDiscovery.class);
        addElectMasterService("zen", ElectMasterService.class);
        // always add the unicast hosts, or things get angry!
        addZenPing(UnicastZenPing.class);
    }

    /**
     * Adds a custom unicast hosts provider to build a dynamic list of unicast hosts list when doing unicast discovery.
     *
     * @param type discovery for which this provider is relevant
     * @param unicastHostProvider the host provider
     */
    public void addUnicastHostProvider(String type, Class<? extends UnicastHostsProvider> unicastHostProvider) {
        List<Class<? extends UnicastHostsProvider>> providerList = unicastHostProviders.get(type);
        if (providerList == null) {
            providerList = new ArrayList<>();
            unicastHostProviders.put(type, providerList);
        }
        providerList.add(unicastHostProvider);
    }

    /**
     * Adds a custom Discovery type.
     */
    public void addDiscoveryType(String type, Class<? extends Discovery> clazz) {
        if (discoveryTypes.containsKey(type)) {
            throw new IllegalArgumentException("discovery type [" + type + "] is already registered");
        }
        discoveryTypes.put(type, clazz);
    }

    /**
     * Adds a custom zen master service type.
     */
    public void addElectMasterService(String type, Class<? extends ElectMasterService> masterService) {
        if (masterServiceType.containsKey(type)) {
            throw new IllegalArgumentException("master service type [" + type + "] is already registered");
        }
        this.masterServiceType.put(type, masterService);
    }

    public void addZenPing(Class<? extends ZenPing> clazz) {
        zenPings.registerExtension(clazz);
    }

    @Override
    protected void configure() {
        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);
        Class<? extends Discovery> discoveryClass = discoveryTypes.get(discoveryType);
        if (discoveryClass == null) {
            throw new IllegalArgumentException("Unknown Discovery type [" + discoveryType + "]");
        }

        if (discoveryType.equals("local") == false) {
            String masterServiceTypeKey = ZEN_MASTER_SERVICE_TYPE_SETTING.get(settings);
            final Class<? extends ElectMasterService> masterService = masterServiceType.get(masterServiceTypeKey);
            if (masterService == null) {
                throw new IllegalArgumentException("Unknown master service type [" + masterServiceTypeKey + "]");
            }
            if (masterService == ElectMasterService.class) {
                bind(ElectMasterService.class).asEagerSingleton();
            } else {
                bind(ElectMasterService.class).to(masterService).asEagerSingleton();
            }
            bind(ZenPingService.class).asEagerSingleton();
            Multibinder<UnicastHostsProvider> unicastHostsProviderMultibinder = Multibinder.newSetBinder(binder(), UnicastHostsProvider.class);
            for (Class<? extends UnicastHostsProvider> unicastHostProvider :
                    unicastHostProviders.getOrDefault(discoveryType, Collections.emptyList())) {
                unicastHostsProviderMultibinder.addBinding().to(unicastHostProvider);
            }
            zenPings.bind(binder());
        }
        bind(Discovery.class).to(discoveryClass).asEagerSingleton();
    }
}
