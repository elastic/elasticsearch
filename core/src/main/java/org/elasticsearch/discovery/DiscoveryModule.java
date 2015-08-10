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

import com.google.common.collect.Lists;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.local.LocalDiscovery;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A module for loading classes for node discovery.
 */
public class DiscoveryModule extends AbstractModule {

    public static final String DISCOVERY_TYPE_KEY = "discovery.type";

    private final Settings settings;
    private final List<Class<? extends UnicastHostsProvider>> unicastHostProviders = Lists.newArrayList();
    private final Map<String, Class<? extends Discovery>> discoveryTypes = new HashMap<>();

    public DiscoveryModule(Settings settings) {
        this.settings = settings;
        addDiscoveryType("local", LocalDiscovery.class);
        addDiscoveryType("zen", ZenDiscovery.class);
    }

    /**
     * Adds a custom unicast hosts provider to build a dynamic list of unicast hosts list when doing unicast discovery.
     */
    public void addUnicastHostProvider(Class<? extends UnicastHostsProvider> unicastHostProvider) {
        unicastHostProviders.add(unicastHostProvider);
    }

    /**
     * Adds a custom Discovery type.
     */
    public void addDiscoveryType(String type, Class<? extends Discovery> clazz) {
        discoveryTypes.put(type, clazz);
    }

    @Override
    protected void configure() {
        String defaultType = DiscoveryNode.localNode(settings) ? "local" : "zen";
        String discoveryType = settings.get(DISCOVERY_TYPE_KEY, defaultType);
        Class<? extends Discovery> discoveryClass = discoveryTypes.get(discoveryType);
        if (discoveryClass == null) {
            throw new IllegalArgumentException("Unknown Discovery type [" + discoveryType + "]");
        }

        if (discoveryType.equals("local") == false) {
            bind(ElectMasterService.class).asEagerSingleton();
            bind(ZenPingService.class).asEagerSingleton();
            Multibinder<UnicastHostsProvider> unicastHostsProviderMultibinder = Multibinder.newSetBinder(binder(), UnicastHostsProvider.class);
            for (Class<? extends UnicastHostsProvider> unicastHostProvider : unicastHostProviders) {
                unicastHostsProviderMultibinder.addBinding().to(unicastHostProvider);
            }
        }
        bind(Discovery.class).to(discoveryClass).asEagerSingleton();
        bind(DiscoveryService.class).asEagerSingleton();
    }
}