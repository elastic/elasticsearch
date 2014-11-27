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

package org.elasticsearch.discovery.zen;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;

import java.util.List;

/**
 */
public class ZenDiscoveryModule extends AbstractModule {

    private final List<Class<? extends UnicastHostsProvider>> unicastHostProviders = Lists.newArrayList();

    /**
     * Adds a custom unicast hosts provider to build a dynamic list of unicast hosts list when doing unicast discovery.
     */
    public ZenDiscoveryModule addUnicastHostProvider(Class<? extends UnicastHostsProvider> unicastHostProvider) {
        unicastHostProviders.add(unicastHostProvider);
        return this;
    }

    @Override
    protected void configure() {
        bind(ElectMasterService.class).asEagerSingleton();
        bind(ZenPingService.class).asEagerSingleton();
        Multibinder<UnicastHostsProvider> unicastHostsProviderMultibinder = Multibinder.newSetBinder(binder(), UnicastHostsProvider.class);
        for (Class<? extends UnicastHostsProvider> unicastHostProvider : unicastHostProviders) {
            unicastHostsProviderMultibinder.addBinding().to(unicastHostProvider);
        }
        bindDiscovery();
    }

    protected void bindDiscovery() {
        bind(Discovery.class).to(ZenDiscovery.class).asEagerSingleton();
    }
}
