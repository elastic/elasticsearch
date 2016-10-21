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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.test.NoopDiscovery;
import org.elasticsearch.transport.TransportService;

public class DiscoveryModuleTests extends ModuleTestCase {

    public interface DummyDiscoPlugin extends DiscoveryPlugin {
        Map<String, Supplier<UnicastHostsProvider>> impl();
        @Override
        default Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                                 NetworkService networkService) {
            return impl();
        }
    }

    public void testRegisterDefaults() {
        Settings settings = Settings.EMPTY;
        DiscoveryModule module = new DiscoveryModule(settings, null, null, Collections.emptyList());
        assertBinding(module, Discovery.class, ZenDiscovery.class);
    }

    public void testRegisterDiscovery() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "custom").build();
        DummyDiscoPlugin plugin = () -> Collections.singletonMap("custom", () -> Collections::emptyList);
        DiscoveryModule module = new DiscoveryModule(settings, null, null, Collections.singletonList(plugin));
        module.addDiscoveryType("custom", NoopDiscovery.class);
        assertBinding(module, Discovery.class, NoopDiscovery.class);
    }

    public void testHostsProvider() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "custom").build();
        final UnicastHostsProvider provider = Collections::emptyList;
        DummyDiscoPlugin plugin = () -> Collections.singletonMap("custom", () -> provider);
        DiscoveryModule module = new DiscoveryModule(settings, null, null, Collections.singletonList(plugin));
        assertInstanceBinding(module, UnicastHostsProvider.class, instance -> instance == provider);
    }

    public void testHostsProviderBwc() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "custom").build();
        final UnicastHostsProvider provider = Collections::emptyList;
        DummyDiscoPlugin plugin = () -> Collections.singletonMap("custom", () -> provider);
        DiscoveryModule module = new DiscoveryModule(settings, null, null, Collections.singletonList(plugin));
        module.addDiscoveryType("custom", NoopDiscovery.class);
        assertInstanceBinding(module, UnicastHostsProvider.class, instance -> instance == provider);
    }

    public void testUnknownHostsProvider() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "dne").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new DiscoveryModule(settings, null, null, Collections.emptyList()));
        assertEquals("Unknown zen hosts provider [dne]", e.getMessage());
    }

    public void testDuplicateHostsProvider() {
        DummyDiscoPlugin plugin1 = () -> Collections.singletonMap("dup", () -> null);
        DummyDiscoPlugin plugin2 = () -> Collections.singletonMap("dup", () -> null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new DiscoveryModule(Settings.EMPTY, null, null, Arrays.asList(plugin1, plugin2)));
        assertEquals("Cannot specify zen hosts provider [dup] twice", e.getMessage());
    }
}
