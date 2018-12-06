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

package org.elasticsearch.test.discovery;

import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;

/**
 * A alternative zen discovery which allows using mocks for things like pings, as well as
 * giving access to internals.
 */
public class TestZenDiscovery extends ZenDiscovery {

    public static final Setting<Boolean> USE_MOCK_PINGS =
        Setting.boolSetting("discovery.zen.use_mock_pings", true, Setting.Property.NodeScope);

    public static final Setting<Boolean> USE_ZEN2 =
        Setting.boolSetting("discovery.zen.use_zen2", true, Setting.Property.NodeScope);

    /** A plugin which installs mock discovery and configures it to be used. */
    public static class TestPlugin extends Plugin implements DiscoveryPlugin {
        protected final Settings settings;
        public TestPlugin(Settings settings) {
            this.settings = settings;
        }

        @Override
        public Map<String, Supplier<Discovery>> getDiscoveryTypes(ThreadPool threadPool, TransportService transportService,
                                                                  NamedWriteableRegistry namedWriteableRegistry,
                                                                  MasterService masterService, ClusterApplier clusterApplier,
                                                                  ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider,
                                                                  AllocationService allocationService, GatewayMetaState gatewayMetaState) {
            // we don't get the latest setting which were updated by the extra settings for the plugin. TODO: fix.
            Settings fixedSettings = Settings.builder().put(settings).putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()).build();
            return Collections.singletonMap("test-zen", () -> {
                if (USE_ZEN2.get(settings)) {
                    return new Coordinator("test_node", fixedSettings, clusterSettings, transportService, namedWriteableRegistry,
                        allocationService, masterService,
                        () -> gatewayMetaState.getPersistedState(settings, (ClusterApplierService) clusterApplier), hostsProvider,
                        clusterApplier, new Random(Randomness.get().nextLong()));
                } else {
                    return new TestZenDiscovery(fixedSettings, threadPool, transportService, namedWriteableRegistry, masterService,
                        clusterApplier, clusterSettings, hostsProvider, allocationService, gatewayMetaState);
                }
            });
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(USE_MOCK_PINGS, USE_ZEN2);
        }

        @Override
        public Settings additionalSettings() {
            return Settings.builder()
                .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "test-zen")
                .putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey())
                .build();
        }
    }

    private TestZenDiscovery(Settings settings, ThreadPool threadPool, TransportService transportService,
                             NamedWriteableRegistry namedWriteableRegistry, MasterService masterService,
                             ClusterApplier clusterApplier, ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider,
                             AllocationService allocationService, GatewayMetaState gatewayMetaState) {
        super(settings, threadPool, transportService, namedWriteableRegistry, masterService, clusterApplier, clusterSettings,
            hostsProvider, allocationService, Collections.emptyList(), gatewayMetaState);
    }

    @Override
    protected ZenPing newZenPing(Settings settings, ThreadPool threadPool, TransportService transportService,
                                 UnicastHostsProvider hostsProvider) {
        if (USE_MOCK_PINGS.get(settings) && USE_ZEN2.get(settings) == false) {
            return new MockZenPing(this);
        } else {
            return super.newZenPing(settings, threadPool, transportService, hostsProvider);
        }
    }

    public ZenPing getZenPing() {
        return zenPing;
    }
}
