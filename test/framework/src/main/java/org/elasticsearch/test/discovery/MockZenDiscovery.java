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

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * A mock version of zen discovery for tests which uses
 */
public class MockZenDiscovery extends ZenDiscovery {

    /** A plugin which installs mock discovery and configures it to be used. */
    public static class TestPlugin extends Plugin implements DiscoveryPlugin {
        private Settings settings;
        public TestPlugin(Settings settings) {
            this.settings = settings;
        }
        @Override
        public Map<String, Supplier<Discovery>> getDiscoveryTypes(ThreadPool threadPool, TransportService transportService,
                                                                  ClusterService clusterService, UnicastHostsProvider hostsProvider) {
            return Collections.singletonMap("mock-zen",
                () -> new MockZenDiscovery(settings, threadPool, transportService, clusterService, hostsProvider));
        }

        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "mock-zen").build();
        }
    }

    private MockZenDiscovery(Settings settings, ThreadPool threadPool, TransportService transportService,
                            ClusterService clusterService, UnicastHostsProvider hostsProvider) {
        super(settings, threadPool, transportService, clusterService, hostsProvider);
    }

    @Override
    protected ZenPing newZenPing(Settings settings, ThreadPool threadPool, TransportService transportService,
                                 UnicastHostsProvider hostsProvider) {
        return new MockZenPing(settings);
    }
}
