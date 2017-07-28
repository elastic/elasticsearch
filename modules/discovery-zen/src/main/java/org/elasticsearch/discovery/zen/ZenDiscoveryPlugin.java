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

import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.UnicastHostsProvider;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ZenDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    public static final String ZEN_DISCOVERY_TYPE = "zen";

    protected Settings settings;

    public ZenDiscoveryPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Map<String, Supplier<Discovery>> getDiscoveryTypes(ThreadPool threadPool, TransportService transportService,
                                                              NamedWriteableRegistry namedWriteableRegistry,
                                                              MasterService masterService, ClusterApplier clusterApplier,
                                                              ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider,
                                                              AllocationService allocationService) {
        return Collections.singletonMap(ZEN_DISCOVERY_TYPE,
            () -> new ZenDiscovery(settings, threadPool, transportService, namedWriteableRegistry, masterService,
                clusterApplier, clusterSettings, hostsProvider, allocationService));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING,
            DiscoverySettings.PUBLISH_TIMEOUT_SETTING,
            DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING,
            DiscoverySettings.COMMIT_TIMEOUT_SETTING,
            DiscoverySettings.NO_MASTER_BLOCK_SETTING,
            FaultDetection.PING_RETRIES_SETTING,
            FaultDetection.PING_TIMEOUT_SETTING,
            FaultDetection.REGISTER_CONNECTION_LISTENER_SETTING,
            FaultDetection.PING_INTERVAL_SETTING,
            FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING,
            ZenDiscovery.PING_TIMEOUT_SETTING,
            ZenDiscovery.JOIN_TIMEOUT_SETTING,
            ZenDiscovery.JOIN_RETRY_ATTEMPTS_SETTING,
            ZenDiscovery.JOIN_RETRY_DELAY_SETTING,
            ZenDiscovery.MAX_PINGS_FROM_ANOTHER_MASTER_SETTING,
            ZenDiscovery.SEND_LEAVE_REQUEST_SETTING,
            ZenDiscovery.MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING,
            ZenDiscovery.MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING,
            ZenDiscovery.MAX_PENDING_CLUSTER_STATES_SETTING,
            UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING,
            UnicastHostsProvider.DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING,
            UnicastHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT
        );
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), ZEN_DISCOVERY_TYPE).build();
    }
}
