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

package org.elasticsearch.tribe;

import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class TribePlugin extends Plugin implements DiscoveryPlugin, ClusterPlugin {

    private final Settings settings;
    private TribeService tribeService;

    public TribePlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Map<String, Supplier<Discovery>> getDiscoveryTypes(ThreadPool threadPool, TransportService transportService,
                                                              NamedWriteableRegistry namedWriteableRegistry,
                                                              MasterService masterService, ClusterApplier clusterApplier,
                                                              ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider,
                                                              AllocationService allocationService) {
        return Collections.singletonMap("tribe", () -> new TribeDiscovery(settings, transportService, masterService, clusterApplier));
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        tribeService = new TribeService(settings, nodeEnvironment, clusterService, namedWriteableRegistry,
            nodeBuilder(environment.configFile()));
        return Collections.singleton(tribeService);
    }

    protected Function<Settings, Node> nodeBuilder(Path configPath) {
        return settings -> new Node(new Environment(settings, configPath));
    }

    @Override
    public void onNodeStarted() {
        tribeService.startNodes();
    }

    @Override
    public Settings additionalSettings() {
        if (TribeService.TRIBE_NAME_SETTING.exists(settings) == false) {
            Map<String, Settings> nodesSettings = settings.getGroups("tribe", true);
            if (nodesSettings.isEmpty()) {
                return Settings.EMPTY;
            }
            Settings.Builder sb = Settings.builder();

            if (Node.NODE_MASTER_SETTING.exists(settings)) {
                if (Node.NODE_MASTER_SETTING.get(settings)) {
                    throw new IllegalArgumentException("node cannot be tribe as well as master node");
                }
            } else {
                sb.put(Node.NODE_MASTER_SETTING.getKey(), false);
            }
            if (Node.NODE_DATA_SETTING.exists(settings)) {
                if (Node.NODE_DATA_SETTING.get(settings)) {
                    throw new IllegalArgumentException("node cannot be tribe as well as data node");
                }
            } else {
                sb.put(Node.NODE_DATA_SETTING.getKey(), false);
            }
            if (Node.NODE_INGEST_SETTING.exists(settings)) {
                if (Node.NODE_INGEST_SETTING.get(settings)) {
                    throw new IllegalArgumentException("node cannot be tribe as well as ingest node");
                }
            } else {
                sb.put(Node.NODE_INGEST_SETTING.getKey(), false);
            }

            if (!NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.exists(settings)) {
                sb.put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), nodesSettings.size());
            }
            sb.put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "tribe"); // there is a special discovery implementation for tribe
            // nothing is going to be discovered, since no master will be elected
            sb.put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), 0);
            if (sb.get("cluster.name") == null) {
                sb.put("cluster.name", "tribe_" + UUIDs.randomBase64UUID()); // make sure it won't join other tribe nodes in the same JVM
            }
            sb.put(TransportMasterNodeReadAction.FORCE_LOCAL_SETTING.getKey(), true);

            return sb.build();
        } else {
            for (String s : settings.keySet()) {
                if (s.startsWith("tribe.") && !s.equals(TribeService.TRIBE_NAME_SETTING.getKey())) {
                    throw new IllegalArgumentException("tribe cannot contain inner tribes: " + s);
                }
            }
        }
        return Settings.EMPTY;
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> defaults = Arrays.asList(
            TribeService.BLOCKS_METADATA_SETTING,
            TribeService.BLOCKS_WRITE_SETTING,
            TribeService.BLOCKS_WRITE_INDICES_SETTING,
            TribeService.BLOCKS_READ_INDICES_SETTING,
            TribeService.BLOCKS_METADATA_INDICES_SETTING,
            TribeService.ON_CONFLICT_SETTING,
            TribeService.TRIBE_NAME_SETTING
        );
        Map<String, Settings> nodesSettings = settings.getGroups("tribe", true);
        if (nodesSettings.isEmpty()) {
            return defaults;
        }
        List<Setting<?>> allSettings = new ArrayList<>(defaults);
        for (Map.Entry<String, Settings> entry : nodesSettings.entrySet()) {
            String prefix = "tribe." + entry.getKey() + ".";
            if (TribeService.TRIBE_SETTING_KEYS.stream().anyMatch(s -> s.startsWith(prefix))) {
                continue;
            }
            // create dummy setting just so that setting validation does not complain, these settings are going to be validated
            // again by the SettingsModule of the nested tribe node.
            Setting<String> setting = Setting.prefixKeySetting(prefix, (key) -> new Setting<>(key, "", Function.identity(),
                Setting.Property.NodeScope));
            allSettings.add(setting);
        }

        return allSettings;
    }

}
