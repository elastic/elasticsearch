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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.primitives.Ints;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SettingsSource;
import org.elasticsearch.transport.local.LocalTransport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ClusterDiscoveryConfiguration extends SettingsSource {

    static Settings DEFAULT_NODE_SETTINGS = Settings.settingsBuilder().put("discovery.type", "zen").build();

    final int numOfNodes;
    final Settings nodeSettings;
    final Settings transportClientSettings;

    public ClusterDiscoveryConfiguration(int numOfNodes, Settings extraSettings) {
        this.numOfNodes = numOfNodes;
        this.nodeSettings = Settings.builder().put(DEFAULT_NODE_SETTINGS).put(extraSettings).build();
        this.transportClientSettings = Settings.builder().put(extraSettings).build();
    }

    @Override
    public Settings node(int nodeOrdinal) {
        return nodeSettings;
    }

    @Override
    public Settings transportClient() {
        return transportClientSettings;
    }

    public static class UnicastZen extends ClusterDiscoveryConfiguration {

        // this variable is incremented on each bind attempt and will maintain the next port that should be tried
        private static int nextPort = calcBasePort();

        // since we run multiple test iterations, we need some flexibility in the choice of ports a node can have (port may
        // stay in use by previous iterations on some OSes - read CentOs). This controls the amount of ports each node
        // is assigned. All ports in range will be added to the unicast hosts, which is OK because we know only one will be used.
        private static final int NUM_PORTS_PER_NODE = 3;

        private final String[] unicastHosts;
        private final boolean localMode;

        public UnicastZen(int numOfNodes) {
            this(numOfNodes, numOfNodes);
        }

        public UnicastZen(int numOfNodes, Settings extraSettings) {
            this(numOfNodes, numOfNodes, extraSettings);
        }

        public UnicastZen(int numOfNodes, int numOfUnicastHosts) {
            this(numOfNodes, numOfUnicastHosts, Settings.EMPTY);
        }

        public UnicastZen(int numOfNodes, int numOfUnicastHosts, Settings extraSettings) {
            super(numOfNodes, extraSettings);
            int[] unicastHostOrdinals;
            if (numOfUnicastHosts == numOfNodes) {
                unicastHostOrdinals = new int[numOfNodes];
                for (int i = 0; i < numOfNodes; i++) {
                    unicastHostOrdinals[i] = i;
                }
            } else {
                Set<Integer> ordinals = new HashSet<>(numOfUnicastHosts);
                while (ordinals.size() != numOfUnicastHosts) {
                    ordinals.add(RandomizedTest.randomInt(numOfNodes - 1));
                }
                unicastHostOrdinals = Ints.toArray(ordinals);
            }
            this.localMode = nodeSettings.get("node.mode", InternalTestCluster.NODE_MODE).equals("local");
            this.unicastHosts = buildUnicastHostSetting(unicastHostOrdinals, localMode);
        }

        public UnicastZen(int numOfNodes, int[] unicastHostOrdinals) {
            this(numOfNodes, Settings.EMPTY, unicastHostOrdinals);
        }

        public UnicastZen(int numOfNodes, Settings extraSettings, int[] unicastHostOrdinals) {
            super(numOfNodes, extraSettings);
            this.localMode = nodeSettings.get("node.mode", InternalTestCluster.NODE_MODE).equals("local");
            this.unicastHosts = buildUnicastHostSetting(unicastHostOrdinals, localMode);
        }

        private static int calcBasePort() {
            return 30000 + InternalTestCluster.BASE_PORT;
        }

        private static String[] buildUnicastHostSetting(int[] unicastHostOrdinals, boolean localMode) {
            ArrayList<String> unicastHosts = new ArrayList<>();
            for (int i = 0; i < unicastHostOrdinals.length; i++) {
                final int hostOrdinal = unicastHostOrdinals[i];
                if (localMode) {
                    unicastHosts.add("node_" + hostOrdinal);
                } else {
                    // we need to pin the node port & host so we'd know where to point things
                    final int[] ports = nodePorts(hostOrdinal);
                    for (int port : ports) {
                        unicastHosts.add("localhost:" + port);
                    }
                }
            }
            return unicastHosts.toArray(new String[unicastHosts.size()]);
        }

        @Override
        public Settings node(int nodeOrdinal) {
            Settings.Builder builder = Settings.builder()
                    .put("discovery.zen.ping.multicast.enabled", false);

            if (localMode) {
                builder.put(LocalTransport.TRANSPORT_LOCAL_ADDRESS, "node_" + nodeOrdinal);
            } else {
                // we need to pin the node port & host so we'd know where to point things
                String ports = "";
                for (int port : nodePorts(nodeOrdinal)) {
                    ports += "," + port;
                }
                builder.put("transport.tcp.port", ports.substring(1));
                builder.put("transport.host", "localhost");
            }
            builder.putArray("discovery.zen.ping.unicast.hosts", unicastHosts);
            return builder.put(super.node(nodeOrdinal)).build();
        }

        protected static int[] nodePorts(int nodeOridnal) {
            int[] unicastHostPorts = new int[NUM_PORTS_PER_NODE];

            final int basePort = calcBasePort() + nodeOridnal * NUM_PORTS_PER_NODE;
            for (int i = 0; i < unicastHostPorts.length; i++) {
                unicastHostPorts[i] = basePort + i;
            }

            return unicastHostPorts;
        }
    }
}
