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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SettingsSource;
import org.elasticsearch.transport.local.LocalTransport;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

public class ClusterDiscoveryConfiguration extends SettingsSource {

    static Settings DEFAULT_NODE_SETTINGS = ImmutableSettings.settingsBuilder().put("discovery.type", "zen").build();

    final int numOfNodes;
    final Settings nodeSettings;
    final Settings transportClientSettings;

    public ClusterDiscoveryConfiguration(int numOfNodes, Settings extraSettings) {
        this.numOfNodes = numOfNodes;
        this.nodeSettings = ImmutableSettings.builder().put(DEFAULT_NODE_SETTINGS).put(extraSettings).build();
        this.transportClientSettings = ImmutableSettings.builder().put(extraSettings).build();
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

        private final int[] unicastHostOrdinals;
        private final int[] unicastHostPorts;
        private final boolean localMode;

        public UnicastZen(int numOfNodes) {
            this(numOfNodes, numOfNodes);
        }

        public UnicastZen(int numOfNodes, Settings extraSettings) {
            this(numOfNodes, numOfNodes, extraSettings);
        }

        public UnicastZen(int numOfNodes, int numOfUnicastHosts) {
            this(numOfNodes, numOfUnicastHosts, ImmutableSettings.EMPTY);
        }

        public UnicastZen(int numOfNodes, int numOfUnicastHosts, Settings extraSettings) {
            super(numOfNodes, extraSettings);
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
            this.unicastHostPorts = localMode ? new int[0] : unicastHostPorts(numOfNodes);
            assert localMode || unicastHostOrdinals.length <= unicastHostPorts.length;
        }

        public UnicastZen(int numOfNodes, int[] unicastHostOrdinals) {
            this(numOfNodes, ImmutableSettings.EMPTY, unicastHostOrdinals);
        }

        public UnicastZen(int numOfNodes, Settings extraSettings, int[] unicastHostOrdinals) {
            super(numOfNodes, extraSettings);
            this.unicastHostOrdinals = unicastHostOrdinals;
            this.localMode = nodeSettings.get("node.mode", InternalTestCluster.NODE_MODE).equals("local");
            this.unicastHostPorts = localMode ? new int[0] : unicastHostPorts(numOfNodes);
            assert localMode || unicastHostOrdinals.length <= unicastHostPorts.length;
        }

        private static int calcBasePort() {
            return 30000 + InternalTestCluster.BASE_PORT;
        }

        @Override
        public Settings node(int nodeOrdinal) {
            ImmutableSettings.Builder builder = ImmutableSettings.builder()
                    .put("discovery.zen.ping.multicast.enabled", false);

            String[] unicastHosts = new String[unicastHostOrdinals.length];
            if (localMode) {
                builder.put(LocalTransport.TRANSPORT_LOCAL_ADDRESS, "node_" + nodeOrdinal);
                for (int i = 0; i < unicastHosts.length; i++) {
                    unicastHosts[i] = "node_" + unicastHostOrdinals[i];
                }
            } else if (nodeOrdinal >= unicastHostPorts.length) {
                throw new ElasticsearchException("nodeOrdinal [" + nodeOrdinal + "] is greater than the number unicast ports [" + unicastHostPorts.length + "]");
            } else {
                // we need to pin the node port & host so we'd know where to point things
                builder.put("transport.tcp.port", unicastHostPorts[nodeOrdinal]);
                builder.put("transport.host", "localhost");
                for (int i = 0; i < unicastHostOrdinals.length; i++) {
                    unicastHosts[i] = "localhost:" + (unicastHostPorts[unicastHostOrdinals[i]]);
                }
            }
            builder.putArray("discovery.zen.ping.unicast.hosts", unicastHosts);
            return builder.put(super.node(nodeOrdinal)).build();
        }

        protected synchronized static int[] unicastHostPorts(int numHosts) {
            int[] unicastHostPorts = new int[numHosts];

            final int basePort = calcBasePort();
            final int maxPort = basePort + 1000;
            int tries = 0;
            for (int i = 0; i < unicastHostPorts.length; i++) {
                boolean foundPortInRange = false;
                while (tries < 1000 && !foundPortInRange) {
                    try (ServerSocket socket = new ServerSocket(nextPort)) {
                        // bind was a success
                        foundPortInRange = true;
                        unicastHostPorts[i] = nextPort;
                    } catch (IOException e) {
                        // Do nothing
                    }

                    nextPort++;
                    if (nextPort >= maxPort) {
                        // Roll back to the beginning of the range and do not go into another JVM's port range
                        nextPort = basePort;
                    }
                    tries++;
                }

                if (!foundPortInRange) {
                    throw new ElasticsearchException("could not find enough open ports in range [" + basePort + "-" + maxPort + "]. required [" + unicastHostPorts.length + "] ports");
                }
            }
            return unicastHostPorts;
        }
    }
}
