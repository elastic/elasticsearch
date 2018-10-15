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

import com.carrotsearch.randomizedtesting.SysGlobals;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.test.NodeConfigurationSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;

public class ClusterDiscoveryConfiguration extends NodeConfigurationSource {

    /**
     * The number of ports in the range used for this JVM
     */
    private static final int PORTS_PER_JVM = 100;

    private static final int JVM_ORDINAL = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_ID, "0"));

    /**
     * a per-JVM unique offset to be used for calculating unique port ranges.
     */
    private static final int JVM_BASE_PORT_OFFSET = PORTS_PER_JVM * (JVM_ORDINAL + 1);


    static Settings DEFAULT_NODE_SETTINGS = Settings.EMPTY;
    private static final String IP_ADDR = "127.0.0.1";

    final int numOfNodes;
    final Settings nodeSettings;
    final Settings transportClientSettings;

    public ClusterDiscoveryConfiguration(int numOfNodes, Settings extraSettings) {
        this.numOfNodes = numOfNodes;
        this.nodeSettings = Settings.builder().put(DEFAULT_NODE_SETTINGS).put(extraSettings).build();
        this.transportClientSettings = Settings.builder().put(extraSettings).build();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return nodeSettings;
    }

    @Override
    public Path nodeConfigPath(int nodeOrdinal) {
        return null;
    }

    @Override
    public Settings transportClientSettings() {
        return transportClientSettings;
    }

    public static class UnicastZen extends ClusterDiscoveryConfiguration {

        // this variable is incremented on each bind attempt and will maintain the next port that should be tried
        private static int nextPort = calcBasePort();

        private final int[] unicastHostOrdinals;
        private final int[] unicastHostPorts;

        public UnicastZen(int numOfNodes, Settings extraSettings) {
            super(numOfNodes, extraSettings);
            unicastHostOrdinals = new int[numOfNodes];
            for (int i = 0; i < numOfNodes; i++) {
                    unicastHostOrdinals[i] = i;
            }
            this.unicastHostPorts = unicastHostPorts(numOfNodes);
            assert unicastHostOrdinals.length <= unicastHostPorts.length;
        }

        private static int calcBasePort() {
            return 30000 + JVM_BASE_PORT_OFFSET;
        }

        @Override
        public Settings nodeSettings(int nodeOrdinal) {
            Settings.Builder builder = Settings.builder().put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), numOfNodes);
            return builder.put(super.nodeSettings(nodeOrdinal)).build();
        }

        @SuppressForbidden(reason = "we know we pass a IP address")
        protected static synchronized int[] unicastHostPorts(int numHosts) {
            int[] unicastHostPorts = new int[numHosts];

            final int basePort = calcBasePort();
            final int maxPort = basePort + PORTS_PER_JVM;
            int tries = 0;
            for (int i = 0; i < unicastHostPorts.length; i++) {
                boolean foundPortInRange = false;
                while (tries < PORTS_PER_JVM && !foundPortInRange) {
                    try (ServerSocket serverSocket = new MockServerSocket()) {
                        // Set SO_REUSEADDR as we may bind here and not be able to reuse the address immediately without it.
                        serverSocket.setReuseAddress(NetworkUtils.defaultReuseAddress());
                        serverSocket.bind(new InetSocketAddress(IP_ADDR, nextPort));
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
                    throw new ElasticsearchException("could not find enough open ports in range [" + basePort + "-" + maxPort
                            + "]. required [" + unicastHostPorts.length + "] ports");
                }
            }
            return unicastHostPorts;
        }
    }
}
