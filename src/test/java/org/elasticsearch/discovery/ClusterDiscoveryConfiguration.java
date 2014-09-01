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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.primitives.Ints;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SettingsSource;
import org.elasticsearch.transport.local.LocalTransport;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterDiscoveryConfiguration extends SettingsSource {

    public static Settings DEFAULT_SETTINGS = ImmutableSettings.settingsBuilder()
            .put("gateway.type", "local")
            .put("discovery.type", "zen")
            .build();

    final int numOfNodes;

    final Settings baseSettings;

    public ClusterDiscoveryConfiguration(int numOfNodes) {
        this(numOfNodes, ImmutableSettings.EMPTY);
    }

    public ClusterDiscoveryConfiguration(int numOfNodes, Settings extraSettings) {
        this.numOfNodes = numOfNodes;
        this.baseSettings = ImmutableSettings.builder().put(DEFAULT_SETTINGS).put(extraSettings).build();
    }

    @Override
    public Settings node(int nodeOrdinal) {
        return baseSettings;
    }

    @Override
    public Settings transportClient() {
        return baseSettings;
    }

    public static class UnicastZen extends ClusterDiscoveryConfiguration {

        private final static AtomicInteger portRangeCounter = new AtomicInteger();

        private final int[] unicastHostOrdinals;
        private final int basePort;

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
            this.basePort = calcBasePort();
        }

        public UnicastZen(int numOfNodes, int[] unicastHostOrdinals) {
            this(numOfNodes, ImmutableSettings.EMPTY, unicastHostOrdinals);
        }

        public UnicastZen(int numOfNodes, Settings extraSettings, int[] unicastHostOrdinals) {
            super(numOfNodes, extraSettings);
            this.unicastHostOrdinals = unicastHostOrdinals;
            this.basePort = calcBasePort();
        }

        private final static int calcBasePort() {
            // note that this has properly co-exist with the port logic at InternalTestCluster's constructor
            return 30000 +
                    1000 * (ElasticsearchIntegrationTest.CHILD_JVM_ID % 60) + // up to 60 jvms
                    100 * portRangeCounter.incrementAndGet(); // up to 100 nodes
        }


        @Override
        public Settings node(int nodeOrdinal) {
            ImmutableSettings.Builder builder = ImmutableSettings.builder()
                    .put("discovery.zen.ping.multicast.enabled", false);

            String[] unicastHosts = new String[unicastHostOrdinals.length];
            String mode = baseSettings.get("node.mode", InternalTestCluster.NODE_MODE);
            if (mode.equals("local")) {
                builder.put(LocalTransport.TRANSPORT_LOCAL_ADDRESS, "node_" + nodeOrdinal);
                for (int i = 0; i < unicastHosts.length; i++) {
                    unicastHosts[i] = "node_" + unicastHostOrdinals[i];
                }
            } else {
                // we need to pin the node port & host so we'd know where to point things
                builder.put("transport.tcp.port", basePort + nodeOrdinal);
                builder.put("transport.host", "localhost");
                for (int i = 0; i < unicastHosts.length; i++) {
                    unicastHosts[i] = "localhost:" + (basePort + unicastHostOrdinals[i]);
                }
            }
            builder.putArray("discovery.zen.ping.unicast.hosts", unicastHosts);
            return builder.put(super.node(nodeOrdinal)).build();
        }
    }
}
