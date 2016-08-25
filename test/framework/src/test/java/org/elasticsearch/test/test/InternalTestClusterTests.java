
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
package org.elasticsearch.test.test;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.node.DiscoveryNode.Role.DATA;
import static org.elasticsearch.cluster.node.DiscoveryNode.Role.INGEST;
import static org.elasticsearch.cluster.node.DiscoveryNode.Role.MASTER;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileNotExists;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;

/**
 * Basic test that ensure that the internal cluster reproduces the same
 * configuration given the same seed / input.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // doesn't work with potential multi data path from test cluster yet
public class InternalTestClusterTests extends ESTestCase {

    public void testInitializiationIsConsistent() {
        long clusterSeed = randomLong();
        boolean masterNodes = randomBoolean();
        int minNumDataNodes = randomIntBetween(0, 9);
        int maxNumDataNodes = randomIntBetween(minNumDataNodes, 10);
        String clusterName = randomRealisticUnicodeOfCodepointLengthBetween(1, 10);
        NodeConfigurationSource nodeConfigurationSource = NodeConfigurationSource.EMPTY;
        int numClientNodes = randomIntBetween(0, 10);
        boolean enableHttpPipelining = randomBoolean();
        String nodePrefix = randomRealisticUnicodeOfCodepointLengthBetween(1, 10);

        Path baseDir = createTempDir();
        InternalTestCluster cluster0 = new InternalTestCluster(clusterSeed, baseDir, masterNodes,
            minNumDataNodes, maxNumDataNodes, clusterName, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());
        InternalTestCluster cluster1 = new InternalTestCluster(clusterSeed, baseDir, masterNodes,
            minNumDataNodes, maxNumDataNodes, clusterName, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());
        // TODO: this is not ideal - we should have a way to make sure ports are initialized in the same way
        assertClusters(cluster0, cluster1, false);

    }

    /**
     * a set of settings that are expected to have different values betweem clusters, even they have been initialized with the same
     * base settins.
     */
    static final Set<String> clusterUniqueSettings = new HashSet<>();

    static {
        clusterUniqueSettings.add(ClusterName.CLUSTER_NAME_SETTING.getKey());
        clusterUniqueSettings.add(TransportSettings.PORT.getKey());
        clusterUniqueSettings.add("http.port");
    }

    public static void assertClusters(InternalTestCluster cluster0, InternalTestCluster cluster1, boolean checkClusterUniqueSettings) {
        Settings defaultSettings0 = cluster0.getDefaultSettings();
        Settings defaultSettings1 = cluster1.getDefaultSettings();
        assertSettings(defaultSettings0, defaultSettings1, checkClusterUniqueSettings);
        assertThat(cluster0.numDataNodes(), equalTo(cluster1.numDataNodes()));
        if (checkClusterUniqueSettings) {
            assertThat(cluster0.getClusterName(), equalTo(cluster1.getClusterName()));
        }
    }

    public static void assertSettings(Settings left, Settings right, boolean checkClusterUniqueSettings) {
        Set<Map.Entry<String, String>> entries0 = left.getAsMap().entrySet();
        Map<String, String> entries1 = right.getAsMap();
        assertThat(entries0.size(), equalTo(entries1.size()));
        for (Map.Entry<String, String> entry : entries0) {
            if (clusterUniqueSettings.contains(entry.getKey()) && checkClusterUniqueSettings == false) {
                continue;
            }
            assertThat(entries1, hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    public void testBeforeTest() throws Exception {
        long clusterSeed = randomLong();
        boolean masterNodes = randomBoolean();
        int minNumDataNodes = randomIntBetween(0, 3);
        int maxNumDataNodes = randomIntBetween(minNumDataNodes, 4);
        int numClientNodes = randomIntBetween(0, 2);
        final String clusterName1 = "shared1";
        final String clusterName2 = "shared2";
        NodeConfigurationSource nodeConfigurationSource = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings.builder()
                    .put(
                        NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(),
                        2 * ((masterNodes ? InternalTestCluster.DEFAULT_HIGH_NUM_MASTER_NODES : 0) + maxNumDataNodes + numClientNodes))
                    .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                    .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "local")
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
            }

            @Override
            public Settings transportClientSettings() {
                return Settings.builder()
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
            }
        };

        boolean enableHttpPipelining = randomBoolean();
        String nodePrefix = "foobar";

        Path baseDir = createTempDir();
        InternalTestCluster cluster0 = new InternalTestCluster(clusterSeed, baseDir, masterNodes,
            minNumDataNodes, maxNumDataNodes, clusterName1, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());
        InternalTestCluster cluster1 = new InternalTestCluster(clusterSeed, baseDir, masterNodes,
            minNumDataNodes, maxNumDataNodes, clusterName2, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());

        assertClusters(cluster0, cluster1, false);
        long seed = randomLong();
        try {
            {
                Random random = new Random(seed);
                cluster0.beforeTest(random, random.nextDouble());
            }
            {
                Random random = new Random(seed);
                cluster1.beforeTest(random, random.nextDouble());
            }
            assertArrayEquals(cluster0.getNodeNames(), cluster1.getNodeNames());
            Iterator<Client> iterator1 = cluster1.getClients().iterator();
            for (Client client : cluster0.getClients()) {
                assertTrue(iterator1.hasNext());
                Client other = iterator1.next();
                assertSettings(client.settings(), other.settings(), false);
            }
            assertArrayEquals(cluster0.getNodeNames(), cluster1.getNodeNames());
            cluster0.afterTest();
            cluster1.afterTest();
        } finally {
            IOUtils.close(cluster0, cluster1);
        }
    }

    public void testDataFolderAssignmentAndCleaning() throws IOException, InterruptedException {
        long clusterSeed = randomLong();
        boolean masterNodes = randomBoolean();
        // we need one stable node
        final int minNumDataNodes = 2;
        final int maxNumDataNodes = 2;
        final int numClientNodes = randomIntBetween(0, 2);
        final String clusterName1 = "shared1";
        NodeConfigurationSource nodeConfigurationSource = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings.builder().put(NetworkModule.HTTP_ENABLED.getKey(), false)
                    .put(
                        NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(),
                        2 + (masterNodes ? InternalTestCluster.DEFAULT_HIGH_NUM_MASTER_NODES : 0) + maxNumDataNodes + numClientNodes)
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, "local")
                    .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "local")
                    .build();
            }
            @Override
            public Settings transportClientSettings() {
                return Settings.builder()
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
            }
        };
        boolean enableHttpPipelining = randomBoolean();
        String nodePrefix = "test";
        Path baseDir = createTempDir();
        InternalTestCluster cluster = new InternalTestCluster(clusterSeed, baseDir, masterNodes,
            minNumDataNodes, maxNumDataNodes, clusterName1, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());
        try {
            cluster.beforeTest(random(), 0.0);
            final Map<String,Path[]> shardNodePaths = new HashMap<>();
            for (String name: cluster.getNodeNames()) {
                shardNodePaths.put(name, getNodePaths(cluster, name));
            }
            String poorNode = randomFrom(cluster.getNodeNames());
            Path dataPath = getNodePaths(cluster, poorNode)[0];
            final Path testMarker = dataPath.resolve("testMarker");
            Files.createDirectories(testMarker);
            cluster.stopRandomNode(InternalTestCluster.nameFilter(poorNode));
            assertFileExists(testMarker); // stopping a node half way shouldn't clean data

            final String stableNode = randomFrom(cluster.getNodeNames());
            final Path stableDataPath = getNodePaths(cluster, stableNode)[0];
            final Path stableTestMarker = stableDataPath.resolve("stableTestMarker");
            assertThat(stableDataPath, not(dataPath));
            Files.createDirectories(stableTestMarker);

            final String newNode1 =  cluster.startNode();
            assertThat(getNodePaths(cluster, newNode1)[0], equalTo(dataPath));
            assertFileExists(testMarker); // starting a node should re-use data folders and not clean it

            final String newNode2 =  cluster.startNode();
            final Path newDataPath = getNodePaths(cluster, newNode2)[0];
            final Path newTestMarker = newDataPath.resolve("newTestMarker");
            assertThat(newDataPath, not(dataPath));
            Files.createDirectories(newTestMarker);
            cluster.beforeTest(random(), 0.0);
            assertFileNotExists(newTestMarker); // the cluster should be reset for a new test, cleaning up the extra path we made
            assertFileNotExists(testMarker); // a new unknown node used this path, it should be cleaned
            assertFileExists(stableTestMarker); // but leaving the structure of existing, reused nodes
            for (String name: cluster.getNodeNames()) {
                assertThat("data paths for " + name + " changed", getNodePaths(cluster, name), equalTo(shardNodePaths.get(name)));
            }

            cluster.beforeTest(random(), 0.0);
            assertFileExists(stableTestMarker); // but leaving the structure of existing, reused nodes
            for (String name: cluster.getNodeNames()) {
                assertThat("data paths for " + name + " changed", getNodePaths(cluster, name),
                    equalTo(shardNodePaths.get(name)));
            }

        } finally {
            cluster.close();
        }
    }

    private Path[] getNodePaths(InternalTestCluster cluster, String name) {
        final NodeEnvironment nodeEnvironment = cluster.getInstance(NodeEnvironment.class, name);
        if (nodeEnvironment.hasNodeFile()) {
            return nodeEnvironment.nodeDataPaths();
        } else {
            return new Path[0];
        }
    }

    public void testDifferentRolesMaintainPathOnRestart() throws Exception {
        final Path baseDir = createTempDir();
        final int numNodes = 5;
        InternalTestCluster cluster = new InternalTestCluster(randomLong(), baseDir, true, 0, 0, "test",
            new NodeConfigurationSource() {
                @Override
                public Settings nodeSettings(int nodeOrdinal) {
                    return Settings.builder()
                        .put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), numNodes)
                        .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, "local")
                        .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "local")
                        .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), 0).build();
                }

                @Override
                public Settings transportClientSettings() {
                    return Settings.builder()
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
                }
            }, 0, randomBoolean(), "", Collections.emptyList(), Function.identity());
        cluster.beforeTest(random(), 0.0);
        try {
            Map<DiscoveryNode.Role, Set<String>> pathsPerRole = new HashMap<>();
            for (int i = 0; i < numNodes; i++) {
                final DiscoveryNode.Role role = randomFrom(MASTER, DiscoveryNode.Role.DATA, DiscoveryNode.Role.INGEST);
                final String node;
                switch (role) {
                    case MASTER:
                        node = cluster.startMasterOnlyNode(Settings.EMPTY);
                        break;
                    case DATA:
                        node = cluster.startDataOnlyNode(Settings.EMPTY);
                        break;
                    case INGEST:
                        node = cluster.startCoordinatingOnlyNode(Settings.EMPTY);
                        break;
                    default:
                        throw new IllegalStateException("get your story straight");
                }
                Set<String> rolePaths = pathsPerRole.computeIfAbsent(role, k -> new HashSet<>());
                for (Path path : getNodePaths(cluster, node)) {
                    assertTrue(rolePaths.add(path.toString()));
                }
            }
            cluster.fullRestart();

            Map<DiscoveryNode.Role, Set<String>> result = new HashMap<>();
            for (String name : cluster.getNodeNames()) {
                DiscoveryNode node = cluster.getInstance(ClusterService.class, name).localNode();
                List<String> paths = Arrays.stream(getNodePaths(cluster, name)).map(Path::toString).collect(Collectors.toList());
                if (node.isMasterNode()) {
                    result.computeIfAbsent(MASTER, k -> new HashSet<>()).addAll(paths);
                } else if (node.isDataNode()) {
                    result.computeIfAbsent(DATA, k -> new HashSet<>()).addAll(paths);
                } else {
                    result.computeIfAbsent(INGEST, k -> new HashSet<>()).addAll(paths);
                }
            }

            assertThat(result.size(), equalTo(pathsPerRole.size()));
            for (DiscoveryNode.Role role : result.keySet()) {
                assertThat("path are not the same for " + role, result.get(role), equalTo(pathsPerRole.get(role)));
            }
        } finally {
            cluster.close();
        }

    }
}
