
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
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.transport.TcpTransport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
import static org.elasticsearch.discovery.zen.ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING;
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
            randomBoolean(), minNumDataNodes, maxNumDataNodes, clusterName, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Collections.emptyList(), Function.identity());
        InternalTestCluster cluster1 = new InternalTestCluster(clusterSeed, baseDir, masterNodes,
            randomBoolean(), minNumDataNodes, maxNumDataNodes, clusterName, nodeConfigurationSource, numClientNodes,
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
        clusterUniqueSettings.add(TcpTransport.PORT.getKey());
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
        Set<String> keys0 = left.keySet();
        Set<String> keys1 = right.keySet();
        assertThat("--> left:\n" + left.toDelimitedString('\n') +  "\n-->right:\n" + right.toDelimitedString('\n'),
            keys0.size(), equalTo(keys1.size()));
        for (String key : keys0) {
            if (clusterUniqueSettings.contains(key) && checkClusterUniqueSettings == false) {
                continue;
            }
            assertTrue("key [" + key + "] is missing in " + keys1, keys1.contains(key));
            assertEquals(right.get(key), left.get(key));
        }
    }

    private void assertMMNinNodeSetting(InternalTestCluster cluster, int masterNodes) {
        for (final String node : cluster.getNodeNames()) {
            assertMMNinNodeSetting(node, cluster, masterNodes);
        }
    }

    private void assertMMNinNodeSetting(String node, InternalTestCluster cluster, int masterNodes) {
        final int minMasterNodes = masterNodes / 2 + 1;
        Settings nodeSettings = cluster.client(node).admin().cluster().prepareNodesInfo(node).get().getNodes().get(0).getSettings();
        assertEquals("node setting of node [" + node + "] has the wrong min_master_node setting: ["
            + nodeSettings.get(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey()) + "]",
            DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(nodeSettings).intValue(), minMasterNodes);
    }

    private void assertMMNinClusterSetting(InternalTestCluster cluster, int masterNodes) {
        final int minMasterNodes = masterNodes / 2 + 1;
        for (final String node : cluster.getNodeNames()) {
            Settings stateSettings = cluster.client(node).admin().cluster().prepareState().setLocal(true)
                .get().getState().getMetaData().settings();

            assertEquals("dynamic setting for node [" + node + "] has the wrong min_master_node setting : ["
                    + stateSettings.get(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey()) + "]",
                DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(stateSettings).intValue(), minMasterNodes);
        }
    }

    public void testBeforeTest() throws Exception {
        final boolean autoManageMinMasterNodes = randomBoolean();
        long clusterSeed = randomLong();
        final boolean masterNodes;
        final int minNumDataNodes;
        final int maxNumDataNodes;
        if (autoManageMinMasterNodes) {
            masterNodes = randomBoolean();
            minNumDataNodes = randomIntBetween(0, 3);
            maxNumDataNodes = randomIntBetween(minNumDataNodes, 4);
        } else {
            // if we manage min master nodes, we need to lock down the number of nodes
            minNumDataNodes = randomIntBetween(0, 4);
            maxNumDataNodes = minNumDataNodes;
            masterNodes = false;
        }
        final int numClientNodes = randomIntBetween(0, 2);
        final String clusterName1 = "shared1";
        final String clusterName2 = "shared2";
        String transportClient = getTestTransportType();
        NodeConfigurationSource nodeConfigurationSource = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                final Settings.Builder settings = Settings.builder()
                    .put(
                        NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(),
                        2 * ((masterNodes ? InternalTestCluster.DEFAULT_HIGH_NUM_MASTER_NODES : 0) + maxNumDataNodes + numClientNodes))
                    .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
                if (autoManageMinMasterNodes == false) {
                    assert minNumDataNodes == maxNumDataNodes;
                    assert masterNodes == false;
                    settings.put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), minNumDataNodes / 2 + 1);
                }
                return settings.build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Settings transportClientSettings() {
                return Settings.builder()
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, transportClient).build();
            }
        };

        boolean enableHttpPipelining = randomBoolean();
        String nodePrefix = "foobar";

        Path baseDir = createTempDir();
        final List<Class<? extends Plugin>> mockPlugins = Arrays.asList(getTestTransportPlugin(), TestZenDiscovery.TestPlugin.class);
        InternalTestCluster cluster0 = new InternalTestCluster(clusterSeed, baseDir, masterNodes,
            autoManageMinMasterNodes, minNumDataNodes, maxNumDataNodes, clusterName1, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, mockPlugins, Function.identity());
        InternalTestCluster cluster1 = new InternalTestCluster(clusterSeed, baseDir, masterNodes,
            autoManageMinMasterNodes, minNumDataNodes, maxNumDataNodes, clusterName2, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, mockPlugins, Function.identity());

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
            assertMMNinNodeSetting(cluster0, cluster0.numMasterNodes());
            assertMMNinNodeSetting(cluster1, cluster0.numMasterNodes());
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
        String transportClient = getTestTransportType();
        NodeConfigurationSource nodeConfigurationSource = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings.builder().put(NetworkModule.HTTP_ENABLED.getKey(), false)
                    .put(
                        NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(),
                        2 + (masterNodes ? InternalTestCluster.DEFAULT_HIGH_NUM_MASTER_NODES : 0) + maxNumDataNodes + numClientNodes)
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                    .build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Settings transportClientSettings() {
                return Settings.builder()
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, transportClient).build();
            }
        };
        boolean enableHttpPipelining = randomBoolean();
        String nodePrefix = "test";
        Path baseDir = createTempDir();
        InternalTestCluster cluster = new InternalTestCluster(clusterSeed, baseDir, masterNodes,
            true, minNumDataNodes, maxNumDataNodes, clusterName1, nodeConfigurationSource, numClientNodes,
            enableHttpPipelining, nodePrefix, Arrays.asList(getTestTransportPlugin(), TestZenDiscovery.TestPlugin.class),
            Function.identity());
        try {
            cluster.beforeTest(random(), 0.0);
            final int originalMasterCount = cluster.numMasterNodes();
            assertMMNinNodeSetting(cluster, originalMasterCount);
            final Map<String,Path[]> shardNodePaths = new HashMap<>();
            for (String name: cluster.getNodeNames()) {
                shardNodePaths.put(name, getNodePaths(cluster, name));
            }
            String poorNode = randomFrom(cluster.getNodeNames());
            Path dataPath = getNodePaths(cluster, poorNode)[0];
            final Path testMarker = dataPath.resolve("testMarker");
            Files.createDirectories(testMarker);
            int expectedMasterCount = originalMasterCount;
            if (cluster.getInstance(ClusterService.class, poorNode).localNode().isMasterNode()) {
                expectedMasterCount--;
            }
            cluster.stopRandomNode(InternalTestCluster.nameFilter(poorNode));
            if (expectedMasterCount != originalMasterCount) {
                // check for updated
                assertMMNinClusterSetting(cluster, expectedMasterCount);
            }
            assertFileExists(testMarker); // stopping a node half way shouldn't clean data

            final String stableNode = randomFrom(cluster.getNodeNames());
            final Path stableDataPath = getNodePaths(cluster, stableNode)[0];
            final Path stableTestMarker = stableDataPath.resolve("stableTestMarker");
            assertThat(stableDataPath, not(dataPath));
            Files.createDirectories(stableTestMarker);

            final String newNode1 =  cluster.startNode();
            expectedMasterCount++;
            assertThat(getNodePaths(cluster, newNode1)[0], equalTo(dataPath));
            assertFileExists(testMarker); // starting a node should re-use data folders and not clean it
            if (expectedMasterCount > 1) { // this is the first master, it's in cluster state settings won't be updated
                assertMMNinClusterSetting(cluster, expectedMasterCount);
            }
            assertMMNinNodeSetting(newNode1, cluster, expectedMasterCount);

            final String newNode2 =  cluster.startNode();
            expectedMasterCount++;
            assertMMNinClusterSetting(cluster, expectedMasterCount);
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
            assertMMNinNodeSetting(cluster, originalMasterCount);

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

        String transportClient = getTestTransportType();
        InternalTestCluster cluster = new InternalTestCluster(randomLong(), baseDir, false,
                false, 0, 0, "test", new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings.builder()
                        .put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), numNodes)
                        .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                        .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), 0)
                        // speedup join timeout as setting initial state timeout to 0 makes split
                        // elections more likely
                        .put(ZenDiscovery.JOIN_TIMEOUT_SETTING.getKey(), "3s")
                        .build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Settings transportClientSettings() {
                return Settings.builder()
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, transportClient).build();
            }
        }, 0, randomBoolean(), "", Arrays.asList(getTestTransportPlugin(), TestZenDiscovery.TestPlugin.class), Function.identity());
        cluster.beforeTest(random(), 0.0);
        List<DiscoveryNode.Role> roles = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            final DiscoveryNode.Role role = i == numNodes - 1 && roles.contains(MASTER) == false ?
                MASTER : // last node and still no master
                randomFrom(MASTER, DiscoveryNode.Role.DATA, DiscoveryNode.Role.INGEST);
            roles.add(role);
        }

        final Settings minMasterNodes = Settings.builder()
            .put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(),
                roles.stream().filter(role -> role == MASTER).count() / 2 + 1
            ).build();
        try {
            Map<DiscoveryNode.Role, Set<String>> pathsPerRole = new HashMap<>();
            for (int i = 0; i < numNodes; i++) {
                final DiscoveryNode.Role role = roles.get(i);
                final String node;
                switch (role) {
                    case MASTER:
                        node = cluster.startMasterOnlyNode(minMasterNodes);
                        break;
                    case DATA:
                        node = cluster.startDataOnlyNode(minMasterNodes);
                        break;
                    case INGEST:
                        node = cluster.startCoordinatingOnlyNode(minMasterNodes);
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

    public void testTwoNodeCluster() throws Exception {
        String transportClient = getTestTransportType();
        NodeConfigurationSource nodeConfigurationSource = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings.builder().put(NetworkModule.HTTP_ENABLED.getKey(), false)
                    .put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), 2)
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                    .build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Settings transportClientSettings() {
                return Settings.builder()
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, transportClient).build();
            }
        };
        boolean enableHttpPipelining = randomBoolean();
        String nodePrefix = "test";
        Path baseDir = createTempDir();
        InternalTestCluster cluster = new InternalTestCluster(randomLong(), baseDir, false, true, 2, 2,
            "test", nodeConfigurationSource, 0, enableHttpPipelining, nodePrefix,
            Arrays.asList(getTestTransportPlugin(), TestZenDiscovery.TestPlugin.class), Function.identity());
        try {
            cluster.beforeTest(random(), 0.0);
            assertMMNinNodeSetting(cluster, 2);
            switch (randomInt(2)) {
                case 0:
                    cluster.stopRandomDataNode();
                    assertMMNinClusterSetting(cluster, 1);
                    cluster.startNode();
                    assertMMNinClusterSetting(cluster, 2);
                    assertMMNinNodeSetting(cluster, 2);
                    break;
                case 1:
                    cluster.rollingRestart(new InternalTestCluster.RestartCallback() {
                        @Override
                        public Settings onNodeStopped(String nodeName) throws Exception {
                            assertMMNinClusterSetting(cluster, 1);
                            return super.onNodeStopped(nodeName);
                        }
                    });
                    assertMMNinClusterSetting(cluster, 2);
                    break;
                case 2:
                    cluster.fullRestart();
                    break;
            }
            assertMMNinNodeSetting(cluster, 2);
        } finally {
            cluster.close();
        }
    }
}
