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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Allocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.elasticsearch.test.discovery.TestZenDiscovery;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.cluster.coordination.Coordinator.ZEN1_BWC_TERM;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class Zen1IT extends ESIntegTestCase {

    private static Settings ZEN1_SETTINGS = Coordinator.addZen1Attribute(true, Settings.builder()
        .put(TestZenDiscovery.USE_ZEN2.getKey(), false)
        .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false)) // Zen2 does not know about mock pings
        .build();

    private static Settings ZEN2_SETTINGS = Settings.builder()
        .put(TestZenDiscovery.USE_ZEN2.getKey(), true)
        .build();

    public void testZen2NodesJoiningZen1Cluster() {
        internalCluster().startNodes(randomIntBetween(1, 3), ZEN1_SETTINGS);
        internalCluster().startNodes(randomIntBetween(1, 3), ZEN2_SETTINGS);
        createIndex("test");
    }

    public void testZen1NodesJoiningZen2Cluster() {
        internalCluster().startNodes(randomIntBetween(1, 3), ZEN2_SETTINGS);
        internalCluster().startNodes(randomIntBetween(1, 3), ZEN1_SETTINGS);
        createIndex("test");
    }

    public void testMixedClusterFormation() throws Exception {
        final int zen1NodeCount = randomIntBetween(1, 3);
        final int zen2NodeCount = randomIntBetween(zen1NodeCount == 1 ? 2 : 1, 3);
        logger.info("starting cluster of [{}] Zen1 nodes and [{}] Zen2 nodes", zen1NodeCount, zen2NodeCount);
        final List<String> nodes = internalCluster().startNodes(IntStream.range(0, zen1NodeCount + zen2NodeCount)
            .mapToObj(i -> i < zen1NodeCount ? ZEN1_SETTINGS : ZEN2_SETTINGS).toArray(Settings[]::new));

        createIndex("test",
            Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO) // assign shards
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, zen1NodeCount + zen2NodeCount + randomIntBetween(0, 2)) // causes rebalancing
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build());
        ensureGreen("test");

        for (final String node : nodes) {
            // With 1 Zen1 node when you stop the Zen1 node the Zen2 nodes might auto-bootstrap.
            // But there are only 2 Zen2 nodes so you must do the right things with voting config exclusions to keep the cluster
            // alive through the other two restarts.
            final boolean masterNodeIsZen2 = zen1NodeCount <= nodes.indexOf(internalCluster().getMasterName());
            final boolean thisNodeIsZen2 = zen1NodeCount <= nodes.indexOf(node);
            final boolean requiresVotingConfigExclusions = zen1NodeCount == 1 && zen2NodeCount == 2 && masterNodeIsZen2 && thisNodeIsZen2;

            if (requiresVotingConfigExclusions) {
                client().execute(AddVotingConfigExclusionsAction.INSTANCE,
                    new AddVotingConfigExclusionsRequest(new String[]{node})).get();
            }

            internalCluster().restartNode(node, new RestartCallback() {
                @Override
                public Settings onNodeStopped(String restartingNode) {
                    String viaNode = randomValueOtherThan(restartingNode, () -> randomFrom(nodes));
                    final ClusterHealthRequestBuilder clusterHealthRequestBuilder = client(viaNode).admin().cluster().prepareHealth()
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForNodes(Integer.toString(zen1NodeCount + zen2NodeCount - 1))
                        .setTimeout(TimeValue.timeValueSeconds(30));
                    ClusterHealthResponse clusterHealthResponse = clusterHealthRequestBuilder.get();
                    assertFalse(restartingNode, clusterHealthResponse.isTimedOut());
                    return Settings.EMPTY;
                }
            });
            ensureStableCluster(zen1NodeCount + zen2NodeCount);
            ensureGreen("test");

            if (requiresVotingConfigExclusions) {
                final ClearVotingConfigExclusionsRequest clearVotingTombstonesRequest = new ClearVotingConfigExclusionsRequest();
                clearVotingTombstonesRequest.setWaitForRemoval(false);
                client().execute(ClearVotingConfigExclusionsAction.INSTANCE, clearVotingTombstonesRequest).get();
            }
        }
    }

    public void testRollingMigrationFromZen1ToZen2() throws Exception {
        final int nodeCount = randomIntBetween(2, 5);
        final List<String> zen1Nodes = internalCluster().startNodes(nodeCount, ZEN1_SETTINGS);

        createIndex("test",
            Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO) // assign shards
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, nodeCount) // causes rebalancing
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build());
        ensureGreen("test");

        for (final String zen1Node : zen1Nodes) {
            logger.info("--> shutting down {}", zen1Node);
            internalCluster().stopRandomNode(s -> NODE_NAME_SETTING.get(s).equals(zen1Node));

            ensureStableCluster(nodeCount - 1);
            if (nodeCount > 2) {
                ensureGreen("test");
            } else {
                ensureYellow("test");
            }

            logger.info("--> starting replacement for {}", zen1Node);
            final String newNode = internalCluster().startNode(ZEN2_SETTINGS);
            ensureStableCluster(nodeCount);
            ensureGreen("test");
            logger.info("--> successfully replaced {} with {}", zen1Node, newNode);
        }

        assertThat(internalCluster().size(), equalTo(nodeCount));
    }

    public void testRollingUpgradeFromZen1ToZen2() throws Exception {
        final int nodeCount = randomIntBetween(2, 5);
        final List<String> nodes = internalCluster().startNodes(nodeCount, ZEN1_SETTINGS);

        createIndex("test",
            Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO) // assign shards
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, nodeCount) // causes rebalancing
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build());
        ensureGreen("test");

        internalCluster().rollingRestart(new RestartCallback() {
            @Override
            public void doAfterNodes(int n, Client client) {
                ensureGreen("test");
            }

            @Override
            public Settings onNodeStopped(String nodeName) {
                String viaNode = randomValueOtherThan(nodeName, () -> randomFrom(nodes));
                final ClusterHealthRequestBuilder clusterHealthRequestBuilder = client(viaNode).admin().cluster().prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNodes(Integer.toString(nodeCount - 1))
                    .setTimeout(TimeValue.timeValueSeconds(30));
                if (nodeCount == 2) {
                    clusterHealthRequestBuilder.setWaitForYellowStatus();
                } else {
                    clusterHealthRequestBuilder.setWaitForGreenStatus();
                }
                ClusterHealthResponse clusterHealthResponse = clusterHealthRequestBuilder.get();
                assertFalse(nodeName, clusterHealthResponse.isTimedOut());
                return Coordinator.addZen1Attribute(false, Settings.builder().put(ZEN2_SETTINGS)).build();
            }
        });

        ensureStableCluster(nodeCount);
        ensureGreen("test");
        assertThat(internalCluster().size(), equalTo(nodeCount));
    }

    private void testMultipleNodeMigrationFromZen1ToZen2(int nodeCount) throws Exception {
        final List<String> oldNodes = internalCluster().startNodes(nodeCount, ZEN1_SETTINGS);
        createIndex("test",
            Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO) // assign shards
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, nodeCount) // causes rebalancing
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, nodeCount > 1 ? 1 : 0)
                .build());
        ensureGreen("test");

        internalCluster().startNodes(nodeCount, ZEN2_SETTINGS);

        logger.info("--> updating settings to exclude old nodes");
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
            .put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), String.join(",", oldNodes))).get();

        logger.info("--> waiting for old nodes to be vacated");
        waitForRelocation();

        while (internalCluster().size() > nodeCount) {
            internalCluster().stopRandomNode(settings -> oldNodes.contains(NODE_NAME_SETTING.get(settings)));
        }

        ensureGreen("test");
    }

    public void testMultipleNodeMigrationFromZen1ToZen2WithOneNode() throws Exception {
        testMultipleNodeMigrationFromZen1ToZen2(1);
    }

    public void testMultipleNodeMigrationFromZen1ToZen2WithTwoNodes() throws Exception {
        testMultipleNodeMigrationFromZen1ToZen2(2);
    }

    public void testMultipleNodeMigrationFromZen1ToZen2WithThreeNodes() throws Exception {
        testMultipleNodeMigrationFromZen1ToZen2(3);
    }

    public void testFreshestMasterElectedAfterFullClusterRestart() throws Exception {
        final List<String> nodeNames = internalCluster().startNodes(3, ZEN1_SETTINGS);

        assertTrue(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.ALL)).get().isAcknowledged());

        final List<NodeEnvironment> nodeEnvironments
            = StreamSupport.stream(internalCluster().getDataOrMasterNodeInstances(NodeEnvironment.class).spliterator(), false)
            .collect(Collectors.toList());

        final boolean randomiseVersions = rarely();

        internalCluster().fullRestart(new RestartCallback() {
            int nodesStopped;

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                nodesStopped += 1;

                if (nodesStopped == 1) {
                    final Client client = internalCluster().client(randomValueOtherThan(nodeName, () -> randomFrom(nodeNames)));

                    assertFalse(client.admin().cluster().health(Requests.clusterHealthRequest()
                        .waitForEvents(Priority.LANGUID)
                        .waitForNoRelocatingShards(true)
                        .waitForNodes("2")).actionGet().isTimedOut());

                    assertTrue(client.admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
                        .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE)).get().isAcknowledged());
                }

                if (nodesStopped == nodeNames.size()) {
                    for (final NodeEnvironment nodeEnvironment : nodeEnvironments) {
                        // The versions written by nodes following a Zen1 master cannot be trusted. Randomise them to demonstrate they are
                        // not important.
                        final MetaStateService metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry());
                        final Manifest manifest = metaStateService.loadManifestOrEmpty();
                        assertThat(manifest.getCurrentTerm(), is(ZEN1_BWC_TERM));
                        final long newVersion = randomiseVersions ? randomNonNegativeLong() : 0L;
                        metaStateService.writeManifestAndCleanup("altering version to " + newVersion,
                            new Manifest(manifest.getCurrentTerm(), newVersion, manifest.getGlobalGeneration(),
                                manifest.getIndexGenerations()));
                    }
                }

                return Coordinator.addZen1Attribute(false, Settings.builder())
                    .put(ZEN2_SETTINGS)
                    .putList(INITIAL_MASTER_NODES_SETTING.getKey(), nodeNames)
                    .build();
            }
        });

        assertFalse(client().admin().cluster().health(Requests.clusterHealthRequest()
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNodes("3")).actionGet().isTimedOut());

        assertThat(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.get(
            client().admin().cluster().state(new ClusterStateRequest()).get().getState().metaData().settings()),
            equalTo(Allocation.NONE));
    }
}
