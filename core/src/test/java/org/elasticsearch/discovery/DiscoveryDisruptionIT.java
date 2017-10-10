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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen.PublishClusterStateAction;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for discovery during disruptions.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0, autoMinMasterNodes = false)
@TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE")
public class DiscoveryDisruptionIT extends AbstractDisruptionTestCase {

    public void testIsolatedUnicastNodes() throws Exception {
        List<String> nodes = startCluster(4, -1, new int[]{0});
        // Figure out what is the elected master node
        final String unicastTarget = nodes.get(0);

        Set<String> unicastTargetSide = new HashSet<>();
        unicastTargetSide.add(unicastTarget);

        Set<String> restOfClusterSide = new HashSet<>();
        restOfClusterSide.addAll(nodes);
        restOfClusterSide.remove(unicastTarget);

        // Forcefully clean temporal response lists on all nodes. Otherwise the node in the unicast host list
        // includes all the other nodes that have pinged it and the issue doesn't manifest
        ZenPing zenPing = ((TestZenDiscovery) internalCluster().getInstance(Discovery.class)).getZenPing();
        if (zenPing instanceof UnicastZenPing) {
            ((UnicastZenPing) zenPing).clearTemporalResponses();
        }

        // Simulate a network issue between the unicast target node and the rest of the cluster
        NetworkDisruption networkDisconnect = new NetworkDisruption(new TwoPartitions(unicastTargetSide, restOfClusterSide),
                new NetworkDisconnect());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();
        // Wait until elected master has removed that the unlucky node...
        ensureStableCluster(3, nodes.get(1));

        // The isolate master node must report no master, so it starts with pinging
        assertNoMaster(unicastTarget);
        networkDisconnect.stopDisrupting();
        // Wait until the master node sees all 3 nodes again.
        ensureStableCluster(4);
    }

    /**
     * A 4 node cluster with m_m_n set to 3 and each node has one unicast endpoint. One node partitions from the master node.
     * The temporal unicast responses is empty. When partition is solved the one ping response contains a master node.
     * The rejoining node should take this master node and connect.
     */
    public void testUnicastSinglePingResponseContainsMaster() throws Exception {
        List<String> nodes = startCluster(4, -1, new int[]{0});
        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node={}", masterNode);
        List<String> otherNodes = new ArrayList<>(nodes);
        otherNodes.remove(masterNode);
        otherNodes.remove(nodes.get(0)); // <-- Don't isolate the node that is in the unicast endpoint for all the other nodes.
        final String isolatedNode = otherNodes.get(0);

        // Forcefully clean temporal response lists on all nodes. Otherwise the node in the unicast host list
        // includes all the other nodes that have pinged it and the issue doesn't manifest
        ZenPing zenPing = ((TestZenDiscovery) internalCluster().getInstance(Discovery.class)).getZenPing();
        if (zenPing instanceof UnicastZenPing) {
            ((UnicastZenPing) zenPing).clearTemporalResponses();
        }

        // Simulate a network issue between the unlucky node and elected master node in both directions.
        NetworkDisruption networkDisconnect = new NetworkDisruption(new TwoPartitions(masterNode, isolatedNode),
                new NetworkDisconnect());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();
        // Wait until elected master has removed that the unlucky node...
        ensureStableCluster(3, masterNode);

        // The isolate master node must report no master, so it starts with pinging
        assertNoMaster(isolatedNode);
        networkDisconnect.stopDisrupting();
        // Wait until the master node sees all 4 nodes again.
        ensureStableCluster(4);
        // The elected master shouldn't have changed, since the isolated node never could have elected himself as
        // master since m_m_n of 3 could never be satisfied.
        assertMaster(masterNode, nodes);
    }

    /**
     * Test cluster join with issues in cluster state publishing *
     */
    public void testClusterJoinDespiteOfPublishingIssues() throws Exception {
        List<String> nodes = startCluster(2, 1);

        String masterNode = internalCluster().getMasterName();
        String nonMasterNode;
        if (masterNode.equals(nodes.get(0))) {
            nonMasterNode = nodes.get(1);
        } else {
            nonMasterNode = nodes.get(0);
        }

        DiscoveryNodes discoveryNodes = internalCluster().getInstance(ClusterService.class, nonMasterNode).state().nodes();

        TransportService masterTranspotService =
                internalCluster().getInstance(TransportService.class, discoveryNodes.getMasterNode().getName());

        logger.info("blocking requests from non master [{}] to master [{}]", nonMasterNode, masterNode);
        MockTransportService nonMasterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class,
                nonMasterNode);
        nonMasterTransportService.addFailToSendNoConnectRule(masterTranspotService);

        assertNoMaster(nonMasterNode);

        logger.info("blocking cluster state publishing from master [{}] to non master [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService =
                (MockTransportService) internalCluster().getInstance(TransportService.class, masterNode);
        TransportService localTransportService =
                internalCluster().getInstance(TransportService.class, discoveryNodes.getLocalNode().getName());
        if (randomBoolean()) {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, PublishClusterStateAction.SEND_ACTION_NAME);
        } else {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, PublishClusterStateAction.COMMIT_ACTION_NAME);
        }

        logger.info("allowing requests from non master [{}] to master [{}], waiting for two join request", nonMasterNode, masterNode);
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        nonMasterTransportService.addDelegate(masterTranspotService, new MockTransportService.DelegateTransport(nonMasterTransportService
                .original()) {
            @Override
            protected void sendRequest(Transport.Connection connection, long requestId, String action, TransportRequest request,
                                       TransportRequestOptions options) throws IOException {
                if (action.equals(MembershipAction.DISCOVERY_JOIN_ACTION_NAME)) {
                    countDownLatch.countDown();
                }
                super.sendRequest(connection, requestId, action, request, options);
            }

            @Override
            public Transport.Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
                return super.openConnection(node, profile);
            }

        });

        countDownLatch.await();

        logger.info("waiting for cluster to reform");
        masterTransportService.clearRule(localTransportService);
        nonMasterTransportService.clearRule(localTransportService);

        ensureStableCluster(2);

        // shutting down the nodes, to avoid the leakage check tripping
        // on the states associated with the commit requests we may have dropped
        internalCluster().stopRandomNonMasterNode();
    }

    public void testClusterFormingWithASlowNode() throws Exception {
        configureCluster(3, null, 2);

        SlowClusterStateProcessing disruption = new SlowClusterStateProcessing(random(), 0, 0, 1000, 2000);

        // don't wait for initial state, we want to add the disruption while the cluster is forming
        internalCluster().startNodes(3, Settings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "3s").build());

        logger.info("applying disruption while cluster is forming ...");

        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        ensureStableCluster(3);
    }

    public void testElectMasterWithLatestVersion() throws Exception {
        configureCluster(3, null, 2);
        final Set<String> nodes = new HashSet<>(internalCluster().startNodes(3));
        ensureStableCluster(3);
        ServiceDisruptionScheme isolateAllNodes =
                new NetworkDisruption(new NetworkDisruption.IsolateAllNodes(nodes), new NetworkDisconnect());
        internalCluster().setDisruptionScheme(isolateAllNodes);

        logger.info("--> forcing a complete election to make sure \"preferred\" master is elected");
        isolateAllNodes.startDisrupting();
        for (String node : nodes) {
            assertNoMaster(node);
        }
        internalCluster().clearDisruptionScheme();
        ensureStableCluster(3);
        final String preferredMasterName = internalCluster().getMasterName();
        final DiscoveryNode preferredMaster = internalCluster().clusterService(preferredMasterName).localNode();
        for (String node : nodes) {
            DiscoveryNode discoveryNode = internalCluster().clusterService(node).localNode();
            assertThat(discoveryNode.getId(), greaterThanOrEqualTo(preferredMaster.getId()));
        }

        logger.info("--> preferred master is {}", preferredMaster);
        final Set<String> nonPreferredNodes = new HashSet<>(nodes);
        nonPreferredNodes.remove(preferredMasterName);
        final ServiceDisruptionScheme isolatePreferredMaster =
                new NetworkDisruption(
                        new NetworkDisruption.TwoPartitions(
                                Collections.singleton(preferredMasterName), nonPreferredNodes),
                        new NetworkDisconnect());
        internalCluster().setDisruptionScheme(isolatePreferredMaster);
        isolatePreferredMaster.startDisrupting();

        assertAcked(client(randomFrom(nonPreferredNodes)).admin().indices().prepareCreate("test").setSettings(
            Settings.builder().put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)));

        internalCluster().clearDisruptionScheme(false);
        internalCluster().setDisruptionScheme(isolateAllNodes);

        logger.info("--> forcing a complete election again");
        isolateAllNodes.startDisrupting();
        for (String node : nodes) {
            assertNoMaster(node);
        }

        isolateAllNodes.stopDisrupting();

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        if (state.metaData().hasIndex("test") == false) {
            fail("index 'test' was lost. current cluster state: " + state);
        }

    }

    /**
     * Adds an asymmetric break between a master and one of the nodes and makes
     * sure that the node is removed form the cluster, that the node start pinging and that
     * the cluster reforms when healed.
     */
    public void testNodeNotReachableFromMaster() throws Exception {
        startCluster(3);

        String masterNode = internalCluster().getMasterName();
        String nonMasterNode = null;
        while (nonMasterNode == null) {
            nonMasterNode = randomFrom(internalCluster().getNodeNames());
            if (nonMasterNode.equals(masterNode)) {
                nonMasterNode = null;
            }
        }

        logger.info("blocking request from master [{}] to [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class,
                masterNode);
        if (randomBoolean()) {
            masterTransportService.addUnresponsiveRule(internalCluster().getInstance(TransportService.class, nonMasterNode));
        } else {
            masterTransportService.addFailToSendNoConnectRule(internalCluster().getInstance(TransportService.class, nonMasterNode));
        }

        logger.info("waiting for [{}] to be removed from cluster", nonMasterNode);
        ensureStableCluster(2, masterNode);

        logger.info("waiting for [{}] to have no master", nonMasterNode);
        assertNoMaster(nonMasterNode);

        logger.info("healing partition and checking cluster reforms");
        masterTransportService.clearAllRules();

        ensureStableCluster(3);
    }

}
