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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.IntermittentLongGCDisruption;
import org.elasticsearch.test.disruption.LongGCDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.SingleNodeDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests relating to the loss of the master.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0, autoMinMasterNodes = false)
@TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE")
public class MasterDisruptionIT extends AbstractDisruptionTestCase {

    /**
     * Test that no split brain occurs under partial network partition. See https://github.com/elastic/elasticsearch/issues/2488
     */
    public void testFailWithMinimumMasterNodesConfigured() throws Exception {
        List<String> nodes = startCluster(3);

        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node={}", masterNode);

        // Pick a node that isn't the elected master.
        Set<String> nonMasters = new HashSet<>(nodes);
        nonMasters.remove(masterNode);
        final String unluckyNode = randomFrom(nonMasters.toArray(Strings.EMPTY_ARRAY));


        // Simulate a network issue between the unlucky node and elected master node in both directions.

        NetworkDisruption networkDisconnect = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(masterNode, unluckyNode),
                new NetworkDisruption.NetworkDisconnect());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();

        // Wait until elected master has removed that the unlucky node...
        ensureStableCluster(2, masterNode);

        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        assertNoMaster(unluckyNode);

        networkDisconnect.stopDisrupting();

        // Wait until the master node sees all 3 nodes again.
        ensureStableCluster(3);

        // The elected master shouldn't have changed, since the unlucky node never could have elected himself as
        // master since m_m_n of 2 could never be satisfied.
        assertMaster(masterNode, nodes);
    }

    /**
     * Verify that nodes fault detection works after master (re) election
     */
    public void testNodesFDAfterMasterReelection() throws Exception {
        startCluster(4);

        logger.info("--> stopping current master");
        internalCluster().stopCurrentMasterNode();

        ensureStableCluster(3);

        logger.info("--> reducing min master nodes to 2");
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2))
                .get());

        String master = internalCluster().getMasterName();
        String nonMaster = null;
        for (String node : internalCluster().getNodeNames()) {
            if (!node.equals(master)) {
                nonMaster = node;
            }
        }

        logger.info("--> isolating [{}]", nonMaster);
        NetworkDisruption.TwoPartitions partitions = isolateNode(nonMaster);
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);
        networkDisruption.startDisrupting();

        logger.info("--> waiting for master to remove it");
        ensureStableCluster(2, master);
    }

    /**
     * Tests that emulates a frozen elected master node that unfreezes and pushes his cluster state to other nodes
     * that already are following another elected master node. These nodes should reject this cluster state and prevent
     * them from following the stale master.
     */
    @TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE,org.elasticsearch.test.disruption:TRACE")
    public void testStaleMasterNotHijackingMajority() throws Exception {
        // 3 node cluster with unicast discovery and minimum_master_nodes set to 2:
        final List<String> nodes = startCluster(3, 2);

        // Save the current master node as old master node, because that node will get frozen
        final String oldMasterNode = internalCluster().getMasterName();
        for (String node : nodes) {
            ensureStableCluster(3, node);
        }
        assertMaster(oldMasterNode, nodes);

        // Simulating a painful gc by suspending all threads for a long time on the current elected master node.
        SingleNodeDisruption masterNodeDisruption = new LongGCDisruption(random(), oldMasterNode);

        // Save the majority side
        final List<String> majoritySide = new ArrayList<>(nodes);
        majoritySide.remove(oldMasterNode);

        // Keeps track of the previous and current master when a master node transition took place on each node on the majority side:
        final Map<String, List<Tuple<String, String>>> masters = Collections.synchronizedMap(new HashMap<String, List<Tuple<String,
                        String>>>());
        for (final String node : majoritySide) {
            masters.put(node, new ArrayList<Tuple<String, String>>());
            internalCluster().getInstance(ClusterService.class, node).addListener(event -> {
                DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
                DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
                if (!Objects.equals(previousMaster, currentMaster)) {
                    logger.info("node {} received new cluster state: {} \n and had previous cluster state: {}", node, event.state(),
                            event.previousState());
                    String previousMasterNodeName = previousMaster != null ? previousMaster.getName() : null;
                    String currentMasterNodeName = currentMaster != null ? currentMaster.getName() : null;
                    masters.get(node).add(new Tuple<>(previousMasterNodeName, currentMasterNodeName));
                }
            });
        }

        final CountDownLatch oldMasterNodeSteppedDown = new CountDownLatch(1);
        internalCluster().getInstance(ClusterService.class, oldMasterNode).addListener(event -> {
            if (event.state().nodes().getMasterNodeId() == null) {
                oldMasterNodeSteppedDown.countDown();
            }
        });

        internalCluster().setDisruptionScheme(masterNodeDisruption);
        logger.info("freezing node [{}]", oldMasterNode);
        masterNodeDisruption.startDisrupting();

        // Wait for the majority side to get stable
        assertDifferentMaster(majoritySide.get(0), oldMasterNode);
        assertDifferentMaster(majoritySide.get(1), oldMasterNode);

        // the test is periodically tripping on the following assertion. To find out which threads are blocking the nodes from making
        // progress we print a stack dump
        boolean failed = true;
        try {
            assertDiscoveryCompleted(majoritySide);
            failed = false;
        } finally {
            if (failed) {
                logger.error("discovery failed to complete, probably caused by a blocked thread: {}",
                        new HotThreads().busiestThreads(Integer.MAX_VALUE).ignoreIdleThreads(false).detect());
            }
        }

        // The old master node is frozen, but here we submit a cluster state update task that doesn't get executed,
        // but will be queued and once the old master node un-freezes it gets executed.
        // The old master node will send this update + the cluster state where he is flagged as master to the other
        // nodes that follow the new master. These nodes should ignore this update.
        internalCluster().getInstance(ClusterService.class, oldMasterNode).submitStateUpdateTask("sneaky-update", new
                ClusterStateUpdateTask(Priority.IMMEDIATE) {
                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        return ClusterState.builder(currentState).build();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failure [{}]", source), e);
                    }
                });

        // Save the new elected master node
        final String newMasterNode = internalCluster().getMasterName(majoritySide.get(0));
        logger.info("new detected master node [{}]", newMasterNode);

        // Stop disruption
        logger.info("Unfreeze node [{}]", oldMasterNode);
        masterNodeDisruption.stopDisrupting();

        oldMasterNodeSteppedDown.await(30, TimeUnit.SECONDS);
        // Make sure that the end state is consistent on all nodes:
        assertDiscoveryCompleted(nodes);
        assertMaster(newMasterNode, nodes);

        assertThat(masters.size(), equalTo(2));
        for (Map.Entry<String, List<Tuple<String, String>>> entry : masters.entrySet()) {
            String nodeName = entry.getKey();
            List<Tuple<String, String>> recordedMasterTransition = entry.getValue();
            assertThat("[" + nodeName + "] Each node should only record two master node transitions", recordedMasterTransition.size(),
                    equalTo(2));
            assertThat("[" + nodeName + "] First transition's previous master should be [null]", recordedMasterTransition.get(0).v1(),
                    equalTo(oldMasterNode));
            assertThat("[" + nodeName + "] First transition's current master should be [" + newMasterNode + "]", recordedMasterTransition
                    .get(0).v2(), nullValue());
            assertThat("[" + nodeName + "] Second transition's previous master should be [null]", recordedMasterTransition.get(1).v1(),
                    nullValue());
            assertThat("[" + nodeName + "] Second transition's current master should be [" + newMasterNode + "]",
                    recordedMasterTransition.get(1).v2(), equalTo(newMasterNode));
        }
    }

    /**
     * Test that cluster recovers from a long GC on master that causes other nodes to elect a new one
     */
    public void testMasterNodeGCs() throws Exception {
        List<String> nodes = startCluster(3, -1);

        String oldMasterNode = internalCluster().getMasterName();
        // a very long GC, but it's OK as we remove the disruption when it has had an effect
        SingleNodeDisruption masterNodeDisruption = new IntermittentLongGCDisruption(random(), oldMasterNode, 100, 200, 30000, 60000);
        internalCluster().setDisruptionScheme(masterNodeDisruption);
        masterNodeDisruption.startDisrupting();

        Set<String> oldNonMasterNodesSet = new HashSet<>(nodes);
        oldNonMasterNodesSet.remove(oldMasterNode);

        List<String> oldNonMasterNodes = new ArrayList<>(oldNonMasterNodesSet);

        logger.info("waiting for nodes to de-elect master [{}]", oldMasterNode);
        for (String node : oldNonMasterNodesSet) {
            assertDifferentMaster(node, oldMasterNode);
        }

        logger.info("waiting for nodes to elect a new master");
        ensureStableCluster(2, oldNonMasterNodes.get(0));

        logger.info("waiting for any pinging to stop");
        assertDiscoveryCompleted(oldNonMasterNodes);

        // restore GC
        masterNodeDisruption.stopDisrupting();
        final TimeValue waitTime = new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + masterNodeDisruption.expectedTimeToHeal().millis());
        ensureStableCluster(3, waitTime, false, oldNonMasterNodes.get(0));

        // make sure all nodes agree on master
        String newMaster = internalCluster().getMasterName();
        assertThat(newMaster, not(equalTo(oldMasterNode)));
        assertMaster(newMaster, nodes);
    }

    /**
     * This test isolates the master from rest of the cluster, waits for a new master to be elected, restores the partition
     * and verifies that all node agree on the new cluster state
     */
    @TestLogging(
            "_root:DEBUG,"
                    + "org.elasticsearch.cluster.service:TRACE,"
                    + "org.elasticsearch.gateway:TRACE,"
                    + "org.elasticsearch.indices.store:TRACE")
    public void testIsolateMasterAndVerifyClusterStateConsensus() throws Exception {
        final List<String> nodes = startCluster(3);

        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2))
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                ));

        ensureGreen();
        String isolatedNode = internalCluster().getMasterName();
        TwoPartitions partitions = isolateNode(isolatedNode);
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);
        networkDisruption.startDisrupting();

        String nonIsolatedNode = partitions.getMajoritySide().iterator().next();

        // make sure cluster reforms
        ensureStableCluster(2, nonIsolatedNode);

        // make sure isolated need picks up on things.
        assertNoMaster(isolatedNode, TimeValue.timeValueSeconds(40));

        // restore isolation
        networkDisruption.stopDisrupting();

        for (String node : nodes) {
            ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkDisruption.expectedTimeToHeal().millis()),
                    true, node);
        }

        logger.info("issue a reroute");
        // trigger a reroute now, instead of waiting for the background reroute of RerouteService
        assertAcked(client().admin().cluster().prepareReroute());
        // and wait for it to finish and for the cluster to stabilize
        ensureGreen("test");

        // verify all cluster states are the same
        // use assert busy to wait for cluster states to be applied (as publish_timeout has low value)
        assertBusy(() -> {
            ClusterState state = null;
            for (String node : nodes) {
                ClusterState nodeState = getNodeClusterState(node);
                if (state == null) {
                    state = nodeState;
                    continue;
                }
                // assert nodes are identical
                try {
                    assertEquals("unequal versions", state.version(), nodeState.version());
                    assertEquals("unequal node count", state.nodes().getSize(), nodeState.nodes().getSize());
                    assertEquals("different masters ", state.nodes().getMasterNodeId(), nodeState.nodes().getMasterNodeId());
                    assertEquals("different meta data version", state.metaData().version(), nodeState.metaData().version());
                    assertEquals("different routing", state.routingTable().toString(), nodeState.routingTable().toString());
                } catch (AssertionError t) {
                    fail("failed comparing cluster state: " + t.getMessage() + "\n" +
                            "--- cluster state of node [" + nodes.get(0) + "]: ---\n" + state +
                            "\n--- cluster state [" + node + "]: ---\n" + nodeState);
                }

            }
        });
    }

    /**
     * Verify that the proper block is applied when nodes loose their master
     */
    public void testVerifyApiBlocksDuringPartition() throws Exception {
        startCluster(3);

        // Makes sure that the get request can be executed on each node locally:
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
        ));

        // Everything is stable now, it is now time to simulate evil...
        // but first make sure we have no initializing shards and all is green
        // (waiting for green here, because indexing / search in a yellow index is fine as long as no other nodes go down)
        ensureGreen("test");

        TwoPartitions partitions = TwoPartitions.random(random(), internalCluster().getNodeNames());
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);

        assertEquals(1, partitions.getMinoritySide().size());
        final String isolatedNode = partitions.getMinoritySide().iterator().next();
        assertEquals(2, partitions.getMajoritySide().size());
        final String nonIsolatedNode = partitions.getMajoritySide().iterator().next();

        // Simulate a network issue between the unlucky node and the rest of the cluster.
        networkDisruption.startDisrupting();


        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, DiscoverySettings.NO_MASTER_BLOCK_WRITES, TimeValue.timeValueSeconds(10));


        logger.info("wait until elected master has been removed and a new 2 node cluster was from (via [{}])", isolatedNode);
        ensureStableCluster(2, nonIsolatedNode);

        for (String node : partitions.getMajoritySide()) {
            ClusterState nodeState = getNodeClusterState(node);
            boolean success = true;
            if (nodeState.nodes().getMasterNode() == null) {
                success = false;
            }
            if (!nodeState.blocks().global().isEmpty()) {
                success = false;
            }
            if (!success) {
                fail("node [" + node + "] has no master or has blocks, despite of being on the right side of the partition. State dump:\n"
                        + nodeState);
            }
        }


        networkDisruption.stopDisrupting();

        // Wait until the master node sees al 3 nodes again.
        ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkDisruption.expectedTimeToHeal().millis()));

        logger.info("Verify no master block with {} set to {}", DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "all");
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "all"))
                .get();

        networkDisruption.startDisrupting();


        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, DiscoverySettings.NO_MASTER_BLOCK_ALL, TimeValue.timeValueSeconds(10));

        // make sure we have stable cluster & cross partition recoveries are canceled by the removal of the missing node
        // the unresponsive partition causes recoveries to only time out after 15m (default) and these will cause
        // the test to fail due to unfreed resources
        ensureStableCluster(2, nonIsolatedNode);

    }

    void assertDiscoveryCompleted(List<String> nodes) throws InterruptedException {
        for (final String node : nodes) {
            assertTrue(
                    "node [" + node + "] is still joining master",
                    awaitBusy(
                            () -> !((ZenDiscovery) internalCluster().getInstance(Discovery.class, node)).joiningCluster(),
                            30,
                            TimeUnit.SECONDS
                    )
            );
        }
    }

}
