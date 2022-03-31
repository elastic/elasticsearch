/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.LongGCDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.SingleNodeDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests relating to the loss of the master, but which work with the default fault detection settings which are rather lenient and will
 * not detect a master failure too quickly.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class StableMasterDisruptionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    /**
     * Test that no split brain occurs under partial network partition. See https://github.com/elastic/elasticsearch/issues/2488
     */
    public void testFailWithMinimumMasterNodesConfigured() throws Exception {
        List<String> nodes = internalCluster().startNodes(3);
        ensureStableCluster(3);

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
            NetworkDisruption.DISCONNECT
        );
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();

        // Wait until elected master has removed that the unlucky node...
        ensureStableCluster(2, masterNode);

        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        ensureNoMaster(unluckyNode);

        networkDisconnect.stopDisrupting();

        // Wait until the master node sees all 3 nodes again.
        ensureStableCluster(3);

        // The elected master shouldn't have changed, since the unlucky node never could have elected itself as master
        assertThat(internalCluster().getMasterName(), equalTo(masterNode));
    }

    private void ensureNoMaster(String node) throws Exception {
        assertBusy(
            () -> assertNull(
                client(node).admin().cluster().state(new ClusterStateRequest().local(true)).get().getState().nodes().getMasterNode()
            )
        );
    }

    /**
     * Verify that nodes fault detection detects a disconnected node after master reelection
     */
    public void testFollowerCheckerDetectsDisconnectedNodeAfterMasterReelection() throws Exception {
        testFollowerCheckerAfterMasterReelection(NetworkDisruption.DISCONNECT, Settings.EMPTY);
    }

    /**
     * Verify that nodes fault detection detects an unresponsive node after master reelection
     */
    public void testFollowerCheckerDetectsUnresponsiveNodeAfterMasterReelection() throws Exception {
        testFollowerCheckerAfterMasterReelection(
            NetworkDisruption.UNRESPONSIVE,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "4")
                .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "10s")
                .build()
        );
    }

    private void testFollowerCheckerAfterMasterReelection(NetworkLinkDisruptionType networkLinkDisruptionType, Settings settings)
        throws Exception {
        internalCluster().startNodes(4, settings);
        ensureStableCluster(4);

        logger.info("--> stopping current master");
        internalCluster().stopCurrentMasterNode();

        ensureStableCluster(3);

        final String master = internalCluster().getMasterName();
        final List<String> nonMasters = Arrays.stream(internalCluster().getNodeNames()).filter(n -> master.equals(n) == false).toList();
        final String isolatedNode = randomFrom(nonMasters);
        final String otherNode = nonMasters.get(nonMasters.get(0).equals(isolatedNode) ? 1 : 0);

        logger.info("--> isolating [{}]", isolatedNode);

        final NetworkDisruption networkDisruption = new NetworkDisruption(
            new TwoPartitions(singleton(isolatedNode), Sets.newHashSet(master, otherNode)),
            networkLinkDisruptionType
        );
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        logger.info("--> waiting for master to remove it");
        ensureStableCluster(2, master);
        ensureNoMaster(isolatedNode);

        networkDisruption.stopDisrupting();
        ensureStableCluster(3);
    }

    /**
     * Tests that emulates a frozen elected master node that unfreezes and pushes its cluster state to other nodes that already are
     * following another elected master node. These nodes should reject this cluster state and prevent them from following the stale master.
     */
    public void testStaleMasterNotHijackingMajority() throws Exception {
        final List<String> nodes = internalCluster().startNodes(
            3,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                .build()
        );
        ensureStableCluster(3);

        // Save the current master node as old master node, because that node will get frozen
        final String oldMasterNode = internalCluster().getMasterName();

        // Simulating a painful gc by suspending all threads for a long time on the current elected master node.
        SingleNodeDisruption masterNodeDisruption = new LongGCDisruption(random(), oldMasterNode);

        // Save the majority side
        final List<String> majoritySide = new ArrayList<>(nodes);
        majoritySide.remove(oldMasterNode);

        // Keeps track of the previous and current master when a master node transition took place on each node on the majority side:
        final Map<String, List<Tuple<String, String>>> masters = Collections.synchronizedMap(new HashMap<>());
        for (final String node : majoritySide) {
            masters.put(node, new ArrayList<>());
            internalCluster().getInstance(ClusterService.class, node).addListener(event -> {
                DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
                DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
                if (Objects.equals(previousMaster, currentMaster) == false) {
                    logger.info(
                        "--> node {} received new cluster state: {} \n and had previous cluster state: {}",
                        node,
                        event.state(),
                        event.previousState()
                    );
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
        logger.info("--> freezing node [{}]", oldMasterNode);
        masterNodeDisruption.startDisrupting();

        // Wait for majority side to elect a new master
        assertBusy(() -> {
            for (final Map.Entry<String, List<Tuple<String, String>>> entry : masters.entrySet()) {
                final List<Tuple<String, String>> transitions = entry.getValue();
                assertTrue(entry.getKey() + ": " + transitions, transitions.stream().anyMatch(transition -> transition.v2() != null));
            }
        });

        // The old master node is frozen, but here we submit a cluster state update task that doesn't get executed, but will be queued and
        // once the old master node un-freezes it gets executed. The old master node will send this update + the cluster state where it is
        // flagged as master to the other nodes that follow the new master. These nodes should ignore this update.
        internalCluster().getInstance(ClusterService.class, oldMasterNode)
            .submitStateUpdateTask("sneaky-update", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).build();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failure [sneaky-update]", e);
                }
            }, ClusterStateTaskExecutor.unbatched());

        // Save the new elected master node
        final String newMasterNode = internalCluster().getMasterName(majoritySide.get(0));
        logger.info("--> new detected master node [{}]", newMasterNode);

        // Stop disruption
        logger.info("--> unfreezing node [{}]", oldMasterNode);
        masterNodeDisruption.stopDisrupting();

        oldMasterNodeSteppedDown.await(30, TimeUnit.SECONDS);
        logger.info("--> [{}] stepped down as master", oldMasterNode);
        ensureStableCluster(3);

        assertThat(masters.size(), equalTo(2));
        for (Map.Entry<String, List<Tuple<String, String>>> entry : masters.entrySet()) {
            String nodeName = entry.getKey();
            List<Tuple<String, String>> transitions = entry.getValue();
            assertTrue(
                "[" + nodeName + "] should not apply state from old master [" + oldMasterNode + "] but it did: " + transitions,
                transitions.stream().noneMatch(t -> oldMasterNode.equals(t.v2()))
            );
        }
    }

}
