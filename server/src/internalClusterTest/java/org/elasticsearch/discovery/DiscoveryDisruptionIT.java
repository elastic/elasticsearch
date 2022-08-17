/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;

/**
 * Tests for discovery during disruptions.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiscoveryDisruptionIT extends AbstractDisruptionTestCase {

    /**
     * Test cluster join with issues in cluster state publishing *
     */
    public void testClusterJoinDespiteOfPublishingIssues() throws Exception {
        String masterNode = internalCluster().startMasterOnlyNode();
        String nonMasterNode = internalCluster().startDataOnlyNode();

        DiscoveryNodes discoveryNodes = internalCluster().getInstance(ClusterService.class, nonMasterNode).state().nodes();

        TransportService masterTranspotService = internalCluster().getInstance(
            TransportService.class,
            discoveryNodes.getMasterNode().getName()
        );

        logger.info("blocking requests from non master [{}] to master [{}]", nonMasterNode, masterNode);
        MockTransportService nonMasterTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            nonMasterNode
        );
        nonMasterTransportService.addFailToSendNoConnectRule(masterTranspotService);

        assertNoMaster(nonMasterNode);

        logger.info("blocking cluster state publishing from master [{}] to non master [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            masterNode
        );
        TransportService localTransportService = internalCluster().getInstance(
            TransportService.class,
            discoveryNodes.getLocalNode().getName()
        );
        if (randomBoolean()) {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME);
        } else {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, Coordinator.COMMIT_STATE_ACTION_NAME);
        }

        logger.info("allowing requests from non master [{}] to master [{}], waiting for two join request", nonMasterNode, masterNode);
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        nonMasterTransportService.addSendBehavior(masterTransportService, (connection, requestId, action, request, options) -> {
            if (action.equals(JoinHelper.JOIN_ACTION_NAME)) {
                countDownLatch.countDown();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        nonMasterTransportService.addConnectBehavior(masterTransportService, Transport::openConnection);

        countDownLatch.await();

        logger.info("waiting for cluster to reform");
        masterTransportService.clearOutboundRules(localTransportService);
        nonMasterTransportService.clearOutboundRules(localTransportService);

        ensureStableCluster(2);

        // shutting down the nodes, to avoid the leakage check tripping
        // on the states associated with the commit requests we may have dropped
        internalCluster().stopRandomNonMasterNode();
    }

    public void testClusterFormingWithASlowNode() {

        SlowClusterStateProcessing disruption = new SlowClusterStateProcessing(random(), 0, 0, 1000, 2000);

        // don't wait for initial state, we want to add the disruption while the cluster is forming
        internalCluster().startNodes(3);

        logger.info("applying disruption while cluster is forming ...");

        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        ensureStableCluster(3);
    }

    public void testElectMasterWithLatestVersion() throws Exception {
        final Set<String> nodes = new HashSet<>(internalCluster().startNodes(3));
        ensureStableCluster(3);
        ServiceDisruptionScheme isolateAllNodes = new NetworkDisruption(
            new NetworkDisruption.IsolateAllNodes(nodes),
            NetworkDisruption.DISCONNECT
        );
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
        logger.info("--> preferred master is {}", preferredMaster);
        final Set<String> nonPreferredNodes = new HashSet<>(nodes);
        nonPreferredNodes.remove(preferredMasterName);
        final ServiceDisruptionScheme isolatePreferredMaster = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(isolatePreferredMaster);
        isolatePreferredMaster.startDisrupting();

        client(randomFrom(nonPreferredNodes)).admin()
            .indices()
            .prepareCreate("test")
            .setSettings(
                Settings.builder().put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            )
            .get();

        internalCluster().clearDisruptionScheme(false);
        internalCluster().setDisruptionScheme(isolateAllNodes);

        logger.info("--> forcing a complete election again");
        isolateAllNodes.startDisrupting();
        for (String node : nodes) {
            assertNoMaster(node);
        }

        isolateAllNodes.stopDisrupting();

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        if (state.metadata().hasIndex("test") == false) {
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
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            masterNode
        );
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

    public void testJoinWaitsForClusterApplier() throws Exception {
        startCluster(3);

        final var masterName = internalCluster().getMasterName();
        final var victimName = randomValueOtherThan(masterName, () -> randomFrom(internalCluster().getNodeNames()));
        logger.info("--> master [{}], victim [{}]", masterName, victimName);

        // block the cluster applier thread on the victim (we expect no further cluster state applications at this point)
        logger.info("--> blocking victim's applier service");
        final var barrier = new CyclicBarrier(2);
        internalCluster().getInstance(ClusterService.class, victimName).getClusterApplierService().onNewClusterState("block", () -> {
            try {
                barrier.await(10, TimeUnit.SECONDS);
                barrier.await(10, TimeUnit.SECONDS);
                return null;
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, ActionListener.wrap(() -> {}));
        barrier.await(10, TimeUnit.SECONDS);

        // drop the victim from the cluster with a network disruption
        final var masterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, masterName);
        masterTransportService.addFailToSendNoConnectRule(internalCluster().getInstance(TransportService.class, victimName));
        logger.info("--> waiting for victim's departure");
        ensureStableCluster(2, masterName);

        // verify that the victim sends no joins while the applier is blocked
        final var victimTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, victimName);
        victimTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            assertNotEquals(action, JoinHelper.JOIN_ACTION_NAME);
            connection.sendRequest(requestId, action, request, options);
        });

        // fix the network disruption
        logger.info("--> removing network disruption");
        masterTransportService.clearAllRules();
        ensureStableCluster(2, masterName);

        // permit joins again
        victimTransportService.addSendBehavior(null);

        // release the cluster applier thread
        logger.info("--> releasing block on victim's applier service");
        barrier.await(10, TimeUnit.SECONDS);

        logger.info("--> waiting for cluster to heal");
        ensureStableCluster(3);
    }

    public void testJoinWaitsForCircuitBreaker() throws InterruptedException {
        startCluster(3);

        final var masterName = internalCluster().getMasterName();
        final var victimName = randomValueOtherThan(masterName, () -> randomFrom(internalCluster().getNodeNames()));
        logger.info("--> master [{}], victim [{}]", masterName, victimName);

        // fill up the circuit breaker to breaking point
        final var circuitBreaker = internalCluster().getInstance(CircuitBreakerService.class, victimName)
            .getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        long allocationSize = 1;
        while (true) {
            try {
                circuitBreaker.addEstimateBytesAndMaybeBreak(allocationSize, "test");
            } catch (CircuitBreakingException e) {
                circuitBreaker.addWithoutBreaking(allocationSize);
                break;
            }
            allocationSize <<= 1;
            assert 0 <= allocationSize;
        }

        // drop the victim from the cluster with a network disruption
        final var masterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, masterName);
        masterTransportService.addFailToSendNoConnectRule(internalCluster().getInstance(TransportService.class, victimName));
        logger.info("--> waiting for victim's departure");
        ensureStableCluster(2, masterName);

        // verify that the victim sends no joins while the circuit breaker is breaking
        final var victimTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, victimName);
        victimTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            assertNotEquals(action, JoinHelper.JOIN_ACTION_NAME);
            connection.sendRequest(requestId, action, request, options);
        });

        // fix the network disruption
        logger.info("--> removing network disruption");
        masterTransportService.clearAllRules();
        ensureStableCluster(2, masterName);

        // permit joins again
        victimTransportService.addSendBehavior(null);

        // release the breaker
        logger.info("--> releasing allocations from circuit breaker");
        while (0 < allocationSize) {
            circuitBreaker.addWithoutBreaking(-allocationSize);
            allocationSize >>= 1;
        }

        logger.info("--> waiting for cluster to heal");
        ensureStableCluster(3);
    }
}
