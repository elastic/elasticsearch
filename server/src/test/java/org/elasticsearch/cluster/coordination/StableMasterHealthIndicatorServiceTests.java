/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StableMasterHealthIndicatorServiceTests extends AbstractCoordinatorTestCase {
    DiscoveryNode node1;
    DiscoveryNode node2;
    DiscoveryNode node3;
    private ClusterState nullMasterClusterState;
    private ClusterState node1MasterClusterState;
    private ClusterState node2MasterClusterState;
    private ClusterState node3MasterClusterState;
    private static final String TEST_SOURCE = "test";

    @Before
    public void setup() throws Exception {
        node1 = new DiscoveryNode(randomNodeId(), buildNewFakeTransportAddress(), Version.CURRENT);
        node2 = new DiscoveryNode(randomNodeId(), buildNewFakeTransportAddress(), Version.CURRENT);
        node3 = new DiscoveryNode(randomNodeId(), buildNewFakeTransportAddress(), Version.CURRENT);
        nullMasterClusterState = createClusterState(null);
        node1MasterClusterState = createClusterState(node1);
        node2MasterClusterState = createClusterState(node2);
        node3MasterClusterState = createClusterState(node3);
    }

    @SuppressWarnings("unchecked")
    public void testMoreThanThreeMasterChanges() throws Exception {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        StableMasterHealthIndicatorService service = createAllocationHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        // First master:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        HealthIndicatorResult result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        // Change 1:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, nullMasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node2MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        // Change 2:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        // Change 3:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, nullMasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node3MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        // Still node 3, so no change:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, nullMasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        // Change 4:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, node3MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.summary(), equalTo("The master has changed 4 times in the last 30m"));
        assertThat(3, equalTo(result.impacts().size()));
        SimpleHealthIndicatorDetails details = (SimpleHealthIndicatorDetails) result.details();
        assertThat(2, equalTo(details.details().size()));
        // We don't show nulls in the recent_masters list:
        assertThat(6, equalTo(((Collection<DiscoveryNode>) details.details().get("recent_masters")).size()));
    }

    public void testMasterGoesNull() throws Exception {
        /*
         * On the local node:
         *   node1 -> null -> node1 -> null -> node1 -> null -> node1 -> null -> node1
         * On the master node:
         *   node1 -> null -> node1 -> null -> node1 -> null -> node1-> null -> node1
         * In this case, the master identity changed 0 times as seen from the local node. The same master went null 4 times as seen from
         * the local node. So we check the remote history. The remote history sees that the master went to null 4 times, the status is
         * YELLOW.
         */
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        StableMasterHealthIndicatorService service = createAllocationHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        // Only start counting nulls once the master has been node1, so 1:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        HealthIndicatorResult result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        // 2:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        // 3:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        // 4:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        // It has now gone null 4 times, but the master reports that it's ok because the remote history says it has not gone null:
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));

        List<DiscoveryNode> sameAsLocalHistory = localMasterHistory.getNodes();
        when(masterHistoryService.getRemoteMasterHistory()).thenReturn(sameAsLocalHistory);
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.summary(), startsWith("The cluster's master has alternated between "));
        assertThat(result.summary(), endsWith("and no master multiple times in the last 30m"));
        assertThat(result.impacts().size(), equalTo(3));
        SimpleHealthIndicatorDetails details = (SimpleHealthIndicatorDetails) result.details();
        assertThat(details.details().size(), equalTo(1));
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder = ((ToXContent) details.details().get("current_master")).toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        assertThat(builder.getOutputStream().toString(), equalTo("null"));

        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.summary(), startsWith("The cluster's master has alternated between "));

    }

    public void testMasterGoesNullWithRemoteException() throws Exception {
        /*
         * On the local node:
         *   node1 -> null -> node1 -> null -> node1 -> null -> node1 -> null -> node1
         * Connecting to the master node throws an exception
         * In this case, the master identity changed 0 times as seen from the local node. The same master went null 4 times as seen from
         * the local node. So we check the remote history. The remote history throws an exception, so the status is YELLOW.
         */
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        when(masterHistoryService.getRemoteMasterHistory()).thenThrow(new Exception("Failure on master"));
        StableMasterHealthIndicatorService service = createAllocationHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        HealthIndicatorResult result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.summary(), startsWith("The cluster's master has alternated between "));
        assertThat(result.summary(), endsWith("and no master multiple times in the last 30m"));
        assertThat(result.impacts().size(), equalTo(3));
        SimpleHealthIndicatorDetails details = (SimpleHealthIndicatorDetails) result.details();
        assertThat(details.details().size(), equalTo(3));
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder = ((ToXContent) details.details().get("current_master")).toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        assertThat(builder.getOutputStream().toString(), equalTo("null"));
        assertThat(details.details().get("exception_fetching_history"), equalTo("Failure on master"));
    }

    public void testMasterGoesNullLocallyButRemotelyChangesIdentity() throws Exception {
        /*
         * On the local node:
         *   node1 -> null -> node1 -> null -> node1 -> null -> node1 -> null -> node1
         * On the master node:
         *   node1 -> null -> node1 -> node2 -> node3 -> node2 -> node3
         * In this case, the master identity changed 0 times as seen from the local node. The same master went null 4 times as seen from
         * the local node. So we check the remote history. The master only went null here one time, but it changed identity 4 times. So we
         * still get a status of YELLOW. (Note: This scenario might not be possible in the real world for a couple of reasons, but it tests
         * edge cases)
         */
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        List<DiscoveryNode> remoteMasterHistory = new ArrayList<>();
        remoteMasterHistory.add(node1);
        remoteMasterHistory.add(null);
        remoteMasterHistory.add(node1);
        remoteMasterHistory.add(node2);
        remoteMasterHistory.add(node3);
        remoteMasterHistory.add(node2);
        remoteMasterHistory.add(node3);
        when(masterHistoryService.getRemoteMasterHistory()).thenReturn(remoteMasterHistory);
        StableMasterHealthIndicatorService service = createAllocationHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        HealthIndicatorResult result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
    }

    public void testMultipleChangesButIdentityNeverChanges() throws Exception {
        /*
         * On the local node:
         *   node1 -> node1 -> node1 -> node1 -> node1
         * On the master node:
         *   node1 -> node1 -> node1 -> node1 -> node1
         * In this case, the master changed 4 times but there are 0 identity changes since there is only ever node1. So we never even
         * check the remote master, and get a status of GREEN. (Note: This scenario is not possible in the real world because we would
         * see null values in between, so it is just here to test an edge case)
         */
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        when(masterHistoryService.getRemoteMasterHistory()).thenThrow(new RuntimeException("Should never call this"));
        StableMasterHealthIndicatorService service = createAllocationHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node1MasterClusterState));
        HealthIndicatorResult result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));
    }

    public void testYellowOnProblematicRemoteHistory() throws Exception {
        /*
         * On the local node:
         *   node1 -> null -> node1 -> null -> node1 -> null -> node2 -> null -> node1 -> null -> node1
         * On the master node:
         *   node1 -> null -> node1 -> null -> node1 -> null -> node2 -> null -> node1 -> null -> node1
         * In this case we detect 2 identity changes (node1 -> node2, and node2 -> node1). We detect that node1 has gone to null 5 times. So
         * we get a status of YELLOW.
         */
        testTooManyTransitionsToNull(false, HealthStatus.YELLOW);
    }

    public void testGreenOnNullRemoteHistory() throws Exception {
        /*
         * On the local node:
         *   node1 -> null -> node1 -> null -> node1 -> null -> node2 -> null -> node1 -> null -> node1
         * We don't get the remote master history in time so we don't know what it is.
         * In this case we detect 2 identity changes (node1 -> node2, and node2 -> node1). We detect that node1 has gone to null 5 times. So
         * we contact the remote master, and in this test get null in return as the master history. Since it is not definitive, we return
         *  GREEN.
         */
        testTooManyTransitionsToNull(true, HealthStatus.GREEN);
    }

    private void testTooManyTransitionsToNull(boolean remoteHistoryIsNull, HealthStatus expectedStatus) throws Exception {
        /*
         * On the local node:
         *   node1 -> null -> node1 -> null -> node1 -> null -> node2 -> null -> node1 -> null -> node1
         * On the master node:
         *   node1 -> null -> node1 -> null -> node1 -> null -> node2 -> null -> node1 -> null -> node1
         * In this case we detect 2 identity changes (node1 -> node2, and node2 -> node1). We detect that node1 has gone to 5 times. So
         * we get a status of YELLOW.
         */
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        StableMasterHealthIndicatorService service = createAllocationHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node2MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        List<DiscoveryNode> remoteHistory = remoteHistoryIsNull ? null : localMasterHistory.getNodes();
        when(masterHistoryService.getRemoteMasterHistory()).thenReturn(remoteHistory);
        HealthIndicatorResult result = service.calculate(true);
        assertThat(result.status(), equalTo(expectedStatus));
    }

    public void testGreenForStableCluster() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.runRandomly();
            cluster.stabilise();
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                HealthIndicatorResult healthIndicatorResult = node.stableMasterHealthIndicatorService.calculate(true);
                assertThat(healthIndicatorResult.status(), equalTo(HealthStatus.GREEN));
            }
        }
    }

    public void testRedForNoMaster() {
        try (Cluster cluster = new Cluster(5, false, Settings.EMPTY)) {
            cluster.runRandomly();
            cluster.stabilise();
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                if (node.getLocalNode().isMasterNode()) {
                    node.disconnect();
                }
            }
            cluster.runFor(DEFAULT_STABILISATION_TIME, "stabilizing"); // allow for a reconfiguration
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                HealthIndicatorResult healthIndicatorResult = node.stableMasterHealthIndicatorService.calculate(true);
                if (node.getLocalNode().isMasterNode() == false) {
                    assertThat(healthIndicatorResult.status(), equalTo(HealthStatus.RED));
                }
            }
            while (cluster.clusterNodes.stream().filter(Cluster.ClusterNode::deliverBlackholedRequests).count() != 0L) {
                logger.debug("--> stabilising again after delivering blackholed requests");
                cluster.runFor(DEFAULT_STABILISATION_TIME, "avoiding failure");
            }
        }
    }

    public void testYellowWithTooManyMasterChanges() {
        testChangeMasterThreeTimes(2, 100, "The master has changed");
    }

    public void testYellowWithTooManyMasterNullTransitions() {
        testChangeMasterThreeTimes(100, 2, "no master multiple times");
    }

    private void testChangeMasterThreeTimes(int acceptableIdentityChanges, int acceptableNullTransitions, String expectedSummarySubstring) {
        int clusterSize = 5;
        int masterChanges = 3;
        Settings settings = Settings.builder()
            .put(StableMasterHealthIndicatorService.ACCEPTABLE_IDENTITY_CHANGES_SETTING.getKey(), acceptableIdentityChanges)
            .put(StableMasterHealthIndicatorService.ACCEPTABLE_NULL_TRANSITIONS_SETTING.getKey(), acceptableNullTransitions)
            .build();
        try (Cluster cluster = new Cluster(clusterSize, true, settings)) {
            cluster.runRandomly();
            cluster.stabilise();

            // Force the master to change by disconnecting it:
            for (int i = 0; i < masterChanges; i++) {
                final Cluster.ClusterNode leader = cluster.getAnyLeader();
                logger.info("--> blackholing leader {}", leader);
                leader.disconnect();
                cluster.stabilise();
            }

            final Cluster.ClusterNode currentLeader = cluster.getAnyLeader();
            HealthIndicatorResult healthIndicatorResult = currentLeader.stableMasterHealthIndicatorService.calculate(true);
            assertThat(healthIndicatorResult.status(), equalTo(HealthStatus.YELLOW));
            assertThat(healthIndicatorResult.summary(), containsString(expectedSummarySubstring));
        }
    }

    public void testGreenAfterShrink() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.runRandomly();
            cluster.stabilise();
            {
                final Cluster.ClusterNode leader = cluster.getAnyLeader();
                logger.info("setting auto-shrink reconfiguration to false");
                leader.submitSetAutoShrinkVotingConfiguration(false);
                cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            }
            final Cluster.ClusterNode disconnect1 = cluster.getAnyNode();
            final Cluster.ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);

            logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
            disconnect1.disconnect();
            disconnect2.disconnect();
            cluster.stabilise();

            final Cluster.ClusterNode leader = cluster.getAnyLeader();
            logger.info("setting auto-shrink reconfiguration to true");
            leader.submitSetAutoShrinkVotingConfiguration(true);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY * 2); // allow for a reconfiguration
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                HealthIndicatorResult healthIndicatorResult = node.stableMasterHealthIndicatorService.calculate(true);
                if (leader.getLastAppliedClusterState().getLastCommittedConfiguration().getNodeIds().contains(node.getId())) {
                    assertThat(healthIndicatorResult.status(), equalTo(HealthStatus.GREEN));
                }
            }
        }
    }

    private static ClusterState createClusterState(DiscoveryNode masterNode) {
        var routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        if (masterNode != null) {
            nodesBuilder.masterNodeId(masterNode.getId());
            nodesBuilder.add(masterNode);
        }
        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(routingTableBuilder.build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder)
            .build();
    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }

    /*
     * Creates a mocked MasterHistoryService with a non-mocked local master history (which can be updated with clusterChanged calls). The
     * remote master history is mocked.
     */
    private static MasterHistoryService createMasterHistoryService() throws Exception {
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillis()).thenReturn(System.currentTimeMillis());
        MasterHistory localMasterHistory = new MasterHistory(threadPool, clusterService);
        MasterHistoryService masterHistoryService = mock(MasterHistoryService.class);
        when(masterHistoryService.getLocalMasterHistory()).thenReturn(localMasterHistory);
        List<DiscoveryNode> remoteMasterHistory = new ArrayList<>();
        when(masterHistoryService.getRemoteMasterHistory()).thenReturn(remoteMasterHistory);
        return masterHistoryService;
    }

    private static StableMasterHealthIndicatorService createAllocationHealthIndicatorService(
        ClusterState clusterState,
        MasterHistoryService masterHistoryService
    ) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.state()).thenReturn(clusterState);
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(clusterService.localNode()).thenReturn(localNode);
        when(localNode.isMasterNode()).thenReturn(false);
        Coordinator coordinator = mock(Coordinator.class);
        when(coordinator.getFoundPeers()).thenReturn(Collections.emptyList());
        DiscoveryModule discoveryModule = mock(DiscoveryModule.class);
        TransportService transportService = mock(TransportService.class);
        return new StableMasterHealthIndicatorService(clusterService, discoveryModule, masterHistoryService, transportService);
    }
}
