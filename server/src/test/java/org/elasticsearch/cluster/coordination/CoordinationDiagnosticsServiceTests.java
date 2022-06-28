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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinationDiagnosticsServiceTests extends AbstractCoordinatorTestCase {
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
        node1 = new DiscoveryNode(
            "node1",
            randomNodeId(),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        node2 = new DiscoveryNode(
            "node2",
            randomNodeId(),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        node3 = new DiscoveryNode(
            "node3",
            randomNodeId(),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        nullMasterClusterState = createClusterState(null);
        node1MasterClusterState = createClusterState(node1);
        node2MasterClusterState = createClusterState(node2);
        node3MasterClusterState = createClusterState(node3);
    }

    @SuppressWarnings("unchecked")
    public void testMoreThanThreeMasterChanges() throws Exception {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        CoordinationDiagnosticsService service = createCoordinationDiagnosticsService(nullMasterClusterState, masterHistoryService);
        // First master:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));

        // Change 1:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, nullMasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node2MasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));

        // Change 2:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));

        // Change 3:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, nullMasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node3MasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));

        // Still node 3, so no change:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, nullMasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));

        // Change 4:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, node3MasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.YELLOW));
        assertThat(result.summary(), equalTo("The elected master node has changed 4 times in the last 30m"));
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails details = result.details();
        List<DiscoveryNode> recentMasters = details.recentMasters();
        // We don't show nulls in the recent_masters list:
        assertThat(recentMasters.size(), equalTo(6));
        for (DiscoveryNode recentMaster : recentMasters) {
            assertThat(recentMaster.getName(), not(emptyOrNullString()));
            assertThat(recentMaster.getId(), not(emptyOrNullString()));
        }
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
        CoordinationDiagnosticsService service = createCoordinationDiagnosticsService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        // Only start counting nulls once the master has been node1, so 1:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        // 2:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        // 3:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        // 4:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        // It has now gone null 4 times, but the master reports that it's ok because the remote history says it has not gone null:
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));

        List<DiscoveryNode> sameAsLocalHistory = localMasterHistory.getNodes();
        when(masterHistoryService.getRemoteMasterHistory()).thenReturn(sameAsLocalHistory);
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.YELLOW));
        assertThat(result.summary(), startsWith("The cluster's master has alternated between "));
        assertThat(result.summary(), endsWith("and no master multiple times in the last 30m"));
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails details = result.details();
        assertThat(details.currentMaster(), equalTo(null));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.YELLOW));
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
        CoordinationDiagnosticsService service = createCoordinationDiagnosticsService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.YELLOW));
        assertThat(result.summary(), startsWith("The cluster's master has alternated between "));
        assertThat(result.summary(), endsWith("and no master multiple times in the last 30m"));
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails details = result.details();
        assertThat(details.currentMaster(), equalTo(null));
        assertThat(details.remoteExceptionMessage(), equalTo("Failure on master"));
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
        CoordinationDiagnosticsService service = createCoordinationDiagnosticsService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.YELLOW));
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
        CoordinationDiagnosticsService service = createCoordinationDiagnosticsService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node1MasterClusterState));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node1MasterClusterState));
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));
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
        testTooManyTransitionsToNull(false, CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.YELLOW);
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
        testTooManyTransitionsToNull(true, CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN);
    }

    private void testTooManyTransitionsToNull(
        boolean remoteHistoryIsNull,
        CoordinationDiagnosticsService.CoordinationDiagnosticsStatus expectedStatus
    ) throws Exception {
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
        CoordinationDiagnosticsService service = createCoordinationDiagnosticsService(nullMasterClusterState, masterHistoryService);
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
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = service.diagnoseMasterStability(true);
        assertThat(result.status(), equalTo(expectedStatus));
    }

    public void testGreenForStableCluster() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.runRandomly();
            cluster.stabilise();
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult healthIndicatorResult = node.coordinationDiagnosticsService
                    .diagnoseMasterStability(true);
                assertThat(healthIndicatorResult.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));
            }
        }
    }

    public void testRedForNoMasterNoQuorum() {
        try (Cluster cluster = new Cluster(4, false, Settings.EMPTY)) {
            // The allNodesMasterEligible=false passed to the Cluster constructor does not guarantee a non-master node in the cluster:
            createAndAddNonMasterNode(cluster);
            cluster.runRandomly();
            cluster.stabilise();
            int masterNodeCount = 0;
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                if (node.getLocalNode().isMasterNode()) {
                    node.disconnect();
                    masterNodeCount++;
                }
            }
            int redNonMasterCount = 0;
            int redMasterCount = 0;
            int greenMasterCount = 0;
            cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                /*
                 * The following line puts the local node's cluster formation state into its map of cluster formation states. This would
                 * ordinarily happen 10s after the master went null (and long before diagnoseMasterStability was called), but this
                 * avoids having to wait 10 seconds.
                 */
                if (node.getLocalNode().isMasterNode()) {
                    node.coordinationDiagnosticsService.nodeToClusterFormationStateOrExceptionMap.put(
                        node.getLocalNode(),
                        new CoordinationDiagnosticsService.ClusterFormationStateOrException(node.coordinator.getClusterFormationState())
                    );
                }
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult healthIndicatorResult = node.coordinationDiagnosticsService
                    .diagnoseMasterStability(true);
                if (node.getLocalNode().isMasterNode() == false) {
                    assertThat(healthIndicatorResult.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                    assertThat(healthIndicatorResult.summary(), containsString("No master eligible nodes found in the cluster"));
                    redNonMasterCount++;
                } else if (healthIndicatorResult.status().equals(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED)) {
                    assertThat(healthIndicatorResult.summary(), containsString("unable to form a quorum"));
                    redMasterCount++;
                } else {
                    assertThat(healthIndicatorResult.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));
                    greenMasterCount++;
                }
            }
            // Non-master nodes see only themselves and think that there are no master nodes:
            assertThat(redNonMasterCount, equalTo(5 - masterNodeCount));
            // The original master node sees only itself, but might think it is still master:
            assertThat(greenMasterCount, lessThanOrEqualTo(1));
            // The other master nodes only see themselves and cannot form a quorum (and sometimes the original master already sees this):
            assertThat(redMasterCount, greaterThanOrEqualTo(masterNodeCount - 1));

            while (cluster.clusterNodes.stream().anyMatch(Cluster.ClusterNode::deliverBlackholedRequests)) {
                logger.debug("--> stabilising again after delivering blackholed requests");
                cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            }
        }
    }

    public void testRedForDiscoveryProblems() {
        try (Cluster cluster = new Cluster(4, false, Settings.EMPTY)) {
            // The allNodesMasterEligible=false passed to the Cluster constructor does not guarantee a non-master node in the cluster:
            createAndAddNonMasterNode(cluster);
            cluster.runRandomly();
            cluster.stabilise();
            int masterNodeCount = 0;
            ConcurrentHashMap<DiscoveryNode, CoordinationDiagnosticsService.ClusterFormationStateOrException> clusterFormationStates =
                new ConcurrentHashMap<>();
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                if (node.getLocalNode().isMasterNode()) {
                    clusterFormationStates.put(
                        node.getLocalNode(),
                        new CoordinationDiagnosticsService.ClusterFormationStateOrException(node.coordinator.getClusterFormationState())
                    );
                    node.disconnect();
                    masterNodeCount++;
                }
            }
            int redNonMasterCount = 0;
            int redMasterCount = 0;
            int greenMasterCount = 0;
            cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                if (node.getLocalNode().isMasterNode()) {
                    // This is artificially forcing a discovery problem:
                    node.coordinationDiagnosticsService.nodeToClusterFormationStateOrExceptionMap = clusterFormationStates;
                }
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult healthIndicatorResult = node.coordinationDiagnosticsService
                    .diagnoseMasterStability(true);
                if (node.getLocalNode().isMasterNode() == false) {
                    assertThat(healthIndicatorResult.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                    assertThat(healthIndicatorResult.summary(), containsString("No master eligible nodes found in the cluster"));
                    redNonMasterCount++;
                } else if (healthIndicatorResult.status().equals(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED)) {
                    assertThat(healthIndicatorResult.summary(), containsString("unable to discover other master eligible nodes"));
                    redMasterCount++;
                } else {
                    assertThat(healthIndicatorResult.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN));
                    greenMasterCount++;
                }
            }
            // Non-master nodes see only themselves and think that there are no master nodes:
            assertThat(redNonMasterCount, equalTo(5 - masterNodeCount));
            // The original master node sees only itself, but might think it is still master:
            assertThat(greenMasterCount, lessThanOrEqualTo(1));
            // The other master nodes only see themselves and cannot form a quorum (and sometimes the original master already sees this):
            assertThat(redMasterCount, greaterThanOrEqualTo(masterNodeCount - 1));

            while (cluster.clusterNodes.stream().anyMatch(Cluster.ClusterNode::deliverBlackholedRequests)) {
                logger.debug("--> stabilising again after delivering blackholed requests");
                cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            }
        }
    }

    public void testYellowWithTooManyMasterChanges() {
        testChangeMasterThreeTimes(2, 100, "The elected master node has changed");
    }

    public void testYellowWithTooManyMasterNullTransitions() {
        testChangeMasterThreeTimes(100, 2, "no master multiple times");
    }

    private void testChangeMasterThreeTimes(int acceptableIdentityChanges, int acceptableNullTransitions, String expectedSummarySubstring) {
        int clusterSize = 5;
        int masterChanges = 3;
        Settings settings = Settings.builder()
            .put(CoordinationDiagnosticsService.IDENTITY_CHANGES_THRESHOLD_SETTING.getKey(), acceptableIdentityChanges)
            .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), acceptableNullTransitions)
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
                leader.heal(); // putting it back in the cluster after another leader has been elected so that we always keep a quorum
                cluster.stabilise();
            }

            final Cluster.ClusterNode currentLeader = cluster.getAnyLeader();
            CoordinationDiagnosticsService.CoordinationDiagnosticsResult healthIndicatorResult =
                currentLeader.coordinationDiagnosticsService.diagnoseMasterStability(true);
            assertThat(healthIndicatorResult.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.YELLOW));
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
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult coordinationDiagnosticsResult =
                    node.coordinationDiagnosticsService.diagnoseMasterStability(true);
                if (leader.getLastAppliedClusterState().getLastCommittedConfiguration().getNodeIds().contains(node.getId())) {
                    assertThat(
                        coordinationDiagnosticsResult.status(),
                        equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN)
                    );
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testRedForNoMasterAndNoMasterEligibleNodes() throws IOException {
        try (Cluster cluster = new Cluster(4, false, Settings.EMPTY)) {
            // The allNodesMasterEligible=false passed to the Cluster constructor does not guarantee a non-master node in the cluster:
            createAndAddNonMasterNode(cluster);
            cluster.runRandomly();
            cluster.stabilise();
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                if (node.getLocalNode().isMasterNode()) {
                    node.disconnect();
                }
            }
            List<Cluster.ClusterNode> removedClusterNodes = new ArrayList<>();
            for (Cluster.ClusterNode clusterNode : cluster.clusterNodes) {
                if (clusterNode.getLocalNode().isMasterNode()) {
                    removedClusterNodes.add(clusterNode);
                }
            }
            cluster.clusterNodes.removeAll(removedClusterNodes);
            cluster.clusterNodes.removeIf(node -> node.getLocalNode().isMasterNode());
            cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = node.coordinationDiagnosticsService
                    .diagnoseMasterStability(true);
                assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                assertThat(result.summary(), equalTo("No master eligible nodes found in the cluster"));
                List<DiscoveryNode> recentMasters = result.details().recentMasters();
                // We don't show nulls in the recent_masters list:
                assertThat(recentMasters.size(), greaterThanOrEqualTo(1));
                for (DiscoveryNode recentMaster : recentMasters) {
                    assertThat(recentMaster.getName(), notNullValue());
                    assertThat(recentMaster.getId(), not(emptyOrNullString()));
                }
                assertThat(result.details().clusterFormationDescription(), startsWith("master not discovered yet"));
            }
            cluster.clusterNodes.addAll(removedClusterNodes);
            while (cluster.clusterNodes.stream().anyMatch(Cluster.ClusterNode::deliverBlackholedRequests)) {
                logger.debug("--> stabilising again after delivering blackholed requests");
                cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testRedForNoMasterAndWithMasterEligibleNodesAndLeader() throws IOException {
        try (Cluster cluster = new Cluster(4, false, Settings.EMPTY)) {
            // The allNodesMasterEligible=false passed to the Cluster constructor does not guarantee a non-master node in the cluster:
            createAndAddNonMasterNode(cluster);
            cluster.runRandomly();
            cluster.stabilise();
            Cluster.ClusterNode currentLeader = cluster.getAnyLeader();
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                if (node.getLocalNode().isMasterNode()) {
                    node.disconnect();
                }
            }
            cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                if (currentLeader.equals(node) == false) { // The current leader still thinks it is leader
                    DiscoveryNodes lastAcceptedNodes = node.coordinator.getLastAcceptedState().nodes();
                    /*
                     * The following has the effect of making the PeerFinder say that there is a leader, even though there is not. It is
                     * effectively saying that there is some leader (this node) which this node has not been able to join. This is just the
                     * easiest way to set up the condition for the test.
                     */
                    node.coordinator.getPeerFinder().deactivate(node.getLocalNode());
                    CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = node.coordinationDiagnosticsService
                        .diagnoseMasterStability(true);
                    assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                    assertThat(result.summary(), containsString("has been elected master, but the node being queried"));
                    List<DiscoveryNode> recentMasters = result.details().recentMasters();
                    // We don't show nulls in the recent_masters list:
                    assertThat(recentMasters.size(), greaterThanOrEqualTo(1));
                    for (DiscoveryNode recentMaster : recentMasters) {
                        assertThat(recentMaster.getName(), notNullValue());
                        assertThat(recentMaster.getId(), not(emptyOrNullString()));
                    }
                    assertThat(result.details().clusterFormationDescription(), startsWith("master not discovered"));
                    // This restores the PeerFinder so that the test cleanup doesn't fail:
                    node.coordinator.getPeerFinder().activate(lastAcceptedNodes);
                }
            }

            while (cluster.clusterNodes.stream().anyMatch(Cluster.ClusterNode::deliverBlackholedRequests)) {
                logger.debug("--> stabilising again after delivering blackholed requests");
                cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testRedForNoMasterAndWithMasterEligibleNodesAndNoLeader() throws IOException {
        try (Cluster cluster = new Cluster(4, false, Settings.EMPTY)) {
            // The allNodesMasterEligible=false passed to the Cluster constructor does not guarantee a non-master node in the cluster:
            createAndAddNonMasterNode(cluster);
            cluster.runRandomly();
            cluster.stabilise();
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                if (node.getLocalNode().isMasterNode()) {
                    node.disconnect();
                }
            }
            cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = node.coordinationDiagnosticsService
                    .diagnoseMasterStability(true);
                if (node.getLocalNode().isMasterNode() == false) {
                    assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                    List<DiscoveryNode> recentMasters = result.details().recentMasters();
                    // We don't show nulls in the recent_masters list:
                    assertThat(recentMasters.size(), greaterThanOrEqualTo(1));
                    for (DiscoveryNode recentMaster : recentMasters) {
                        assertThat(recentMaster.getName(), notNullValue());
                        assertThat(recentMaster.getId(), not(emptyOrNullString()));
                    }
                    assertThat(result.details().clusterFormationDescription(), startsWith("master not discovered"));
                }
            }
            while (cluster.clusterNodes.stream().anyMatch(Cluster.ClusterNode::deliverBlackholedRequests)) {
                logger.debug("--> stabilising again after delivering blackholed requests");
                cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
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

    private static CoordinationDiagnosticsService createCoordinationDiagnosticsService(
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
        TransportService transportService = mock(TransportService.class);
        return new CoordinationDiagnosticsService(clusterService, transportService, coordinator, masterHistoryService);
    }

    private void createAndAddNonMasterNode(Cluster cluster) {
        Cluster.ClusterNode nonMasterNode = cluster.new ClusterNode(
            nextNodeIndex.getAndIncrement(), false, Settings.EMPTY, () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        cluster.clusterNodes.add(nonMasterNode);
    }
}
