/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.EXTREME_DELAY_VARIABILITY;
import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.ClusterFormationStateOrException;
import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsStatus;
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
        node1 = TestDiscoveryNode.create("node1", randomNodeId());
        node2 = TestDiscoveryNode.create("node2", randomNodeId());
        node3 = TestDiscoveryNode.create("node3", randomNodeId());
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
        /*
         * This test brings up a 4-node cluster (3 master-eligible, one not). After the cluster stabilizes, one node is elected master.
         * Then we disconnect all master nodes from all other nodes and allow the cluster to simulate running for a little longer. At
         * that point, the non-master node thinks that there are no master nodes. The two master eligible nodes that were not initially
         * elected master think that no quorum can be formed. Depending on timing, the node that had originally been elected master
         * either thinks that it is still master, or that no quorum can be formed.
         */
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
        /*
         * In this test we bring up a 4-node cluster (3 master-eligible) and allow it to stabilize. Then we disconnect all master nodes.
         * This actually causes a quorum failure (see testRedForNoMasterNoQuorum()). The reason for this is so that we get into the state
         *  where we have not had a master node recently. Then we swap out he clusterFormationResponses so that the reason appears to be
         * a discovery problem instead of a quorum problem.
         */
        try (Cluster cluster = new Cluster(3, true, Settings.EMPTY)) {
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
                        new CoordinationDiagnosticsService.ClusterFormationStateOrException(getClusterFormationState(false, true))
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
                    node.coordinationDiagnosticsService.clusterFormationResponses = clusterFormationStates;
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
            assertThat(redNonMasterCount, equalTo(1));
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

    public void testRedForNoMasterQueryingNonMaster() {
        /*
         * This test simulates a cluster with 3 master-eligible nodes and two data nodes. It disconnects all master-eligible nodes
         * except one random one, and then asserts that we get the expected response from calling diagnoseMasterStability() on each of
         * the data nodes. It then sets various values for
         * remoteCoordinationDiagnosisResult on each of the non-master-eligible nodes (simulating different
         * responses from a master-eligible node that it has polled), and then asserts that the correct result comes back from
         * diagnoseMasterStability().
         */
        try (Cluster cluster = new Cluster(3, true, Settings.EMPTY)) {
            createAndAddNonMasterNode(cluster);
            createAndAddNonMasterNode(cluster);
            cluster.runRandomly(false, true, EXTREME_DELAY_VARIABILITY);
            cluster.stabilise();
            DiscoveryNode nonKilledMasterNode = cluster.getAnyLeader().getLocalNode();
            for (Cluster.ClusterNode node : cluster.clusterNodes) {
                if (node.getLocalNode().isMasterNode() && node.getLocalNode().equals(nonKilledMasterNode) == false) {
                    node.disconnect();
                }
            }
            cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            for (Cluster.ClusterNode node : cluster.clusterNodes.stream()
                .filter(node -> node.getLocalNode().isMasterNode() == false)
                .toList()) {
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult healthIndicatorResult = node.coordinationDiagnosticsService
                    .diagnoseMasterStability(true);
                assertThat(healthIndicatorResult.status(), equalTo(CoordinationDiagnosticsStatus.RED));
                String summary = healthIndicatorResult.summary();
                assertThat(
                    summary,
                    containsString("No master node observed in the last 30s, and the master eligible nodes are unable to form a quorum")
                );
                CoordinationDiagnosticsStatus artificialRemoteStatus = randomValueOtherThan(
                    CoordinationDiagnosticsStatus.GREEN,
                    () -> randomFrom(CoordinationDiagnosticsStatus.values())
                );
                String artificialRemoteStatusSummary = "Artificial failure";
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult artificialRemoteResult =
                    new CoordinationDiagnosticsService.CoordinationDiagnosticsResult(
                        artificialRemoteStatus,
                        artificialRemoteStatusSummary,
                        null
                    );
                node.coordinationDiagnosticsService.remoteCoordinationDiagnosisResult = new AtomicReference<>(
                    new CoordinationDiagnosticsService.RemoteMasterHealthResult(nonKilledMasterNode, artificialRemoteResult, null)
                );
                healthIndicatorResult = node.coordinationDiagnosticsService.diagnoseMasterStability(true);
                assertThat(healthIndicatorResult.status(), equalTo(artificialRemoteStatus));
                assertThat(healthIndicatorResult.summary(), containsString(artificialRemoteStatusSummary));

                artificialRemoteResult = new CoordinationDiagnosticsService.CoordinationDiagnosticsResult(
                    CoordinationDiagnosticsStatus.GREEN,
                    artificialRemoteStatusSummary,
                    null
                );
                node.coordinationDiagnosticsService.remoteCoordinationDiagnosisResult = new AtomicReference<>(
                    new CoordinationDiagnosticsService.RemoteMasterHealthResult(nonKilledMasterNode, artificialRemoteResult, null)
                );
                healthIndicatorResult = node.coordinationDiagnosticsService.diagnoseMasterStability(true);
                assertThat(healthIndicatorResult.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                assertThat(healthIndicatorResult.summary(), containsString("reports that the status is GREEN"));

                Exception artificialRemoteResultException = new RuntimeException(artificialRemoteStatusSummary);
                node.coordinationDiagnosticsService.remoteCoordinationDiagnosisResult = new AtomicReference<>(
                    new CoordinationDiagnosticsService.RemoteMasterHealthResult(nonKilledMasterNode, null, artificialRemoteResultException)
                );
                healthIndicatorResult = node.coordinationDiagnosticsService.diagnoseMasterStability(true);
                assertThat(healthIndicatorResult.status(), equalTo(CoordinationDiagnosticsStatus.RED));
                assertThat(healthIndicatorResult.summary(), containsString("received an exception"));
            }

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
                assertThat(result.details().nodeToClusterFormationDescriptionMap().size(), equalTo(1));
                assertThat(
                    result.details().nodeToClusterFormationDescriptionMap().values().iterator().next(),
                    startsWith("master not" + " discovered")
                );
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
                    assertThat(result.details().nodeToClusterFormationDescriptionMap().size(), equalTo(1));
                    assertThat(
                        result.details().nodeToClusterFormationDescriptionMap().values().iterator().next(),
                        startsWith("master not" + " discovered")
                    );
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
                    assertThat(result.details().nodeToClusterFormationDescriptionMap().size(), equalTo(1));
                    assertThat(
                        result.details().nodeToClusterFormationDescriptionMap().values().iterator().next(),
                        startsWith("master not" + " discovered")
                    );
                }
            }
            while (cluster.clusterNodes.stream().anyMatch(Cluster.ClusterNode::deliverBlackholedRequests)) {
                logger.debug("--> stabilising again after delivering blackholed requests");
                cluster.runFor(DEFAULT_STABILISATION_TIME, "Cannot call stabilise() because there is no master");
            }
        }
    }

    public void testDiagnoseOnHaveNotSeenMasterRecentlyAndWeAreMasterEligible() {
        MasterHistory localMasterHistory = mock(MasterHistory.class);
        Collection<DiscoveryNode> masterEligibleNodes = List.of(node1, node2, node3);
        boolean explain = true;

        /*
         * In this test, we have an exception rather than the ClusterFormationState for one of the nodes, we expect the status to be RED
         * and the summary to indicate that there was an exception.
         */
        ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> clusterFormationResponses = new ConcurrentHashMap<>();
        clusterFormationResponses.put(
            node1,
            new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), randomBoolean()))
        );
        clusterFormationResponses.put(
            node2,
            new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), randomBoolean()))
        );
        clusterFormationResponses.put(node3, new ClusterFormationStateOrException(new RuntimeException()));
        TimeValue nodeHasMasterLookupTimeframe = new TimeValue(1, TimeUnit.MILLISECONDS);
        Coordinator coordinator = mock(Coordinator.class);
        ClusterFormationFailureHelper.ClusterFormationState localClusterFormationState = getClusterFormationState(true, true);
        when(coordinator.getClusterFormationState()).thenReturn(localClusterFormationState);
        when(coordinator.getLocalNode()).thenReturn(node1);
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = CoordinationDiagnosticsService
            .diagnoseOnHaveNotSeenMasterRecentlyAndWeAreMasterEligible(
                localMasterHistory,
                masterEligibleNodes,
                coordinator,
                clusterFormationResponses,
                nodeHasMasterLookupTimeframe,
                explain
            );
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
        assertThat(result.summary(), containsString(" an exception occurred while reaching out to "));

        /*
         * In this test, the ClusterFormationStates of all of the nodes appear fine. Since we only run this method when there has been no
         *  master recently, we expect the status to be RED and the summary to indicate that we don't know what the problem is.
         */
        clusterFormationResponses = new ConcurrentHashMap<>();
        clusterFormationResponses.put(node1, new ClusterFormationStateOrException(getClusterFormationState(true, true)));
        clusterFormationResponses.put(node2, new ClusterFormationStateOrException(getClusterFormationState(true, true)));
        clusterFormationResponses.put(node3, new ClusterFormationStateOrException(getClusterFormationState(true, true)));
        result = CoordinationDiagnosticsService.diagnoseOnHaveNotSeenMasterRecentlyAndWeAreMasterEligible(
            localMasterHistory,
            masterEligibleNodes,
            coordinator,
            clusterFormationResponses,
            nodeHasMasterLookupTimeframe,
            explain
        );
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
        assertThat(result.summary(), containsString(" the cause has not been determined"));

        /*
         * In this test, we have the ClusterFormationState of one of the node reports that it cannot form a quorum, so we expect the
         * status to be RED and the summary to indicate that we are unable to form a quorum.
         */
        clusterFormationResponses = new ConcurrentHashMap<>();
        clusterFormationResponses.put(node1, new ClusterFormationStateOrException(getClusterFormationState(true, true)));
        clusterFormationResponses.put(node2, new ClusterFormationStateOrException(getClusterFormationState(true, true)));
        clusterFormationResponses.put(node3, new ClusterFormationStateOrException(getClusterFormationState(true, false)));
        result = CoordinationDiagnosticsService.diagnoseOnHaveNotSeenMasterRecentlyAndWeAreMasterEligible(
            localMasterHistory,
            masterEligibleNodes,
            coordinator,
            clusterFormationResponses,
            nodeHasMasterLookupTimeframe,
            explain
        );
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
        assertThat(result.summary(), containsString(" the master eligible nodes are unable to form a quorum"));

        /*
         * In this test, we have the ClusterFormationState of one of the node reports that its foundPeers does not include all known
         * master eligible nodes, so we expect the status to be RED and the summary to indicate that we are unable to discover all nodes.
         */
        clusterFormationResponses = new ConcurrentHashMap<>();
        clusterFormationResponses.put(node1, new ClusterFormationStateOrException(getClusterFormationState(true, true)));
        clusterFormationResponses.put(node2, new ClusterFormationStateOrException(getClusterFormationState(false, true)));
        clusterFormationResponses.put(node3, new ClusterFormationStateOrException(getClusterFormationState(true, false)));
        result = CoordinationDiagnosticsService.diagnoseOnHaveNotSeenMasterRecentlyAndWeAreMasterEligible(
            localMasterHistory,
            masterEligibleNodes,
            coordinator,
            clusterFormationResponses,
            nodeHasMasterLookupTimeframe,
            explain
        );
        assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
        assertThat(result.summary(), containsString(" some master eligible nodes are unable to discover other master eligible nodes"));
    }

    public void testAnyNodeInClusterReportsDiscoveryProblems() {
        Collection<DiscoveryNode> masterEligibleNodes = List.of(node1, node2, node3);
        /*
         * In this test, the foundPeers of the ClusterFormationStates of all nodes contain all known master eligible nodes, so we expect
         * anyNodeInClusterReportsDiscoveryProblems() to be false since there are no discovery problems.
         */
        ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> clusterFormationResponses = new ConcurrentHashMap<>();
        clusterFormationResponses.put(node1, new ClusterFormationStateOrException(getClusterFormationState(true, randomBoolean())));
        clusterFormationResponses.put(node2, new ClusterFormationStateOrException(getClusterFormationState(true, randomBoolean())));
        clusterFormationResponses.put(node3, new ClusterFormationStateOrException(getClusterFormationState(true, randomBoolean())));
        Map<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> nodeClusterFormationStateMap = clusterFormationResponses
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().clusterFormationState()));
        assertFalse(
            CoordinationDiagnosticsService.anyNodeInClusterReportsDiscoveryProblems(masterEligibleNodes, nodeClusterFormationStateMap)
        );

        /*
         * In this test, the foundPeers of at least one ClusterFormationState does _not_ include all known master eligible nodes, so we
         * expect anyNodeInClusterReportsDiscoveryProblems() to be true.
         */
        clusterFormationResponses = new ConcurrentHashMap<>();
        clusterFormationResponses.put(
            node1,
            new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), randomBoolean()))
        );
        clusterFormationResponses.put(node2, new ClusterFormationStateOrException(getClusterFormationState(false, randomBoolean())));
        clusterFormationResponses.put(
            node3,
            new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), randomBoolean()))
        );
        nodeClusterFormationStateMap = clusterFormationResponses.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().clusterFormationState()));
        assertTrue(
            CoordinationDiagnosticsService.anyNodeInClusterReportsDiscoveryProblems(masterEligibleNodes, nodeClusterFormationStateMap)
        );
    }

    public void testAnyNodeInClusterReportsQuorumProblems() {
        /*
         * In this test the hasDiscoveredQuorum field of all ClusterFormationStates is true, so we expect
         * anyNodeInClusterReportsQuorumProblems() to return false.
         */
        ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> clusterFormationResponses = new ConcurrentHashMap<>();
        clusterFormationResponses.put(node1, new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), true)));
        clusterFormationResponses.put(node2, new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), true)));
        clusterFormationResponses.put(node3, new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), true)));
        Map<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> nodeClusterFormationStateMap = clusterFormationResponses
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().clusterFormationState()));
        assertFalse(CoordinationDiagnosticsService.anyNodeInClusterReportsQuorumProblems(nodeClusterFormationStateMap));

        /*
         * In this test the hasDiscoveredQuorum field of at least one ClusterFormationState is false, so we expect
         * anyNodeInClusterReportsQuorumProblems() to return true.
         */
        clusterFormationResponses = new ConcurrentHashMap<>();
        clusterFormationResponses.put(
            node1,
            new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), randomBoolean()))
        );
        clusterFormationResponses.put(node2, new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), false)));
        clusterFormationResponses.put(
            node3,
            new ClusterFormationStateOrException(getClusterFormationState(randomBoolean(), randomBoolean()))
        );
        nodeClusterFormationStateMap = clusterFormationResponses.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().clusterFormationState()));
        assertTrue(CoordinationDiagnosticsService.anyNodeInClusterReportsQuorumProblems(nodeClusterFormationStateMap));
    }

    /*
     * Creates a new ClusterFormationState for testing. If hasDiscoveredAllNodes is false, then the foundPeers on the returned
     * ClusterFormationState will be missing at least one known master eligible node. The hasDiscoveredQuorum field in the returned
     * ClusterFormationState is set to the hasDiscoveredQuorum argument of this method.
     */
    private ClusterFormationFailureHelper.ClusterFormationState getClusterFormationState(
        boolean hasDiscoveredAllNodes,
        boolean hasDiscoveredQuorum
    ) {
        Map<String, DiscoveryNode> masterEligibleNodesMap = Map.of("node1", node1, "node2", node2, "node3", node3);
        List<String> initialMasterNodesSetting = Arrays.asList(generateRandomStringArray(7, 30, false, false));
        DiscoveryNode localNode = masterEligibleNodesMap.values().stream().findAny().get();
        List<DiscoveryNode> allMasterEligibleNodes = List.of(node1, node2, node3);
        return new ClusterFormationFailureHelper.ClusterFormationState(
            initialMasterNodesSetting,
            localNode,
            Map.copyOf(masterEligibleNodesMap),
            randomLong(),
            randomLong(),
            new CoordinationMetadata.VotingConfiguration(Collections.emptySet()),
            new CoordinationMetadata.VotingConfiguration(Collections.emptySet()),
            Collections.emptyList(),
            hasDiscoveredAllNodes
                ? allMasterEligibleNodes
                : randomSubsetOf(randomInt(allMasterEligibleNodes.size() - 1), allMasterEligibleNodes),
            randomLong(),
            hasDiscoveredQuorum,
            new StatusInfo(randomFrom(StatusInfo.Status.HEALTHY, StatusInfo.Status.UNHEALTHY), randomAlphaOfLength(20)),
            Collections.emptyList()
        );
    }

    public void testBeginPollingClusterFormationInfo() throws Exception {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.state()).thenReturn(nullMasterClusterState);
        DiscoveryNode localNode = node3;
        when(clusterService.localNode()).thenReturn(localNode);
        Coordinator coordinator = mock(Coordinator.class);
        when(coordinator.getFoundPeers()).thenReturn(List.of(node1, node2, localNode));
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        CoordinationDiagnosticsService coordinationDiagnosticsService = new CoordinationDiagnosticsService(
            clusterService,
            transportService,
            coordinator,
            masterHistoryService
        );

        coordinationDiagnosticsService.beginPollingClusterFormationInfo();
        assertThat(coordinationDiagnosticsService.clusterFormationInfoTasks.size(), equalTo(3));
        coordinationDiagnosticsService.cancelPollingClusterFormationInfo();
        assertThat(coordinationDiagnosticsService.clusterFormationInfoTasks, Matchers.nullValue());
        coordinationDiagnosticsService.clusterChanged(
            new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState)
        );
        assertThat(coordinationDiagnosticsService.clusterFormationInfoTasks.size(), equalTo(3));
        coordinationDiagnosticsService.clusterChanged(
            new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState)
        );
        assertThat(coordinationDiagnosticsService.clusterFormationInfoTasks, Matchers.nullValue());
        /*
         * Note that in this test we will never find any values in clusterFormationResponses because transportService is mocked out.
         * There is not a reasonable way to plug in a transportService to this simple unit test, so testing that is left to an
         * integration test.
         */
    }

    public void testBeginPollingClusterFormationInfoCancel() {
        /*
         * This test sets up a 4-node cluster (3 master eligible). We call beginPollingClusterFormationInfo() on each node. We then
         * cancel all tasks. This simulates what will happen most often in practice -- polling is triggered when the master node goes
         * null, and then polling is cancelled immediately when a new master node is elected, well within the 10 second initial delay. We
         * then simulate the cluster running for a little while, and assert that there are no results from
         * beginPollingClusterFormationInfo().
         */
        try (Cluster cluster = new Cluster(3, true, Settings.EMPTY)) {
            createAndAddNonMasterNode(cluster);
            cluster.runRandomly();
            cluster.stabilise();
            List<DiscoveryNode> masterNodes = cluster.clusterNodes.stream()
                .map(Cluster.ClusterNode::getLocalNode)
                .filter(DiscoveryNode::isMasterNode)
                .toList();
            cluster.clusterNodes.stream().filter(node -> node.getLocalNode().isMasterNode()).forEach(node -> {
                ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> nodeToClusterFormationStateMap = new ConcurrentHashMap<>();
                Map<DiscoveryNode, Scheduler.Cancellable> cancellables = new ConcurrentHashMap<>();
                node.coordinationDiagnosticsService.beginPollingClusterFormationInfo(
                    masterNodes,
                    nodeToClusterFormationStateMap::put,
                    cancellables
                );
                cancellables.values().forEach(Scheduler.Cancellable::cancel); // This is what will most often happen in practice
                cluster.runRandomly(false, true, EXTREME_DELAY_VARIABILITY);
                cluster.stabilise();
                assertThat(nodeToClusterFormationStateMap.size(), equalTo(0));  // Everything was cancelled
            });
        }
    }

    public void testBeginPollingRemoteMasterStabilityDiagnostic() throws Exception {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        var clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.state()).thenReturn(nullMasterClusterState);
        DiscoveryNode localNode = TestDiscoveryNode.create(
            "node4",
            randomNodeId(),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE)
        );
        when(clusterService.localNode()).thenReturn(localNode);
        Coordinator coordinator = mock(Coordinator.class);
        when(coordinator.getFoundPeers()).thenReturn(List.of(node1, node2, localNode));
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        CoordinationDiagnosticsService coordinationDiagnosticsService = new CoordinationDiagnosticsService(
            clusterService,
            transportService,
            coordinator,
            masterHistoryService
        );

        coordinationDiagnosticsService.beginPollingRemoteMasterStabilityDiagnostic();
        assertNotNull(coordinationDiagnosticsService.remoteCoordinationDiagnosisTask);
        assertNotNull(coordinationDiagnosticsService.remoteCoordinationDiagnosisTask.get());
        coordinationDiagnosticsService.cancelPollingRemoteMasterStabilityDiagnostic();
        assertThat(coordinationDiagnosticsService.remoteCoordinationDiagnosisTask, Matchers.nullValue());
        coordinationDiagnosticsService.clusterChanged(
            new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState)
        );
        assertNotNull(coordinationDiagnosticsService.remoteCoordinationDiagnosisTask);
        assertNotNull(coordinationDiagnosticsService.remoteCoordinationDiagnosisTask.get());
        coordinationDiagnosticsService.clusterChanged(
            new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState)
        );
        assertThat(coordinationDiagnosticsService.remoteCoordinationDiagnosisTask, Matchers.nullValue());
        /*
         * Note that in this test we will never find any values in remoteCoordinationDiagnosisResult because transportService is mocked out.
         * There is not a reasonable way to plug in a transportService to this simple unit test, so testing that is left to an
         * integration test.
         */
    }

    public void testBeginPollingRemoteMasterStabilityDiagnosticCancel() {
        /*
         * This test sets up a 5-node cluster (3 master eligible). We call beginPollingRemoteMasterStabilityDiagnostic() on each
         * non-master-eligible node. But we immediately call cancel, which is what will happen in practice most often since usually the
         * master becomes null and then is immediately non-null when a new master is elected. This means that polling will not be started
         *  since there is a 10-second delay, and we expect no results.
         */
        try (Cluster cluster = new Cluster(3, true, Settings.EMPTY)) {
            createAndAddNonMasterNode(cluster);
            createAndAddNonMasterNode(cluster);
            cluster.runRandomly();
            cluster.stabilise();
            List<DiscoveryNode> masterNodes = cluster.clusterNodes.stream()
                .map(Cluster.ClusterNode::getLocalNode)
                .filter(DiscoveryNode::isMasterNode)
                .toList();
            cluster.clusterNodes.stream().filter(node -> node.getLocalNode().isMasterNode() == false).forEach(node -> {
                List<CoordinationDiagnosticsService.RemoteMasterHealthResult> healthResults = new ArrayList<>();
                AtomicReference<Scheduler.Cancellable> cancellableReference = new AtomicReference<>();
                node.coordinationDiagnosticsService.beginPollingRemoteMasterStabilityDiagnostic(healthResults::add, cancellableReference);
                cancellableReference.get().cancel();
                cluster.runRandomly(false, true, EXTREME_DELAY_VARIABILITY);
                cluster.stabilise();

                /*
                 * The cluster has now run normally for some period of time, but cancel() was called before polling began, so we expect
                 * no results:
                 */
                assertThat(healthResults.size(), equalTo(0));
            });

        }
    }

    public void testRemoteMasterHealthResult() {
        expectThrows(IllegalArgumentException.class, () -> new CoordinationDiagnosticsService.RemoteMasterHealthResult(null, null, null));
        expectThrows(
            IllegalArgumentException.class,
            () -> new CoordinationDiagnosticsService.RemoteMasterHealthResult(null, null, new RuntimeException())
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new CoordinationDiagnosticsService.RemoteMasterHealthResult(mock(DiscoveryNode.class), null, null)
        );
    }

    public void testRandomMasterEligibleNode() throws Exception {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.state()).thenReturn(nullMasterClusterState);
        when(clusterService.localNode()).thenReturn(node3);
        Coordinator coordinator = mock(Coordinator.class);
        Set<DiscoveryNode> allMasterEligibleNodes = Set.of(node1, node2, node3);
        when(coordinator.getFoundPeers()).thenReturn(allMasterEligibleNodes);
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        CoordinationDiagnosticsService coordinationDiagnosticsService = new CoordinationDiagnosticsService(
            clusterService,
            transportService,
            coordinator,
            masterHistoryService
        );

        /*
         * Here we're just checking that with a large number of runs (1000) relative to the number of master eligible nodes (3) we always
         * get all 3 master eligible nodes back. This is just so that we know we're not always getting back the same node.
         */
        Set<DiscoveryNode> seenMasterEligibleNodes = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            seenMasterEligibleNodes.add(coordinationDiagnosticsService.getRandomMasterEligibleNode());
        }
        assertThat(seenMasterEligibleNodes, equalTo(allMasterEligibleNodes));
    }

    public void testResultSerialization() {
        CoordinationDiagnosticsService.CoordinationDiagnosticsStatus status = getRandomStatus();
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails details = getRandomDetails();
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult result =
            new CoordinationDiagnosticsService.CoordinationDiagnosticsResult(status, randomAlphaOfLength(30), details);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            result,
            history -> copyWriteable(result, writableRegistry(), CoordinationDiagnosticsService.CoordinationDiagnosticsResult::new),
            this::mutateResult
        );
    }

    public void testStatusSerialization() {
        CoordinationDiagnosticsService.CoordinationDiagnosticsStatus status = getRandomStatus();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            status,
            history -> copyWriteable(
                status,
                writableRegistry(),
                CoordinationDiagnosticsService.CoordinationDiagnosticsStatus::fromStreamInput
            ),
            this::mutateStatus
        );
    }

    public void testDetailsSerialization() {
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails details = getRandomDetails();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            details,
            history -> copyWriteable(details, writableRegistry(), CoordinationDiagnosticsService.CoordinationDiagnosticsDetails::new),
            this::mutateDetails
        );
    }

    private CoordinationDiagnosticsService.CoordinationDiagnosticsDetails mutateDetails(
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails originalDetails
    ) {
        switch (randomIntBetween(1, 5)) {
            case 1 -> {
                return new CoordinationDiagnosticsService.CoordinationDiagnosticsDetails(
                    node2,
                    originalDetails.recentMasters(),
                    originalDetails.remoteExceptionMessage(),
                    originalDetails.remoteExceptionStackTrace(),
                    originalDetails.nodeToClusterFormationDescriptionMap()
                );
            }
            case 2 -> {
                return new CoordinationDiagnosticsService.CoordinationDiagnosticsDetails(
                    originalDetails.currentMaster(),
                    List.of(node1, node2, node3),
                    originalDetails.remoteExceptionMessage(),
                    originalDetails.remoteExceptionStackTrace(),
                    originalDetails.nodeToClusterFormationDescriptionMap()
                );
            }
            case 3 -> {
                return new CoordinationDiagnosticsService.CoordinationDiagnosticsDetails(
                    originalDetails.currentMaster(),
                    originalDetails.recentMasters(),
                    randomAlphaOfLength(30),
                    originalDetails.remoteExceptionStackTrace(),
                    originalDetails.nodeToClusterFormationDescriptionMap()
                );
            }
            case 4 -> {
                return new CoordinationDiagnosticsService.CoordinationDiagnosticsDetails(
                    originalDetails.currentMaster(),
                    originalDetails.recentMasters(),
                    originalDetails.remoteExceptionMessage(),
                    randomAlphaOfLength(100),
                    originalDetails.nodeToClusterFormationDescriptionMap()
                );
            }
            case 5 -> {
                return new CoordinationDiagnosticsService.CoordinationDiagnosticsDetails(
                    originalDetails.currentMaster(),
                    originalDetails.recentMasters(),
                    originalDetails.remoteExceptionMessage(),
                    originalDetails.remoteExceptionStackTrace(),
                    randomMap(0, 7, () -> new Tuple<>(randomNodeId(), randomAlphaOfLength(100)))
                );
            }
            default -> throw new IllegalStateException();
        }
    }

    private CoordinationDiagnosticsService.CoordinationDiagnosticsStatus mutateStatus(
        CoordinationDiagnosticsService.CoordinationDiagnosticsStatus originalStatus
    ) {
        List<CoordinationDiagnosticsService.CoordinationDiagnosticsStatus> notUsedStatuses = Arrays.stream(
            CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.values()
        ).filter(status -> status.equals(originalStatus) == false).toList();
        return randomFrom(notUsedStatuses);
    }

    private CoordinationDiagnosticsService.CoordinationDiagnosticsResult mutateResult(
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult originalResult
    ) {
        switch (randomIntBetween(1, 3)) {
            case 1 -> {
                return new CoordinationDiagnosticsService.CoordinationDiagnosticsResult(
                    originalResult.status(),
                    randomAlphaOfLength(30),
                    originalResult.details()
                );
            }
            case 2 -> {
                return new CoordinationDiagnosticsService.CoordinationDiagnosticsResult(
                    getRandomStatusOtherThan(originalResult.status()),
                    originalResult.summary(),
                    originalResult.details()
                );
            }
            case 3 -> {
                return new CoordinationDiagnosticsService.CoordinationDiagnosticsResult(
                    originalResult.status(),
                    originalResult.summary(),
                    getRandomDetails()
                );
            }
            default -> throw new IllegalStateException();
        }
    }

    private CoordinationDiagnosticsService.CoordinationDiagnosticsStatus getRandomStatus() {
        return randomFrom(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.values());
    }

    private CoordinationDiagnosticsService.CoordinationDiagnosticsStatus getRandomStatusOtherThan(
        CoordinationDiagnosticsService.CoordinationDiagnosticsStatus otherThanThis
    ) {
        return randomFrom(
            Arrays.stream(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.values())
                .filter(status -> status.equals(otherThanThis) == false)
                .toList()
        );
    }

    private CoordinationDiagnosticsService.CoordinationDiagnosticsDetails getRandomDetails() {
        return new CoordinationDiagnosticsService.CoordinationDiagnosticsDetails(
            node1,
            List.of(node1, node2),
            randomNullableStringOfLengthBetween(0, 30),
            randomNullableStringOfLengthBetween(0, 30),
            Map.of(randomNodeId(), randomAlphaOfLengthBetween(0, 30))
        );
    }

    public static String randomNullableStringOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
        if (randomBoolean()) {
            return null;
        }
        return randomAlphaOfLengthBetween(minCodeUnits, maxCodeUnits);
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
