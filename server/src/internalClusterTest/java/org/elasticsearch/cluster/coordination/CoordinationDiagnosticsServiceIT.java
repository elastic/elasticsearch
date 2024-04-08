/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class CoordinationDiagnosticsServiceIT extends ESIntegTestCase {
    @Before
    private void setBootstrapMasterNodeIndex() {
        internalCluster().setBootstrapMasterNodeIndex(0);
    }

    @After
    public void restoreDefaultInitialDelay() {
        CoordinationDiagnosticsService.remoteRequestInitialDelay = new TimeValue(10, TimeUnit.SECONDS);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testBlockClusterStateProcessingOnOneNode() throws Exception {
        /*
         * This test picks a node that is not elected master, and then blocks cluster state processing on it. The reason is so that we
         * can call CoordinationDiagnosticsService#beginPollingClusterFormationInfo without a cluster changed event resulting in the
         * values we pass in being overwritten.
         */
        final List<String> nodeNames = internalCluster().startNodes(3);

        final String master = internalCluster().getMasterName();
        assertThat(nodeNames, hasItem(master));
        String blockedNode = nodeNames.stream().filter(n -> n.equals(master) == false).findAny().get();
        assertNotNull(blockedNode);
        ensureStableCluster(3);

        DiscoveryNodes discoveryNodes = internalCluster().getInstance(ClusterService.class, master).state().nodes();
        Set<DiscoveryNode> nodesWithoutBlockedNode = discoveryNodes.getNodes()
            .values()
            .stream()
            .filter(n -> n.getName().equals(blockedNode) == false)
            .collect(Collectors.toSet());

        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(blockedNode, random());
        internalCluster().setDisruptionScheme(disruption);
        // stop processing cluster state changes
        disruption.startDisrupting();

        CoordinationDiagnosticsService diagnosticsOnBlockedNode = internalCluster().getInstance(
            CoordinationDiagnosticsService.class,
            blockedNode
        );
        ConcurrentMap<DiscoveryNode, CoordinationDiagnosticsService.ClusterFormationStateOrException> nodeToClusterFormationStateMap =
            new ConcurrentHashMap<>();
        ConcurrentHashMap<DiscoveryNode, Scheduler.Cancellable> cancellables = new ConcurrentHashMap<>();
        diagnosticsOnBlockedNode.clusterFormationResponses = nodeToClusterFormationStateMap;
        diagnosticsOnBlockedNode.clusterFormationInfoTasks = cancellables;

        CoordinationDiagnosticsService.remoteRequestInitialDelay = TimeValue.ZERO;
        diagnosticsOnBlockedNode.beginPollingClusterFormationInfo(
            nodesWithoutBlockedNode,
            nodeToClusterFormationStateMap::put,
            cancellables
        );

        // while the node is blocked from processing cluster state changes it should reach out to the other 2
        // master eligible nodes and get a successful response
        assertBusy(() -> {
            assertThat(cancellables.size(), is(2));
            assertThat(nodeToClusterFormationStateMap.size(), is(2));
            nodesWithoutBlockedNode.forEach(node -> {
                CoordinationDiagnosticsService.ClusterFormationStateOrException result = nodeToClusterFormationStateMap.get(node);
                assertNotNull(result);
                assertNotNull(result.clusterFormationState());
                assertNull(result.exception());
                ClusterFormationFailureHelper.ClusterFormationState clusterFormationState = result.clusterFormationState();
                assertThat(clusterFormationState.getDescription(), not(emptyOrNullString()));
            });
        });

        disruption.stopDisrupting();
    }

    public void testBeginPollingRemoteStableMasterHealthIndicatorService() throws Exception {
        /*
         * This test picks a node that is not elected master, and then blocks cluster state processing on it. The reason is so that we
         * can call CoordinationDiagnosticsService#beginPollingRemoteMasterStabilityDiagnostic without a cluster changed event
         * resulting in the values we pass in being overwritten.
         */
        final List<String> nodeNames = internalCluster().startNodes(3);
        ensureStableCluster(3);

        final String master = internalCluster().getMasterName();
        assertThat(nodeNames, hasItem(master));
        String blockedNode = nodeNames.stream().filter(n -> n.equals(master) == false).findAny().get();
        assertNotNull(blockedNode);

        DiscoveryNodes discoveryNodes = internalCluster().getInstance(ClusterService.class, master).state().nodes();
        Set<DiscoveryNode> nodesWithoutBlockedNode = discoveryNodes.getNodes()
            .values()
            .stream()
            .filter(n -> n.getName().equals(blockedNode) == false)
            .collect(Collectors.toSet());

        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(blockedNode, random());
        internalCluster().setDisruptionScheme(disruption);
        // stop processing cluster state changes
        disruption.startDisrupting();

        CoordinationDiagnosticsService diagnosticsOnBlockedNode = internalCluster().getInstance(
            CoordinationDiagnosticsService.class,
            blockedNode
        );
        AtomicReference<CoordinationDiagnosticsService.RemoteMasterHealthResult> result = new AtomicReference<>();
        AtomicReference<Scheduler.Cancellable> cancellable = new AtomicReference<>();
        diagnosticsOnBlockedNode.remoteCoordinationDiagnosisResult = result;
        diagnosticsOnBlockedNode.remoteCoordinationDiagnosisTask = cancellable;

        CoordinationDiagnosticsService.remoteRequestInitialDelay = TimeValue.ZERO;
        diagnosticsOnBlockedNode.beginPollingRemoteMasterStabilityDiagnostic(result::set, cancellable);

        // while the node is blocked from processing cluster state changes it should reach out to the other 2
        // master eligible nodes and get a successful response
        assertBusy(() -> {
            assertNotNull(result.get());
            assertNotNull(cancellable.get());
            assertNotNull(result.get().result());
            assertNull(result.get().remoteException());
        });

        disruption.stopDisrupting();
    }

    public void testNoQuorumSeenFromNonMasterNodes() throws Exception {
        /*
         * In this test we have three master-eligible nodes. We make it so that the two non-active ones cannot communicate, and then we
         * stop the active master node. Now there is no quorum so a new master cannot be elected. We set the master lookup threshold very
         * low on the data nodes, so when we run the master stability check on each of the master nodes, it will see that there has been no
         * master recently because there is no quorum, so it returns a RED status. In this test we then check the value of
         * remoteCoordinationDiagnosisResult on each of the non-master-eligible nodes to make sure that they have reached out to one of
         * the master-eligible nodes to get the expected result.
         */
        final List<String> masterNodes = internalCluster().startMasterOnlyNodes(
            3,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1)
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
                .put(CoordinationDiagnosticsService.NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.getKey(), new TimeValue(1, TimeUnit.SECONDS))
                .build()
        );
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(
            2,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1)
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
                .put(CoordinationDiagnosticsService.NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.getKey(), new TimeValue(1, TimeUnit.SECONDS))
                .build()
        );
        CoordinationDiagnosticsService.remoteRequestInitialDelay = TimeValue.ZERO;
        ensureStableCluster(5);
        String firstMasterNode = internalCluster().getMasterName();
        List<String> nonActiveMasterNodes = masterNodes.stream().filter(nodeName -> firstMasterNode.equals(nodeName) == false).toList();
        NetworkDisruption networkDisconnect = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(
                Set.of(nonActiveMasterNodes.get(0), dataNodes.get(0)),
                Set.of(nonActiveMasterNodes.get(1), dataNodes.get(1))
            ),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().clearDisruptionScheme();
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();
        internalCluster().stopNode(firstMasterNode);

        assertBusy(() -> {
            dataNodes.forEach(dataNode -> {
                CoordinationDiagnosticsService diagnosticsOnBlockedNode = internalCluster().getInstance(
                    CoordinationDiagnosticsService.class,
                    dataNode
                );
                assertNotNull(diagnosticsOnBlockedNode.remoteCoordinationDiagnosisResult);
                assertNotNull(diagnosticsOnBlockedNode.remoteCoordinationDiagnosisResult.get());
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult result =
                    diagnosticsOnBlockedNode.remoteCoordinationDiagnosisResult.get().result();
                assertNotNull(result);
                assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                assertThat(result.summary(), containsString("unable to form a quorum"));
            });
        });
    }

    public void testNoMasterElected() throws Exception {
        /*
         * This test starts up a 4-node cluster where 3 nodes are master eligible. It then shuts down two of the master eligible nodes and
         * restarts one of the master eligible nodes and the data-only node. We then assert that diagnoseMasterStability returns a red
         * status because a quorum can't be formed on both of those nodes. This is an edge case because since there is no elected master,
         * clusterChanged() is never called (which is what usually kicks off the polling that drives the quorum check).
         */
        Settings settings = Settings.builder().put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build();
        final List<String> masterNodeNames = internalCluster().startMasterOnlyNodes(3, settings);
        final String dataNodeName = internalCluster().startDataOnlyNode(settings);
        ensureStableCluster(4);
        String randomMasterNodeName = randomFrom(masterNodeNames);
        masterNodeNames.stream().filter(nodeName -> nodeName.equals(randomMasterNodeName) == false).forEach(nodeName -> {
            try {
                internalCluster().stopNode(nodeName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        InternalTestCluster.RestartCallback nonValidatingRestartCallback = new InternalTestCluster.RestartCallback() {
            public boolean validateClusterForming() {
                return false;
            }
        };
        CoordinationDiagnosticsService.remoteRequestInitialDelay = TimeValue.ZERO;
        internalCluster().restartNode(randomMasterNodeName, nonValidatingRestartCallback);
        internalCluster().restartNode(dataNodeName, nonValidatingRestartCallback);

        try {
            CoordinationDiagnosticsService diagnosticsOnMasterEligibleNode = internalCluster().getInstance(
                CoordinationDiagnosticsService.class,
                randomMasterNodeName
            );

            CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = diagnosticsOnMasterEligibleNode.diagnoseMasterStability(
                true
            );
            assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
            assertThat(result.summary(), containsString("the master eligible nodes are unable to form a quorum"));
            CoordinationDiagnosticsService diagnosticsOnDataNode = internalCluster().getInstance(
                CoordinationDiagnosticsService.class,
                dataNodeName
            );

            assertBusy(() -> {
                assertNotNull(diagnosticsOnDataNode.remoteCoordinationDiagnosisResult.get());
                assertNotNull(diagnosticsOnDataNode.remoteCoordinationDiagnosisResult.get().result());
                assertThat(
                    diagnosticsOnDataNode.remoteCoordinationDiagnosisResult.get().result().summary(),
                    containsString("the master eligible nodes are unable to form a quorum")
                );
                assertThat(
                    diagnosticsOnDataNode.remoteCoordinationDiagnosisResult.get().result().status(),
                    equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED)
                );
            });
        } finally {
            internalCluster().stopNode(randomMasterNodeName); // This is needed for the test to clean itself up happily
            internalCluster().stopNode(dataNodeName); // This is needed for the test to clean itself up happily
        }
    }

    public void testNoQuorum() throws Exception {
        /*
         * In this test we have three master-eligible nodes and two data-only nodes. We make it so that the two non-active
         * master-eligible nodes cannot communicate with each other but can each communicate with one data-only node, and then we
         * stop the active master node. Now there is no quorum so a new master cannot be elected. We set the master lookup threshold very
         * low on the data nodes, so when we run the master stability check on each of the master nodes, it will see that there has been no
         * master recently and because there is no quorum, so it returns a RED status. We also check that each of the data-only nodes
         * reports a RED status because there is no quorum (having polled that result from the master-eligible node it can communicate
         * with).
         */
        CoordinationDiagnosticsService.remoteRequestInitialDelay = TimeValue.ZERO;
        var settings = Settings.builder()
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1)
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
            .put(CoordinationDiagnosticsService.NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.getKey(), new TimeValue(1, TimeUnit.SECONDS))
            .build();
        var masterNodes = internalCluster().startMasterOnlyNodes(3, settings);
        var dataNodes = internalCluster().startDataOnlyNodes(2, settings);
        ensureStableCluster(5);
        String firstMasterNode = internalCluster().getMasterName();
        List<String> nonActiveMasterNodes = masterNodes.stream().filter(nodeName -> firstMasterNode.equals(nodeName) == false).toList();
        NetworkDisruption networkDisconnect = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(
                Set.of(nonActiveMasterNodes.get(0), dataNodes.get(0)),
                Set.of(nonActiveMasterNodes.get(1), dataNodes.get(1))
            ),
            NetworkDisruption.UNRESPONSIVE
        );

        internalCluster().clearDisruptionScheme();
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();
        internalCluster().stopNode(firstMasterNode);
        for (String nonActiveMasterNode : nonActiveMasterNodes) {
            CoordinationDiagnosticsService diagnosticsOnMasterEligibleNode = internalCluster().getInstance(
                CoordinationDiagnosticsService.class,
                nonActiveMasterNode
            );
            assertBusy(() -> {
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = diagnosticsOnMasterEligibleNode
                    .diagnoseMasterStability(true);
                assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                assertThat(
                    result.summary(),
                    anyOf(
                        containsString("the master eligible nodes are unable to form a quorum"),
                        containsString("the cause has not been determined.")
                    )
                );
            });
        }
        for (String dataNode : dataNodes) {
            CoordinationDiagnosticsService diagnosticsOnDataNode = internalCluster().getInstance(
                CoordinationDiagnosticsService.class,
                dataNode
            );
            assertBusy(() -> {
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult result = diagnosticsOnDataNode.diagnoseMasterStability(true);
                assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                assertThat(
                    result.summary(),
                    anyOf(
                        containsString("the master eligible nodes are unable to form a quorum"),
                        containsString("the cause has not been determined.")
                    )
                );
            });
        }
    }
}
