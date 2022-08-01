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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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

        diagnosticsOnBlockedNode.remoteRequestInitialDelay = TimeValue.ZERO;
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
         * can call CoordinationDiagnosticsService#beginPollingRemoteStableMasterHealthIndicatorService without a cluster changed event
         * resulting in the values we pass in being overwritten.
         */
        final List<String> nodeNames = internalCluster().startNodes(3);

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
        diagnosticsOnBlockedNode.remoteStableMasterHealthIndicatorTask = cancellable;

        diagnosticsOnBlockedNode.remoteRequestInitialDelay = TimeValue.ZERO;
        diagnosticsOnBlockedNode.beginPollingRemoteStableMasterHealthIndicatorService(result::set, cancellable);

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
        internalCluster().getInstances(CoordinationDiagnosticsService.class)
            .forEach(coordinationDiagnosticsService -> coordinationDiagnosticsService.remoteRequestInitialDelay = TimeValue.ZERO);
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
                assertNotNull(diagnosticsOnBlockedNode.remoteCoordinationDiagnosisResult.get());
                CoordinationDiagnosticsService.CoordinationDiagnosticsResult result =
                    diagnosticsOnBlockedNode.remoteCoordinationDiagnosisResult.get().result();
                assertNotNull(result);
                assertThat(result.status(), equalTo(CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED));
                assertThat(result.summary(), containsString("unable to form a quorum"));
            });
        });
    }
}
