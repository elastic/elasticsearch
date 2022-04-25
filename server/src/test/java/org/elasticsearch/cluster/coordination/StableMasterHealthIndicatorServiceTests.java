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
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StableMasterHealthIndicatorServiceTests extends ESTestCase {
    private ClusterState nullMasterClusterState;
    private ClusterState node1MasterClusterState;
    private ClusterState node2MasterClusterState;
    private ClusterState node3MasterClusterState;
    private ClusterState node4MasterClusterState;
    private static final String TEST_SOURCE = "test";

    @Before
    public void setup() throws Exception {
        String node1 = randomNodeId();
        String node2 = randomNodeId();
        String node3 = randomNodeId();
        String node4 = randomNodeId();
        nullMasterClusterState = createClusterState(null);
        node1MasterClusterState = createClusterState(node1);
        node2MasterClusterState = createClusterState(node2);
        node3MasterClusterState = createClusterState(node3);
        node4MasterClusterState = createClusterState(node4);
    }

    @SuppressWarnings("unchecked")
    public void testThreeMasters() throws Exception {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        StableMasterHealthIndicatorService service = createAllocationHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        HealthIndicatorResult result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, node1MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, node2MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));

        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, node1MasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.summary(), equalTo("The master has changed 3 times in the last 30 minutes"));
        assertThat(1, equalTo(result.impacts().size()));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertThat(3, equalTo(impact.severity()));
        assertThat(
            "The cluster currently has a master node, but having multiple master node changes in a short time is an indicator that the "
                + "cluster is at risk of of not being able to create, delete, or rebalance indices",
            equalTo(impact.impactDescription())
        );
        assertThat(1, equalTo(impact.impactAreas().size()));
        assertThat(ImpactArea.INGEST, equalTo(impact.impactAreas().get(0)));
        SimpleHealthIndicatorDetails details = (SimpleHealthIndicatorDetails) result.details();
        assertThat(2, equalTo(details.details().size()));
        assertThat(node3MasterClusterState.nodes().getMasterNode(), equalTo(details.details().get("current_master")));
        assertThat(4, equalTo(((Collection<DiscoveryNode>) details.details().get("recent_masters")).size()));
    }

    public void testMasterGoesNull() throws Exception {
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
        // It has now gone null 3 times, but the master reports that it's ok because the remote history says it has not gone null:
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));
        // But if we have the remote history look like the local history and confirm that it has gone null 3 times, we get a yellow status:
        List<DiscoveryNode> sameAsLocalHistory = localMasterHistory.getImmutableView();
        when(masterHistoryService.getRemoteMasterHistory(any())).thenReturn(sameAsLocalHistory);
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.summary(), startsWith("The cluster's master has alternated between "));
        assertThat(result.summary(), endsWith("and no master multiple times in the last 30 minutes"));
        assertThat(1, equalTo(result.impacts().size()));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertThat(3, equalTo(impact.severity()));
        assertThat("The cluster is at risk of not being able to create, delete, or rebalance indices", equalTo(impact.impactDescription()));
        assertThat(1, equalTo(impact.impactAreas().size()));
        assertThat(ImpactArea.INGEST, equalTo(impact.impactAreas().get(0)));
        SimpleHealthIndicatorDetails details = (SimpleHealthIndicatorDetails) result.details();
        assertThat(1, equalTo(details.details().size()));
        assertThat(null, equalTo(details.details().get("current_master")));
    }

    private static ClusterState createClusterState(String masterNodeId) throws UnknownHostException {
        var routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        if (masterNodeId != null) {
            DiscoveryNode node = new DiscoveryNode(masterNodeId, buildNewFakeTransportAddress(), Version.CURRENT);
            nodesBuilder.masterNodeId(masterNodeId);
            nodesBuilder.add(node);
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
    private static MasterHistoryService createMasterHistoryService() throws ExecutionException, InterruptedException {
        var clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillis()).thenReturn(System.currentTimeMillis());
        MasterHistory localMasterHistory = new MasterHistory(threadPool, clusterService);
        MasterHistoryService masterHistoryService = mock(MasterHistoryService.class);
        when(masterHistoryService.getLocalMasterHistory()).thenReturn(localMasterHistory);
        List<DiscoveryNode> remoteMasterHistory = new ArrayList<>();
        when(masterHistoryService.getRemoteMasterHistory(any())).thenReturn(remoteMasterHistory);
        return masterHistoryService;
    }

    private static StableMasterHealthIndicatorService createAllocationHealthIndicatorService(
        ClusterState clusterState,
        MasterHistoryService masterHistoryService
    ) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(clusterService.localNode()).thenReturn(localNode);
        when(localNode.isMasterNode()).thenReturn(false);
        DiscoveryModule discoveryModule = mock(DiscoveryModule.class);
        Coordinator coordinator = mock(Coordinator.class);
        when(discoveryModule.getCoordinator()).thenReturn(coordinator);
        when(coordinator.getFoundPeers()).thenReturn(Collections.emptyList());
        return new StableMasterHealthIndicatorService(clusterService, discoveryModule, masterHistoryService);
    }
}
