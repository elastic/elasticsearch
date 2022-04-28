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
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

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
    private static final String TEST_SOURCE = "test";

    @Before
    public void setup() throws Exception {
        String node1 = randomNodeId();
        String node2 = randomNodeId();
        String node3 = randomNodeId();
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

        // Change 4:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, nullMasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.summary(), equalTo("The master has changed 4 times in the last 30 minutes"));
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
        assertThat(9, equalTo(((Collection<DiscoveryNode>) details.details().get("recent_masters")).size()));
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
        /*
         * Now it has gone null 4 times. If the remote history detects 4 or more _changes_ to master, then the status will be yellow. But
         * 4 master nodes is only 3 changes. So at this point if we have the remote history look like the local history it will say
         * everything is ok because there have only been 3 changes.
         */
        List<DiscoveryNode> sameAsLocalHistory = localMasterHistory.getImmutableView();
        when(masterHistoryService.getRemoteMasterHistory(any())).thenReturn(sameAsLocalHistory);
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.GREEN));
        assertThat(result.summary(), equalTo("The cluster has a stable master node"));

        /*
         * Now make the remote history have another change to bring it up to 4 (the local is unchanged)
         */
        List<DiscoveryNode> remoteHistoryWith4Changes = new ArrayList<>(sameAsLocalHistory);
        remoteHistoryWith4Changes.add(remoteHistoryWith4Changes.get(1)); // Adding another element to the history to make 4 changes

        when(masterHistoryService.getRemoteMasterHistory(any())).thenReturn(remoteHistoryWith4Changes);
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.summary(), startsWith("The cluster's master has alternated between "));
        assertThat(result.summary(), endsWith("and no master multiple times in the last 30 minutes"));
        assertThat(result.impacts().size(), equalTo(1));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertThat(impact.severity(), equalTo(3));
        assertThat(impact.impactDescription(), equalTo("The cluster is at risk of not being able to create, delete, or rebalance indices"));
        assertThat(impact.impactAreas().size(), equalTo(1));
        assertThat(impact.impactAreas().get(0), equalTo(ImpactArea.INGEST));
        SimpleHealthIndicatorDetails details = (SimpleHealthIndicatorDetails) result.details();
        assertThat(details.details().size(), equalTo(1));
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder = ((ToXContent) details.details().get("current_master")).toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        assertThat(builder.getOutputStream().toString(), equalTo("{ }"));

        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.summary(), startsWith("The cluster's master has alternated between "));

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

    public void testGetNumberOfMasterChanges() {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        StableMasterHealthIndicatorService service = createAllocationHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        assertThat(service.getNumberOfMasterChanges(localMasterHistory), equalTo(0L));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertThat(service.getNumberOfMasterChanges(localMasterHistory), equalTo(0L)); // The first master doesn't count as a change
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));
        assertThat(service.getNumberOfMasterChanges(localMasterHistory), equalTo(0L)); // Still only 1 master
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));
        assertThat(service.getNumberOfMasterChanges(localMasterHistory), equalTo(1L));
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, node1MasterClusterState));
        assertThat(service.getNumberOfMasterChanges(localMasterHistory), equalTo(2L));
    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }

    /*
     * Creates a mocked MasterHistoryService with a non-mocked local master history (which can be updated with clusterChanged calls). The
     * remote master history is mocked.
     */
    private static MasterHistoryService createMasterHistoryService() {
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
        Coordinator coordinator = mock(Coordinator.class);
        when(coordinator.getFoundPeers()).thenReturn(Collections.emptyList());
        return new StableMasterHealthIndicatorService(clusterService, masterHistoryService);
    }
}
