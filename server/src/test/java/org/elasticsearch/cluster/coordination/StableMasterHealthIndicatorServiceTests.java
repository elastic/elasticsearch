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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
    public void testGetHealthIndicatorResultNotGreenExplainTrue() throws Exception {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        StableMasterHealthIndicatorService service = createStableMasterHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        List<DiscoveryNode> recentMasters = List.of(node2, node1);
        String node1ClusterFormation = randomAlphaOfLength(100);
        String node2ClusterFormation = randomAlphaOfLength(100);
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails coordinationDiagnosticsDetails =
            new CoordinationDiagnosticsService.CoordinationDiagnosticsDetails(
                node1,
                recentMasters,
                null,
                Map.of(node1.getId(), node1ClusterFormation, node2.getId(), node2ClusterFormation)
            );
        CoordinationDiagnosticsService.CoordinationDiagnosticsStatus inputStatus = randomFrom(
            CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED,
            CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.YELLOW
        );
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult coordinationDiagnosticsResult =
            new CoordinationDiagnosticsService.CoordinationDiagnosticsResult(
                inputStatus,
                "the summary of the diagnostic",
                coordinationDiagnosticsDetails
            );
        HealthIndicatorResult result = service.getHealthIndicatorResult(coordinationDiagnosticsResult, true);
        assertThat(result.status(), equalTo(HealthStatus.fromCoordinationDiagnosticsStatus(inputStatus)));
        assertThat(result.symptom(), equalTo("the summary of the diagnostic"));
        assertThat(result.impacts().size(), equalTo(3));
        assertThat(result.name(), equalTo(StableMasterHealthIndicatorService.NAME));
        HealthIndicatorDetails details = result.details();
        Map<String, Object> detailsMap = xContentToMap(details);
        assertThat(detailsMap.size(), equalTo(3));
        Map<String, String> currentMasterInResult = (Map<String, String>) detailsMap.get("current_master");
        assertThat(currentMasterInResult.get("name"), equalTo(node1.getName()));
        assertThat(currentMasterInResult.get("node_id"), equalTo(node1.getId()));
        Collection<Object> recentMastersInResult = ((Collection<Object>) detailsMap.get("recent_masters"));
        // We don't show nulls in the recent_masters list:
        assertThat(recentMastersInResult.size(), equalTo(2));
        for (Object recentMaster : recentMastersInResult) {
            Map<String, String> recentMasterMap = (Map<String, String>) recentMaster;
            assertThat(recentMasterMap.get("name"), not(emptyOrNullString()));
            assertThat(recentMasterMap.get("node_id"), not(emptyOrNullString()));
        }
        Map<String, String> clusterFormationMap = (Map<String, String>) detailsMap.get("cluster_formation");
        assertThat(clusterFormationMap.size(), equalTo(2));
        assertThat(clusterFormationMap.get(node1.getId()), equalTo(node1ClusterFormation));
        assertThat(clusterFormationMap.get(node2.getId()), equalTo(node2ClusterFormation));
        List<Diagnosis> diagnosis = result.diagnosisList();
        assertThat(diagnosis.size(), equalTo(1));
        assertThat(diagnosis.get(0), is(StableMasterHealthIndicatorService.CONTACT_SUPPORT_USER_ACTION));
    }

    @SuppressWarnings("unchecked")
    public void testGetHealthIndicatorResultNotGreenExplainFalse() throws Exception {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        StableMasterHealthIndicatorService service = createStableMasterHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        List<DiscoveryNode> recentMasters = List.of(node2, node1);
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails coordinationDiagnosticsDetails =
            new CoordinationDiagnosticsService.CoordinationDiagnosticsDetails(node1, recentMasters, null, null);
        CoordinationDiagnosticsService.CoordinationDiagnosticsStatus inputStatus = randomFrom(
            CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.RED,
            CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.YELLOW
        );
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult coordinationDiagnosticsResult =
            new CoordinationDiagnosticsService.CoordinationDiagnosticsResult(
                inputStatus,
                "the summary of the diagnostic",
                coordinationDiagnosticsDetails
            );
        HealthIndicatorResult result = service.getHealthIndicatorResult(coordinationDiagnosticsResult, false);
        assertThat(result.status(), equalTo(HealthStatus.fromCoordinationDiagnosticsStatus(inputStatus)));
        assertThat(result.symptom(), equalTo("the summary of the diagnostic"));
        assertThat(result.impacts().size(), equalTo(3));
        assertThat(result.name(), equalTo(StableMasterHealthIndicatorService.NAME));
        assertThat(result.details(), equalTo(HealthIndicatorDetails.EMPTY));
        List<Diagnosis> diagnosis = result.diagnosisList();
        assertThat(diagnosis.size(), equalTo(0));
    }

    @SuppressWarnings("unchecked")
    public void testGetHealthIndicatorResultGreenOrUnknown() throws Exception {
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        StableMasterHealthIndicatorService service = createStableMasterHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        List<DiscoveryNode> recentMasters = List.of(node2, node1);
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails coordinationDiagnosticsDetails =
            new CoordinationDiagnosticsService.CoordinationDiagnosticsDetails(node1, recentMasters, null, null);
        CoordinationDiagnosticsService.CoordinationDiagnosticsStatus inputStatus = randomFrom(
            CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.GREEN,
            CoordinationDiagnosticsService.CoordinationDiagnosticsStatus.UNKNOWN
        );
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult coordinationDiagnosticsResult =
            new CoordinationDiagnosticsService.CoordinationDiagnosticsResult(
                inputStatus,
                "the summary of the diagnostic",
                coordinationDiagnosticsDetails
            );
        HealthIndicatorResult result = service.getHealthIndicatorResult(coordinationDiagnosticsResult, true);
        assertThat(result.status(), equalTo(HealthStatus.fromCoordinationDiagnosticsStatus(inputStatus)));
        assertThat(result.symptom(), equalTo("the summary of the diagnostic"));
        assertThat(result.impacts().size(), equalTo(0));
        assertThat(result.name(), equalTo(StableMasterHealthIndicatorService.NAME));
        HealthIndicatorDetails details = result.details();
        Map<String, Object> detailsMap = xContentToMap(details);
        assertThat(detailsMap.size(), equalTo(2));
        List<Diagnosis> diagnosis = result.diagnosisList();
        assertThat(diagnosis.size(), equalTo(0));
    }

    @SuppressWarnings("unchecked")
    public void testCalculate() throws Exception {
        /*
         * This method simulates a master flapping null 4 times, and then calling calculate() on the StableMasterHealthIndicatorService.
         */
        MasterHistoryService masterHistoryService = createMasterHistoryService();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        StableMasterHealthIndicatorService service = createStableMasterHealthIndicatorService(nullMasterClusterState, masterHistoryService);
        // First master:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));

        // Change 1:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, nullMasterClusterState));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node2MasterClusterState));

        // Change 2:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node1MasterClusterState, nullMasterClusterState));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node1MasterClusterState));

        // Change 3:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, nullMasterClusterState));

        // Null, so not counted:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, nullMasterClusterState, node3MasterClusterState));

        // Still node 3, so no change:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node3MasterClusterState, nullMasterClusterState));

        // Change 4:
        localMasterHistory.clusterChanged(new ClusterChangedEvent(TEST_SOURCE, node2MasterClusterState, node3MasterClusterState));
        HealthIndicatorResult result = service.calculate(true);
        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.symptom(), equalTo("The elected master node has changed 4 times in the last 30m"));
        assertThat(result.impacts().size(), equalTo(3));
        HealthIndicatorDetails details = result.details();
        Map<String, Object> detailsMap = xContentToMap(details);
        assertThat(detailsMap.size(), equalTo(2));
        Collection<Object> recentMasters = ((Collection<Object>) detailsMap.get("recent_masters"));
        // We don't show nulls in the recent_masters list:
        assertThat(recentMasters.size(), equalTo(6));
        for (Object recentMaster : recentMasters) {
            Map<String, String> recentMasterMap = (Map<String, String>) recentMaster;
            assertThat(recentMasterMap.get("name"), not(emptyOrNullString()));
            assertThat(recentMasterMap.get("node_id"), not(emptyOrNullString()));
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

    private static StableMasterHealthIndicatorService createStableMasterHealthIndicatorService(
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
        return new StableMasterHealthIndicatorService(
            new CoordinationDiagnosticsService(clusterService, transportService, coordinator, masterHistoryService)
        );
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }
}
