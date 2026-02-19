/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PeerConnectionsHealthIndicatorServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private DiscoveryNode node1;
    private DiscoveryNode node2;
    private DiscoveryNode node3;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        node1 = DiscoveryNodeUtils.builder("node_1")
            .name("node_1_name")
            .roles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .build();
        node2 = DiscoveryNodeUtils.builder("node_2").name("node_2_name").roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build();
        node3 = DiscoveryNodeUtils.builder("node_3").name("node_3_name").roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build();
    }

    public void testNameIsPeerConnections() {
        var service = new PeerConnectionsHealthIndicatorService(clusterService);
        assertThat(service.name(), equalTo("peer_connections"));
    }

    public void testGreenWhenNoPeerInfoAvailable() {
        setupClusterState(node1, node2);
        var service = new PeerConnectionsHealthIndicatorService(clusterService);
        HealthInfo healthInfo = new HealthInfo(Map.of(), null, Map.of(), FileSettingsHealthInfo.INDETERMINATE, Map.of());

        HealthIndicatorResult result = service.calculate(true, healthInfo);

        assertThat(result.status(), equalTo(HealthStatus.GREEN));
        assertThat(result.symptom(), containsString("No peer connection issues"));
    }

    public void testGreenWhenAllNodesFullyConnected() {
        setupClusterState(node1, node2, node3);
        var service = new PeerConnectionsHealthIndicatorService(clusterService);
        HealthInfo healthInfo = new HealthInfo(
            Map.of(),
            null,
            Map.of(),
            FileSettingsHealthInfo.INDETERMINATE,
            Map.of(
                node1.getId(),
                PeerConnectionsHealthInfo.HEALTHY,
                node2.getId(),
                PeerConnectionsHealthInfo.HEALTHY,
                node3.getId(),
                PeerConnectionsHealthInfo.HEALTHY
            )
        );

        HealthIndicatorResult result = service.calculate(true, healthInfo);

        assertThat(result.status(), equalTo(HealthStatus.GREEN));
        assertThat(result.symptom(), containsString("No peer connection issues"));
    }

    public void testYellowWhenOneNodeHasDisconnectedPeer() {
        setupClusterState(node1, node2, node3);
        var service = new PeerConnectionsHealthIndicatorService(clusterService);
        HealthInfo healthInfo = new HealthInfo(
            Map.of(),
            null,
            Map.of(),
            FileSettingsHealthInfo.INDETERMINATE,
            Map.of(
                node1.getId(),
                new PeerConnectionsHealthInfo(List.of(node2.getId())),
                node2.getId(),
                PeerConnectionsHealthInfo.HEALTHY,
                node3.getId(),
                PeerConnectionsHealthInfo.HEALTHY
            )
        );

        HealthIndicatorResult result = service.calculate(true, healthInfo);

        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.symptom(), containsString("1 node has disconnected"));
        assertThat(result.impacts(), is(not(empty())));
        assertThat(result.diagnosisList(), is(not(empty())));
    }

    public void testYellowWhenMultipleNodesHaveDisconnectedPeers() {
        setupClusterState(node1, node2, node3);
        var service = new PeerConnectionsHealthIndicatorService(clusterService);
        HealthInfo healthInfo = new HealthInfo(
            Map.of(),
            null,
            Map.of(),
            FileSettingsHealthInfo.INDETERMINATE,
            Map.of(
                node1.getId(),
                new PeerConnectionsHealthInfo(List.of(node2.getId())),
                node2.getId(),
                new PeerConnectionsHealthInfo(List.of(node1.getId())),
                node3.getId(),
                PeerConnectionsHealthInfo.HEALTHY
            )
        );

        HealthIndicatorResult result = service.calculate(true, healthInfo);

        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.symptom(), containsString("2 nodes have disconnected"));
    }

    public void testNoDetailsWhenNotVerbose() {
        setupClusterState(node1, node2);
        var service = new PeerConnectionsHealthIndicatorService(clusterService);
        HealthInfo healthInfo = new HealthInfo(
            Map.of(),
            null,
            Map.of(),
            FileSettingsHealthInfo.INDETERMINATE,
            Map.of(node1.getId(), new PeerConnectionsHealthInfo(List.of(node2.getId())))
        );

        HealthIndicatorResult result = service.calculate(false, healthInfo);

        assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        assertThat(result.diagnosisList(), is(empty()));
    }

    private void setupClusterState(DiscoveryNode... nodes) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(nodes[0].getId()).masterNodeId(nodes[0].getId());
        for (DiscoveryNode node : nodes) {
            nodesBuilder.add(node);
        }
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(nodesBuilder).build();
        when(clusterService.state()).thenReturn(clusterState);
    }
}
