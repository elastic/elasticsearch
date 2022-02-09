/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.indicators;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INGEST_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.TRANSFORM_ROLE;
import static org.hamcrest.CoreMatchers.equalTo;

public class CanFormQuorumIndicatorTests extends ESTestCase {

    public void testGreenIfThereIsOnlyOneNode() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(MASTER_ROLE, DATA_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node1")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new CanFormQuorumIndicator(clusterService(clusterState)).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(HealthIndicatorResult.of("can_form_quorum", "cluster_coordination", HealthStatus.GREEN, "Instances can form a quorum."))
        );
    }

    public void testRedIfThereNoNodes() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(DiscoveryNodes.builder().build())
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node1")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new CanFormQuorumIndicator(clusterService(clusterState)).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(
                HealthIndicatorResult.of(
                    "can_form_quorum",
                    "cluster_coordination",
                    HealthStatus.RED,
                    "Not enough master-eligible instances to form a quorum."
                )
            )
        );
    }

    public void testGreenForSingleMasterForTwoNodes() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node2", transportAddress(), emptyMap(), Set.of(MASTER_ROLE, DATA_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node2")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new CanFormQuorumIndicator(clusterService(clusterState)).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(HealthIndicatorResult.of("can_form_quorum", "cluster_coordination", HealthStatus.GREEN, "Instances can form a quorum."))
        );
    }

    public void testRedIfTwoNodesAndSingleMasterLeaves() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node2")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new CanFormQuorumIndicator(clusterService(clusterState)).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(
                HealthIndicatorResult.of(
                    "can_form_quorum",
                    "cluster_coordination",
                    HealthStatus.RED,
                    "Not enough master-eligible instances to form a quorum."
                )
            )
        );
    }

    public void testRedIfTwoMastersAndSingleMasterLeaves() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(MASTER_ROLE, DATA_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node1", "node2")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new CanFormQuorumIndicator(clusterService(clusterState)).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(
                HealthIndicatorResult.of(
                    "can_form_quorum",
                    "cluster_coordination",
                    HealthStatus.RED,
                    "Not enough master-eligible instances to form a quorum."
                )
            )
        );
    }

    public void testGreenIfThreeMasterNodes() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(MASTER_ROLE, DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node2", transportAddress(), emptyMap(), Set.of(MASTER_ROLE, DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node3", transportAddress(), emptyMap(), Set.of(MASTER_ROLE, DATA_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node1", "node2", "node3")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new CanFormQuorumIndicator(clusterService(clusterState)).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(HealthIndicatorResult.of("can_form_quorum", "cluster_coordination", HealthStatus.GREEN, "Instances can form a quorum."))
        );
    }

    public void testGreenIfThreeMasterNodesAndOneLeaves() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(MASTER_ROLE, DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node2", transportAddress(), emptyMap(), Set.of(MASTER_ROLE, DATA_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node1", "node2", "node3")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new CanFormQuorumIndicator(clusterService(clusterState)).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(HealthIndicatorResult.of("can_form_quorum", "cluster_coordination", HealthStatus.GREEN, "Instances can form a quorum."))
        );
    }

    public void testGreenIfThreeMasterNodesAndTwoLeave() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node2", transportAddress(), emptyMap(), Set.of(MASTER_ROLE, DATA_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node1", "node2", "node3")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new CanFormQuorumIndicator(clusterService(clusterState)).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(
                HealthIndicatorResult.of(
                    "can_form_quorum",
                    "cluster_coordination",
                    HealthStatus.RED,
                    "Not enough master-eligible instances to form a quorum."
                )
            )
        );
    }

    public void testRedIfThereNoMasterNodes() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node_1", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node_2", transportAddress(), emptyMap(), Set.of(TRANSFORM_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node_3", transportAddress(), emptyMap(), Set.of(INGEST_ROLE), Version.CURRENT))
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new CanFormQuorumIndicator(clusterService(clusterState)).calculate();

        assertEquals("can_form_quorum", noEligibleMasterNodes.name());
        assertEquals(HealthStatus.RED, noEligibleMasterNodes.status());
        assertEquals("Not enough master-eligible instances to form a quorum.", noEligibleMasterNodes.summary());
    }

    private static TransportAddress transportAddress() {
        return buildNewFakeTransportAddress();
    }

    private static ClusterService clusterService(ClusterState clusterState) {
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }
}
