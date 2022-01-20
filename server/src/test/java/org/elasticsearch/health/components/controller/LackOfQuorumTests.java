/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.components.controller;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.indicators.LackOfQuorumIndicator;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INGEST_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.TRANSFORM_ROLE;
import static org.hamcrest.CoreMatchers.equalTo;

public class LackOfQuorumTests extends ESTestCase {

    public void testIsGreenIfThereIsOnlyOneNode() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(MASTER_ROLE), Version.CURRENT))
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

        HealthIndicatorResult noEligibleMasterNodes = new LackOfQuorumIndicator(clusterState).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(HealthIndicatorResult.of("lack_of_quorum", "cluster_coordination", HealthStatus.GREEN, "Instance can form a quorum."))
        );
    }

    public void testIsGreenIfThereIsQuorumForOneMaster() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node2", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node3", transportAddress(), emptyMap(), Set.of(MASTER_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node3")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new LackOfQuorumIndicator(clusterState).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(HealthIndicatorResult.of("lack_of_quorum", "cluster_coordination", HealthStatus.GREEN, "Instance can form a quorum."))
        );
    }

    public void testIsGreenIfThereIsQuorumFor3Nodes() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node2", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node3", transportAddress(), emptyMap(), Set.of(MASTER_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node4", transportAddress(), emptyMap(), Set.of(MASTER_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node5", transportAddress(), emptyMap(), Set.of(MASTER_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node3", "node4", "node5")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new LackOfQuorumIndicator(clusterState).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(HealthIndicatorResult.of("lack_of_quorum", "cluster_coordination", HealthStatus.GREEN, "Instance can form a quorum."))
        );
    }

    public void testIsRedIfThereIsNoQuorumIfSingleMasterLeave() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node2", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node3")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new LackOfQuorumIndicator(clusterState).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(
                HealthIndicatorResult.of(
                    "lack_of_quorum",
                    "cluster_coordination",
                    HealthStatus.RED,
                    "Not enough nodes with voting rights to form a quorum."
                )
            )
        );
    }

    public void testIsRedIfThereIsNoQuorumIfMoreThanHalfOfMasterNodesLeave() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node2", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node3", transportAddress(), emptyMap(), Set.of(MASTER_ROLE), Version.CURRENT))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("node3", "node4", "node5")))
                            .build()
                    )
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new LackOfQuorumIndicator(clusterState).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(
                HealthIndicatorResult.of(
                    "lack_of_quorum",
                    "cluster_coordination",
                    HealthStatus.RED,
                    "Not enough nodes with voting rights to form a quorum."
                )
            )
        );
    }

    public void testIsRedIfThereNoMasterNodes() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node_1", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node_2", transportAddress(), emptyMap(), Set.of(TRANSFORM_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node_3", transportAddress(), emptyMap(), Set.of(INGEST_ROLE), Version.CURRENT))
                    .build()
            )
            .build();

        HealthIndicatorResult noEligibleMasterNodes = new LackOfQuorumIndicator(clusterState).calculate();

        assertEquals("lack_of_quorum", noEligibleMasterNodes.name());
        assertEquals(HealthStatus.RED, noEligibleMasterNodes.status());
        assertEquals("Not enough nodes with voting rights to form a quorum.", noEligibleMasterNodes.summary());
    }

    public void testRedIfThereNoNodes() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster")).nodes(DiscoveryNodes.builder().build()).build();

        HealthIndicatorResult noEligibleMasterNodes = new LackOfQuorumIndicator(clusterState).calculate();

        assertThat(
            noEligibleMasterNodes,
            equalTo(
                HealthIndicatorResult.of(
                    "lack_of_quorum",
                    "cluster_coordination",
                    HealthStatus.RED,
                    "Not enough nodes with voting rights to form a quorum."
                )
            )
        );
    }

    private static TransportAddress transportAddress() {
        return buildNewFakeTransportAddress();
    }

}
