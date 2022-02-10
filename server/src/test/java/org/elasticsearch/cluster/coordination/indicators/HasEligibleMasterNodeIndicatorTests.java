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

public class HasEligibleMasterNodeIndicatorTests extends ESTestCase {

    public void testIsGreenIfThereIsMasterNode() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node2", transportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT))
                    .add(new DiscoveryNode("node3", transportAddress(), emptyMap(), Set.of(MASTER_ROLE), Version.CURRENT))
                    .build()
            )
            .build();

        ClusterService clusterService = clusterService(clusterState);
        HealthIndicatorResult noEligibleMasterNodes = new HasEligibleMasterNodeIndicator(clusterService).calculate();

        assertEquals("has_eligible_master", noEligibleMasterNodes.name());
        assertEquals(HealthStatus.GREEN, noEligibleMasterNodes.status());
        assertEquals("There is a master-eligible node.", noEligibleMasterNodes.summary());
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

        HealthIndicatorResult noEligibleMasterNodes = new HasEligibleMasterNodeIndicator(clusterService(clusterState)).calculate();

        assertEquals("has_eligible_master", noEligibleMasterNodes.name());
        assertEquals(HealthStatus.RED, noEligibleMasterNodes.status());
        assertEquals("No master-eligible nodes.", noEligibleMasterNodes.summary());
    }

    public void testRedIfThereNoNodes() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster")).nodes(DiscoveryNodes.builder().build()).build();

        HealthIndicatorResult noEligibleMasterNodes = new HasEligibleMasterNodeIndicator(clusterService(clusterState)).calculate();

        assertEquals("has_eligible_master", noEligibleMasterNodes.name());
        assertEquals(HealthStatus.RED, noEligibleMasterNodes.status());
        assertEquals("No master-eligible nodes.", noEligibleMasterNodes.summary());
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
