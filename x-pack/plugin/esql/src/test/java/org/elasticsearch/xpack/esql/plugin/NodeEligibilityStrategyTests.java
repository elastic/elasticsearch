/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;

public class NodeEligibilityStrategyTests extends ESTestCase {

    public void testAllNodesReturnsEveryNode() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("data-1").roles(Set.of(DATA_HOT_NODE_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("master-1").roles(Set.of(MASTER_ROLE)).build())
            .build();

        List<DiscoveryNode> eligible = NodeEligibilityStrategy.ALL_NODES.eligibleNodes(nodes);

        assertEquals(2, eligible.size());
    }

    public void testDataNodesOnlyExcludesMasterOnly() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("data-1").roles(Set.of(DATA_HOT_NODE_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("master-1").roles(Set.of(MASTER_ROLE)).build())
            .build();

        List<DiscoveryNode> eligible = NodeEligibilityStrategy.DATA_NODES_ONLY.eligibleNodes(nodes);

        assertEquals(1, eligible.size());
        assertEquals("data-1", eligible.get(0).getId());
    }

    public void testDataNodesOnlyIncludesMultiRoleNode() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("multi-1").roles(Set.of(DATA_HOT_NODE_ROLE, MASTER_ROLE)).build())
            .build();

        List<DiscoveryNode> eligible = NodeEligibilityStrategy.DATA_NODES_ONLY.eligibleNodes(nodes);

        assertEquals(1, eligible.size());
    }

    public void testEmptyNodesReturnsEmpty() {
        DiscoveryNodes nodes = DiscoveryNodes.builder().build();

        assertTrue(NodeEligibilityStrategy.ALL_NODES.eligibleNodes(nodes).isEmpty());
        assertTrue(NodeEligibilityStrategy.DATA_NODES_ONLY.eligibleNodes(nodes).isEmpty());
    }
}
