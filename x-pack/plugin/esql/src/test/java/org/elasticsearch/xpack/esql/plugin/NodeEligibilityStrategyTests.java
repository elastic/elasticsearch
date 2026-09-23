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
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;

public class NodeEligibilityStrategyTests extends ESTestCase {

    public void testSearchAndStatefulDataNodesIncluded() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("data-1").roles(Set.of(DATA_HOT_NODE_ROLE)).build())
            .build();

        List<DiscoveryNode> eligible = NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(nodes);

        assertEquals(Set.of("search-1", "data-1"), ids(eligible));
    }

    public void testIndexMasterAndCoordinatingExcluded() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("index-search-1").roles(Set.of(INDEX_ROLE, SEARCH_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("master-1").roles(Set.of(MASTER_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("coord-1").roles(Set.of()).build())
            .add(DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build())
            .build();

        List<DiscoveryNode> eligible = NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(nodes);

        assertEquals(List.of("search-1"), eligible.stream().map(DiscoveryNode::getId).toList());
    }

    public void testMixedIndexAndSearchYieldsOnlySearch() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("index-2").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-2").roles(Set.of(SEARCH_ROLE)).build())
            .build();

        List<DiscoveryNode> eligible = NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(nodes);

        assertEquals(Set.of("search-1", "search-2"), ids(eligible));
    }

    public void testIndexOnlyClusterYieldsEmptyList() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("index-2").roles(Set.of(INDEX_ROLE)).build())
            .build();

        assertTrue(NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(nodes).isEmpty());
    }

    public void testEmptyNodesReturnsEmpty() {
        DiscoveryNodes nodes = DiscoveryNodes.builder().build();

        assertTrue(NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(nodes).isEmpty());
    }

    public void testStatefulMultiRoleNodeIncluded() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("multi-1").roles(Set.of(DATA_HOT_NODE_ROLE, MASTER_ROLE)).build())
            .build();

        List<DiscoveryNode> eligible = NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(nodes);

        assertEquals(1, eligible.size());
        assertEquals("multi-1", eligible.get(0).getId());
    }

    private static Set<String> ids(List<DiscoveryNode> nodes) {
        return nodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
    }
}
