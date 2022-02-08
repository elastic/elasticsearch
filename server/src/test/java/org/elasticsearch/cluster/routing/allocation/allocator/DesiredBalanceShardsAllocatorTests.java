/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;

public class DesiredBalanceShardsAllocatorTests extends ESTestCase {

    public void testNeedsRefreshIfDataNodesChanged() {

        final DiscoveryNodes.Builder initialNodes = DiscoveryNodes.builder();
        for (int i = between(1, 5); i >= 0; i--) {
            initialNodes.add(randomDataNode("data-node-" + i));
        }
        for (int i = between(1, 5); i >= 0; i--) {
            initialNodes.add(randomNonDataNode("non-data-node-" + i));
        }

        final ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT).nodes(initialNodes).build();

        assertFalse(DesiredBalanceShardsAllocator.needsRefresh(initialState, initialState));

        assertFalse(
            DesiredBalanceShardsAllocator.needsRefresh(
                initialState,
                ClusterState.builder(initialState)
                    .nodes(DiscoveryNodes.builder(initialState.nodes()).add(randomNonDataNode("new-node")))
                    .build()
            )
        );

        assertFalse(
            DesiredBalanceShardsAllocator.needsRefresh(
                initialState,
                ClusterState.builder(initialState).nodes(DiscoveryNodes.builder(initialState.nodes()).remove("non-data-node-0")).build()
            )
        );

        assertTrue(
            DesiredBalanceShardsAllocator.needsRefresh(
                initialState,
                ClusterState.builder(initialState)
                    .nodes(DiscoveryNodes.builder(initialState.nodes()).add(randomDataNode("new-node")))
                    .build()
            )
        );

        assertTrue(
            DesiredBalanceShardsAllocator.needsRefresh(
                initialState,
                ClusterState.builder(initialState).nodes(DiscoveryNodes.builder(initialState.nodes()).remove("data-node-0")).build()
            )
        );
    }

    private DiscoveryNode randomNonDataNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(
                randomSubsetOf(DiscoveryNodeRole.roles().stream().filter(r -> r.canContainData() == false).collect(Collectors.toSet()))
            ),
            Version.CURRENT
        );
    }

    private DiscoveryNode randomDataNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(
                randomValueOtherThanMany(
                    roles -> roles.stream().noneMatch(DiscoveryNodeRole::canContainData),
                    () -> randomSubsetOf(DiscoveryNodeRole.roles())
                )
            ),
            Version.CURRENT
        );
    }

    public void testNeedsRefreshIfIndicesChanged() {

    }

}
