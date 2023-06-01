/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static java.util.Collections.emptySet;
import static org.elasticsearch.action.admin.cluster.node.shutdown.TransportPrevalidateNodeRemovalAction.resolveNodes;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;

public class TransportPrevalidateNodeRemovalActionTests extends ESTestCase {

    public void testResolveNodes() {
        String node1Name = "node1";
        String node1Id = "node1-id";
        DiscoveryNode node1 = randomNode(node1Name, node1Id);
        String node2Name = "node2";
        String node2Id = "node2-id";
        DiscoveryNode node2 = randomNode(node2Name, node2Id);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).build();

        assertThat(
            resolveNodes(PrevalidateNodeRemovalRequest.builder().setNames(node1Name).build(), discoveryNodes),
            equalTo(Set.of(node1))
        );
        assertThat(
            resolveNodes(PrevalidateNodeRemovalRequest.builder().setIds(node1Id, node2Id).build(), discoveryNodes),
            equalTo(Set.of(node1, node2))
        );
        expectThrows(
            ResourceNotFoundException.class,
            () -> resolveNodes(PrevalidateNodeRemovalRequest.builder().setNames(node1Name, node1Id).build(), discoveryNodes)
        );
        expectThrows(
            ResourceNotFoundException.class,
            () -> resolveNodes(PrevalidateNodeRemovalRequest.builder().setIds(node1Name, node1Id).build(), discoveryNodes)
        );
        assertThat(
            resolveNodes(
                PrevalidateNodeRemovalRequest.builder().setExternalIds(node1.getExternalId(), node2.getExternalId()).build(),
                discoveryNodes
            ),
            equalTo(Set.of(node1, node2))
        );
    }

    private DiscoveryNode randomNode(String nodeName, String nodeId) {
        return DiscoveryNodeUtils.builder(nodeId).name(nodeName).roles(emptySet()).version(randomVersion(random())).build();
    }
}
