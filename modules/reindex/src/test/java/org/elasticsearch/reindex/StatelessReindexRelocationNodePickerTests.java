/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

public class StatelessReindexRelocationNodePickerTests extends ESTestCase {

    private static final Set<DiscoveryNodeRole> SEARCH_NODE_ROLES = Set.of(
        DiscoveryNodeRole.SEARCH_ROLE,
        DiscoveryNodeRole.TRANSFORM_ROLE,
        DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE
    );
    private static final Set<DiscoveryNodeRole> INDEXING_NODE_ROLES = Set.of(
        DiscoveryNodeRole.INDEX_ROLE,
        DiscoveryNodeRole.INGEST_ROLE,
        DiscoveryNodeRole.MASTER_ROLE,
        DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE
    );
    private static final Set<DiscoveryNodeRole> ML_NODE_ROLES = Set.of(
        DiscoveryNodeRole.ML_ROLE,
        DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE
    );

    private final StatelessReindexRelocationNodePicker picker = new StatelessReindexRelocationNodePicker();

    public void testPickNode_choiceOfIndexingNodes() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("searchNode1", SEARCH_NODE_ROLES))
            .add(createNode("searchNode2", SEARCH_NODE_ROLES))
            .add(createNode("searchNode3", SEARCH_NODE_ROLES))
            .add(createNode("mlNode1", ML_NODE_ROLES))
            .add(createNode("mlNode2", ML_NODE_ROLES))
            .add(createNode("mlNode3", ML_NODE_ROLES))
            .add(createNode("indexingNode1", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode2", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode3", INDEXING_NODE_ROLES)) // this is the local node
            .add(createNode("indexingNode4", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode5", INDEXING_NODE_ROLES))
            .masterNodeId("indexingNode" + randomIntBetween(1, 5))
            .localNodeId("indexingNode3")
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isPresentWith(oneOf("indexingNode1", "indexingNode2", "indexingNode4", "indexingNode5")));
    }

    public void testPickNode_onlyOtherIndexingNode() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("searchNode1", SEARCH_NODE_ROLES))
            .add(createNode("searchNode2", SEARCH_NODE_ROLES))
            .add(createNode("searchNode3", SEARCH_NODE_ROLES))
            .add(createNode("mlNode1", ML_NODE_ROLES))
            .add(createNode("mlNode2", ML_NODE_ROLES))
            .add(createNode("mlNode3", ML_NODE_ROLES))
            .add(createNode("indexingNode1", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode2", INDEXING_NODE_ROLES)) // this is the local node
            .masterNodeId("indexingNode" + randomIntBetween(1, 2))
            .localNodeId("indexingNode2")
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isPresentWith("indexingNode1"));
    }

    public void testPickNode_skipsIndexingNodeThatIsShuttingDown() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("searchNode1", SEARCH_NODE_ROLES))
            .add(createNode("searchNode2", SEARCH_NODE_ROLES))
            .add(createNode("searchNode3", SEARCH_NODE_ROLES))
            .add(createNode("mlNode1", ML_NODE_ROLES))
            .add(createNode("mlNode2", ML_NODE_ROLES))
            .add(createNode("mlNode3", ML_NODE_ROLES))
            .add(createNode("indexingNode1", INDEXING_NODE_ROLES)) // this node is marked for shutdown
            .add(createNode("indexingNode2", INDEXING_NODE_ROLES)) // this is the local node
            .add(createNode("indexingNode3", INDEXING_NODE_ROLES))
            .masterNodeId("indexingNode" + randomIntBetween(1, 3))
            .localNodeId("indexingNode2")
            .build();
        NodesShutdownMetadata nodeShutdowns = randomShutdownForNode("indexingNode1");
        Optional<String> id = picker.pickNode(nodes, nodeShutdowns);
        assertThat(id, isPresentWith("indexingNode3"));
    }

    public void testPickNode_prefersDedicatedCoordinatingNodeIfAvailable() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("searchNode1", SEARCH_NODE_ROLES))
            .add(createNode("searchNode2", SEARCH_NODE_ROLES))
            .add(createNode("searchNode3", SEARCH_NODE_ROLES))
            .add(createNode("mlNode1", ML_NODE_ROLES))
            .add(createNode("mlNode2", ML_NODE_ROLES))
            .add(createNode("mlNode3", ML_NODE_ROLES))
            .add(createNode("indexingNode1", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode2", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode3", INDEXING_NODE_ROLES))
            .add(createNode("coordinatingNode1", Set.of()))
            .add(createNode("coordinatingNode2", Set.of())) // this is the local node
            .add(createNode("coordinatingNode3", Set.of()))
            .masterNodeId("indexingNode" + randomIntBetween(1, 3))
            .localNodeId("coordinatingNode2")
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isPresentWith(oneOf("coordinatingNode1", "coordinatingNode3")));
    }

    public void testPickNode_skipsDedicatedCoordinatingNodeThatIsShuttingDown() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("searchNode1", SEARCH_NODE_ROLES))
            .add(createNode("searchNode2", SEARCH_NODE_ROLES))
            .add(createNode("searchNode3", SEARCH_NODE_ROLES))
            .add(createNode("mlNode1", ML_NODE_ROLES))
            .add(createNode("mlNode2", ML_NODE_ROLES))
            .add(createNode("mlNode3", ML_NODE_ROLES))
            .add(createNode("indexingNode1", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode2", INDEXING_NODE_ROLES)) // this is the local node
            .add(createNode("indexingNode3", INDEXING_NODE_ROLES))
            .add(createNode("coordinatingNode1", Set.of()))
            .add(createNode("coordinatingNode2", Set.of())) // this node is marked for shutdown
            .masterNodeId("indexingNode" + randomIntBetween(1, 3))
            .localNodeId("indexingNode2")
            .build();
        NodesShutdownMetadata nodeShutdowns = randomShutdownForNode("coordinatingNode2");
        Optional<String> id = picker.pickNode(nodes, nodeShutdowns);
        assertThat(id, isPresentWith("coordinatingNode1"));
    }

    public void testPickNode_onlyDedicatedCoordinatingNodeIsLocal_fallsBackToIndexingNode() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("searchNode1", SEARCH_NODE_ROLES))
            .add(createNode("searchNode2", SEARCH_NODE_ROLES))
            .add(createNode("searchNode3", SEARCH_NODE_ROLES))
            .add(createNode("mlNode1", ML_NODE_ROLES))
            .add(createNode("mlNode2", ML_NODE_ROLES))
            .add(createNode("mlNode3", ML_NODE_ROLES))
            .add(createNode("indexingNode1", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode2", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode3", INDEXING_NODE_ROLES))
            .add(createNode("coordinatingNode1", Set.of())) // this is the local node
            .masterNodeId("indexingNode" + randomIntBetween(1, 3))
            .localNodeId("coordinatingNode1")
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isPresentWith(oneOf("indexingNode1", "indexingNode2", "indexingNode3")));
    }

    public void testPickNode_noSuitableNode_returnsNull() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("searchNode1", SEARCH_NODE_ROLES))
            .add(createNode("searchNode2", SEARCH_NODE_ROLES))
            .add(createNode("searchNode3", SEARCH_NODE_ROLES))
            .add(createNode("mlNode1", ML_NODE_ROLES))
            .add(createNode("mlNode2", ML_NODE_ROLES))
            .add(createNode("mlNode3", ML_NODE_ROLES))
            .add(createNode("indexingNode1", INDEXING_NODE_ROLES)) // this is the local node
            .masterNodeId("indexingNode1")
            .localNodeId("indexingNode1")
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isEmpty());
    }

    public void testPickNode_localNodeUnknown_returnsNull() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("searchNode1", SEARCH_NODE_ROLES))
            .add(createNode("searchNode2", SEARCH_NODE_ROLES))
            .add(createNode("searchNode3", SEARCH_NODE_ROLES))
            .add(createNode("mlNode1", ML_NODE_ROLES))
            .add(createNode("mlNode2", ML_NODE_ROLES))
            .add(createNode("mlNode3", ML_NODE_ROLES))
            .add(createNode("indexingNode1", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode2", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode3", INDEXING_NODE_ROLES)) // this is the local node
            .add(createNode("indexingNode4", INDEXING_NODE_ROLES))
            .add(createNode("indexingNode5", INDEXING_NODE_ROLES))
            .masterNodeId("indexingNode" + randomIntBetween(1, 5))
            // We never set the local node
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isEmpty());
    }

    public void testPickNodeCanBeCalledFromDifferentThreads() throws InterruptedException {
        final String[] holder = new String[1];
        final Thread thread = new Thread(() -> {
            DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(createNode("indexingNode1", INDEXING_NODE_ROLES))
                .add(createNode("indexingNode2Local", INDEXING_NODE_ROLES))
                .masterNodeId("indexingNode2Local")
                .localNodeId("indexingNode2Local")
                .build();
            holder[0] = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY).orElseThrow();
        });
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(holder[0], equalTo("indexingNode1"));
    }

    private static DiscoveryNode createNode(String id, Set<DiscoveryNodeRole> roles) {
        return DiscoveryNodeUtils.create(id, buildNewFakeTransportAddress(), Map.of(), roles);
    }

    private static NodesShutdownMetadata randomShutdownForNode(String nodeId) {
        SingleNodeShutdownMetadata.Type type = randomFrom(SingleNodeShutdownMetadata.Type.values());
        return new NodesShutdownMetadata(
            Map.of(
                nodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(nodeId)
                    .setStartedAtMillis(randomMillisUpToYear9999())
                    .setType(type)
                    .setReason(randomAlphaOfLengthBetween(5, 20))
                    .setGracePeriod(type == SingleNodeShutdownMetadata.Type.SIGTERM ? randomPositiveTimeValue() : null)
                    .setTargetNodeName(type == SingleNodeShutdownMetadata.Type.REPLACE ? randomIdentifier("newNode") : null)
                    .build()
            )
        );
    }
}
