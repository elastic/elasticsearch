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

public class StatefulReindexRelocationNodePickerTests extends ESTestCase {

    private final StatefulReindexRelocationNodePicker picker = new StatefulReindexRelocationNodePicker();

    public void testPickNode_prefersDedicatedCoordinatingNode() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("dataMaster1", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster2", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster3", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("data4", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data5", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data6", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("ingest1", DiscoveryNodeRole.INGEST_ROLE))
            .add(createNode("coordinating1"))
            .add(createNode("coordinating2Local"))
            .add(createNode("coordinating3"))
            .masterNodeId("dataMaster" + randomIntBetween(1, 3))
            .localNodeId("coordinating2Local")
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isPresentWith(oneOf("coordinating1", "coordinating3")));
    }

    public void testPickNode_skipsDedicatedCoordinatingNodeThatIsShuttingDown() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("dataMaster1", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster2", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster3", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("data4", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data5", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data6", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("ingest1", DiscoveryNodeRole.INGEST_ROLE))
            .add(createNode("coordinating1ShuttingDown"))
            .add(createNode("coordinating2Local"))
            .add(createNode("coordinating3"))
            .masterNodeId("dataMaster" + randomIntBetween(1, 3))
            .localNodeId("coordinating2Local")
            .build();
        Optional<String> id = picker.pickNode(nodes, randomShutdownForNode("coordinating1ShuttingDown"));
        assertThat(id, isPresentWith("coordinating3"));
    }

    public void testPickNode_noDedicatedCoordinatingNodes_fallsBackToDataNode() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("dataMaster1", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster2", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster3", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("data4", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data5Local", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data6", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("ingest1", DiscoveryNodeRole.INGEST_ROLE))
            .add(createNode("ml1", DiscoveryNodeRole.ML_ROLE))
            .add(createNode("voting1", DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE))
            .masterNodeId("dataMaster" + randomIntBetween(1, 3))
            .localNodeId("data5Local")
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isPresentWith(oneOf("dataMaster1", "dataMaster2", "dataMaster3", "data4", "data6")));
    }

    public void testPickNode_onlyDedicatedCoordinatingNodeIsLocal_fallsBackToDataNode() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("dataMaster1", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster2", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster3", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("data4", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data5", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data6", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("ingest1", DiscoveryNodeRole.INGEST_ROLE))
            .add(createNode("coordinating1Local"))
            .masterNodeId("dataMaster" + randomIntBetween(1, 3))
            .localNodeId("coordinating1Local")
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isPresentWith(oneOf("dataMaster1", "dataMaster2", "dataMaster3", "data4", "data5", "data6")));
    }

    public void testPickNode_noDedicatedCoordinatingNodes_onlyDataNodeIsLocal_returnsNull() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("dataMaster1Local", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("ingestMaster1", DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .masterNodeId(randomFrom("dataMaster1Local", "ingestMaster1"))
            .localNodeId("dataMaster1Local")
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isEmpty());
    }

    public void testPickNode_noDedicatedCoordinatingNodes_skipsDataNodeThatIsShuttingDown() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("dataMaster1Local", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster2ShuttingDown", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster3", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("ingestMaster1", DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .masterNodeId(randomFrom("dataMaster1Local", "ingestMaster1", "dataMaster2ShuttingDown", "dataMaster3"))
            .localNodeId("dataMaster1Local")
            .build();
        Optional<String> id = picker.pickNode(nodes, randomShutdownForNode("dataMaster2ShuttingDown"));
        assertThat(id, isPresentWith("dataMaster3"));
    }

    public void testPickNode_localNodeUnknown_returnsNull() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("dataMaster1", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster2", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("dataMaster3", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("data4", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data5", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("data6", DiscoveryNodeRole.DATA_ROLE))
            .add(createNode("ingest1", DiscoveryNodeRole.INGEST_ROLE))
            .add(createNode("coordinating1"))
            .add(createNode("coordinating2Local"))
            .add(createNode("coordinating3"))
            .masterNodeId("dataMaster" + randomIntBetween(1, 3))
            // We never set the local node
            .build();
        Optional<String> id = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY);
        assertThat(id, isEmpty());
    }

    public void testPickNodeCanBeCalledFromDifferentThreads() throws InterruptedException {
        final String[] holder = new String[1];
        final Thread thread = new Thread(() -> {
            DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(createNode("dataMaster1Local", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
                .add(createNode("dataMaster2", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
                .masterNodeId("dataMaster1Local")
                .localNodeId("dataMaster1Local")
                .build();
            holder[0] = picker.pickNode(nodes, NodesShutdownMetadata.EMPTY).orElseThrow();
        });
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(holder[0], equalTo("dataMaster2"));
    }

    private static DiscoveryNode createNode(String id, DiscoveryNodeRole... roles) {
        return DiscoveryNodeUtils.create(id, buildNewFakeTransportAddress(), Map.of(), Set.of(roles));
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
