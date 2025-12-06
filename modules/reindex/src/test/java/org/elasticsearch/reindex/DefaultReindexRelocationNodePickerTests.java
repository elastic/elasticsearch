/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class DefaultReindexRelocationNodePickerTests extends ESTestCase {

    private DefaultReindexRelocationNodePicker picker = new DefaultReindexRelocationNodePicker();

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
        String id = picker.pickNode(nodes);
        assertThat(id, oneOf("coordinating1", "coordinating3"));
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
            // .add(createNode("ml1", DiscoveryNodeRole.ML_ROLE))
            // .add(createNode("voting1", DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE))
            .masterNodeId("dataMaster" + randomIntBetween(1, 3))
            .localNodeId("data5Local")
            .build();
        String id = picker.pickNode(nodes);
        assertThat(id, oneOf("dataMaster1", "dataMaster2", "dataMaster3", "data4", "data6"));
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
        String id = picker.pickNode(nodes);
        assertThat(id, oneOf("dataMaster1", "dataMaster2", "dataMaster3", "data4", "data5", "data6"));
    }

    public void testPickNode_noDedicatedCoordinatingNodes_onlyDataNodeIsLocal_returnsNull() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(createNode("dataMaster1Local", DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .add(createNode("ingestMaster1", DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .masterNodeId(randomFrom("dataMaster1Local", "ingestMaster1"))
            .localNodeId("dataMaster1Local")
            .build();
        String id = picker.pickNode(nodes);
        assertThat(id, nullValue());
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
        String id = picker.pickNode(nodes);
        assertThat(id, nullValue());
    }

    private static DiscoveryNode createNode(String id, DiscoveryNodeRole... roles) {
        return DiscoveryNodeUtils.create(id, buildNewFakeTransportAddress(), Map.of(), Set.of(roles));
    }
}
