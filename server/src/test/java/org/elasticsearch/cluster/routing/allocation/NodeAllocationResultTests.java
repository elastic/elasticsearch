/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult.ShardStoreInfo;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static java.util.Collections.emptySet;

/**
 * Unit tests for the {@link NodeAllocationResult} class.
 */
public class NodeAllocationResultTests extends ESTestCase {

    public void testSerialization() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        Decision decision = randomFrom(Decision.YES, Decision.THROTTLE, Decision.NO);
        NodeAllocationResult explanation = new NodeAllocationResult(node, decision, 1);
        BytesStreamOutput output = new BytesStreamOutput();
        explanation.writeTo(output);
        NodeAllocationResult readExplanation = new NodeAllocationResult(output.bytes().streamInput());
        assertNodeExplanationEquals(explanation, readExplanation);
    }

    public void testShardStore() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        Decision decision = randomFrom(Decision.YES, Decision.THROTTLE, Decision.NO);
        long matchingBytes = (long) randomIntBetween(1, 1000);
        ShardStoreInfo shardStoreInfo = new ShardStoreInfo(matchingBytes);
        NodeAllocationResult explanation = new NodeAllocationResult(node, shardStoreInfo, decision);
        BytesStreamOutput output = new BytesStreamOutput();
        explanation.writeTo(output);
        NodeAllocationResult readExplanation = new NodeAllocationResult(output.bytes().streamInput());
        assertNodeExplanationEquals(explanation, readExplanation);
        assertEquals(matchingBytes, explanation.getShardStoreInfo().getMatchingBytes());
        assertNull(explanation.getShardStoreInfo().getAllocationId());
        assertFalse(explanation.getShardStoreInfo().isInSync());

        String allocId = randomAlphaOfLength(5);
        boolean inSync = randomBoolean();
        shardStoreInfo = new ShardStoreInfo(allocId, inSync, randomBoolean() ? new Exception("bad stuff") : null);
        explanation = new NodeAllocationResult(node, shardStoreInfo, decision);
        output = new BytesStreamOutput();
        explanation.writeTo(output);
        readExplanation = new NodeAllocationResult(output.bytes().streamInput());
        assertNodeExplanationEquals(explanation, readExplanation);
        assertEquals(inSync, explanation.getShardStoreInfo().isInSync());
        assertEquals(-1, explanation.getShardStoreInfo().getMatchingBytes());
        assertEquals(allocId, explanation.getShardStoreInfo().getAllocationId());
    }

    private void assertNodeExplanationEquals(NodeAllocationResult expl1, NodeAllocationResult expl2) {
        assertEquals(expl1.getNode(), expl2.getNode());
        assertEquals(expl1.getCanAllocateDecision(), expl2.getCanAllocateDecision());
        assertEquals(0, Float.compare(expl1.getWeightRanking(), expl2.getWeightRanking()));
        if (expl1.getShardStoreInfo() != null) {
            assertEquals(expl1.getShardStoreInfo().isInSync(), expl2.getShardStoreInfo().isInSync());
            assertEquals(expl1.getShardStoreInfo().getAllocationId(), expl2.getShardStoreInfo().getAllocationId());
            assertEquals(expl1.getShardStoreInfo().getMatchingBytes(), expl2.getShardStoreInfo().getMatchingBytes());
        } else {
            assertNull(expl2.getShardStoreInfo());
        }
    }
}
