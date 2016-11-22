/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult.ShardStore;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult.StoreStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * Unit tests for the {@link NodeAllocationResult} class.
 */
public class NodeAllocationResultTests extends ESTestCase {

    public void testSerialization() throws IOException {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Decision decision = randomFrom(Decision.YES, Decision.THROTTLE, Decision.NO);
        NodeAllocationResult explanation = new NodeAllocationResult(node, decision, 1);
        BytesStreamOutput output = new BytesStreamOutput();
        explanation.writeTo(output);
        NodeAllocationResult readExplanation = new NodeAllocationResult(output.bytes().streamInput());
        assertNodeExplanationEquals(explanation, readExplanation);
    }

    public void testShardStore() throws IOException {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Decision decision = randomFrom(Decision.YES, Decision.THROTTLE, Decision.NO);
        StoreStatus storeStatus = randomFrom(StoreStatus.values());
        long matchingBytes = (long) randomIntBetween(1, 1000);
        ShardStore shardStore = new ShardStore(storeStatus, matchingBytes);
        NodeAllocationResult explanation = new NodeAllocationResult(node, shardStore, decision);
        BytesStreamOutput output = new BytesStreamOutput();
        explanation.writeTo(output);
        NodeAllocationResult readExplanation = new NodeAllocationResult(output.bytes().streamInput());
        assertNodeExplanationEquals(explanation, readExplanation);
        assertEquals(matchingBytes, explanation.getShardStore().getMatchingBytes());
        assertEquals(-1, explanation.getShardStore().getVersion());
        assertNull(explanation.getShardStore().getAllocationId());

        String allocId = randomAsciiOfLength(5);
        long version = (long) randomIntBetween(1, 1000);
        shardStore = new ShardStore(storeStatus, allocId, version, randomBoolean() ? new Exception("bad stuff") : null);
        explanation = new NodeAllocationResult(node, shardStore, decision);
        output = new BytesStreamOutput();
        explanation.writeTo(output);
        readExplanation = new NodeAllocationResult(output.bytes().streamInput());
        assertNodeExplanationEquals(explanation, readExplanation);
        assertEquals(storeStatus, explanation.getShardStore().getStoreStatus());
        assertEquals(-1, explanation.getShardStore().getMatchingBytes());
        assertEquals(version, explanation.getShardStore().getVersion());
        assertEquals(allocId, explanation.getShardStore().getAllocationId());
    }

    private void assertNodeExplanationEquals(NodeAllocationResult expl1, NodeAllocationResult expl2) {
        assertEquals(expl1.getNode(), expl2.getNode());
        assertEquals(expl1.getCanAllocateDecision(), expl2.getCanAllocateDecision());
        assertEquals(0, Float.compare(expl1.getWeightRanking(), expl2.getWeightRanking()));
        if (expl1.getShardStore() != null) {
            assertEquals(expl1.getShardStore().getStoreStatus(), expl2.getShardStore().getStoreStatus());
            assertEquals(expl1.getShardStore().getVersion(), expl2.getShardStore().getVersion());
            assertEquals(expl1.getShardStore().getAllocationId(), expl2.getShardStore().getAllocationId());
            assertEquals(expl1.getShardStore().getMatchingBytes(), expl2.getShardStore().getMatchingBytes());
        } else {
            assertNull(expl2.getShardStore());
        }
    }
}
