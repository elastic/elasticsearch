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

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * Tests for the cluster allocation explanation
 */
public final class ClusterAllocationExplanationTests extends ESTestCase {

    public void testDecisionEquality() {
        Decision.Multi d = new Decision.Multi();
        Decision.Multi d2 = new Decision.Multi();
        d.add(Decision.single(Decision.Type.NO, "no label", "because I said no"));
        d.add(Decision.single(Decision.Type.YES, "yes label", "yes please"));
        d.add(Decision.single(Decision.Type.THROTTLE, "throttle label", "wait a sec"));
        d2.add(Decision.single(Decision.Type.NO, "no label", "because I said no"));
        d2.add(Decision.single(Decision.Type.YES, "yes label", "yes please"));
        d2.add(Decision.single(Decision.Type.THROTTLE, "throttle label", "wait a sec"));
        assertEquals(d, d2);
    }

    public void testExplanationSerialization() throws Exception {
        ShardId shard = new ShardId("test", "uuid", 0);
        Map<DiscoveryNode, Decision> nodeToDecisions = new HashMap<>();
        Map<DiscoveryNode, Float> nodeToWeight = new HashMap<>();
        for (int i = randomIntBetween(2, 5); i > 0; i--) {
            DiscoveryNode dn = new DiscoveryNode("node-" + i, DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
            Decision.Multi d = new Decision.Multi();
            d.add(Decision.single(Decision.Type.NO, "no label", "because I said no"));
            d.add(Decision.single(Decision.Type.YES, "yes label", "yes please"));
            d.add(Decision.single(Decision.Type.THROTTLE, "throttle label", "wait a sec"));
            nodeToDecisions.put(dn, d);
            nodeToWeight.put(dn, randomFloat());
        }

        long remainingDelay = randomIntBetween(0, 500);
        ClusterAllocationExplanation cae = new ClusterAllocationExplanation(shard, true, "assignedNode", null,
                nodeToDecisions, nodeToWeight, remainingDelay);
        BytesStreamOutput out = new BytesStreamOutput();
        cae.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes());
        ClusterAllocationExplanation cae2 = new ClusterAllocationExplanation(in);
        assertEquals(shard, cae2.getShard());
        assertTrue(cae2.isPrimary());
        assertTrue(cae2.isAssigned());
        assertEquals("assignedNode", cae2.getAssignedNodeId());
        assertNull(cae2.getUnassignedInfo());
        for (Map.Entry<DiscoveryNode, Decision> entry : cae2.getNodeDecisions().entrySet()) {
            assertEquals(nodeToDecisions.get(entry.getKey()), entry.getValue());
        }
        assertEquals(nodeToWeight, cae2.getNodeWeights());
        assertEquals(remainingDelay, cae2.getRemainingDelayNanos());
    }
}
