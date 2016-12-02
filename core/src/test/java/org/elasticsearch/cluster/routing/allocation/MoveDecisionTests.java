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
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * Unit tests for the {@link MoveDecision} class.
 */
public class MoveDecisionTests extends ESTestCase {

    public void testCachedDecisions() {
        // cached stay decision
        MoveDecision stay1 = MoveDecision.stay(null, null);
        MoveDecision stay2 = MoveDecision.stay(null, null);
        assertSame(stay1, stay2); // not in explain mode, so should use cached decision
        DiscoveryNode discoveryNode = randomBoolean() ? randomDiscoveryNode() : null;
        stay1 = MoveDecision.stay(discoveryNode, Decision.YES);
        stay2 = MoveDecision.stay(discoveryNode, Decision.YES);
        assertNotSame(stay1, stay2);

        // cached cannot move decision
        discoveryNode = randomBoolean() ? randomDiscoveryNode() : null;
        stay1 = MoveDecision.decision(discoveryNode, Decision.NO, Type.NO, null, null);
        stay2 = MoveDecision.decision(discoveryNode, Decision.NO, Type.NO, null, null);
        assertSame(stay1, stay2);
        // final decision is YES, so shouldn't use cached decision
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        discoveryNode = randomDiscoveryNode();
        stay1 = MoveDecision.decision(discoveryNode, Decision.NO, Type.YES, node1, null);
        stay2 = MoveDecision.decision(discoveryNode, Decision.NO, Type.YES, node1, null);
        assertNotSame(stay1, stay2);
        assertEquals(stay1.getTargetNode(), stay2.getTargetNode());
        // final decision is NO, but in explain mode, so shouldn't use cached decision
        discoveryNode = randomDiscoveryNode();
        stay1 = MoveDecision.decision(discoveryNode, Decision.NO, Type.NO, null, new ArrayList<>());
        stay2 = MoveDecision.decision(discoveryNode, Decision.NO, Type.NO, null, new ArrayList<>());
        assertNotSame(stay1, stay2);
        assertSame(stay1.getDecisionType(), stay2.getDecisionType());
        assertNotNull(stay1.getExplanation());
        assertEquals(stay1.getExplanation(), stay2.getExplanation());
    }

    public void testStayDecision() {
        DiscoveryNode currentNode = randomDiscoveryNode();
        MoveDecision stay = MoveDecision.stay(currentNode, Decision.YES);
        assertFalse(stay.cannotRemain());
        assertFalse(stay.move());
        assertTrue(stay.isDecisionTaken());
        assertNull(stay.getNodeDecisions());
        assertNotNull(stay.getExplanation());
        assertEquals(Type.NO, stay.getDecisionType());
        assertEquals(currentNode, stay.getCurrentNode());

        stay = MoveDecision.stay(currentNode, Decision.YES);
        assertFalse(stay.cannotRemain());
        assertFalse(stay.move());
        assertTrue(stay.isDecisionTaken());
        assertNull(stay.getNodeDecisions());
        assertEquals("can remain on its current node", stay.getExplanation());
        assertEquals(Type.NO, stay.getDecisionType());
        assertEquals(currentNode, stay.getCurrentNode());
    }

    public void testDecisionWithNodeExplanations() {
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Decision nodeDecision = randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES);
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, nodeDecision, 2));
        nodeDecisions.add(new NodeAllocationResult(node2, nodeDecision, 1));
        DiscoveryNode currentNode = randomDiscoveryNode();
        MoveDecision decision = MoveDecision.decision(currentNode, Decision.NO, Type.NO, null, nodeDecisions);
        assertNotNull(decision.getDecisionType());
        assertNotNull(decision.getExplanation());
        assertNotNull(decision.getNodeDecisions());
        assertEquals(2, decision.getNodeDecisions().size());
        assertEquals(currentNode, decision.getCurrentNode());
        // both nodes have the same decision type but node2 has a higher weight ranking, so node2 comes first
        assertEquals("node2", decision.getNodeDecisions().keySet().iterator().next());

        decision = MoveDecision.decision(currentNode, Decision.NO, Type.YES, node2, null);
        assertEquals("node2", decision.getTargetNode().getId());
    }

    public void testSerialization() throws IOException {
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Type finalDecision = randomFrom(Type.values());
        DiscoveryNode assignedNode = finalDecision == Type.YES ? node1 : null;
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 2));
        nodeDecisions.add(new NodeAllocationResult(node2, finalDecision == Type.YES ? Decision.YES :
                                                              randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES), 1));
        MoveDecision moveDecision = MoveDecision.decision(randomDiscoveryNode(), Decision.NO, finalDecision, assignedNode, nodeDecisions);
        BytesStreamOutput output = new BytesStreamOutput();
        moveDecision.writeTo(output);
        MoveDecision readDecision = new MoveDecision(output.bytes().streamInput());
        assertEquals(moveDecision.cannotRemain(), readDecision.cannotRemain());
        assertEquals(moveDecision.getExplanation(), readDecision.getExplanation());
        assertEquals(moveDecision.move(), readDecision.move());
        assertEquals(moveDecision.getNodeDecisions().size(), readDecision.getNodeDecisions().size());
        assertEquals(moveDecision.getTargetNode(), readDecision.getTargetNode());
        assertEquals(moveDecision.getDecisionType(), readDecision.getDecisionType());
        assertEquals(moveDecision.getCurrentNode(), readDecision.getCurrentNode());
        // node2 should have the highest sort order
        assertEquals("node2", readDecision.getNodeDecisions().keySet().iterator().next());
    }

    private static DiscoveryNode randomDiscoveryNode() {
        return new DiscoveryNode("randomNode", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
    }
}
