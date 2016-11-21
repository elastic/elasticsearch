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
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * Unit tests for the {@link MoveDecision} class.
 */
public class MoveDecisionTests extends ESTestCase {

    public void testCachedDecisions() {
        // cached stay decision
        MoveDecision stay1 = MoveDecision.stay(null);
        MoveDecision stay2 = MoveDecision.stay(null);
        assertSame(stay1, stay2); // not in explain mode, so should use cached decision
        stay1 = MoveDecision.stay(Decision.YES);
        stay2 = MoveDecision.stay(Decision.YES);
        assertNotSame(stay1, stay2);

        // cached cannot move decision
        stay1 = MoveDecision.decision(Decision.NO, Type.NO, null, null);
        stay2 = MoveDecision.decision(Decision.NO, Type.NO, null, null);
        assertSame(stay1, stay2);
        // final decision is YES, so shouldn't use cached decision
        stay1 = MoveDecision.decision(Decision.NO, Type.YES, "node1", null);
        stay2 = MoveDecision.decision(Decision.NO, Type.YES, "node1", null);
        assertNotSame(stay1, stay2);
        assertEquals(stay1.getAssignedNodeId(), stay2.getAssignedNodeId());
        // final decision is NO, but in explain mode, so shouldn't use cached decision
        stay1 = MoveDecision.decision(Decision.NO, Type.NO, null, new HashMap<>());
        stay2 = MoveDecision.decision(Decision.NO, Type.NO, null, new HashMap<>());
        assertNotSame(stay1, stay2);
        assertSame(stay1.getFinalDecisionType(), stay2.getFinalDecisionType());
        assertNotNull(stay1.getFinalExplanation());
        assertEquals(stay1.getFinalExplanation(), stay2.getFinalExplanation());
    }

    public void testStayDecision() {
        MoveDecision stay = MoveDecision.stay(Decision.YES);
        assertFalse(stay.cannotRemain());
        assertFalse(stay.move());
        assertTrue(stay.isDecisionTaken());
        assertNull(stay.getNodeDecisions());
        assertNotNull(stay.getFinalExplanation());
        assertEquals(Type.NO, stay.getFinalDecisionType());

        stay = MoveDecision.stay(Decision.YES);
        assertFalse(stay.cannotRemain());
        assertFalse(stay.move());
        assertTrue(stay.isDecisionTaken());
        assertNull(stay.getNodeDecisions());
        assertEquals("shard is allowed to remain on its current node, so no reason to move", stay.getFinalExplanation());
        assertEquals(Type.NO, stay.getFinalDecisionType());
    }

    public void testDecisionWithNodeExplanations() {
        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        nodeDecisions.put("node1", new NodeAllocationResult(node1, randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES),
                                                               randomFloat()));
        nodeDecisions.put("node2", new NodeAllocationResult(node2, randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES),
                                                               randomFloat()));
        MoveDecision decision = MoveDecision.decision(Decision.NO, Type.NO, null, nodeDecisions);
        assertNotNull(decision.getFinalDecisionType());
        assertNotNull(decision.getFinalExplanation());
        assertNotNull(decision.getNodeDecisions());
        assertEquals(2, decision.getNodeDecisions().size());

        decision = MoveDecision.decision(Decision.NO, Type.YES, "node2", null);
        assertEquals("node2", decision.getAssignedNodeId());
    }
}
