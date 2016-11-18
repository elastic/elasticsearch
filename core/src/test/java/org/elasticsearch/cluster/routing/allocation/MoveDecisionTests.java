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

import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the {@link MoveDecision} class.
 */
public class MoveDecisionTests extends ESTestCase {

    public void testCachedDecisions() {
        // cached stay decision
        MoveDecision stay1 = MoveDecision.stay(Decision.YES, false);
        MoveDecision stay2 = MoveDecision.stay(Decision.YES, false);
        assertSame(stay1, stay2); // not in explain mode, so should use cached decision
        stay1 = MoveDecision.stay(Decision.YES, true);
        stay2 = MoveDecision.stay(Decision.YES, true);
        assertNotSame(stay1, stay2);

        // cached cannot move decision
        stay1 = MoveDecision.decision(Decision.NO, Type.NO, false, null, null, null);
        stay2 = MoveDecision.decision(Decision.NO, Type.NO, false, null, null, null);
        assertSame(stay1, stay2);
        // final decision is YES, so shouldn't use cached decision
        stay1 = MoveDecision.decision(Decision.NO, Type.YES, false, null, "node1", null);
        stay2 = MoveDecision.decision(Decision.NO, Type.YES, false, null, "node1", null);
        assertNotSame(stay1, stay2);
        assertEquals(stay1.getAssignedNodeId(), stay2.getAssignedNodeId());
        // final decision is NO, but in explain mode, so shouldn't use cached decision
        stay1 = MoveDecision.decision(Decision.NO, Type.NO, true, "node1", null, null);
        stay2 = MoveDecision.decision(Decision.NO, Type.NO, true, "node1", null, null);
        assertNotSame(stay1, stay2);
        assertSame(stay1.getFinalDecisionType(), stay2.getFinalDecisionType());
        assertNotNull(stay1.getFinalExplanation());
        assertEquals(stay1.getFinalExplanation(), stay2.getFinalExplanation());
    }

    public void testStayDecision() {
        MoveDecision stay = MoveDecision.stay(Decision.YES, true);
        assertFalse(stay.cannotRemain());
        assertFalse(stay.move());
        assertTrue(stay.isDecisionTaken());
        assertNull(stay.getNodeDecisions());
        assertNotNull(stay.getFinalExplanation());
        assertEquals(Type.NO, stay.getFinalDecisionType());

        stay = MoveDecision.stay(Decision.YES, false);
        assertFalse(stay.cannotRemain());
        assertFalse(stay.move());
        assertTrue(stay.isDecisionTaken());
        assertNull(stay.getNodeDecisions());
        assertNull(stay.getFinalExplanation());
        assertEquals(Type.NO, stay.getFinalDecisionType());
    }

    public void testDecisionWithExplain() {
        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES), randomFloat()));
        nodeDecisions.put("node2", new NodeAllocationResult(randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES), randomFloat()));
        MoveDecision decision = MoveDecision.decision(Decision.NO, Type.NO, true, "node1", null, nodeDecisions);
        assertNotNull(decision.getFinalDecisionType());
        assertNotNull(decision.getFinalExplanation());
        assertNotNull(decision.getNodeDecisions());
        assertEquals(2, decision.getNodeDecisions().size());

        decision = MoveDecision.decision(Decision.NO, Type.YES, true, "node1", "node2", null);
        assertEquals("node2", decision.getAssignedNodeId());
    }
}
