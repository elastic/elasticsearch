/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptySet;

/**
 * Unit tests for the {@link MoveDecision} class.
 */
public class MoveDecisionTests extends ESTestCase {

    public void testCachedDecisions() {
        // cached stay decision
        MoveDecision stay1 = MoveDecision.stay(Decision.YES);
        MoveDecision stay2 = MoveDecision.stay(Decision.YES);
        assertSame(stay1, stay2); // not in explain mode, so should use cached decision

        stay1 = MoveDecision.stay(new Decision.Single(Type.YES, null, null, (Object[]) null));
        stay2 = MoveDecision.stay(new Decision.Single(Type.YES, null, null, (Object[]) null));
        assertNotSame(stay1, stay2);

        // cached cannot move decision
        stay1 = MoveDecision.cannotRemain(Decision.NO, AllocationDecision.NO, null, null);
        stay2 = MoveDecision.cannotRemain(Decision.NO, AllocationDecision.NO, null, null);
        assertSame(stay1, stay2);
        // final decision is YES, so shouldn't use cached decision
        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        stay1 = MoveDecision.cannotRemain(Decision.NO, AllocationDecision.YES, node1, null);
        stay2 = MoveDecision.cannotRemain(Decision.NO, AllocationDecision.YES, node1, null);
        assertNotSame(stay1, stay2);
        assertEquals(stay1.getTargetNode(), stay2.getTargetNode());
        // final decision is NO, but in explain mode, so shouldn't use cached decision
        stay1 = MoveDecision.cannotRemain(Decision.NO, AllocationDecision.NO, null, new ArrayList<>());
        stay2 = MoveDecision.cannotRemain(Decision.NO, AllocationDecision.NO, null, new ArrayList<>());
        assertNotSame(stay1, stay2);
        assertSame(stay1.getAllocationDecision(), stay2.getAllocationDecision());
        assertNotNull(stay1.getExplanation());
        assertEquals(stay1.getExplanation(), stay2.getExplanation());
    }

    public void testStayDecision() {
        MoveDecision stay = MoveDecision.stay(Decision.YES);
        assertTrue(stay.canRemain());
        assertFalse(stay.forceMove());
        assertTrue(stay.isDecisionTaken());
        assertNull(stay.getNodeDecisions());
        assertEquals(AllocationDecision.NO_ATTEMPT, stay.getAllocationDecision());

        stay = MoveDecision.stay(Decision.YES);
        assertTrue(stay.canRemain());
        assertFalse(stay.forceMove());
        assertTrue(stay.isDecisionTaken());
        assertNull(stay.getNodeDecisions());
        assertEquals(AllocationDecision.NO_ATTEMPT, stay.getAllocationDecision());
    }

    public void testDecisionWithNodeExplanations() {
        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        DiscoveryNode node2 = DiscoveryNodeUtils.builder("node2").roles(emptySet()).build();
        Decision nodeDecision = randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES);
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, nodeDecision, 2));
        nodeDecisions.add(new NodeAllocationResult(node2, nodeDecision, 1));
        MoveDecision decision = MoveDecision.cannotRemain(Decision.NO, AllocationDecision.NO, null, nodeDecisions);
        assertNotNull(decision.getAllocationDecision());
        assertNotNull(decision.getExplanation());
        assertNotNull(decision.getNodeDecisions());
        assertEquals(2, decision.getNodeDecisions().size());
        // both nodes have the same decision type but node2 has a higher weight ranking, so node2 comes first
        assertEquals("node2", decision.getNodeDecisions().iterator().next().getNode().getId());

        decision = MoveDecision.cannotRemain(Decision.NO, AllocationDecision.YES, node2, null);
        assertEquals("node2", decision.getTargetNode().getId());
    }

    public void testSerialization() throws IOException {
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        DiscoveryNode node2 = DiscoveryNodeUtils.builder("node2").roles(emptySet()).build();
        Type finalDecision = randomFrom(Type.values());
        DiscoveryNode assignedNode = finalDecision == Type.YES ? node1 : null;
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 2));
        nodeDecisions.add(
            new NodeAllocationResult(
                node2,
                finalDecision == Type.YES ? Decision.YES : randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES),
                1
            )
        );
        MoveDecision moveDecision = MoveDecision.cannotRemain(
            Decision.NO,
            AllocationDecision.fromDecisionType(finalDecision),
            assignedNode,
            nodeDecisions
        );
        BytesStreamOutput output = new BytesStreamOutput();
        moveDecision.writeTo(output);
        MoveDecision readDecision = new MoveDecision(output.bytes().streamInput());
        assertEquals(moveDecision.canRemain(), readDecision.canRemain());
        assertEquals(moveDecision.getExplanation(), readDecision.getExplanation());
        assertEquals(moveDecision.forceMove(), readDecision.forceMove());
        assertEquals(moveDecision.getNodeDecisions().size(), readDecision.getNodeDecisions().size());
        assertEquals(moveDecision.getTargetNode(), readDecision.getTargetNode());
        assertEquals(moveDecision.getAllocationDecision(), readDecision.getAllocationDecision());
        // node2 should have the highest sort order
        assertEquals("node2", readDecision.getNodeDecisions().iterator().next().getNode().getId());
    }

}
