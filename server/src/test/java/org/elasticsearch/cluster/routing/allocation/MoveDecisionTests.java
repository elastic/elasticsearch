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
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for the {@link MoveDecision} class.
 */
public class MoveDecisionTests extends ESTestCase {

    public void testCachedDecisions() {
        // cached stay decision
        MoveDecision stay1 = MoveDecision.createRemainYesDecision(Decision.YES);
        MoveDecision stay2 = MoveDecision.createRemainYesDecision(Decision.YES);
        assertSame(stay1, stay2); // not in explain mode, so should use cached decision

        stay1 = MoveDecision.createRemainYesDecision(new Decision.Single(Type.YES, null, null, (Object[]) null));
        stay2 = MoveDecision.createRemainYesDecision(new Decision.Single(Type.YES, null, null, (Object[]) null));
        assertNotSame(stay1, stay2);

        // cached cannot move decision
        stay1 = MoveDecision.move(Decision.NO, AllocationDecision.NO, null, null);
        stay2 = MoveDecision.move(Decision.NO, AllocationDecision.NO, null, null);
        assertSame(stay1, stay2);
        // final decision is YES, so shouldn't use cached decision
        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        stay1 = MoveDecision.move(Decision.NO, AllocationDecision.YES, node1, null);
        stay2 = MoveDecision.move(Decision.NO, AllocationDecision.YES, node1, null);
        assertNotSame(stay1, stay2);
        assertEquals(stay1.getTargetNode(), stay2.getTargetNode());
        // final decision is NO, but in explain mode, so shouldn't use cached decision
        stay1 = MoveDecision.move(Decision.NO, AllocationDecision.NO, null, new ArrayList<>());
        stay2 = MoveDecision.move(Decision.NO, AllocationDecision.NO, null, new ArrayList<>());
        assertNotSame(stay1, stay2);
        assertSame(stay1.getAllocationDecision(), stay2.getAllocationDecision());
        assertNotNull(stay1.getExplanation());
        assertEquals(stay1.getExplanation(), stay2.getExplanation());
    }

    public void testStayDecision() {
        MoveDecision stay = MoveDecision.createRemainYesDecision(Decision.YES);
        assertTrue(stay.canRemain());
        assertFalse(stay.cannotRemainAndCanMove());
        assertTrue(stay.isDecisionTaken());
        assertNull(stay.getNodeDecisions());
        assertEquals(AllocationDecision.NO_ATTEMPT, stay.getAllocationDecision());

        stay = MoveDecision.createRemainYesDecision(Decision.YES);
        assertTrue(stay.canRemain());
        assertFalse(stay.cannotRemainAndCanMove());
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
        MoveDecision decision = MoveDecision.move(Decision.NO, AllocationDecision.NO, null, nodeDecisions);
        assertNotNull(decision.getAllocationDecision());
        assertNotNull(decision.getExplanation());
        assertNotNull(decision.getNodeDecisions());
        assertEquals(2, decision.getNodeDecisions().size());
        // both nodes have the same decision type but node2 has a higher weight ranking, so node2 comes first
        assertEquals("node2", decision.getNodeDecisions().iterator().next().getNode().getId());

        decision = MoveDecision.move(Decision.NO, AllocationDecision.YES, node2, null);
        assertEquals("node2", decision.getTargetNode().getId());
    }

    public void testCanRemainDecisionsXContent() throws IOException {
        // --- canRemain = YES ---

        // debug ON → Multi with YES sub-decisions: can_remain_decisions present
        Decision.Multi yesMulti = new Decision.Multi();
        yesMulti.add(Decision.single(Type.YES, "filter_decider", "no filter applied"));
        yesMulti.add(Decision.single(Type.YES, "disk_decider", "enough disk space"));
        String debugOnJson = toJsonObject(MoveDecision.rebalance(yesMulti, Decision.YES, AllocationDecision.NO, null, 3, null));
        assertThat(debugOnJson, containsString("can_remain_on_current_node\":\"yes\""));
        assertThat(debugOnJson, containsString("can_remain_decisions"));
        assertThat(debugOnJson, containsString("filter_decider"));
        assertThat(debugOnJson, containsString("no filter applied"));
        assertThat(debugOnJson, containsString("disk_decider"));
        assertThat(debugOnJson, containsString("enough disk space"));

        // EXCLUDE_YES_DECISIONS → empty Multi (all YES filtered out): can_remain_decisions absent
        String excludeYesJson = toJsonObject(
            MoveDecision.rebalance(new Decision.Multi(), Decision.YES, AllocationDecision.NO, null, 3, null)
        );
        assertThat(excludeYesJson, containsString("can_remain_on_current_node\":\"yes\""));
        assertThat(excludeYesJson, not(containsString("can_remain_decisions")));

        // debug OFF → Decision.YES singleton: can_remain_decisions absent
        String debugOffYesJson = toJsonObject(MoveDecision.rebalance(Decision.YES, Decision.YES, AllocationDecision.NO, null, 3, null));
        assertThat(debugOffYesJson, containsString("can_remain_on_current_node\":\"yes\""));
        assertThat(debugOffYesJson, not(containsString("can_remain_decisions")));

        // --- canRemain = NO ---

        // debug ON → Multi with YES+NO sub-decisions (type=NO): can_remain_decisions present
        Decision.Multi debugOnNoMulti = new Decision.Multi();
        debugOnNoMulti.add(Decision.single(Type.NO, "filter_decider", "filter says no"));
        debugOnNoMulti.add(Decision.single(Type.YES, "disk_decider", "enough disk space"));
        String debugOnNoJson = toJsonObject(MoveDecision.move(debugOnNoMulti, AllocationDecision.NO, null, new ArrayList<>()));
        assertThat(debugOnNoJson, containsString("can_remain_on_current_node\":\"no\""));
        assertThat(debugOnNoJson, containsString("can_remain_decisions"));
        assertThat(debugOnNoJson, containsString("filter says no"));
        assertThat(debugOnNoJson, containsString("disk_decider"));
        assertThat(debugOnNoJson, containsString("enough disk space"));

        // EXCLUDE_YES_DECISIONS → Multi with NO only (YES filtered out): can_remain_decisions present
        Decision.Multi excludeYesNoMulti = new Decision.Multi();
        excludeYesNoMulti.add(Decision.single(Type.NO, "filter_decider", "filter says no"));
        String excludeYesNoJson = toJsonObject(MoveDecision.move(excludeYesNoMulti, AllocationDecision.NO, null, new ArrayList<>()));
        assertThat(excludeYesNoJson, containsString("can_remain_on_current_node\":\"no\""));
        assertThat(excludeYesNoJson, containsString("can_remain_decisions"));
        assertThat(excludeYesNoJson, containsString("filter says no"));

        // debug OFF → Single NO (specific decider): can_remain_decisions present with decider and explanation
        Decision.Single singleNo = new Decision.Single(Type.NO, "filter_decider", "filter says no");
        String debugOffNoJson = toJsonObject(MoveDecision.move(singleNo, AllocationDecision.NO, null, new ArrayList<>()));
        assertThat(debugOffNoJson, containsString("can_remain_on_current_node\":\"no\""));
        assertThat(debugOffNoJson, containsString("can_remain_decisions"));
        assertThat(debugOffNoJson, containsString("filter_decider"));
        assertThat(debugOffNoJson, containsString("filter says no"));
    }

    private static String toJsonObject(MoveDecision moveDecision) {
        return Strings.toString(ChunkedToXContent.wrapAsToXContent(moveDecision::toXContentChunked));
    }

    public void testSerialization() throws IOException {
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        DiscoveryNode node1 = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        DiscoveryNode node2 = DiscoveryNodeUtils.builder("node2").roles(emptySet()).build();
        Type finalDecision = randomFrom(Type.values());
        DiscoveryNode assignedNode = finalDecision.assignmentAllowed() ? node1 : null;
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 2));
        nodeDecisions.add(
            new NodeAllocationResult(
                node2,
                finalDecision.assignmentAllowed()
                    ? randomFrom(Decision.NOT_PREFERRED, Decision.YES)
                    : randomFrom(Decision.NO, Decision.THROTTLE),
                1
            )
        );
        MoveDecision moveDecision = MoveDecision.move(
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
        assertEquals(moveDecision.cannotRemainAndCanMove(), readDecision.cannotRemainAndCanMove());
        assertEquals(moveDecision.getNodeDecisions().size(), readDecision.getNodeDecisions().size());
        assertEquals(moveDecision.getTargetNode(), readDecision.getTargetNode());
        assertEquals(moveDecision.getAllocationDecision(), readDecision.getAllocationDecision());
        // node2 should have the highest sort order
        assertEquals("node2", moveDecision.getNodeDecisions().iterator().next().getNode().getId());
        assertEquals("node2", readDecision.getNodeDecisions().iterator().next().getNode().getId());
    }

}
