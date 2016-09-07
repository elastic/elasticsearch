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

import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the {@link UnassignedShardDecision} class.
 */
public class UnassignedShardDecisionTests extends ESTestCase {

    public void testDecisionNotTaken() {
        UnassignedShardDecision unassignedShardDecision = UnassignedShardDecision.DECISION_NOT_TAKEN;
        assertFalse(unassignedShardDecision.isDecisionTaken());
        assertNull(unassignedShardDecision.getFinalDecision());
        assertNull(unassignedShardDecision.getAllocationStatus());
        assertNull(unassignedShardDecision.getAllocationId());
        assertNull(unassignedShardDecision.getAssignedNodeId());
        assertNull(unassignedShardDecision.getFinalExplanation());
        assertNull(unassignedShardDecision.getNodeDecisions());
        expectThrows(IllegalArgumentException.class, () -> unassignedShardDecision.getFinalDecisionSafe());
        expectThrows(IllegalArgumentException.class, () -> unassignedShardDecision.getFinalExplanationSafe());
    }

    public void testNoDecision() {
        final AllocationStatus allocationStatus = randomFrom(
            AllocationStatus.DELAYED_ALLOCATION, AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA
        );
        UnassignedShardDecision noDecision = UnassignedShardDecision.noDecision(allocationStatus, "something is wrong");
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getFinalDecision().type());
        assertEquals(allocationStatus, noDecision.getAllocationStatus());
        assertEquals("something is wrong", noDecision.getFinalExplanation());
        assertNull(noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        Map<String, Decision> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", Decision.NO);
        nodeDecisions.put("node2", Decision.NO);
        noDecision = UnassignedShardDecision.noDecision(AllocationStatus.DECIDERS_NO, "something is wrong", nodeDecisions);
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getFinalDecision().type());
        assertEquals(AllocationStatus.DECIDERS_NO, noDecision.getAllocationStatus());
        assertEquals("something is wrong", noDecision.getFinalExplanation());
        assertEquals(nodeDecisions, noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        // test bad values
        expectThrows(NullPointerException.class, () -> UnassignedShardDecision.noDecision(null, "a"));
        expectThrows(NullPointerException.class, () -> UnassignedShardDecision.noDecision(AllocationStatus.DECIDERS_NO, null));
    }

    public void testThrottleDecision() {
        Map<String, Decision> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", Decision.NO);
        nodeDecisions.put("node2", Decision.THROTTLE);
        UnassignedShardDecision throttleDecision = UnassignedShardDecision.throttleDecision("too much happening", nodeDecisions);
        assertTrue(throttleDecision.isDecisionTaken());
        assertEquals(Decision.Type.THROTTLE, throttleDecision.getFinalDecision().type());
        assertEquals(AllocationStatus.DECIDERS_THROTTLED, throttleDecision.getAllocationStatus());
        assertEquals("too much happening", throttleDecision.getFinalExplanation());
        assertEquals(nodeDecisions, throttleDecision.getNodeDecisions());
        assertNull(throttleDecision.getAssignedNodeId());
        assertNull(throttleDecision.getAllocationId());

        // test bad values
        expectThrows(NullPointerException.class, () -> UnassignedShardDecision.throttleDecision(null, Collections.emptyMap()));
    }

    public void testYesDecision() {
        Map<String, Decision> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", Decision.YES);
        nodeDecisions.put("node2", Decision.NO);
        String allocId = randomBoolean() ? "allocId" : null;
        UnassignedShardDecision yesDecision = UnassignedShardDecision.yesDecision(
            "node was very kind", "node1", allocId, nodeDecisions
        );
        assertTrue(yesDecision.isDecisionTaken());
        assertEquals(Decision.Type.YES, yesDecision.getFinalDecision().type());
        assertNull(yesDecision.getAllocationStatus());
        assertEquals("node was very kind", yesDecision.getFinalExplanation());
        assertEquals(nodeDecisions, yesDecision.getNodeDecisions());
        assertEquals("node1", yesDecision.getAssignedNodeId());
        assertEquals(allocId, yesDecision.getAllocationId());

        expectThrows(NullPointerException.class,
            () -> UnassignedShardDecision.yesDecision(null, "a", randomBoolean() ? "a" : null, Collections.emptyMap()));
        expectThrows(NullPointerException.class,
            () -> UnassignedShardDecision.yesDecision("a", null, null, Collections.emptyMap()));
    }
}
