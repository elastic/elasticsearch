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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Unit tests for the {@link AllocateUnassignedDecision} class.
 */
public class AllocateUnassignedDecisionTests extends ESTestCase {

    public void testDecisionNotTaken() {
        AllocateUnassignedDecision allocateUnassignedDecision = AllocateUnassignedDecision.NOT_TAKEN;
        assertFalse(allocateUnassignedDecision.isDecisionTaken());
        assertNull(allocateUnassignedDecision.getFinalDecisionType());
        assertNull(allocateUnassignedDecision.getAllocationStatus());
        assertNull(allocateUnassignedDecision.getAllocationId());
        assertNull(allocateUnassignedDecision.getAssignedNodeId());
        assertNull(allocateUnassignedDecision.getFinalExplanation());
        assertNull(allocateUnassignedDecision.getNodeDecisions());
        expectThrows(IllegalArgumentException.class, () -> allocateUnassignedDecision.getFinalDecisionSafe());
    }

    public void testNoDecision() {
        final AllocationStatus allocationStatus = randomFrom(
            AllocationStatus.DELAYED_ALLOCATION, AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA
        );
        AllocateUnassignedDecision noDecision = AllocateUnassignedDecision.no(allocationStatus, "something is wrong");
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getFinalDecisionType());
        assertEquals(allocationStatus, noDecision.getAllocationStatus());
        assertEquals("something is wrong", noDecision.getFinalExplanation());
        assertNull(noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(Decision.NO));
        nodeDecisions.put("node2", new NodeAllocationResult(Decision.NO));
        noDecision = AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, "something is wrong",
            nodeDecisions.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getDecision()))
        );
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getFinalDecisionType());
        assertEquals(AllocationStatus.DECIDERS_NO, noDecision.getAllocationStatus());
        assertEquals("something is wrong", noDecision.getFinalExplanation());
        assertEquals(nodeDecisions, noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        // test bad values
        expectThrows(NullPointerException.class, () -> AllocateUnassignedDecision.no((AllocationStatus)null, "a"));
    }

    public void testThrottleDecision() {
        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(Decision.NO));
        nodeDecisions.put("node2", new NodeAllocationResult(Decision.THROTTLE));
        AllocateUnassignedDecision throttleDecision = AllocateUnassignedDecision.throttle("too much happening",
            nodeDecisions.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getDecision()))
        );
        assertTrue(throttleDecision.isDecisionTaken());
        assertEquals(Decision.Type.THROTTLE, throttleDecision.getFinalDecisionType());
        assertEquals(AllocationStatus.DECIDERS_THROTTLED, throttleDecision.getAllocationStatus());
        assertEquals("too much happening", throttleDecision.getFinalExplanation());
        assertEquals(nodeDecisions, throttleDecision.getNodeDecisions());
        assertNull(throttleDecision.getAssignedNodeId());
        assertNull(throttleDecision.getAllocationId());
    }

    public void testYesDecision() {
        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(Decision.YES));
        nodeDecisions.put("node2", new NodeAllocationResult(Decision.NO));
        String allocId = randomBoolean() ? "allocId" : null;
        AllocateUnassignedDecision yesDecision = AllocateUnassignedDecision.yes(
            "node1", "node was very kind", allocId, nodeDecisions.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getDecision())
            )
        );
        assertTrue(yesDecision.isDecisionTaken());
        assertEquals(Decision.Type.YES, yesDecision.getFinalDecisionType());
        assertNull(yesDecision.getAllocationStatus());
        assertEquals("node was very kind", yesDecision.getFinalExplanation());
        assertEquals(nodeDecisions, yesDecision.getNodeDecisions());
        assertEquals("node1", yesDecision.getAssignedNodeId());
        assertEquals(allocId, yesDecision.getAllocationId());
    }

    public void testCachedDecisions() {
        List<AllocationStatus> cachableStatuses = Arrays.asList(AllocationStatus.DECIDERS_NO, AllocationStatus.DECIDERS_THROTTLED,
            AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA, AllocationStatus.DELAYED_ALLOCATION);
        for (AllocationStatus allocationStatus : cachableStatuses) {
            if (allocationStatus == AllocationStatus.DECIDERS_THROTTLED) {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.throttle(null, null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.throttle(null, null);
                assertSame(cached, another);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.throttle("abc", null);
                another = AllocateUnassignedDecision.throttle("abc", null);
                assertNotSame(notCached, another);
            } else {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.no(allocationStatus, null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.no(allocationStatus, null);
                assertSame(cached, another);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.no(allocationStatus, "abc");
                another = AllocateUnassignedDecision.no(allocationStatus, "abc");
                assertNotSame(notCached, another);
            }
        }

        // yes decisions are not precomputed and cached
        Map<String, Decision> dummyMap = Collections.emptyMap();
        AllocateUnassignedDecision first = AllocateUnassignedDecision.yes("node1", "abc", "alloc1", dummyMap);
        AllocateUnassignedDecision second = AllocateUnassignedDecision.yes("node1", "abc", "alloc1", dummyMap);
        // same fields for the AllocateUnassignedDecision, but should be different instances
        assertNotSame(first, second);
    }

}
