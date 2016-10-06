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

/**
 * Unit tests for the {@link ShardAllocationDecision} class.
 */
public class ShardAllocationDecisionTests extends ESTestCase {

    public void testDecisionNotTaken() {
        ShardAllocationDecision shardAllocationDecision = ShardAllocationDecision.DECISION_NOT_TAKEN;
        assertFalse(shardAllocationDecision.isDecisionTaken());
        assertNull(shardAllocationDecision.getFinalDecision());
        assertNull(shardAllocationDecision.getAllocationStatus());
        assertNull(shardAllocationDecision.getAllocationId());
        assertNull(shardAllocationDecision.getAssignedNodeId());
        assertNull(shardAllocationDecision.getFinalExplanation());
        assertNull(shardAllocationDecision.getNodeDecisions());
        expectThrows(IllegalArgumentException.class, () -> shardAllocationDecision.getFinalDecisionSafe());
    }

    public void testNoDecision() {
        final AllocationStatus allocationStatus = randomFrom(
            AllocationStatus.DELAYED_ALLOCATION, AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA
        );
        ShardAllocationDecision noDecision = ShardAllocationDecision.no(allocationStatus, "something is wrong");
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getFinalDecision());
        assertEquals(allocationStatus, noDecision.getAllocationStatus());
        assertEquals("something is wrong", noDecision.getFinalExplanation());
        assertNull(noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        Map<String, Decision> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", Decision.NO);
        nodeDecisions.put("node2", Decision.NO);
        noDecision = ShardAllocationDecision.no(AllocationStatus.DECIDERS_NO, "something is wrong", nodeDecisions);
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getFinalDecision());
        assertEquals(AllocationStatus.DECIDERS_NO, noDecision.getAllocationStatus());
        assertEquals("something is wrong", noDecision.getFinalExplanation());
        assertEquals(nodeDecisions, noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        // test bad values
        expectThrows(NullPointerException.class, () -> ShardAllocationDecision.no(null, "a"));
    }

    public void testThrottleDecision() {
        Map<String, Decision> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", Decision.NO);
        nodeDecisions.put("node2", Decision.THROTTLE);
        ShardAllocationDecision throttleDecision = ShardAllocationDecision.throttle("too much happening", nodeDecisions);
        assertTrue(throttleDecision.isDecisionTaken());
        assertEquals(Decision.Type.THROTTLE, throttleDecision.getFinalDecision());
        assertEquals(AllocationStatus.DECIDERS_THROTTLED, throttleDecision.getAllocationStatus());
        assertEquals("too much happening", throttleDecision.getFinalExplanation());
        assertEquals(nodeDecisions, throttleDecision.getNodeDecisions());
        assertNull(throttleDecision.getAssignedNodeId());
        assertNull(throttleDecision.getAllocationId());
    }

    public void testYesDecision() {
        Map<String, Decision> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", Decision.YES);
        nodeDecisions.put("node2", Decision.NO);
        String allocId = randomBoolean() ? "allocId" : null;
        ShardAllocationDecision yesDecision = ShardAllocationDecision.yes(
            "node1", "node was very kind", allocId, nodeDecisions
        );
        assertTrue(yesDecision.isDecisionTaken());
        assertEquals(Decision.Type.YES, yesDecision.getFinalDecision());
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
                ShardAllocationDecision cached = ShardAllocationDecision.throttle(null, null);
                ShardAllocationDecision another = ShardAllocationDecision.throttle(null, null);
                assertSame(cached, another);
                ShardAllocationDecision notCached = ShardAllocationDecision.throttle("abc", null);
                another = ShardAllocationDecision.throttle("abc", null);
                assertNotSame(notCached, another);
            } else {
                ShardAllocationDecision cached = ShardAllocationDecision.no(allocationStatus, null);
                ShardAllocationDecision another = ShardAllocationDecision.no(allocationStatus, null);
                assertSame(cached, another);
                ShardAllocationDecision notCached = ShardAllocationDecision.no(allocationStatus, "abc");
                another = ShardAllocationDecision.no(allocationStatus, "abc");
                assertNotSame(notCached, another);
            }
        }

        // yes decisions are not precomputed and cached
        Map<String, Decision> dummyMap = Collections.emptyMap();
        ShardAllocationDecision first = ShardAllocationDecision.yes("node1", "abc", "alloc1", dummyMap);
        ShardAllocationDecision second = ShardAllocationDecision.yes("node1", "abc", "alloc1", dummyMap);
        // same fields for the ShardAllocationDecision, but should be different instances
        assertNotSame(first, second);
    }

}
