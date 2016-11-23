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
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.startsWith;

/**
 * Unit tests for the {@link AllocateUnassignedDecision} class.
 */
public class AllocateUnassignedDecisionTests extends ESTestCase {

    private DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
    private DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

    public void testDecisionNotTaken() {
        AllocateUnassignedDecision allocateUnassignedDecision = AllocateUnassignedDecision.NOT_TAKEN;
        assertFalse(allocateUnassignedDecision.isDecisionTaken());
        assertNull(allocateUnassignedDecision.getDecision());
        assertNull(allocateUnassignedDecision.getAllocationStatus());
        assertNull(allocateUnassignedDecision.getAllocationId());
        assertNull(allocateUnassignedDecision.getAssignedNodeId());
        assertNull(allocateUnassignedDecision.getNodeDecisions());
        expectThrows(IllegalArgumentException.class, () -> allocateUnassignedDecision.getDecisionSafe());
        expectThrows(IllegalStateException.class, () -> allocateUnassignedDecision.getExplanation());
    }

    public void testNoDecision() {
        final AllocationStatus allocationStatus = randomFrom(
            AllocationStatus.DELAYED_ALLOCATION, AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA
        );
        AllocateUnassignedDecision noDecision = AllocateUnassignedDecision.no(allocationStatus, null);
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getDecision());
        assertEquals(allocationStatus, noDecision.getAllocationStatus());
        if (allocationStatus == AllocationStatus.FETCHING_SHARD_DATA) {
            assertEquals("cannot allocate because information about existing shard data is still being retrieved from " +
                             "some of the nodes", noDecision.getExplanation());
        } else if (allocationStatus == AllocationStatus.DELAYED_ALLOCATION) {
            assertThat(noDecision.getExplanation(), startsWith("cannot allocate because the cluster is waiting"));
        } else {
            assertThat(noDecision.getExplanation(),
                startsWith("cannot allocate because a previous copy of the shard existed"));
        }
        assertNull(noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(node1, Decision.NO, 1));
        nodeDecisions.put("node2", new NodeAllocationResult(node2, Decision.NO, 2));
        final boolean reuseStore = randomBoolean();
        noDecision = AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, nodeDecisions, reuseStore);
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getDecision());
        assertEquals(AllocationStatus.DECIDERS_NO, noDecision.getAllocationStatus());
        if (reuseStore) {
            assertEquals("cannot allocate because allocation is not permitted to any of the nodes that hold a valid shard copy",
                noDecision.getExplanation());
        } else {
            assertEquals("cannot allocate because allocation is not permitted to any of the nodes", noDecision.getExplanation());
        }
        assertEquals(nodeDecisions, noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        // test bad values
        expectThrows(NullPointerException.class, () -> AllocateUnassignedDecision.no((AllocationStatus) null, null));
    }

    public void testThrottleDecision() {
        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(node1, Decision.NO, 1));
        nodeDecisions.put("node2", new NodeAllocationResult(node2, Decision.THROTTLE, 2));
        AllocateUnassignedDecision throttleDecision = AllocateUnassignedDecision.throttle(nodeDecisions);
        assertTrue(throttleDecision.isDecisionTaken());
        assertEquals(Decision.Type.THROTTLE, throttleDecision.getDecision());
        assertEquals(AllocationStatus.DECIDERS_THROTTLED, throttleDecision.getAllocationStatus());
        assertThat(throttleDecision.getExplanation(), startsWith("allocation temporarily throttled"));
        assertEquals(nodeDecisions, throttleDecision.getNodeDecisions());
        assertNull(throttleDecision.getAssignedNodeId());
        assertNull(throttleDecision.getAllocationId());
    }

    public void testYesDecision() {
        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(node1, Decision.YES, 1));
        nodeDecisions.put("node2", new NodeAllocationResult(node2, Decision.NO, 2));
        String allocId = randomBoolean() ? "allocId" : null;
        AllocateUnassignedDecision yesDecision = AllocateUnassignedDecision.yes(
            node1, allocId, nodeDecisions, randomBoolean());
        assertTrue(yesDecision.isDecisionTaken());
        assertEquals(Decision.Type.YES, yesDecision.getDecision());
        assertNull(yesDecision.getAllocationStatus());
        assertEquals("can allocate the shard", yesDecision.getExplanation());
        assertEquals(nodeDecisions, yesDecision.getNodeDecisions());
        assertEquals("node1", yesDecision.getAssignedNodeId());
        assertEquals(allocId, yesDecision.getAllocationId());
    }

    public void testCachedDecisions() {
        List<AllocationStatus> cachableStatuses = Arrays.asList(AllocationStatus.DECIDERS_NO, AllocationStatus.DECIDERS_THROTTLED,
            AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA, AllocationStatus.DELAYED_ALLOCATION);
        for (AllocationStatus allocationStatus : cachableStatuses) {
            if (allocationStatus == AllocationStatus.DECIDERS_THROTTLED) {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.throttle(null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.throttle(null);
                assertSame(cached, another);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.throttle(new HashMap<>());
                another = AllocateUnassignedDecision.throttle(new HashMap<>());
                assertNotSame(notCached, another);
            } else {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.no(allocationStatus, null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.no(allocationStatus, null);
                assertSame(cached, another);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.no(allocationStatus, new HashMap<>());
                another = AllocateUnassignedDecision.no(allocationStatus, new HashMap<>());
                assertNotSame(notCached, another);
            }
        }

        // yes decisions are not precomputed and cached
        Map<String, NodeAllocationResult> dummyMap = emptyMap();
        AllocateUnassignedDecision first = AllocateUnassignedDecision.yes(node1, "abc", dummyMap, randomBoolean());
        AllocateUnassignedDecision second = AllocateUnassignedDecision.yes(node1, "abc", dummyMap, randomBoolean());
        // same fields for the ShardAllocationDecision, but should be different instances
        assertNotSame(first, second);
    }

}
