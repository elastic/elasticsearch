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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
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
        expectThrows(IllegalStateException.class, () -> allocateUnassignedDecision.getAllocationDecision());
        expectThrows(IllegalStateException.class, () -> allocateUnassignedDecision.getAllocationStatus());
        expectThrows(IllegalStateException.class, () -> allocateUnassignedDecision.getAllocationId());
        expectThrows(IllegalStateException.class, () -> allocateUnassignedDecision.getTargetNode());
        expectThrows(IllegalStateException.class, () -> allocateUnassignedDecision.getNodeDecisions());
        expectThrows(IllegalStateException.class, () -> allocateUnassignedDecision.getExplanation());
    }

    public void testNoDecision() {
        final AllocationStatus allocationStatus = randomFrom(
            AllocationStatus.DELAYED_ALLOCATION, AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA
        );
        AllocateUnassignedDecision noDecision = AllocateUnassignedDecision.no(allocationStatus, null);
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(AllocationDecision.fromAllocationStatus(allocationStatus), noDecision.getAllocationDecision());
        assertEquals(allocationStatus, noDecision.getAllocationStatus());
        if (allocationStatus == AllocationStatus.FETCHING_SHARD_DATA) {
            assertEquals("cannot allocate because information about existing shard data is still being retrieved from " +
                             "some of the nodes", noDecision.getExplanation());
        } else if (allocationStatus == AllocationStatus.DELAYED_ALLOCATION) {
            assertThat(noDecision.getExplanation(), startsWith("cannot allocate because the cluster is still waiting"));
        } else {
            assertThat(noDecision.getExplanation(),
                startsWith("cannot allocate because a previous copy of the primary shard existed"));
        }
        assertNull(noDecision.getNodeDecisions());
        assertNull(noDecision.getTargetNode());
        assertNull(noDecision.getAllocationId());

        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 1));
        nodeDecisions.add(new NodeAllocationResult(node2, Decision.NO, 2));
        final boolean reuseStore = randomBoolean();
        noDecision = AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, nodeDecisions, reuseStore);
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(AllocationDecision.NO, noDecision.getAllocationDecision());
        assertEquals(AllocationStatus.DECIDERS_NO, noDecision.getAllocationStatus());
        if (reuseStore) {
            assertEquals("cannot allocate because allocation is not permitted to any of the nodes that hold an in-sync shard copy",
                noDecision.getExplanation());
        } else {
            assertEquals("cannot allocate because allocation is not permitted to any of the nodes", noDecision.getExplanation());
        }
        assertEquals(nodeDecisions.stream().sorted().collect(Collectors.toList()), noDecision.getNodeDecisions());
        // node1 should be sorted first b/c of better weight ranking
        assertEquals("node1", noDecision.getNodeDecisions().iterator().next().getNode().getId());
        assertNull(noDecision.getTargetNode());
        assertNull(noDecision.getAllocationId());

        // test bad values
        expectThrows(NullPointerException.class, () -> AllocateUnassignedDecision.no(null, null));
    }

    public void testThrottleDecision() {
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 1));
        nodeDecisions.add(new NodeAllocationResult(node2, Decision.THROTTLE, 2));
        AllocateUnassignedDecision throttleDecision = AllocateUnassignedDecision.throttle(nodeDecisions);
        assertTrue(throttleDecision.isDecisionTaken());
        assertEquals(AllocationDecision.THROTTLED, throttleDecision.getAllocationDecision());
        assertEquals(AllocationStatus.DECIDERS_THROTTLED, throttleDecision.getAllocationStatus());
        assertThat(throttleDecision.getExplanation(), startsWith("allocation temporarily throttled"));
        assertEquals(nodeDecisions.stream().sorted().collect(Collectors.toList()), throttleDecision.getNodeDecisions());
        // node2 should be sorted first b/c a THROTTLE is higher than a NO decision
        assertEquals("node2", throttleDecision.getNodeDecisions().iterator().next().getNode().getId());
        assertNull(throttleDecision.getTargetNode());
        assertNull(throttleDecision.getAllocationId());
    }

    public void testYesDecision() {
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 1));
        nodeDecisions.add(new NodeAllocationResult(node2, Decision.YES, 2));
        String allocId = randomBoolean() ? "allocId" : null;
        AllocateUnassignedDecision yesDecision = AllocateUnassignedDecision.yes(
            node2, allocId, nodeDecisions, randomBoolean());
        assertTrue(yesDecision.isDecisionTaken());
        assertEquals(AllocationDecision.YES, yesDecision.getAllocationDecision());
        assertNull(yesDecision.getAllocationStatus());
        assertEquals("can allocate the shard", yesDecision.getExplanation());
        assertEquals(nodeDecisions.stream().sorted().collect(Collectors.toList()), yesDecision.getNodeDecisions());
        assertEquals("node2", yesDecision.getTargetNode().getId());
        assertEquals(allocId, yesDecision.getAllocationId());
        // node1 should be sorted first b/c YES decisions are the highest
        assertEquals("node2", yesDecision.getNodeDecisions().iterator().next().getNode().getId());
    }

    public void testCachedDecisions() {
        List<AllocationStatus> cachableStatuses = Arrays.asList(AllocationStatus.DECIDERS_NO, AllocationStatus.DECIDERS_THROTTLED,
            AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA, AllocationStatus.DELAYED_ALLOCATION);
        for (AllocationStatus allocationStatus : cachableStatuses) {
            if (allocationStatus == AllocationStatus.DECIDERS_THROTTLED) {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.throttle(null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.throttle(null);
                assertSame(cached, another);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.throttle(new ArrayList<>());
                another = AllocateUnassignedDecision.throttle(new ArrayList<>());
                assertNotSame(notCached, another);
            } else {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.no(allocationStatus, null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.no(allocationStatus, null);
                assertSame(cached, another);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.no(allocationStatus, new ArrayList<>());
                another = AllocateUnassignedDecision.no(allocationStatus, new ArrayList<>());
                assertNotSame(notCached, another);
            }
        }

        // yes decisions are not precomputed and cached
        AllocateUnassignedDecision first = AllocateUnassignedDecision.yes(node1, "abc", emptyList(), randomBoolean());
        AllocateUnassignedDecision second = AllocateUnassignedDecision.yes(node1, "abc", emptyList(), randomBoolean());
        // same fields for the ShardAllocationDecision, but should be different instances
        assertNotSame(first, second);
    }

    public void testSerialization() throws IOException {
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Decision.Type finalDecision = randomFrom(Decision.Type.values());
        DiscoveryNode assignedNode = finalDecision == Decision.Type.YES ? node1 : null;
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 2));
        nodeDecisions.add(new NodeAllocationResult(node2, finalDecision == Decision.Type.YES ? Decision.YES :
                                                              randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES), 1));
        AllocateUnassignedDecision decision;
        if (finalDecision == Decision.Type.YES) {
            decision = AllocateUnassignedDecision.yes(assignedNode, randomBoolean() ? randomAlphaOfLength(5) : null,
                nodeDecisions, randomBoolean());
        } else {
            decision = AllocateUnassignedDecision.no(randomFrom(
                AllocationStatus.DELAYED_ALLOCATION, AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA
            ), nodeDecisions, randomBoolean());
        }
        BytesStreamOutput output = new BytesStreamOutput();
        decision.writeTo(output);
        AllocateUnassignedDecision readDecision = new AllocateUnassignedDecision(output.bytes().streamInput());
        assertEquals(decision.getTargetNode(), readDecision.getTargetNode());
        assertEquals(decision.getAllocationStatus(), readDecision.getAllocationStatus());
        assertEquals(decision.getExplanation(), readDecision.getExplanation());
        assertEquals(decision.getNodeDecisions().size(), readDecision.getNodeDecisions().size());
        assertEquals(decision.getAllocationId(), readDecision.getAllocationId());
        assertEquals(decision.getAllocationDecision(), readDecision.getAllocationDecision());
        // node2 should have the highest sort order
        assertEquals("node2", readDecision.getNodeDecisions().iterator().next().getNode().getId());
    }

}
