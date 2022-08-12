/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AllocationDecidersTests extends ESTestCase {

    public void testDebugMode() {
        verifyDebugMode(RoutingAllocation.DebugMode.ON, Matchers.hasSize(1));
    }

    public void testNoDebugMode() {
        verifyDebugMode(RoutingAllocation.DebugMode.OFF, Matchers.empty());
    }

    public void testDebugExcludeYesMode() {
        verifyDebugMode(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS, Matchers.empty());
    }

    private void verifyDebugMode(RoutingAllocation.DebugMode mode, Matcher<Collection<? extends Decision>> matcher) {
        AllocationDeciders deciders = new AllocationDeciders(List.of(new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return Decision.YES;
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canRebalance(RoutingAllocation allocation) {
                return Decision.YES;
            }
        }));

        IndexMetadata idx = IndexMetadata.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build();
        IndexMetadata testIdx = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(idx, false).put(testIdx, false).build())
            .build();
        final RoutingAllocation allocation = new RoutingAllocation(deciders, clusterState, null, null, 0L);

        allocation.setDebugMode(mode);
        final ShardRouting shardRouting = createShardRouting(testIdx.getIndex());

        RoutingNode routingNode = RoutingNodesHelper.routingNode("testNode", null);
        verify(deciders.canAllocate(shardRouting, routingNode, allocation), matcher);
        verify(deciders.canAllocate(idx, routingNode, allocation), matcher);
        verify(deciders.canAllocate(shardRouting, allocation), matcher);
        verify(deciders.canRebalance(shardRouting, allocation), matcher);
        verify(deciders.canRebalance(allocation), matcher);
        final Decision canRemainResult = deciders.canRemain(shardRouting, routingNode, allocation);
        if (allocation.debugDecision()) {
            verify(canRemainResult, matcher);
        } else {
            assertSame(canRemainResult, Decision.YES);
        }
        verify(deciders.canForceAllocatePrimary(shardRouting, routingNode, allocation), matcher);
        verify(deciders.shouldAutoExpandToNode(idx, null, allocation), matcher);
    }

    private void verify(Decision decision, Matcher<Collection<? extends Decision>> matcher) {
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision, Matchers.instanceOf(Decision.Multi.class));
        Decision.Multi multi = (Decision.Multi) decision;
        assertThat(multi.getDecisions(), matcher);
    }

    public void testEarlyTermination() {
        final Decision decisionOne = randomFrom(Decision.NO, Decision.single(Decision.Type.NO, "label1", "explanation"));
        final Decision decisionTwo = randomFrom(Decision.NO, Decision.single(Decision.Type.NO, "label2", "explanation"));
        final AllocationDeciders allocationDeciders = new AllocationDeciders(List.of(new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return decisionOne;
            }

            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return decisionOne;
            }

            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return decisionOne;
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                return decisionOne;
            }

            @Override
            public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
                return decisionOne;
            }

            @Override
            public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
                return decisionOne;
            }

            @Override
            public Decision canRebalance(RoutingAllocation allocation) {
                return decisionOne;
            }

            @Override
            public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return decisionOne;
            }
        }, new AllocationDecider() {

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return decision(allocation);
            }

            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return decision(allocation);
            }

            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return decision(allocation);
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                return decision(allocation);
            }

            @Override
            public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
                return decision(allocation);
            }

            @Override
            public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
                return decision(allocation);
            }

            @Override
            public Decision canRebalance(RoutingAllocation allocation) {
                return decision(allocation);
            }

            @Override
            public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return decision(allocation);
            }

            private Decision decision(RoutingAllocation allocation) {
                if (allocation.debugDecision() == false) {
                    throw new AssertionError("Should not be called");
                }
                return decisionTwo;
            }
        }));

        IndexMetadata testIdx = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // no debug should just short-circuit to no, no matter what kind of no type return the first decider returns
        final ShardRouting shardRouting = createShardRouting(testIdx.getIndex());
        final RoutingNode routingNode = RoutingNodesHelper.routingNode("testNode", null);
        final IndexMetadata indexMetadata = IndexMetadata.builder("idx")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(testIdx, false).put(indexMetadata, false).build())
            .build();

        final RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState, null, null, 0L);
        assertSame(Decision.NO, allocationDeciders.canAllocate(shardRouting, routingNode, allocation));
        assertSame(Decision.NO, allocationDeciders.canRebalance(shardRouting, allocation));
        assertSame(Decision.NO, allocationDeciders.canRemain(shardRouting, routingNode, allocation));
        assertSame(Decision.NO, allocationDeciders.canAllocate(shardRouting, allocation));
        assertSame(Decision.NO, allocationDeciders.canAllocate(indexMetadata, routingNode, allocation));
        assertSame(Decision.NO, allocationDeciders.shouldAutoExpandToNode(indexMetadata, null, allocation));
        assertSame(Decision.NO, allocationDeciders.canRebalance(allocation));
        assertSame(Decision.NO, allocationDeciders.canForceAllocatePrimary(shardRouting, routingNode, allocation));

        // debug decision should contain both individual decisions in a multi-decision
        allocation.debugDecision(true);
        final Decision expectedDebugDecision = new Decision.Multi().add(decisionOne).add(decisionTwo);
        assertEquals(expectedDebugDecision, allocationDeciders.canAllocate(shardRouting, routingNode, allocation));
        assertEquals(expectedDebugDecision, allocationDeciders.canRebalance(shardRouting, allocation));
        assertEquals(expectedDebugDecision, allocationDeciders.canRemain(shardRouting, routingNode, allocation));
        assertEquals(expectedDebugDecision, allocationDeciders.canAllocate(shardRouting, allocation));
        assertEquals(expectedDebugDecision, allocationDeciders.canAllocate(indexMetadata, routingNode, allocation));
        assertEquals(expectedDebugDecision, allocationDeciders.shouldAutoExpandToNode(indexMetadata, null, allocation));
        assertEquals(expectedDebugDecision, allocationDeciders.canRebalance(allocation));
        assertEquals(expectedDebugDecision, allocationDeciders.canForceAllocatePrimary(shardRouting, routingNode, allocation));
    }

    private static ShardRouting createShardRouting(Index index) {
        return ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "_message")
        );
    }
}
