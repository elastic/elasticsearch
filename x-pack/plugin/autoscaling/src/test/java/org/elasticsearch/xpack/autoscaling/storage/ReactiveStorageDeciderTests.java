/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests the primitive methods in {@link ReactiveStorageDecider}. Tests of higher level methods are in
 * {@link ReactiveStorageDeciderDecisionTests}
 */
public class ReactiveStorageDeciderTests extends ESTestCase {
    private static final List<String> SOME_ALLOCATION_DECIDERS = Arrays.asList(
        SameShardAllocationDecider.NAME,
        AwarenessAllocationDecider.NAME,
        EnableAllocationDecider.NAME
    );

    public void testNoTierSpecified() {
        String attribute = randomBoolean() ? null : randomAlphaOfLength(8);
        String tier = attribute != null || randomBoolean() ? null : randomAlphaOfLength(8);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new ReactiveStorageDecider(attribute, tier)
        );
        assertThat(exception.getMessage(), equalTo("must specify both [tier_attribute] [" + attribute + "] and [tier] [" + tier + "]"));
    }

    public void testIsDiskOnlyDecision() {
        Decision.Multi decision = new Decision.Multi();
        if (randomBoolean()) {
            decision.add(randomFrom(Decision.YES, Decision.ALWAYS, Decision.THROTTLE));
        }
        decision.add(new Decision.Single(Decision.Type.NO, DiskThresholdDecider.NAME, "test"));
        randomSubsetOf(SOME_ALLOCATION_DECIDERS).stream()
            .map(
                label -> new Decision.Single(
                    randomValueOtherThan(Decision.Type.NO, () -> randomFrom(Decision.Type.values())),
                    label,
                    "test " + label
                )
            )
            .forEach(decision::add);

        assertThat(ReactiveStorageDecider.isDiskOnlyNoDecision(decision), is(true));
    }

    public void testIsNotDiskOnlyDecision() {
        Decision.Multi decision = new Decision.Multi();
        if (randomBoolean()) {
            decision.add(randomFrom(Decision.YES, Decision.ALWAYS, Decision.THROTTLE, Decision.NO));
        }
        if (randomBoolean()) {
            decision.add(new Decision.Single(Decision.Type.NO, DiskThresholdDecider.NAME, "test"));
            if (randomBoolean()) {
                decision.add(Decision.NO);
            } else {
                decision.add(new Decision.Single(Decision.Type.NO, randomFrom(SOME_ALLOCATION_DECIDERS), "test"));
            }
        } else if (randomBoolean()) {
            decision.add(new Decision.Single(Decision.Type.YES, DiskThresholdDecider.NAME, "test"));
        }
        randomSubsetOf(SOME_ALLOCATION_DECIDERS).stream()
            .map(label -> new Decision.Single(randomFrom(Decision.Type.values()), label, "test " + label))
            .forEach(decision::add);

        assertThat(ReactiveStorageDecider.isDiskOnlyNoDecision(decision), is(false));
    }

    public void testNodesInTier() {
        int hotNodes = randomIntBetween(0, 8);
        int warmNodes = randomIntBetween(0, 8);
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        state = addDataNodes("hot", "hot", state, hotNodes);
        Set<DiscoveryNode> expectedHotNodes = StreamSupport.stream(state.nodes().spliterator(), false).collect(Collectors.toSet());
        state = addDataNodes("warm", "warm", state, warmNodes);

        Set<DiscoveryNode> hotTier = ReactiveStorageDecider.nodesInTier(state.getRoutingNodes(), n -> n.getName().startsWith("hot"))
            .map(RoutingNode::node)
            .collect(Collectors.toSet());
        assertThat(hotTier, equalTo(expectedHotNodes));
    }

    public void testUpdateClusterState() {
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        state = addDataNodes("hot", "hot", state, randomIntBetween(1, 20));
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ClusterState.Builder builder = ClusterState.builder(state).metadata(Metadata.builder().put(indexMetadata, false));
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(indexMetadata);
        builder.routingTable(routingTableBuilder.build());
        state = builder.build();

        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(state, false),
            state,
            null,
            System.nanoTime()
        );
        assertThat(ReactiveStorageDecider.updateClusterState(state, allocation), sameInstance(state));

        RoutingNodes routingNodes = allocation.routingNodes();
        for (RoutingNodes.UnassignedShards.UnassignedIterator iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
            ShardRouting candidate = iterator.next();
            if (candidate.primary()) {
                iterator.initialize(
                    randomFrom(
                        StreamSupport.stream(state.nodes().spliterator(), false).map(DiscoveryNode::getId).collect(Collectors.toList())
                    ),
                    null,
                    0L,
                    allocation.changes()
                );
            }
        }
        ClusterState updatedState = ReactiveStorageDecider.updateClusterState(state, allocation);
        assertThat(updatedState, not(sameInstance(state)));
        assertThat(
            updatedState.getRoutingNodes().shardsWithState(ShardRoutingState.UNASSIGNED),
            hasSize(indexMetadata.getNumberOfShards() * indexMetadata.getNumberOfReplicas())
        );
        assertThat(
            updatedState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING),
            hasSize(indexMetadata.getNumberOfShards())
        );
    }

    private static ClusterState addDataNodes(String tier, String prefix, ClusterState state, int nodes) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(state.nodes());
        IntStream.range(0, nodes).mapToObj(i -> newDataNode(tier, prefix + "_" + i)).forEach(builder::add);
        return ClusterState.builder(state).nodes(builder).build();
    }

    private static DiscoveryNode newDataNode(String tier, String nodeName) {
        return new DiscoveryNode(
            nodeName,
            UUIDs.randomBase64UUID(),
            buildNewFakeTransportAddress(),
            Map.of("tier", tier),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }
}
