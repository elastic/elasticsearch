/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools.ThreadPoolUsageStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.ArgumentMatchers.any;

public class WriteLoadConstraintDeciderTests extends ESAllocationTestCase {

    public void testCanAlwaysAllocateDuringReplace() {
        var wld = new WriteLoadConstraintDecider(ClusterSettings.createBuiltInClusterSettings());
        assertEquals(Decision.YES, wld.canForceAllocateDuringReplace(any(), any(), any()));
    }

    /**
     * Test the write load decider behavior when disabled
     */
    public void testWriteLoadDeciderDisabled() {
        String indexName = "test-index";
        var testHarness = createClusterStateAndRoutingAllocation(indexName);

        var writeLoadDecider = createWriteLoadConstraintDecider(
            Settings.builder()
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED
                )
                .build()
        );

        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeBelowUtilThreshold,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.belowThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.nearThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingNoWriteLoad,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );

        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
    }

    /**
     * Test the {@link WriteLoadConstraintDecider#canAllocate} implementation.
     */
    public void testWriteLoadDeciderCanAllocate() {
        String indexName = "test-index";
        var testHarness = createClusterStateAndRoutingAllocation(indexName);
        testHarness.routingAllocation.debugDecision(true);

        var writeLoadDecider = createWriteLoadConstraintDecider(
            Settings.builder()
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    randomBoolean()
                        ? WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
                        : WriteLoadConstraintSettings.WriteLoadDeciderStatus.LOW_THRESHOLD_ONLY
                )
                .build()
        );
        assertDecisionMatches(
            "Assigning a new shard to a node that is above the threshold should fail",
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeBelowUtilThreshold,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.NOT_PREFERRED,
            "Node [*] with write thread pool utilization [0.99] already exceeds the high utilization threshold of [0.900000]. "
                + "Cannot allocate shard [[test-index][1]] to node without risking increased write latencies."
        );
        assertDecisionMatches(
            "Unassigned shard should not be preferred on an already hot node",
            writeLoadDecider.canAllocate(
                testHarness.unassignedShardRouting,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.NOT_PREFERRED,
            "*already exceeds the high utilization threshold*"
        );
        assertDecisionMatches(
            "Unassigned shard should be accepted on a node with capacity",
            writeLoadDecider.canAllocate(
                testHarness.unassignedShardRouting,
                testHarness.belowThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.YES,
            "*can be assigned to node*"
        );
        assertDecisionMatches(
            "Unassigned shard shouldn't be allocated to hot-spotting node with low utilisation",
            writeLoadDecider.canAllocate(
                testHarness.unassignedShardRouting,
                testHarness.aboveQueueingThresholdWithLowUtilisationNode,
                testHarness.routingAllocation
            ),
            Decision.Type.NOT_PREFERRED,
            "Node [*] is currently hot-spotting or in a waiting period, and does not prefer shards moved onto it"
        );
        assertDecisionMatches(
            "Assigning a new shard to a node that has capacity should succeed",
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.belowThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.YES,
            "Shard [[test-index][0]] in index [[test-index]] can be assigned to node [*]. The node's utilization would become [*]"
        );
        assertDecisionMatches(
            "Assigning a new shard without a write load estimate to an over-threshold node should be blocked",
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingNoWriteLoad,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.NOT_PREFERRED,
            "Node [*] with write thread pool utilization [0.99] already exceeds the high utilization threshold of "
                + "[0.900000]. Cannot allocate shard [[test-index][2]] to node without risking increased write latencies."
        );
        assertDecisionMatches(
            "Assigning a new shard without a write load estimate to an under-threshold node should be allowed",
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingNoWriteLoad,
                testHarness.belowThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.YES,
            "Shard [[test-index][2]] in index [[test-index]] can be assigned to node [*]. The node's utilization would become [*]"
        );
        assertDecisionMatches(
            "Assigning a new shard that would cause the node to exceed capacity should fail",
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.nearThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.NOT_PREFERRED,
            "The high utilization threshold of [0.900000] would be exceeded on node [*] with utilization [0.89] "
                + "if shard [[test-index][0]] with estimated additional utilisation [0.06250] (write load [0.50000] / threads [8]) were "
                + "assigned to it. Cannot allocate shard to node without risking increased write latencies."
        );
    }

    /**
     * Test the {@link WriteLoadConstraintDecider#canRemain} implementation.
     */
    public void testWriteLoadDeciderCanRemain() {
        String indexName = "test-index";
        var testHarness = createClusterStateAndRoutingAllocation(indexName);
        testHarness.routingAllocation.debugDecision(true);

        var writeLoadDecider = createWriteLoadConstraintDecider(
            Settings.builder()
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
                )
                .build()
        );

        assertEquals(
            "A shard on a node below the util threshold should remain on its node",
            Decision.Type.YES,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeBelowUtilThreshold,
                testHarness.belowThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            "A shard on a node above the util threshold should remain on its node",
            Decision.Type.YES,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            "A shard on a node with queuing below the threshold should remain on its node",
            Decision.Type.YES,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeBelowQueueThreshold,
                testHarness.belowQueuingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            "A shard on a node with queuing above the threshold should not remain",
            Decision.Type.NOT_PREFERRED,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeAboveQueueThreshold,
                testHarness.aboveQueuingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            "A shard with no write load can still return NOT_PREFERRED",
            Decision.Type.NOT_PREFERRED,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingNoWriteLoad,
                testHarness.aboveQueuingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
    }

    public void testWriteLoadDeciderShouldPreventBalancerMovingShardsBack() {
        final var indexName = randomIdentifier();
        final int numThreads = randomIntBetween(1, 10);
        final float hotspotUtilizationThreshold = randomFloatBetween(0.5f, 0.9f, true);
        final float allocationUtilizationThreshold = randomFloatBetween(0.5f, 0.9f, true);
        final TimeValue highLatencyThreshold = randomTimeValue(1000, 10000, TimeUnit.MILLISECONDS);
        final var settings = createSettings(allocationUtilizationThreshold, hotspotUtilizationThreshold, highLatencyThreshold, null);

        final var state = ClusterStateCreationUtils.state(2, new String[] { indexName }, 4);
        final var balancedShardsAllocator = new BalancedShardsAllocator(settings);
        final var overloadedNode = randomFrom(state.nodes().getAllNodes());
        final var otherNode = state.nodes().stream().filter(node -> node != overloadedNode).findFirst().orElseThrow();
        final var clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(
                Map.of(
                    overloadedNode.getId(),
                    new NodeUsageStatsForThreadPools(
                        overloadedNode.getId(),
                        Map.of(
                            ThreadPool.Names.WRITE,
                            new ThreadPoolUsageStats(
                                numThreads,
                                randomFloatBetween(hotspotUtilizationThreshold, 1.1f, false),
                                randomLongBetween(highLatencyThreshold.millis(), highLatencyThreshold.millis() * 2)
                            )
                        )
                    ),
                    otherNode.getId(),
                    new NodeUsageStatsForThreadPools(
                        otherNode.getId(),
                        Map.of(
                            ThreadPool.Names.WRITE,
                            new ThreadPoolUsageStats(
                                numThreads,
                                randomFloatBetween(0.0f, allocationUtilizationThreshold / 2, true),
                                randomLongBetween(0, highLatencyThreshold.millis() / 2)
                            )
                        )
                    )
                )
            )
            // simulate all zero or missing shard write loads
            .shardWriteLoads(
                state.routingTable(ProjectId.DEFAULT)
                    .allShards()
                    .filter(ignored -> randomBoolean()) // some write-loads are missing altogether
                    .collect(Collectors.toMap(ShardRouting::shardId, ignored -> 0.0d))  // the rest are zero
            )
            .nodeIdsWriteLoadHotspotting(Set.of(overloadedNode.getId()))
            .build();

        final var clusterSettings = createBuiltInClusterSettings(settings);
        final var writeLoadConstraintDecider = new WriteLoadConstraintDecider(clusterSettings);
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(state)
            .allocationDeciders(writeLoadConstraintDecider)
            .clusterInfo(clusterInfo)
            .mutable();

        // This should move a shard in an attempt to resolve the hot-spot
        balancedShardsAllocator.allocate(routingAllocation);

        assertEquals(1, routingAllocation.routingNodes().node(overloadedNode.getId()).numberOfOwningShards());
        assertEquals(3, routingAllocation.routingNodes().node(otherNode.getId()).numberOfOwningShards());

        final var clusterInfoSimulator = new ClusterInfoSimulator(routingAllocation);
        final var movedShards = new HashSet<ShardRouting>();
        for (RoutingNode routingNode : routingAllocation.routingNodes()) {
            movedShards.addAll(routingNode.shardsWithState(ShardRoutingState.INITIALIZING).collect(Collectors.toSet()));
        }
        movedShards.forEach(shardRouting -> {
            routingAllocation.routingNodes().startShard(shardRouting, new RoutingChangesObserver() {}, randomNonNegativeLong());
            clusterInfoSimulator.simulateShardStarted(shardRouting);
        });

        // This should run through the balancer without moving any shards back
        ClusterInfo simulatedClusterInfo = clusterInfoSimulator.getClusterInfo();
        balancedShardsAllocator.allocate(
            TestRoutingAllocationFactory.forClusterState(routingAllocation.getClusterState())
                .allocationDeciders(routingAllocation.deciders())
                .routingNodes(routingAllocation.routingNodes())
                .clusterInfo(simulatedClusterInfo)
                .shardSizeInfo(routingAllocation.snapshotShardSizeInfo())
                .mutable()
        );
        assertEquals(1, routingAllocation.routingNodes().node(overloadedNode.getId()).numberOfOwningShards());
        assertEquals(3, routingAllocation.routingNodes().node(otherNode.getId()).numberOfOwningShards());
    }

    public void testWriteLoadDeciderShouldNotPreferAllocationDuringHotspot() {
        final var indexName = randomIdentifier();
        final int numThreads = randomIntBetween(1, 10);
        final float allocationUtilizationThreshold = randomFloatBetween(0.5f, 0.9f, true);
        final TimeValue highLatencyThreshold = randomTimeValue(1000, 10000, TimeUnit.MILLISECONDS);
        final var settings = createSettings(allocationUtilizationThreshold, null, highLatencyThreshold, null);

        final var state = ClusterStateCreationUtils.state(2, new String[] { indexName }, 4);
        final var overloadedNode = randomFrom(state.nodes().getAllNodes());
        final var otherNode = state.nodes().stream().filter(node -> node != overloadedNode).findFirst().orElseThrow();
        final var clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(
                Map.of(
                    overloadedNode.getId(),
                    new NodeUsageStatsForThreadPools(
                        overloadedNode.getId(),
                        Map.of(
                            ThreadPool.Names.WRITE,
                            new ThreadPoolUsageStats(
                                numThreads,
                                randomFloatBetween(0.0f, allocationUtilizationThreshold - .0001f, false),
                                randomLongBetween(0, highLatencyThreshold.millis() - 1)
                            )
                        )
                    ),
                    otherNode.getId(),
                    new NodeUsageStatsForThreadPools(
                        otherNode.getId(),
                        Map.of(
                            ThreadPool.Names.WRITE,
                            new ThreadPoolUsageStats(
                                numThreads,
                                randomFloatBetween(0.0f, allocationUtilizationThreshold - .0001f, false),
                                randomLongBetween(0, highLatencyThreshold.millis() - 1)
                            )
                        )
                    )
                )
            )
            .nodeIdsWriteLoadHotspotting(Set.of(overloadedNode.getId()))
            .build();

        final var writeLoadConstraintDecider = createWriteLoadConstraintDecider(settings);
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(state)
            .allocationDeciders(writeLoadConstraintDecider)
            .clusterInfo(clusterInfo)
            .mutable();
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        var overloadedRoutingNode = routingAllocation.routingNodes().node(overloadedNode.getId());
        var shardRouting = routingAllocation.routingNodes()
            .node(otherNode.getId())
            .shardsWithState(ShardRoutingState.STARTED)
            .findFirst()
            .orElseThrow();
        Decision decision = writeLoadConstraintDecider.canAllocate(shardRouting, overloadedRoutingNode, routingAllocation);
        assertEquals(decision.type(), Decision.NOT_PREFERRED.type());
        assertThat(
            decision.getExplanation(),
            equalTo(
                "Node ["
                    + overloadedNode.getId()
                    + "] is currently hot-spotting or in a waiting "
                    + "period, and does not prefer shards moved onto it"
            )
        );
    }

    public void testWriteLoadDeciderNeedsBothUtilizationAndLatencyForHotspot() {
        final float hotspotUtilizationThreshold = randomFloatBetween(0.5f, 0.9f, true);
        final String hotspotUtilizationThresholdString = RatioValue.parseRatioValue(String.valueOf(hotspotUtilizationThreshold))
            .formatNoTrailingZerosPercent();
        final TimeValue highLatencyThreshold = randomTimeValue(1000, 10000, TimeUnit.MILLISECONDS);
        final String highLatencyThresholdString = highLatencyThreshold.toHumanReadableString(2);
        final int maxShardWriteLoadProportionPercent = randomIntBetween(80, 95);
        final var settings = createSettings(null, hotspotUtilizationThreshold, highLatencyThreshold, maxShardWriteLoadProportionPercent);
        final var decider = createWriteLoadConstraintDecider(settings);
        final var indexName = randomIdentifier();
        final var state = ClusterStateCreationUtils.state(1, new String[] { indexName }, 4);
        final var hotspotNode = randomFrom(state.nodes().getAllNodes());

        // test utilization low, latency high
        var utilization = randomFloatBetween(0.0f, hotspotUtilizationThreshold, true);
        var latencyMillis = randomLongBetween(highLatencyThreshold.millis() + 1, 2 * highLatencyThreshold.millis());
        var routingAllocation = buildRoutingAllocation(state, decider, hotspotNode.getId(), utilization, latencyMillis);
        var hotspotRoutingNode = routingAllocation.routingNodes().node(hotspotNode.getId());
        var shardRouting = routingAllocation.routingNodes()
            .node(hotspotNode.getId())
            .shardsWithState(ShardRoutingState.STARTED)
            .findFirst()
            .orElseThrow();

        Decision decision = decider.canRemain(
            state.metadata().getProject().index(indexName),
            shardRouting,
            hotspotRoutingNode,
            routingAllocation
        );
        assertEquals(decision.type(), Decision.YES.type());
        assertThat(
            decision.getExplanation(),
            matchesPattern(
                Strings.format(
                    """
                        Node \\[.*\\]'s queue latency of \\[%d\\] does not exceed the latency threshold of \\[%s\\], or the thread pool \
                        utilization of \\[%f\\] does not exceed the utilization threshold of \\[%s\\]""",
                    latencyMillis,
                    highLatencyThresholdString,
                    utilization,
                    hotspotUtilizationThresholdString
                )
            )
        );

        // test utilization high, latency low
        utilization = randomFloatBetween(hotspotUtilizationThreshold, 1.2f, false);
        latencyMillis = randomLongBetween(0, highLatencyThreshold.millis() - 1);
        routingAllocation = buildRoutingAllocation(state, decider, hotspotNode.getId(), utilization, latencyMillis);
        hotspotRoutingNode = routingAllocation.routingNodes().node(hotspotNode.getId());
        shardRouting = routingAllocation.routingNodes()
            .node(hotspotNode.getId())
            .shardsWithState(ShardRoutingState.STARTED)
            .findFirst()
            .orElseThrow();

        decision = decider.canRemain(state.metadata().getProject().index(indexName), shardRouting, hotspotRoutingNode, routingAllocation);
        assertEquals(decision.type(), Decision.YES.type());
        assertThat(
            decision.getExplanation(),
            matchesPattern(
                Strings.format(
                    """
                        Node \\[.*\\]'s queue latency of \\[%d\\] does not exceed the latency threshold of \\[%s\\], or the thread pool \
                        utilization of \\[%f\\] does not exceed the utilization threshold of \\[%s\\]""",
                    latencyMillis,
                    highLatencyThresholdString,
                    utilization,
                    hotspotUtilizationThresholdString
                )
            )
        );

        // test utilization high and latency high
        utilization = randomFloatBetween(hotspotUtilizationThreshold, 1.2f, false);
        latencyMillis = randomLongBetween(highLatencyThreshold.millis() + 1, highLatencyThreshold.millis() * 2);
        routingAllocation = buildRoutingAllocation(state, decider, hotspotNode.getId(), utilization, latencyMillis);
        hotspotRoutingNode = routingAllocation.routingNodes().node(hotspotNode.getId());
        shardRouting = routingAllocation.routingNodes()
            .node(hotspotNode.getId())
            .shardsWithState(ShardRoutingState.STARTED)
            .findFirst()
            .orElseThrow();

        decision = decider.canRemain(state.metadata().getProject().index(indexName), shardRouting, hotspotRoutingNode, routingAllocation);
        assertEquals(decision.type(), Decision.NOT_PREFERRED.type());
        assertThat(
            decision.getExplanation(),
            matchesPattern(
                Strings.format(
                    """
                        Node \\[.*\\] has a queue latency of \\[%d\\] millis that exceeds the queue latency threshold of \\[%s\\] and a \
                        thread pool utilization of \\[%f\\] that exceeds the utilization threshold of \\[%s\\]. This node is \
                        hot-spotting. Shard write load \\[.*\\]. The max shard write-load proportion on this node is 0.0%%, below \
                        the single-hot-shard threshold of %d%%. Should move shard\\(s\\) away""",
                    latencyMillis,
                    highLatencyThresholdString,
                    utilization,
                    hotspotUtilizationThresholdString,
                    maxShardWriteLoadProportionPercent
                )
            )
        );
    }

    /**
     * Test that when {@code maxShardWriteLoadThreshold == 0.0} (the feature is disabled) and the node is hotspotting
     * (both latency and utilization thresholds exceeded), the explain string says "Max shard proportion is disabled"
     * rather than reporting a calculated proportion.
     */
    public void testCanRemainExplainStringWhenMaxShardProportionDisabledAndNodeHotspotting() {
        final float hotspotUtilizationThreshold = randomFloatBetween(0.5f, 0.9f, true);
        final String hotspotUtilizationThresholdString = RatioValue.parseRatioValue(String.valueOf(hotspotUtilizationThreshold))
            .formatNoTrailingZerosPercent();
        final TimeValue highLatencyThreshold = randomTimeValue(1000, 10000, TimeUnit.MILLISECONDS);
        final String highLatencyThresholdString = highLatencyThreshold.toHumanReadableString(2);

        final var settings = createSettings(null, hotspotUtilizationThreshold, highLatencyThreshold, 0);
        final var decider = createWriteLoadConstraintDecider(settings);
        final var indexName = randomIdentifier();
        final var state = ClusterStateCreationUtils.state(1, new String[] { indexName }, 4);
        final var hotspotNode = randomFrom(state.nodes().getAllNodes());

        final float utilization = randomFloatBetween(hotspotUtilizationThreshold, 1.2f, false);
        final long latencyMillis = randomLongBetween(highLatencyThreshold.millis() + 1, highLatencyThreshold.millis() * 2);
        final var routingAllocation = buildRoutingAllocation(state, decider, hotspotNode.getId(), utilization, latencyMillis);
        final var hotspotRoutingNode = routingAllocation.routingNodes().node(hotspotNode.getId());
        final var shardRouting = routingAllocation.routingNodes()
            .node(hotspotNode.getId())
            .shardsWithState(ShardRoutingState.STARTED)
            .findFirst()
            .orElseThrow();

        final Decision decision = decider.canRemain(
            state.metadata().getProject().index(indexName),
            shardRouting,
            hotspotRoutingNode,
            routingAllocation
        );
        assertEquals(Decision.Type.NOT_PREFERRED, decision.type());
        assertThat(
            decision.getExplanation(),
            matchesPattern(
                Strings.format(
                    """
                        Node \\[.*\\] has a queue latency of \\[%d\\] millis that exceeds the queue latency threshold of \\[%s\\] and a \
                        thread pool utilization of \\[%f\\] that exceeds the utilization threshold of \\[%s\\]. This node is hot-spotting. \
                        Shard write load \\[.*\\]. Max shard write-load proportion is disabled. Should move shard\\(s\\) away""",
                    latencyMillis,
                    highLatencyThresholdString,
                    utilization,
                    hotspotUtilizationThresholdString
                )
            )
        );
    }

    public void testMaxSingleShardWriteLoadSingleShard() {
        ShardId testShardId = new ShardId(randomIndexName(), randomUUID(), 0);

        // only shard means 1.0
        assertThat(
            WriteLoadConstraintDecider.maxShardWriteLoadProportion(
                List.of(testShardId),
                Map.of(testShardId, randomDoubleBetween(0.001, 20.0, false))
            ),
            equalTo(1.0)
        );

        // inexplicably not in map means 0.0
        assertThat(WriteLoadConstraintDecider.maxShardWriteLoadProportion(List.of(testShardId), Map.of()), equalTo(0.0));

        // shard is in map with zero load
        assertThat(WriteLoadConstraintDecider.maxShardWriteLoadProportion(List.of(testShardId), Map.of(testShardId, 0.0)), equalTo(0.0));

        // shard with 0 load
        assertThat(
            WriteLoadConstraintDecider.maxShardWriteLoadProportion(
                List.of(testShardId),
                Map.of(testShardId, 0.0, new ShardId(randomIndexName(), randomUUID(), 0), randomDoubleBetween(0.0001, 20.0, true))
            ),
            equalTo(0.0)
        );
    }

    public void testmaxSingleShardWriteLoadMultipleShards() {
        ShardId testShardId1 = new ShardId(randomIndexName(), randomUUID(), 0);
        ShardId testShardId2 = new ShardId(randomIndexName(), randomUUID(), 0);

        double shard1Load = randomDoubleBetween(0.0001, 10.0, true);
        double shard2Load = randomDoubleBetween(shard1Load, 20.0, false);

        // picks the biggest one
        assertThat(
            WriteLoadConstraintDecider.maxShardWriteLoadProportion(
                List.of(testShardId1, testShardId2),
                Map.of(testShardId1, shard1Load, testShardId2, shard2Load)
            ),
            equalTo(shard2Load / (shard1Load + shard2Load))
        );

        // both zero
        assertThat(
            WriteLoadConstraintDecider.maxShardWriteLoadProportion(
                List.of(testShardId1, testShardId2),
                Map.of(testShardId1, 0.0, testShardId2, 0.0)
            ),
            equalTo(0.0)
        );

        // not in map
        assertThat(WriteLoadConstraintDecider.maxShardWriteLoadProportion(List.of(testShardId1, testShardId2), Map.of()), equalTo(0.0));

        // one in map
        assertThat(
            WriteLoadConstraintDecider.maxShardWriteLoadProportion(List.of(testShardId1, testShardId2), Map.of(testShardId1, 0.0)),
            equalTo(0.0)
        );

        // one shard has 100% of the load
        assertThat(
            WriteLoadConstraintDecider.maxShardWriteLoadProportion(
                List.of(testShardId1, testShardId2),
                randomBoolean()
                    ? Map.of(testShardId1, randomDoubleBetween(0.0001, 10.0, true), testShardId2, 0.0)
                    : Map.of(testShardId1, randomDoubleBetween(0.0001, 10.0, true))
            ),
            equalTo(1.0)
        );

        // totally random map entry
        assertThat(
            WriteLoadConstraintDecider.maxShardWriteLoadProportion(
                List.of(testShardId1, testShardId2),
                Map.of(new ShardId(randomIndexName(), randomUUID(), 0), randomDoubleBetween(0.0001, 20.0, true))
            ),
            equalTo(0.0)
        );
    }

    public void testHotspotUtilizationSingleShardProportionCheck() {
        /* Test that a hotspot that is too focused on a single shard (over 90%) is left alone, as
         * rebalancing won't do anything and the hotspot shard should not be moved. Test that when this
         * proportion is not exceeded, the same check sees both shards flagged for migration away */
        var writeLoadDecider = createWriteLoadConstraintDecider(createSettings(null, null, null, 90));

        String indexName = randomIndexName();
        int numberOfShards = 3;

        ClusterState clusterState = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(
            new String[] { indexName },
            numberOfShards,
            randomIntBetween(0, 2)
        );

        var node = clusterState.nodes().iterator().next();
        var indexMetadata = clusterState.metadata().getProject().index(indexName);
        var index = indexMetadata.getIndex();

        var hotspotStats = createNodeUsageStatsForThreadPools(node, 8, 0.99f, 15_000);

        ShardId highShard = new ShardId(index, 0);
        ShardId lowShard = new ShardId(index, 1);
        // a shard that is not on the node, to test correct exclusion and calculation
        ShardId allocatedElsewhereShard = new ShardId(index, 2);

        var usageStats = Map.of(node.getId(), hotspotStats);
        var hotspotIds = Set.of(node.getId());

        double scalar = randomDoubleBetween(0.1, 20.0, true);
        var writeLoadsRemain = Map.of(
            highShard,
            0.95 * scalar,
            lowShard,
            0.05 * scalar,
            allocatedElsewhereShard,
            randomDoubleBetween(20.0, 40.0, true)
        );

        ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(usageStats)
            .shardWriteLoads(writeLoadsRemain)
            .nodeIdsWriteLoadHotspotting(hotspotIds)
            .build();

        var routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).clusterInfo(clusterInfo).build();
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        ShardRouting highShardRouting = TestShardRouting.newShardRouting(highShard, node.getId(), null, true, ShardRoutingState.STARTED);

        ShardRouting lowShardRouting = TestShardRouting.newShardRouting(lowShard, node.getId(), null, true, ShardRoutingState.STARTED);

        RoutingNode routingNode = RoutingNodesHelper.routingNode(node.getId(), node, highShardRouting, lowShardRouting);

        // both high and low shards decide YES to canRemain, as the proportion
        // of load is above 90% on one shard
        Decision moveDecision = writeLoadDecider.canRemain(indexMetadata, highShardRouting, routingNode, routingAllocation);
        assertEquals(Decision.Type.YES, moveDecision.type());
        String explanationRegex = Strings.format("""
            Node \\[%s\\] is hot-spotting due to a single shard executing \\[95.00\\] percent of the writes. But since this is above the \
            single shard write load threshold \\(\\[90%%\\]\\), moving shards away from this node is not expected to resolve the \
            hot-spot.""", node.getShortNodeDescription());

        assertThat(moveDecision.getExplanation(), matchesPattern(explanationRegex));

        moveDecision = writeLoadDecider.canRemain(indexMetadata, lowShardRouting, routingNode, routingAllocation);
        assertEquals(Decision.Type.YES, moveDecision.type());
        assertThat(moveDecision.getExplanation(), matchesPattern(explanationRegex));

        // retry test, but turn off setting with a 0 value
        var writeLoadDeciderProportionDisabled = createWriteLoadConstraintDecider(createSettings(null, null, null, 0));

        assertEquals(
            Decision.Type.NOT_PREFERRED,
            writeLoadDeciderProportionDisabled.canRemain(indexMetadata, highShardRouting, routingNode, routingAllocation).type()
        );

        assertEquals(
            Decision.Type.NOT_PREFERRED,
            writeLoadDeciderProportionDisabled.canRemain(indexMetadata, lowShardRouting, routingNode, routingAllocation).type()
        );

        // retry test, with proportions under the threshold
        scalar = randomDoubleBetween(0.1, 20.0, true);
        var writeLoadsMigrate = Map.of(
            highShard,
            0.85 * scalar,
            lowShard,
            0.15 * scalar,
            allocatedElsewhereShard,
            randomDoubleBetween(20.0, 40.0, true)
        );

        clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(usageStats)
            .shardWriteLoads(writeLoadsMigrate)
            .nodeIdsWriteLoadHotspotting(hotspotIds)
            .build();

        routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).clusterInfo(clusterInfo).build();

        // both high and low shards decide NOT_PREFERRED to canRemain, as the proportion
        // of load is under 90% on any one shard
        assertEquals(
            Decision.Type.NOT_PREFERRED,
            writeLoadDecider.canRemain(indexMetadata, highShardRouting, routingNode, routingAllocation).type()
        );

        assertEquals(
            Decision.Type.NOT_PREFERRED,
            writeLoadDecider.canRemain(indexMetadata, lowShardRouting, routingNode, routingAllocation).type()
        );
    }

    public RoutingAllocation buildRoutingAllocation(
        ClusterState state,
        WriteLoadConstraintDecider decider,
        String nodeId,
        float utilization,
        long latency
    ) {
        ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(
                Map.of(
                    nodeId,
                    new NodeUsageStatsForThreadPools(
                        nodeId,
                        Map.of(ThreadPool.Names.WRITE, new ThreadPoolUsageStats(randomIntBetween(1, 10), utilization, latency))
                    )
                )
            )
            .build();

        var routingAllocation = TestRoutingAllocationFactory.forClusterState(state)
            .allocationDeciders(decider)
            .clusterInfo(clusterInfo)
            .mutable();
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);
        return routingAllocation;
    }

    /**
     * Carries all the cluster state objects needed for testing after {@link #createClusterStateAndRoutingAllocation} sets them up.
     */
    private record TestHarness(
        ClusterState clusterState,
        RoutingAllocation routingAllocation,
        RoutingNode exceedingThresholdRoutingNode,
        RoutingNode belowThresholdRoutingNode,
        RoutingNode nearThresholdRoutingNode,
        RoutingNode belowQueuingThresholdRoutingNode,
        RoutingNode aboveQueuingThresholdRoutingNode,
        RoutingNode aboveQueueingThresholdWithLowUtilisationNode,
        ShardRouting shardRoutingOnNodeExceedingUtilThreshold,
        ShardRouting shardRoutingOnNodeBelowUtilThreshold,
        ShardRouting shardRoutingNoWriteLoad,
        ShardRouting shardRoutingOnNodeBelowQueueThreshold,
        ShardRouting shardRoutingOnNodeAboveQueueThreshold,
        ShardRouting unassignedShardRouting
    ) {}

    /**
     * Create settings with write load decider enabled, and the given configurations set if they're not null
     */
    private static Settings createSettings(
        @Nullable Float allocationUtilizationThreshold,
        @Nullable Float hotspotUtilizationThreshold,
        @Nullable TimeValue highLatencyThreshold,
        @Nullable Integer maxShardWriteLoadProportion
    ) {
        Settings.Builder builder = Settings.builder()
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            );
        if (allocationUtilizationThreshold != null) {
            builder.put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ALLOCATION_UTILIZATION_THRESHOLD_SETTING.getKey(),
                allocationUtilizationThreshold
            );
        }
        if (hotspotUtilizationThreshold != null) {
            builder.put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_HOTSPOT_UTILIZATION_THRESHOLD_SETTING.getKey(),
                hotspotUtilizationThreshold
            );
        }
        if (maxShardWriteLoadProportion != null) {
            builder.put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_HOTSPOT_MAX_SHARD_WRITE_LOAD_PROPORTION_THRESHOLD_SETTING.getKey(),
                maxShardWriteLoadProportion + "%"
            );
        }
        if (highLatencyThreshold != null) {
            builder.put(WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_QUEUE_LATENCY_THRESHOLD_SETTING.getKey(), highLatencyThreshold);
        }
        return builder.build();
    }

    /**
     * Creates all the cluster state and objects needed to test the {@link WriteLoadConstraintDecider}.
     */
    private TestHarness createClusterStateAndRoutingAllocation(String indexName) {
        /**
         * Create the ClusterState for multiple nodes and multiple index shards.
         */

        int numberOfShards = 6;
        ClusterState clusterState = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(
            new String[] { indexName },
            numberOfShards,
            4
        );
        // The number of data nodes the util method above creates is numberOfReplicas+2, and six data nodes are needed for this test.
        assertEquals(6, clusterState.nodes().size());
        assertEquals(1, clusterState.metadata().getTotalNumberOfIndices());

        /**
         * Fetch references to the nodes and index shards from the generated ClusterState, so the ClusterInfo can be created from them.
         */

        var discoveryNodeIterator = clusterState.nodes().iterator();
        assertTrue(discoveryNodeIterator.hasNext());
        var exceedingThresholdDiscoveryNode = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var belowThresholdDiscoveryNode2 = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var nearThresholdDiscoveryNode3 = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var queuingBelowThresholdDiscoveryNode4 = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var queuingAboveThresholdDiscoveryNode5 = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var belowThresholdWithHighQueueLatencyNode6 = discoveryNodeIterator.next();
        assertFalse(discoveryNodeIterator.hasNext());

        var indexIterator = clusterState.metadata().indicesAllProjects().iterator();
        assertTrue(indexIterator.hasNext());
        IndexMetadata testIndexMetadata = indexIterator.next();
        assertFalse(indexIterator.hasNext());
        Index testIndex = testIndexMetadata.getIndex();
        assertEquals(numberOfShards, testIndexMetadata.getNumberOfShards());
        ShardId testShardId1 = new ShardId(testIndex, 0);
        ShardId testShardId2 = new ShardId(testIndex, 1);
        ShardId testShardId3NoWriteLoad = new ShardId(testIndex, 2);
        ShardId testShardId4 = new ShardId(testIndex, 3);
        ShardId testShardId5 = new ShardId(testIndex, 4);
        ShardId testShardId6Unassigned = new ShardId(testIndex, 5);

        /**
         * Create a ClusterInfo that includes the node and shard level write load estimates for a variety of node capacity situations.
         */

        var nodeThreadPoolStatsWithWriteExceedingThreshold = createNodeUsageStatsForThreadPools(
            exceedingThresholdDiscoveryNode,
            8,
            0.99f,
            0
        );
        var nodeThreadPoolStatsWithWriteBelowThreshold = createNodeUsageStatsForThreadPools(belowThresholdDiscoveryNode2, 8, 0.50f, 0);
        var nodeThreadPoolStatsWithWriteNearThreshold = createNodeUsageStatsForThreadPools(nearThresholdDiscoveryNode3, 8, 0.89f, 0);
        var nodeThreadPoolStatsWithQueuingBelowThreshold = createNodeUsageStatsForThreadPools(
            exceedingThresholdDiscoveryNode,
            8,
            0.99f,
            5_000
        );
        var nodeThreadPoolStatsWithQueuingAboveThreshold = createNodeUsageStatsForThreadPools(
            exceedingThresholdDiscoveryNode,
            8,
            0.99f,
            15_000
        );
        var nodeThreadPoolStatsWithLowUtilisationAndHighQueueing = createNodeUsageStatsForThreadPools(
            belowThresholdDiscoveryNode2,
            8,
            0.50f,
            15_000
        );

        // Create a map of usage per node.
        var nodeIdToNodeUsageStatsForThreadPools = new HashMap<String, NodeUsageStatsForThreadPools>();
        nodeIdToNodeUsageStatsForThreadPools.put(exceedingThresholdDiscoveryNode.getId(), nodeThreadPoolStatsWithWriteExceedingThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(belowThresholdDiscoveryNode2.getId(), nodeThreadPoolStatsWithWriteBelowThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(nearThresholdDiscoveryNode3.getId(), nodeThreadPoolStatsWithWriteNearThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(queuingBelowThresholdDiscoveryNode4.getId(), nodeThreadPoolStatsWithQueuingBelowThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(queuingAboveThresholdDiscoveryNode5.getId(), nodeThreadPoolStatsWithQueuingAboveThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(
            belowThresholdWithHighQueueLatencyNode6.getId(),
            nodeThreadPoolStatsWithLowUtilisationAndHighQueueing
        );

        // create a set of hotspots
        var nodeIdsWriteLoadHotspotting = Set.of(
            queuingAboveThresholdDiscoveryNode5.getId(),
            belowThresholdWithHighQueueLatencyNode6.getId()
        );

        // Create a map of usage per shard.
        var shardIdToWriteLoadEstimate = new HashMap<ShardId, Double>();
        shardIdToWriteLoadEstimate.put(testShardId1, 0.5);
        shardIdToWriteLoadEstimate.put(testShardId2, 0.5);
        shardIdToWriteLoadEstimate.put(testShardId3NoWriteLoad, 0d);
        shardIdToWriteLoadEstimate.put(testShardId4, 0.5);
        shardIdToWriteLoadEstimate.put(testShardId5, 0.5d);
        if (randomBoolean()) {
            shardIdToWriteLoadEstimate.put(testShardId6Unassigned, randomDoubleBetween(0.0, 2.0, true));
        }

        ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(nodeIdToNodeUsageStatsForThreadPools)
            .shardWriteLoads(shardIdToWriteLoadEstimate)
            .nodeIdsWriteLoadHotspotting(nodeIdsWriteLoadHotspotting)
            .build();

        /**
         * Create the RoutingAllocation from the ClusterState and ClusterInfo above, and set up the other input for the WriteLoadDecider.
         */

        var routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).clusterInfo(clusterInfo).build();

        ShardRouting shardRoutingOnNodeExceedingUtilThreshold = TestShardRouting.newShardRouting(
            testShardId1,
            exceedingThresholdDiscoveryNode.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting shardRoutingOnNodeBelowUtilThreshold = TestShardRouting.newShardRouting(
            testShardId2,
            belowThresholdDiscoveryNode2.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting shardRoutingNoWriteLoad = TestShardRouting.newShardRouting(
            testShardId3NoWriteLoad,
            belowThresholdDiscoveryNode2.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting shardRoutingOnNodeBelowQueueThreshold = TestShardRouting.newShardRouting(
            testShardId4,
            queuingBelowThresholdDiscoveryNode4.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting shardRoutingOnNodeAboveQueueThreshold = TestShardRouting.newShardRouting(
            testShardId5,
            queuingAboveThresholdDiscoveryNode5.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting unassignedShardRouting = TestShardRouting.newShardRouting(
            testShardId6Unassigned,
            null,
            true,
            ShardRoutingState.UNASSIGNED
        );

        RoutingNode exceedingThresholdRoutingNode = RoutingNodesHelper.routingNode(
            exceedingThresholdDiscoveryNode.getId(),
            exceedingThresholdDiscoveryNode,
            shardRoutingOnNodeExceedingUtilThreshold
        );
        RoutingNode belowThresholdRoutingNode = RoutingNodesHelper.routingNode(
            belowThresholdDiscoveryNode2.getId(),
            belowThresholdDiscoveryNode2,
            shardRoutingOnNodeBelowUtilThreshold
        );
        RoutingNode nearThresholdRoutingNode = RoutingNodesHelper.routingNode(
            nearThresholdDiscoveryNode3.getId(),
            nearThresholdDiscoveryNode3
        );
        RoutingNode belowQueuingThresholdRoutingNode = RoutingNodesHelper.routingNode(
            queuingBelowThresholdDiscoveryNode4.getId(),
            queuingBelowThresholdDiscoveryNode4,
            shardRoutingOnNodeBelowQueueThreshold
        );
        RoutingNode aboveQueuingThresholdRoutingNode = RoutingNodesHelper.routingNode(
            queuingAboveThresholdDiscoveryNode5.getId(),
            queuingAboveThresholdDiscoveryNode5,
            shardRoutingOnNodeAboveQueueThreshold,
            shardRoutingOnNodeBelowQueueThreshold
        );
        RoutingNode belowUtilisationThresholdButHighQueueLatencyNode = RoutingNodesHelper.routingNode(
            belowThresholdWithHighQueueLatencyNode6.getId(),
            belowThresholdWithHighQueueLatencyNode6
        );

        return new TestHarness(
            clusterState,
            routingAllocation,
            exceedingThresholdRoutingNode,
            belowThresholdRoutingNode,
            nearThresholdRoutingNode,
            belowQueuingThresholdRoutingNode,
            aboveQueuingThresholdRoutingNode,
            belowUtilisationThresholdButHighQueueLatencyNode,
            shardRoutingOnNodeExceedingUtilThreshold,
            shardRoutingOnNodeBelowUtilThreshold,
            shardRoutingNoWriteLoad,
            shardRoutingOnNodeBelowQueueThreshold,
            shardRoutingOnNodeAboveQueueThreshold,
            unassignedShardRouting
        );
    }

    private WriteLoadConstraintDecider createWriteLoadConstraintDecider(Settings settings) {
        return new WriteLoadConstraintDecider(createBuiltInClusterSettings(settings));
    }

    /**
     * Helper to create a {@link NodeUsageStatsForThreadPools} for the given node with the given WRITE thread pool usage stats.
     */
    private NodeUsageStatsForThreadPools createNodeUsageStatsForThreadPools(
        DiscoveryNode discoveryNode,
        int totalWriteThreadPoolThreads,
        float averageWriteThreadPoolUtilization,
        long averageWriteThreadPoolQueueLatencyMillis
    ) {

        // Create thread pool usage stats map for node1.
        var writeThreadPoolUsageStats = new ThreadPoolUsageStats(
            totalWriteThreadPoolThreads,
            averageWriteThreadPoolUtilization,
            averageWriteThreadPoolQueueLatencyMillis
        );
        var threadPoolUsageMap = new HashMap<String, ThreadPoolUsageStats>();
        threadPoolUsageMap.put(ThreadPool.Names.WRITE, writeThreadPoolUsageStats);

        // Create the node's thread pool usage map
        return new NodeUsageStatsForThreadPools(discoveryNode.getId(), threadPoolUsageMap);
    }
}
