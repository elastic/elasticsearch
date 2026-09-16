/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeHeapEstimates;
import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/** Tests the shared decision algorithm independently of either production decider's memory model. */
public class AbstractEstimatedHeapAllocationDeciderTests extends ESAllocationTestCase {
    private static final String NODE_ID = "node";
    private final IndexMetadata indexMetadata = IndexMetadata.builder("index")
        .settings(settings(IndexVersion.current()))
        .numberOfShards(2)
        .numberOfReplicas(0)
        .build();
    private final ShardRouting shard = TestShardRouting.newShardRouting(
        new ShardId(indexMetadata.getIndex(), 0),
        null,
        true,
        ShardRoutingState.UNASSIGNED
    );

    public void testDisabled() {
        final var decider = new TestHeapDecider();
        decider.enabled = false;
        assertDecisions(decider, node(false), clusterInfo(), Decision.Type.YES, Decision.Type.YES, "allocation decider is disabled");
    }

    public void testInapplicableRole() {
        final var node = RoutingNodesHelper.routingNode(NODE_ID, newNode(NODE_ID, Set.of(DiscoveryNodeRole.SEARCH_ROLE)));
        assertDecisions(new TestHeapDecider(), node, clusterInfo(), Decision.Type.YES, Decision.Type.YES, "applicable only to index nodes");
    }

    public void testMissingNodeMetrics() {
        assertDecisions(
            new TestHeapDecider(),
            node(false),
            ClusterInfo.EMPTY,
            Decision.Type.YES,
            Decision.Type.YES,
            "no estimated heap estimation"
        );
    }

    public void testMinimumHeapSize() {
        final var settings = new ClusterSettings(
            Settings.builder().put(AbstractEstimatedHeapAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), "2gb").build(),
            Set.of(
                AbstractEstimatedHeapAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT,
                AbstractEstimatedHeapAllocationDecider.MINIMUM_LOGGING_INTERVAL
            )
        );
        final var decider = new TestHeapDecider(settings);
        assertDecisions(decider, node(false), clusterInfo(), Decision.Type.YES, Decision.Type.YES, "heap size is below");
        settings.applySettings(
            Settings.builder().put(AbstractEstimatedHeapAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), "1gb").build()
        );
        // At the minimum heap size the guard no longer bypasses the watermark checks.
        assertDecisions(decider, node(false), clusterInfo(), Decision.Type.NO, Decision.Type.NO, "insufficient test heap");
    }

    public void testMissingCapacity() {
        final var decider = new TestHeapDecider();
        decider.capacity = null;
        assertDecisions(decider, node(false), clusterInfo(), Decision.Type.YES, Decision.Type.YES, "no test heap capacity data");
    }

    public void testWatermarkBoundaries() {
        final var decider = new TestHeapDecider();
        for (long usage : new long[] { 840, 850, 860, 900, 910 }) {
            decider.usage = usage;
            assertDecisions(
                decider,
                node(false),
                clusterInfo(),
                usage > 850 ? Decision.Type.NO : Decision.Type.YES,
                usage > 900 ? Decision.Type.NO : Decision.Type.YES,
                "test heap"
            );
        }
    }

    public void testHighWatermarkDisabled() {
        final var decider = new TestHeapDecider();
        decider.highWatermarkEnabled = false;
        final var allocation = allocation(decider, clusterInfo());
        assertThat(decider.canAllocate(shard, node(false), allocation).type(), equalTo(Decision.Type.NO));
        final var decision = decider.canRemain(indexMetadata, shard, node(false), allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), containsString("can remain disabled"));
    }

    public void testMissingShardMetricsUsesCurrentUsage() {
        final var decider = new TestHeapDecider();
        for (long usage : new long[] { 800, 950 }) {
            decider.usage = usage;
            assertDecisions(
                decider,
                node(false),
                clusterInfo(),
                usage == 800 ? Decision.Type.YES : Decision.Type.NO,
                usage == 800 ? Decision.Type.YES : Decision.Type.NO,
                "test heap"
            );
        }
    }

    public void testProjectedUsageAtAndAroundLowWatermark() {
        final var decider = new TestHeapDecider();
        decider.usage = 800;
        for (long shardBytes : new long[] { 49, 50, 51 }) {
            final var allocation = allocation(decider, clusterInfo(shardBytes, 0));
            final var decision = decider.canAllocate(shard, node(false), allocation);
            assertThat(decision.toString(), decision.type(), equalTo(shardBytes > 50 ? Decision.Type.NO : Decision.Type.YES));
            assertThat(decider.canRemain(indexMetadata, shard, node(false), allocation).type(), equalTo(Decision.Type.YES));
        }
    }

    public void testFirstShardIncludesIndexOverhead() {
        assertIndexOverhead(false);
    }

    public void testExistingIndexOverheadIsNotCountedAgain() {
        assertIndexOverhead(true);
    }

    private void assertIndexOverhead(boolean alreadyHostsIndex) {
        final var decider = new TestHeapDecider();
        decider.usage = 800;
        // Shard cost alone reaches 820; adding index overhead reaches 880, above the 850 low watermark.
        final var allocation = allocation(decider, clusterInfo(20, 60));
        final var node = node(alreadyHostsIndex);
        assertEquals(alreadyHostsIndex, node.hasIndex(shard.index()));
        final var decision = decider.canAllocate(shard, node, allocation);
        assertThat(decision.toString(), decision.type(), equalTo(alreadyHostsIndex ? Decision.Type.YES : Decision.Type.NO));
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.stateless.allocation.AbstractEstimatedHeapAllocationDeciderTests$TestHeapDecider:DEBUG",
        reason = "verify shared allocation and can-remain logging"
    )
    public void testRejectionLogging() {
        final var decider = new TestHeapDecider();
        final var allocation = allocation(decider, clusterInfo(60, 0));
        allocation.debugDecision(false);
        // Nested-class loggers use the binary name, whereas MockLog.capture(Class) uses the canonical name.
        try (MockLog mockLog = MockLog.capture(TestHeapDecider.class.getName())) {
            for (String message : List.of(
                "*usage percentage *exceeds low watermark*",
                "*usage percentage *exceeds high watermark*",
                "*would add [60] bytes*exceeds low watermark*"
            )) {
                mockLog.addExpectation(new MockLog.SeenEventExpectation(message, TestHeapDecider.class.getName(), Level.DEBUG, message));
            }
            decider.canAllocate(shard, node(false), allocation);
            decider.canRemain(indexMetadata, shard, node(false), allocation);
            decider.usage = 800;
            decider.canAllocate(shard, node(false), allocation);
            mockLog.assertAllExpectationsMatched();
        }
    }

    private void assertDecisions(
        TestHeapDecider decider,
        RoutingNode node,
        ClusterInfo info,
        Decision.Type allocate,
        Decision.Type remain,
        String explanation
    ) {
        final var allocation = allocation(decider, info);
        for (boolean debug : new boolean[] { false, true }) {
            allocation.debugDecision(debug);
            final var allocateDecision = decider.canAllocate(shard, node, allocation);
            final var remainDecision = decider.canRemain(indexMetadata, shard, node, allocation);
            assertThat(allocateDecision.toString(), allocateDecision.type(), equalTo(allocate));
            assertThat(remainDecision.toString(), remainDecision.type(), equalTo(remain));
            if (debug) {
                assertThat(allocateDecision.label(), equalTo("test_heap"));
                assertThat(remainDecision.label(), equalTo("test_heap"));
                assertThat(allocateDecision.getExplanation(), containsString(explanation));
                assertThat(remainDecision.getExplanation(), containsString(explanation));
            }
        }
    }

    private RoutingNode node(boolean alreadyHostsIndex) {
        final Index index = alreadyHostsIndex ? shard.index() : new Index("other", "other-uuid");
        return RoutingNodesHelper.routingNode(
            NODE_ID,
            newNode(NODE_ID, Set.of(DiscoveryNodeRole.INDEX_ROLE)),
            TestShardRouting.newShardRouting(new ShardId(index, 1), NODE_ID, true, ShardRoutingState.STARTED)
        );
    }

    private RoutingAllocation allocation(TestHeapDecider decider, ClusterInfo info) {
        final var allocation = TestRoutingAllocationFactory.forClusterState(ClusterState.EMPTY_STATE)
            .allocationDeciders(new AllocationDeciders(List.of(decider)))
            .clusterInfo(info)
            .build();
        allocation.debugDecision(true);
        return allocation;
    }

    private ClusterInfo clusterInfo() {
        return clusterInfo(Map.of());
    }

    private ClusterInfo clusterInfo(long shardBytes, long indexBytes) {
        return clusterInfo(Map.of(shard.shardId(), new ShardAndIndexHeapUsage(shardBytes, indexBytes)));
    }

    private ClusterInfo clusterInfo(Map<ShardId, ShardAndIndexHeapUsage> shardUsages) {
        return ClusterInfo.builder()
            .nodeHeapMetrics(Map.of(NODE_ID, new NodeHeapMetrics(NODE_ID, ByteSizeValue.ofGb(1).getBytes(), new NodeHeapEstimates(0, 0))))
            .estimatedShardHeapUsages(shardUsages)
            .build();
    }

    /** Supplies independent inputs so these tests exercise the algorithm without either production memory model. */
    private static class TestHeapDecider extends AbstractEstimatedHeapAllocationDecider {
        private boolean enabled = true;
        private boolean highWatermarkEnabled = true;
        private Long capacity = 1000L;
        private long usage = 950;

        TestHeapDecider() {
            this(
                new ClusterSettings(
                    Settings.builder().put(MINIMUM_LOGGING_INTERVAL.getKey(), "0ms").build(),
                    Set.of(MINIMUM_HEAP_SIZE_FOR_ENABLEMENT, MINIMUM_LOGGING_INTERVAL)
                )
            );
        }

        TestHeapDecider(ClusterSettings settings) {
            super("test_heap", "test heap", Set.of(DiscoveryNodeRole.INDEX_ROLE), settings);
        }

        @Override
        protected boolean isEnabled() {
            return enabled;
        }

        @Override
        protected double getLowWatermarkPercent() {
            return 85;
        }

        @Override
        protected double getHighWatermarkPercent() {
            return 90;
        }

        @Override
        protected boolean isHighWatermarkEnabled() {
            return highWatermarkEnabled;
        }

        @Override
        protected Long resolveCapacityBytes(NodeHeapMetrics metrics, RoutingNode node, RoutingAllocation allocation) {
            return capacity;
        }

        @Override
        protected long getCurrentUsageBytes(NodeHeapMetrics metrics) {
            return usage;
        }
    }
}
