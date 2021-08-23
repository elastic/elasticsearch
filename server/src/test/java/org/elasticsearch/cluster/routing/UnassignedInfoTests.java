/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UnassignedInfoTests extends ESAllocationTestCase {

    public void testReasonOrdinalOrder() {
        UnassignedInfo.Reason[] order = new UnassignedInfo.Reason[]{
                UnassignedInfo.Reason.INDEX_CREATED,
                UnassignedInfo.Reason.CLUSTER_RECOVERED,
                UnassignedInfo.Reason.INDEX_REOPENED,
                UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED,
                UnassignedInfo.Reason.NEW_INDEX_RESTORED,
                UnassignedInfo.Reason.EXISTING_INDEX_RESTORED,
                UnassignedInfo.Reason.REPLICA_ADDED,
                UnassignedInfo.Reason.ALLOCATION_FAILED,
                UnassignedInfo.Reason.NODE_LEFT,
                UnassignedInfo.Reason.REROUTE_CANCELLED,
                UnassignedInfo.Reason.REINITIALIZED,
                UnassignedInfo.Reason.REALLOCATED_REPLICA,
                UnassignedInfo.Reason.PRIMARY_FAILED,
                UnassignedInfo.Reason.FORCED_EMPTY_PRIMARY,
                UnassignedInfo.Reason.MANUAL_ALLOCATION,
                UnassignedInfo.Reason.INDEX_CLOSED,
                UnassignedInfo.Reason.NODE_RESTARTING};
        for (int i = 0; i < order.length; i++) {
            assertThat(order[i].ordinal(), equalTo(i));
        }
        assertThat(UnassignedInfo.Reason.values().length, equalTo(order.length));
    }

    public void testSerialization() throws Exception {
        UnassignedInfo.Reason reason = RandomPicks.randomFrom(random(), UnassignedInfo.Reason.values());
        int failedAllocations = randomIntBetween(1, 100);
        Set<String> failedNodes = IntStream.range(0, between(0, failedAllocations))
            .mapToObj(n -> "failed-node-" + n).collect(Collectors.toSet());

        UnassignedInfo meta;
        if (reason == UnassignedInfo.Reason.ALLOCATION_FAILED) {
            meta = new UnassignedInfo(
                reason,
                randomBoolean() ? randomAlphaOfLength(4) : null,
                null,
                failedAllocations,
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                AllocationStatus.NO_ATTEMPT,
                failedNodes,
                null);
        } else if (reason == UnassignedInfo.Reason.NODE_LEFT || reason == UnassignedInfo.Reason.NODE_RESTARTING) {
            String lastAssignedNodeId = randomAlphaOfLength(10);
            if (reason == UnassignedInfo.Reason.NODE_LEFT && randomBoolean()) {
                // If the reason is `NODE_LEFT`, sometimes we'll have an empty lastAllocatedNodeId due to BWC
                lastAssignedNodeId = null;
            }
            meta = new UnassignedInfo(
                reason,
                randomBoolean() ? randomAlphaOfLength(4) : null,
                null,
                0,
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                AllocationStatus.NO_ATTEMPT,
                Collections.emptySet(),
                lastAssignedNodeId
            );
        } else {
            meta = new UnassignedInfo(reason, randomBoolean() ? randomAlphaOfLength(4) : null);
        }
        BytesStreamOutput out = new BytesStreamOutput();
        meta.writeTo(out);
        out.close();

        UnassignedInfo read = new UnassignedInfo(out.bytes().streamInput());
        assertThat(read.getReason(), equalTo(meta.getReason()));
        assertThat(read.getUnassignedTimeInMillis(), equalTo(meta.getUnassignedTimeInMillis()));
        assertThat(read.getMessage(), equalTo(meta.getMessage()));
        assertThat(read.getDetails(), equalTo(meta.getDetails()));
        assertThat(read.getNumFailedAllocations(), equalTo(meta.getNumFailedAllocations()));
        assertThat(read.getFailedNodeIds(), equalTo(meta.getFailedNodeIds()));
        assertThat(read.getLastAllocatedNodeId(), equalTo(meta.getLastAllocatedNodeId()));
    }

    public void testBwcSerialization() throws Exception {
        final UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CLOSED, "message");
        BytesStreamOutput out = new BytesStreamOutput();
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.CURRENT);
        out.setVersion(version);
        unassignedInfo.writeTo(out);
        out.close();

        StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        UnassignedInfo read = new UnassignedInfo(in);
        if (version.before(Version.V_7_0_0)) {
            assertThat(read.getReason(), equalTo(UnassignedInfo.Reason.REINITIALIZED));
        } else {
            assertThat(read.getReason(), equalTo(UnassignedInfo.Reason.INDEX_CLOSED));
        }
        assertThat(read.getUnassignedTimeInMillis(), equalTo(unassignedInfo.getUnassignedTimeInMillis()));
        assertThat(read.getMessage(), equalTo(unassignedInfo.getMessage()));
        assertThat(read.getDetails(), equalTo(unassignedInfo.getDetails()));
        assertThat(read.getNumFailedAllocations(), equalTo(unassignedInfo.getNumFailedAllocations()));
    }

    public void testIndexCreated() {
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT))
                    .numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.INDEX_CREATED));
        }
    }

    public void testClusterRecovered() {
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT))
                    .numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsRecovery(metadata.index("test")).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.CLUSTER_RECOVERED));
        }
    }

    public void testIndexReopened() {
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT))
                    .numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsFromCloseToOpen(metadata.index("test")).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.INDEX_REOPENED));
        }
    }

    public void testNewIndexRestored() {
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT))
                    .numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsNewRestore(metadata.index("test"), new SnapshotRecoverySource(
                    UUIDs.randomBase64UUID(),
                    new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())), Version.CURRENT,
                        new IndexId("test", UUIDs.randomBase64UUID(random()))),
                    new IntHashSet()).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.NEW_INDEX_RESTORED));
        }
    }

    public void testExistingIndexRestored() {
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT))
                    .numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsRestore(metadata.index("test"),
                    new SnapshotRecoverySource(
                        UUIDs.randomBase64UUID(), new Snapshot("rep1",
                        new SnapshotId("snp1", UUIDs.randomBase64UUID())), Version.CURRENT,
                        new IndexId("test", UUIDs.randomBase64UUID(random())))).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED));
        }
    }

    public void testDanglingIndexImported() {
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT))
                    .numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsFromDangling(metadata.index("test")).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED));
        }
    }

    public void testReplicaAdded() {
        AllocationService allocation = createAllocationService();
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();
        final Index index = metadata.index("test").getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsNew(metadata.index(index)).build()).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        for (IndexShardRoutingTable indexShardRoutingTable : clusterState.routingTable().index(index)) {
            builder.addIndexShard(indexShardRoutingTable);
        }
        builder.addReplica();
        clusterState = ClusterState.builder(clusterState)
            .routingTable(RoutingTable.builder(clusterState.routingTable()).add(builder).build()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo(), notNullValue());
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getReason(),
            equalTo(UnassignedInfo.Reason.REPLICA_ADDED));
    }

    /**
     * The unassigned meta is kept when a shard goes to INITIALIZING, but cleared when it moves to STARTED.
     */
    public void testStateTransitionMetaHandling() {
        ShardRouting shard = TestShardRouting.newShardRouting("test", 1, null, null,
            true, ShardRoutingState.UNASSIGNED, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        assertThat(shard.unassignedInfo(), notNullValue());
        shard = shard.initialize("test_node", null, -1);
        assertThat(shard.state(), equalTo(ShardRoutingState.INITIALIZING));
        assertThat(shard.unassignedInfo(), notNullValue());
        shard = shard.moveToStarted();
        assertThat(shard.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(shard.unassignedInfo(), nullValue());
    }

    /**
     * Tests that during reroute when a node is detected as leaving the cluster, the right unassigned meta is set
     */
    public void testNodeLeave() {
        AllocationService allocation = createAllocationService();
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build()).build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");
        // verify that NODE_LEAVE is the reason for meta
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(true));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo(), notNullValue());
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getReason(),
            equalTo(UnassignedInfo.Reason.NODE_LEFT));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getUnassignedTimeInMillis(),
            greaterThan(0L));
    }

    /**
     * Verifies that when a shard fails, reason is properly set and details are preserved.
     */
    public void testFailedShard() {
        AllocationService allocation = createAllocationService();
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build()).build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // fail shard
        ShardRouting shardToFail = clusterState.getRoutingNodes().shardsWithState(STARTED).get(0);
        clusterState = allocation.applyFailedShards(clusterState,
            Collections.singletonList(new FailedShard(shardToFail, "test fail", null, randomBoolean())));
        // verify the reason and details
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(true));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo(), notNullValue());
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getReason(),
            equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getMessage(),
            equalTo("failed shard on node [" + shardToFail.currentNodeId() + "]: test fail"));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getDetails(),
            equalTo("failed shard on node [" + shardToFail.currentNodeId() + "]: test fail"));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getUnassignedTimeInMillis(),
            greaterThan(0L));
    }

    /**
     * Verifies that delayed allocation calculation are correct when there are no registered node shutdowns.
     */
    public void testRemainingDelayCalculationWithNoShutdowns() throws Exception {
        checkRemainingDelayCalculation(
            "bogusNodeId",
            TimeValue.timeValueNanos(10),
            Collections.emptyMap(),
            TimeValue.timeValueNanos(10),
            false
        );
    }

    /**
     * Verifies that delayed allocation calculations are correct when there are registered node shutdowns for nodes which are not relevant
     * to the shard currently being evaluated.
     */
    public void testRemainingDelayCalculationsWithUnrelatedShutdowns() throws Exception {
        String lastNodeId = "bogusNodeId";
        Map<String, SingleNodeShutdownMetadata> shutdowns = new HashMap<>();
        int numberOfShutdowns = randomIntBetween(1,15);
        for (int i = 0; i <= numberOfShutdowns; i++) {
            SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
                .setNodeId(randomValueOtherThan(lastNodeId, () -> randomAlphaOfLengthBetween(5,10)))
                .setReason(this.getTestName())
                .setStartedAtMillis(randomNonNegativeLong())
                .setType(randomFrom(EnumSet.allOf(SingleNodeShutdownMetadata.Type.class)))
                .build();
            shutdowns.put(shutdown.getNodeId(), shutdown);
        }
        checkRemainingDelayCalculation(lastNodeId, TimeValue.timeValueNanos(10), shutdowns, TimeValue.timeValueNanos(10), false);
    }

    /**
     * Verifies that delay calculation is not impacted when the node the shard was last assigned to was registered for removal.
     */
    public void testRemainingDelayCalculationWhenNodeIsShuttingDownForRemoval() throws Exception {
        String lastNodeId = "bogusNodeId";
        Map<String, SingleNodeShutdownMetadata> shutdowns = new HashMap<>();
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(lastNodeId)
            .setReason(this.getTestName())
            .setStartedAtMillis(randomNonNegativeLong())
            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
            .build();
        shutdowns.put(shutdown.getNodeId(), shutdown);

        checkRemainingDelayCalculation(lastNodeId, TimeValue.timeValueNanos(10), shutdowns, TimeValue.timeValueNanos(10), false);
    }

    /**
     * Verifies that the delay calculation uses the configured delay value for nodes known to be restarting, because they are registered for
     * a `RESTART`-type shutdown, rather than the default global delay.
     */
    public void testRemainingDelayCalculationWhenNodeIsKnownToBeRestartingWithCustomDelay() throws Exception {
        String lastNodeId = "bogusNodeId";
        Map<String, SingleNodeShutdownMetadata> shutdowns = new HashMap<>();
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(lastNodeId)
            .setReason(this.getTestName())
            .setStartedAtMillis(randomNonNegativeLong())
            .setType(SingleNodeShutdownMetadata.Type.RESTART)
            .setAllocationDelay(TimeValue.timeValueMinutes(1))
            .build();
        shutdowns.put(shutdown.getNodeId(), shutdown);

        // Use a different index-level delay so this test will fail if that one gets used instead of the one from the shutdown metadata
        checkRemainingDelayCalculation(lastNodeId, TimeValue.timeValueNanos(10), shutdowns, TimeValue.timeValueMinutes(1), true);
    }

    /**
     * Verifies that the delay calculation uses the default delay value for nodes known to be restarting, because they are registered for
     * a `RESTART`-type shutdown, rather than the default global delay.
     */
    public void testRemainingDelayCalculationWhenNodeIsKnownToBeRestartingWithDefaultDelay() throws Exception {
        String lastNodeId = "bogusNodeId";
        Map<String, SingleNodeShutdownMetadata> shutdowns = new HashMap<>();

        // Note that we do not explicitly configure the reallocation delay here.
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(lastNodeId)
            .setReason(this.getTestName())
            .setStartedAtMillis(randomNonNegativeLong())
            .setType(SingleNodeShutdownMetadata.Type.RESTART)
            .build();
        shutdowns.put(shutdown.getNodeId(), shutdown);

        // Use a different index-level delay so this test will fail if that one gets used instead of the one from the shutdown metadata
        checkRemainingDelayCalculation(
            lastNodeId,
            TimeValue.timeValueNanos(10),
            shutdowns,
            SingleNodeShutdownMetadata.DEFAULT_RESTART_SHARD_ALLOCATION_DELAY,
            true
        );
    }

    public void testRemainingDelayUsesIndexLevelDelayIfNodeWasNotRestartingWhenShardBecameUnassigned() throws Exception {
        String lastNodeId = "bogusNodeId";
        Map<String, SingleNodeShutdownMetadata> shutdowns = new HashMap<>();

        // Generate a random time value - but don't use nanos as extremely small values of nanos can break assertion calculations
        final TimeValue shutdownDelay = TimeValue.parseTimeValue(
            randomTimeValue(100, 1000, "d", "h", "ms", "s", "m", "micros"),
            this.getTestName()
        );
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(lastNodeId)
            .setReason(this.getTestName())
            .setStartedAtMillis(randomNonNegativeLong())
            .setType(SingleNodeShutdownMetadata.Type.RESTART)
            .setAllocationDelay(shutdownDelay)
            .build();
        shutdowns.put(shutdown.getNodeId(), shutdown);

        // We want an index level delay that's less than the shutdown delay to avoid picking the index-level delay because it's larger
        final TimeValue indexLevelDelay = randomValueOtherThanMany(
            tv -> shutdownDelay.compareTo(tv) < 0,
            () -> TimeValue.parseTimeValue(randomTimeValue(1, 1000, "d", "h", "ms", "s", "m", "micros"), this.getTestName())
        );

        logger.info("index level delay: {}, shutdown delay: {}", indexLevelDelay, shutdownDelay);
        checkRemainingDelayCalculation(
            lastNodeId,
            indexLevelDelay,
            shutdowns,
            indexLevelDelay,
            false
        );
    }

    private void checkRemainingDelayCalculation(
        String lastNodeId,
        TimeValue indexLevelTimeoutSetting,
        Map<String, SingleNodeShutdownMetadata> nodeShutdowns,
        TimeValue expectedTotalDelay,
        boolean nodeRestarting
    ) throws Exception {
        final long baseTime = System.nanoTime();
        UnassignedInfo unassignedInfo = new UnassignedInfo(
            nodeRestarting ? UnassignedInfo.Reason.NODE_RESTARTING : UnassignedInfo.Reason.NODE_LEFT,
            "test",
            null,
            0,
            baseTime,
            System.currentTimeMillis(),
            randomBoolean(),
            AllocationStatus.NO_ATTEMPT,
            Collections.emptySet(),
            lastNodeId
        );
        final long totalDelayNanos = expectedTotalDelay.nanos();
        final Settings indexSettings = Settings.builder()
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), indexLevelTimeoutSetting)
            .build();
        long delay = unassignedInfo.getRemainingDelay(baseTime, indexSettings, nodeShutdowns);
        assertThat(delay, equalTo(totalDelayNanos));
        long delta1 = randomLongBetween(1, (totalDelayNanos - 1));
        delay = unassignedInfo.getRemainingDelay(baseTime + delta1, indexSettings, nodeShutdowns);
        assertThat(delay, equalTo(totalDelayNanos - delta1));
        delay = unassignedInfo.getRemainingDelay(baseTime + totalDelayNanos, indexSettings, nodeShutdowns);
        assertThat(delay, equalTo(0L));
        delay = unassignedInfo.getRemainingDelay(baseTime + totalDelayNanos + randomIntBetween(1, 20), indexSettings, nodeShutdowns);
        assertThat(delay, equalTo(0L));
    }


    public void testNumberOfDelayedUnassigned() throws Exception {
        MockAllocationService allocation = createAllocationService(Settings.EMPTY, new DelayedShardsMockGatewayAllocator());
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsNew(metadata.index("test1")).addAsNew(metadata.index("test2")).build()).build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");
        assertThat(UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), equalTo(0));
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        // make sure both replicas are marked as delayed (i.e. not reallocated)
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");
        assertThat(clusterState.toString(), UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), equalTo(2));
    }

    public void testFindNextDelayedAllocation() {
        MockAllocationService allocation = createAllocationService(Settings.EMPTY, new DelayedShardsMockGatewayAllocator());
        final TimeValue delayTest1 = TimeValue.timeValueMillis(randomIntBetween(1, 200));
        final TimeValue delayTest2 = TimeValue.timeValueMillis(randomIntBetween(1, 200));
        final long expectMinDelaySettingsNanos = Math.min(delayTest1.nanos(), delayTest2.nanos());

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT).put(
                    UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayTest1)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT).put(
                    UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayTest2)).numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsNew(metadata.index("test1")).addAsNew(metadata.index("test2")).build()).build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");
        assertThat(UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), equalTo(0));
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        final long baseTime = System.nanoTime();
        allocation.setNanoTimeOverride(baseTime);
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        final long delta = randomBoolean() ? 0 : randomInt((int) expectMinDelaySettingsNanos - 1);

        if (delta > 0) {
            allocation.setNanoTimeOverride(baseTime + delta);
            clusterState = allocation.reroute(clusterState, "time moved");
        }

        assertThat(UnassignedInfo.findNextDelayedAllocation(baseTime + delta, clusterState),
            equalTo(expectMinDelaySettingsNanos - delta));
    }

    public void testAllocationStatusSerialization() throws IOException {
        for (AllocationStatus allocationStatus : AllocationStatus.values()) {
            BytesStreamOutput out = new BytesStreamOutput();
            allocationStatus.writeTo(out);
            ByteBufferStreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(out.bytes().toBytesRef().bytes));
            AllocationStatus readStatus = AllocationStatus.readFrom(in);
            assertThat(readStatus, equalTo(allocationStatus));
        }
    }

    public static UnassignedInfo randomUnassignedInfo(String message) {
        return randomUnassignedInfo(message, null);
    }

    /**
     * Randomly generates an UnassignedInfo.
     * @param message The message to be used.
     * @param delayed Used for the `delayed` flag if provided.
     * @return A randomly-generated UnassignedInfo with the given message and delayed value (if any)
     */
    public static UnassignedInfo randomUnassignedInfo(String message, @Nullable Boolean delayed) {
        UnassignedInfo.Reason reason = randomFrom(UnassignedInfo.Reason.values());
        String lastAllocatedNodeId = null;
        boolean delayedFlag = delayed == null ? false : delayed;
        if (reason == UnassignedInfo.Reason.NODE_LEFT || reason == UnassignedInfo.Reason.NODE_RESTARTING) {
            if (randomBoolean() && delayed == null) {
                delayedFlag = true;
            }
            lastAllocatedNodeId = randomAlphaOfLength(10);
        }
        int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;
        return new UnassignedInfo(
            reason,
            message,
            null,
            failedAllocations,
            System.nanoTime(),
            System.currentTimeMillis(),
            delayedFlag,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Collections.emptySet(),
            lastAllocatedNodeId
        );
    }
}
