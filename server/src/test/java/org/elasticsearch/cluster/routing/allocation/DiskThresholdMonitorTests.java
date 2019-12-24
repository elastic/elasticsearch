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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class DiskThresholdMonitorTests extends ESAllocationTestCase {

    public void testMarkFloodStageIndicesReadOnly() {
        AllocationService allocation = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)
                .put("index.routing.allocation.require._id", "node2")).numberOfShards(1).numberOfReplicas(0))
            .put(IndexMetaData.builder("test_1").settings(settings(Version.CURRENT)
                .put("index.routing.allocation.require._id", "node1")).numberOfShards(1).numberOfReplicas(0))
            .put(IndexMetaData.builder("test_2").settings(settings(Version.CURRENT)
                .put("index.routing.allocation.require._id", "node1")).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index("test"))
            .addAsNew(metaData.index("test_1"))
            .addAsNew(metaData.index("test_2"))
            .build();
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData).routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build(), allocation);
        AtomicBoolean reroute = new AtomicBoolean(false);
        AtomicReference<Set<String>> indices = new AtomicReference<>();
        AtomicLong currentTime = new AtomicLong();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(Settings.EMPTY, () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null, currentTime::get,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            }) {

            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indices.compareAndSet(null, indicesToMarkReadOnly));
                assertTrue(readOnly);
                listener.onResponse(null);
            }
        };

        ImmutableOpenMap.Builder<String, DiskUsage> builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1","node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2","node2", "/foo/bar", 100, 30));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertFalse(reroute.get());
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indices.get());

        indices.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1","node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2","node2", "/foo/bar", 100, 5));
        currentTime.addAndGet(randomLongBetween(60001, 120000));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertTrue(reroute.get());
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indices.get());
        IndexMetaData indexMetaData = IndexMetaData.builder(clusterState.metaData().index("test_2")).settings(Settings.builder()
            .put(clusterState.metaData()
            .index("test_2").getSettings())
            .put(IndexMetaData.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)).build();

        // now we mark one index as read-only and assert that we don't mark it as such again
        final ClusterState anotherFinalClusterState = ClusterState.builder(clusterState).metaData(MetaData.builder(clusterState.metaData())
            .put(clusterState.metaData().index("test"), false)
            .put(clusterState.metaData().index("test_1"), false)
            .put(indexMetaData, true).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetaData).build()).build();
        assertTrue(anotherFinalClusterState.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test_2"));

        monitor = new DiskThresholdMonitor(Settings.EMPTY, () -> anotherFinalClusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null, currentTime::get,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            }) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indices.compareAndSet(null, indicesToMarkReadOnly));
                assertTrue(readOnly);
                listener.onResponse(null);
            }
        };

        indices.set(null);
        reroute.set(false);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1","node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2","node2", "/foo/bar", 100, 5));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertTrue(reroute.get());
        assertEquals(Collections.singleton("test_1"), indices.get());
    }

    public void testDoesNotSubmitRerouteTaskTooFrequently() {
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        AtomicLong currentTime = new AtomicLong();
        AtomicReference<ActionListener<ClusterState>> listenerReference = new AtomicReference<>();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(Settings.EMPTY, () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null, currentTime::get,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                assertTrue(listenerReference.compareAndSet(null, listener));
            }) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                throw new AssertionError("unexpected");
            }
        };

        final ImmutableOpenMap.Builder<String, DiskUsage> allDisksOkBuilder;
        allDisksOkBuilder = ImmutableOpenMap.builder();
        allDisksOkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, 50));
        allDisksOkBuilder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, 50));
        final ImmutableOpenMap<String, DiskUsage> allDisksOk = allDisksOkBuilder.build();

        final ImmutableOpenMap.Builder<String, DiskUsage> oneDiskAboveWatermarkBuilder = ImmutableOpenMap.builder();
        oneDiskAboveWatermarkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(5, 9)));
        oneDiskAboveWatermarkBuilder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, 50));
        final ImmutableOpenMap<String, DiskUsage> oneDiskAboveWatermark = oneDiskAboveWatermarkBuilder.build();

        // should not reroute when all disks are ok
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(new ClusterInfo(allDisksOk, null, null, null));
        assertNull(listenerReference.get());

        // should reroute when one disk goes over the watermark
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(new ClusterInfo(oneDiskAboveWatermark, null, null, null));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(clusterState);

        if (randomBoolean()) {
            // should not re-route again within the reroute interval
            currentTime.addAndGet(randomLongBetween(0,
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis()));
            monitor.onNewInfo(new ClusterInfo(allDisksOk, null, null, null));
            assertNull(listenerReference.get());
        }

        // should reroute again when one disk is still over the watermark
        currentTime.addAndGet(randomLongBetween(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis() + 1, 120000));
        monitor.onNewInfo(new ClusterInfo(oneDiskAboveWatermark, null, null, null));
        assertNotNull(listenerReference.get());
        final ActionListener<ClusterState> rerouteListener1 = listenerReference.getAndSet(null);

        // should not re-route again before reroute has completed
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(new ClusterInfo(allDisksOk, null, null, null));
        assertNull(listenerReference.get());

        // complete reroute
        rerouteListener1.onResponse(clusterState);

        if (randomBoolean()) {
            // should not re-route again within the reroute interval
            currentTime.addAndGet(randomLongBetween(0,
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis()));
            monitor.onNewInfo(new ClusterInfo(allDisksOk, null, null, null));
            assertNull(listenerReference.get());
        }

        // should reroute again after the reroute interval
        currentTime.addAndGet(randomLongBetween(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis() + 1, 120000));
        monitor.onNewInfo(new ClusterInfo(allDisksOk, null, null, null));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(null);

        // should not reroute again when it is not required
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(new ClusterInfo(allDisksOk, null, null, null));
        assertNull(listenerReference.get());
    }

    public void testAutoReleaseIndices() {
        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<String>> indicesToRelease = new AtomicReference<>();
        AllocationService allocation = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test_1").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .put(IndexMetaData.builder("test_2").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index("test_1"))
            .addAsNew(metaData.index("test_2"))
            .build();
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData).routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build(), allocation);
        assertThat(clusterState.getRoutingTable().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(8));

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(Settings.EMPTY, () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null, () -> 0L,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(clusterState);
            }) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToRelease.compareAndSet(null, indicesToUpdate));
                }
                listener.onResponse(null);
            }
        };
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        ImmutableOpenMap.Builder<String, DiskUsage> builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 4)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(0, 4)));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // Change cluster state so that "test_2" index is blocked (read only)
        IndexMetaData indexMetaData = IndexMetaData.builder(clusterState.metaData().index("test_2")).settings(Settings.builder()
            .put(clusterState.metaData()
                .index("test_2").getSettings())
            .put(IndexMetaData.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)).build();

        ClusterState clusterStateWithBlocks = ClusterState.builder(clusterState).metaData(MetaData.builder(clusterState.metaData())
            .put(indexMetaData, true).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetaData).build()).build();

        assertTrue(clusterStateWithBlocks.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test_2"));
        monitor = new DiskThresholdMonitor(Settings.EMPTY, () -> clusterStateWithBlocks,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null, () -> 0L,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(clusterStateWithBlocks);
            }) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, ActionListener<Void> listener, boolean readOnly) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToRelease.compareAndSet(null, indicesToUpdate));
                }
                listener.onResponse(null);
            }
        };
        // When free disk on any of node1 or node2 goes below 5% flood watermark, then apply index block on indices not having the block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 100)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(0, 4)));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertThat(indicesToMarkReadOnly.get(), contains("test_1"));
        assertNull(indicesToRelease.get());

        // When free disk on node1 and node2 goes above 10% high watermark, then only release index block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(10, 100)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(10, 100)));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertNull(indicesToMarkReadOnly.get());
        assertThat(indicesToRelease.get(), contains("test_2"));

        // When no usage information is present for node2, we don't release the block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 4)));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertThat(indicesToMarkReadOnly.get(), contains("test_1"));
        assertNull(indicesToRelease.get());

        // When disk usage on one node is between the high and flood-stage watermarks, nothing changes
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(5, 9)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(5, 100)));
        if (randomBoolean()) {
            builder.put("node3", new DiskUsage("node3", "node3", "/foo/bar", 100, between(0, 100)));
        }
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // When disk usage on one node is missing and the other is below the high watermark, nothing changes
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(5, 100)));
        if (randomBoolean()) {
            builder.put("node3", new DiskUsage("node3", "node3", "/foo/bar", 100, between(0, 100)));
        }
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // When disk usage on one node is missing and the other is above the flood-stage watermark, affected indices are blocked
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 4)));
        if (randomBoolean()) {
            builder.put("node3", new DiskUsage("node3", "node3", "/foo/bar", 100, between(0, 100)));
        }
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertThat(indicesToMarkReadOnly.get(), contains("test_1"));
        assertNull(indicesToRelease.get());
    }

    @TestLogging(value="org.elasticsearch.cluster.routing.allocation.DiskThresholdMonitor:INFO", reason="testing INFO/WARN logging")
    public void testDiskMonitorLogging() throws IllegalAccessException {
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(clusterState);
        final AtomicBoolean advanceTime = new AtomicBoolean(randomBoolean());

        final LongSupplier timeSupplier = new LongSupplier() {
            long time;

            @Override
            public long getAsLong() {
                if (advanceTime.get()) {
                    time += DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).getMillis() + 1;
                }
                logger.info("time: [{}]", time);
                return time;
            }
        };

        final AtomicLong relocatingShardSizeRef = new AtomicLong();

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(Settings.EMPTY, clusterStateRef::get,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null, timeSupplier,
            (reason, priority, listener) -> listener.onResponse(clusterStateRef.get())) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                listener.onResponse(null);
            }

            @Override
            long sizeOfRelocatingShards(RoutingNode routingNode, DiskUsage diskUsage, ClusterInfo info, ClusterState reroutedClusterState) {
                return relocatingShardSizeRef.get();
            }
        };

        final ImmutableOpenMap.Builder<String, DiskUsage> allDisksOkBuilder;
        allDisksOkBuilder = ImmutableOpenMap.builder();
        allDisksOkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(15, 100)));
        final ImmutableOpenMap<String, DiskUsage> allDisksOk = allDisksOkBuilder.build();

        final ImmutableOpenMap.Builder<String, DiskUsage> aboveLowWatermarkBuilder = ImmutableOpenMap.builder();
        aboveLowWatermarkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(10, 14)));
        final ImmutableOpenMap<String, DiskUsage> aboveLowWatermark = aboveLowWatermarkBuilder.build();

        final ImmutableOpenMap.Builder<String, DiskUsage> aboveHighWatermarkBuilder = ImmutableOpenMap.builder();
        aboveHighWatermarkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(5, 9)));
        final ImmutableOpenMap<String, DiskUsage> aboveHighWatermark = aboveHighWatermarkBuilder.build();

        final ImmutableOpenMap.Builder<String, DiskUsage> aboveFloodStageWatermarkBuilder = ImmutableOpenMap.builder();
        aboveFloodStageWatermarkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 4)));
        final ImmutableOpenMap<String, DiskUsage> aboveFloodStageWatermark = aboveFloodStageWatermarkBuilder.build();

        assertNoLogging(monitor, allDisksOk);

        assertSingleInfoMessage(monitor, aboveLowWatermark,
            "low disk watermark [85%] exceeded on * replicas will not be assigned to this node");

        advanceTime.set(false); // will do one reroute and emit warnings, but subsequent reroutes and associated messages are delayed
        assertSingleWarningMessage(monitor, aboveHighWatermark,
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* " +
                "the node is expected to continue to exceed the high disk watermark when these relocations are complete");

        advanceTime.set(true);
        assertRepeatedWarningMessages(monitor, aboveHighWatermark,
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* " +
                "the node is expected to continue to exceed the high disk watermark when these relocations are complete");

        advanceTime.set(randomBoolean());
        assertRepeatedWarningMessages(monitor, aboveFloodStageWatermark,
            "flood stage disk watermark [95%] exceeded on * all indices on this node will be marked read-only");

        relocatingShardSizeRef.set(-5L);
        advanceTime.set(true);
        assertSingleInfoMessage(monitor, aboveHighWatermark,
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* " +
                "the node is expected to be below the high disk watermark when these relocations are complete");

        relocatingShardSizeRef.set(0L);
        timeSupplier.getAsLong(); // advance time long enough to do another reroute
        advanceTime.set(false); // will do one reroute and emit warnings, but subsequent reroutes and associated messages are delayed
        assertSingleWarningMessage(monitor, aboveHighWatermark,
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* " +
                "the node is expected to continue to exceed the high disk watermark when these relocations are complete");

        advanceTime.set(true);
        assertRepeatedWarningMessages(monitor, aboveHighWatermark,
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* " +
                "the node is expected to continue to exceed the high disk watermark when these relocations are complete");

        advanceTime.set(randomBoolean());
        assertSingleInfoMessage(monitor, aboveLowWatermark,
            "high disk watermark [90%] no longer exceeded on * but low disk watermark [85%] is still exceeded");

        advanceTime.set(true); // only log about dropping below the low disk watermark on a reroute
        assertSingleInfoMessage(monitor, allDisksOk,
            "low disk watermark [85%] no longer exceeded on *");

        advanceTime.set(randomBoolean());
        assertRepeatedWarningMessages(monitor, aboveFloodStageWatermark,
            "flood stage disk watermark [95%] exceeded on * all indices on this node will be marked read-only");

        assertSingleInfoMessage(monitor, allDisksOk,
            "low disk watermark [85%] no longer exceeded on *");

        advanceTime.set(true);
        assertRepeatedWarningMessages(monitor, aboveHighWatermark,
            "high disk watermark [90%] exceeded on * shards will be relocated away from this node* " +
                "the node is expected to continue to exceed the high disk watermark when these relocations are complete");

        assertSingleInfoMessage(monitor, allDisksOk,
            "low disk watermark [85%] no longer exceeded on *");

        assertRepeatedWarningMessages(monitor, aboveFloodStageWatermark,
            "flood stage disk watermark [95%] exceeded on * all indices on this node will be marked read-only");

        assertSingleInfoMessage(monitor, aboveLowWatermark,
            "high disk watermark [90%] no longer exceeded on * but low disk watermark [85%] is still exceeded");

    }

    private void assertNoLogging(DiskThresholdMonitor monitor,
                                 ImmutableOpenMap<String, DiskUsage> diskUsages) throws IllegalAccessException {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(new MockLogAppender.UnseenEventExpectation(
            "any INFO message",
            DiskThresholdMonitor.class.getCanonicalName(),
            Level.INFO,
            "*"));
        mockAppender.addExpectation(new MockLogAppender.UnseenEventExpectation(
            "any WARN message",
            DiskThresholdMonitor.class.getCanonicalName(),
            Level.WARN,
            "*"));

        Logger diskThresholdMonitorLogger = LogManager.getLogger(DiskThresholdMonitor.class);
        Loggers.addAppender(diskThresholdMonitorLogger, mockAppender);

        for (int i = between(1, 3); i >= 0; i--) {
            monitor.onNewInfo(new ClusterInfo(diskUsages, null, null, null));
        }

        mockAppender.assertAllExpectationsMatched();
        Loggers.removeAppender(diskThresholdMonitorLogger, mockAppender);
        mockAppender.stop();
    }

    private void assertRepeatedWarningMessages(DiskThresholdMonitor monitor,
                                               ImmutableOpenMap<String, DiskUsage> diskUsages,
                                               String message) throws IllegalAccessException {
        for (int i = between(1, 3); i >= 0; i--) {
            assertLogging(monitor, diskUsages, Level.WARN, message);
        }
    }

    private void assertSingleWarningMessage(DiskThresholdMonitor monitor,
                                            ImmutableOpenMap<String, DiskUsage> diskUsages,
                                            String message) throws IllegalAccessException {
        assertLogging(monitor, diskUsages, Level.WARN, message);
        assertNoLogging(monitor, diskUsages);
    }

    private void assertSingleInfoMessage(DiskThresholdMonitor monitor,
                                         ImmutableOpenMap<String, DiskUsage> diskUsages,
                                         String message) throws IllegalAccessException {
        assertLogging(monitor, diskUsages, Level.INFO, message);
        assertNoLogging(monitor, diskUsages);
    }

    private void assertLogging(DiskThresholdMonitor monitor,
                               ImmutableOpenMap<String, DiskUsage> diskUsages,
                               Level level,
                               String message) throws IllegalAccessException {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            "expected message",
            DiskThresholdMonitor.class.getCanonicalName(),
            level,
            message));
        mockAppender.addExpectation(new MockLogAppender.UnseenEventExpectation(
            "any message of another level",
            DiskThresholdMonitor.class.getCanonicalName(),
            level == Level.INFO ? Level.WARN : Level.INFO,
            "*"));

        Logger diskThresholdMonitorLogger = LogManager.getLogger(DiskThresholdMonitor.class);
        Loggers.addAppender(diskThresholdMonitorLogger, mockAppender);

        monitor.onNewInfo(new ClusterInfo(diskUsages, null, null, null));

        mockAppender.assertAllExpectationsMatched();
        Loggers.removeAppender(diskThresholdMonitorLogger, mockAppender);
        mockAppender.stop();
    }
}
