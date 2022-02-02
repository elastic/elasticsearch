/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class DiskThresholdMonitorTests extends ESAllocationTestCase {

    public void testMarkFloodStageIndicesReadOnly() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "node2"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("test_1")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "node1"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("test_2")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "node1"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("frozen")
                    .settings(settings(Version.CURRENT).put("index.routing.allocation.require._id", "frozen"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .addAsNew(metadata.index("test_1"))
            .addAsNew(metadata.index("test_2"))
            .addAsNew(metadata.index("frozen"))
            .build();
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(newNormalNode("node1")).add(newNormalNode("node2")).add(newFrozenOnlyNode("frozen")))
                .build(),
            allocation
        );
        AtomicBoolean reroute = new AtomicBoolean(false);
        AtomicReference<Set<String>> indices = new AtomicReference<>();
        AtomicLong currentTime = new AtomicLong();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            currentTime::get,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            }
        ) {

            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToMarkReadOnly, ActionListener<Void> listener, boolean readOnly) {
                assertTrue(indices.compareAndSet(null, indicesToMarkReadOnly));
                assertTrue(readOnly);
                listener.onResponse(null);
            }
        };

        ImmutableOpenMap.Builder<String, DiskUsage> builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, 30));
        builder.put("frozen", new DiskUsage("frozen", "frozen", "/foo/bar", 100, between(0, 100)));
        final ClusterInfo initialClusterInfo = clusterInfo(builder.build());
        monitor.onNewInfo(initialClusterInfo);
        assertTrue(reroute.get()); // reroute on new nodes
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indices.get());

        indices.set(null);
        reroute.set(false);
        monitor.onNewInfo(initialClusterInfo);
        assertFalse(reroute.get()); // no reroute if no change

        indices.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, 5));
        builder.put("frozen", new DiskUsage("frozen", "frozen", "/foo/bar", 100, between(0, 4)));
        currentTime.addAndGet(randomLongBetween(60000, 120000));
        monitor.onNewInfo(clusterInfo(builder.build()));
        assertTrue(reroute.get());
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indices.get());
        IndexMetadata indexMetadata = IndexMetadata.builder(clusterState.metadata().index("test_2"))
            .settings(
                Settings.builder()
                    .put(clusterState.metadata().index("test_2").getSettings())
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)
            )
            .build();

        // now we mark one index as read-only and assert that we don't mark it as such again
        final ClusterState anotherFinalClusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(clusterState.metadata().index("test"), false)
                    .put(clusterState.metadata().index("test_1"), false)
                    .put(indexMetadata, true)
                    .build()
            )
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata).build())
            .build();
        assertTrue(anotherFinalClusterState.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test_2"));

        monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> anotherFinalClusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            currentTime::get,
            (reason, priority, listener) -> {
                assertTrue(reroute.compareAndSet(false, true));
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            }
        ) {
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
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, 5));
        builder.put("frozen", new DiskUsage("frozen", "frozen", "/foo/bar", 100, between(0, 4)));
        monitor.onNewInfo(clusterInfo(builder.build()));
        assertTrue(reroute.get());
        assertEquals(Collections.singleton("test_1"), indices.get());
    }

    public void testDoesNotSubmitRerouteTaskTooFrequently() {
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(newNormalNode("node1")).add(newNormalNode("node2")))
            .build();
        AtomicLong currentTime = new AtomicLong();
        AtomicReference<ActionListener<ClusterState>> listenerReference = new AtomicReference<>();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            currentTime::get,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                assertTrue(listenerReference.compareAndSet(null, listener));
            }
        ) {
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

        // should reroute when receiving info about previously-unknown nodes
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(clusterState);

        // should not reroute when all disks are ok and no new info received
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNull(listenerReference.get());

        // might or might not reroute when a node crosses a watermark, depending on whether the reroute interval has elapsed or not
        if (randomBoolean()) {
            currentTime.addAndGet(randomLongBetween(0, 120000));
            monitor.onNewInfo(clusterInfo(oneDiskAboveWatermark));
            Optional.ofNullable(listenerReference.getAndSet(null)).ifPresent(l -> l.onResponse(clusterState));
        }

        // however once the reroute interval has elapsed then we must reroute again
        currentTime.addAndGet(
            randomLongBetween(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis(),
                120000
            )
        );
        monitor.onNewInfo(clusterInfo(oneDiskAboveWatermark));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(clusterState);

        if (randomBoolean()) {
            // should not re-route again within the reroute interval
            currentTime.addAndGet(
                randomLongBetween(
                    0,
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis() - 1
                )
            );
            monitor.onNewInfo(clusterInfo(allDisksOk));
            assertNull(listenerReference.get());
        }

        // should reroute again when one disk is still over the watermark
        currentTime.addAndGet(
            randomLongBetween(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis(),
                120000
            )
        );
        monitor.onNewInfo(clusterInfo(oneDiskAboveWatermark));
        assertNotNull(listenerReference.get());
        final ActionListener<ClusterState> rerouteListener1 = listenerReference.getAndSet(null);

        // should not re-route again before reroute has completed
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNull(listenerReference.get());

        // complete reroute
        rerouteListener1.onResponse(clusterState);

        if (randomBoolean()) {
            // should not re-route again within the reroute interval
            currentTime.addAndGet(
                randomLongBetween(
                    0,
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis() - 1
                )
            );
            monitor.onNewInfo(clusterInfo(allDisksOk));
            assertNull(listenerReference.get());
        }

        // should reroute again after the reroute interval
        currentTime.addAndGet(
            randomLongBetween(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis(),
                120000
            )
        );
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(null);

        // should not reroute again when it is not required
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNull(listenerReference.get());

        // should reroute again when one disk has reserved space that pushes it over the high watermark
        final ImmutableOpenMap.Builder<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> builder = ImmutableOpenMap.builder(1);
        builder.put(
            new ClusterInfo.NodeAndPath("node1", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("baz", "quux", 0), between(41, 100)).build()
        );
        final ImmutableOpenMap<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpaces = builder.build();

        currentTime.addAndGet(
            randomLongBetween(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(Settings.EMPTY).millis(),
                120000
            )
        );
        monitor.onNewInfo(clusterInfo(allDisksOk, reservedSpaces));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(null);

    }

    public void testAutoReleaseIndices() {
        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<String>> indicesToRelease = new AtomicReference<>();
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test_1").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .put(IndexMetadata.builder("test_2").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test_1")).addAsNew(metadata.index("test_2")).build();
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(newNormalNode("node1")).add(newNormalNode("node2")))
                .build(),
            allocation
        );
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(8));

        final ImmutableOpenMap.Builder<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpacesBuilder = ImmutableOpenMap
            .builder();
        final int reservedSpaceNode1 = between(0, 10);
        reservedSpacesBuilder.put(
            new ClusterInfo.NodeAndPath("node1", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode1).build()
        );
        final int reservedSpaceNode2 = between(0, 10);
        reservedSpacesBuilder.put(
            new ClusterInfo.NodeAndPath("node2", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode2).build()
        );
        ImmutableOpenMap<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpaces = reservedSpacesBuilder.build();

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(clusterState);
            }
        ) {
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
        monitor.onNewInfo(clusterInfo(builder.build(), reservedSpaces));
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // Reserved space is ignored when applying block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(5, 90)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(5, 90)));
        monitor.onNewInfo(clusterInfo(builder.build(), reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // Change cluster state so that "test_2" index is blocked (read only)
        IndexMetadata indexMetadata = IndexMetadata.builder(clusterState.metadata().index("test_2"))
            .settings(
                Settings.builder()
                    .put(clusterState.metadata().index("test_2").getSettings())
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)
            )
            .build();

        ClusterState clusterStateWithBlocks = ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadata, true).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata).build())
            .build();

        assertTrue(clusterStateWithBlocks.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test_2"));
        monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterStateWithBlocks,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(clusterStateWithBlocks);
            }
        ) {
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
        monitor.onNewInfo(clusterInfo(builder.build(), reservedSpaces));
        assertThat(indicesToMarkReadOnly.get(), contains("test_1"));
        assertNull(indicesToRelease.get());

        // When free disk on node1 and node2 goes above 10% high watermark then release index block, ignoring reserved space
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(10, 100)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(10, 100)));
        monitor.onNewInfo(clusterInfo(builder.build(), reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertThat(indicesToRelease.get(), contains("test_2"));

        // When no usage information is present for node2, we don't release the block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 4)));
        monitor.onNewInfo(clusterInfo(builder.build()));
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
        monitor.onNewInfo(clusterInfo(builder.build()));
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
        monitor.onNewInfo(clusterInfo(builder.build()));
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
        monitor.onNewInfo(clusterInfo(builder.build()));
        assertThat(indicesToMarkReadOnly.get(), contains("test_1"));
        assertNull(indicesToRelease.get());
    }

    public void testNoAutoReleaseOfIndicesOnReplacementNodes() {
        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<String>> indicesToRelease = new AtomicReference<>();
        AtomicReference<ClusterState> currentClusterState = new AtomicReference<>();
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test_1").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .put(IndexMetadata.builder("test_2").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test_1")).addAsNew(metadata.index("test_2")).build();
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .nodes(DiscoveryNodes.builder().add(newNormalNode("node1", "my-node1")).add(newNormalNode("node2", "my-node2")))
                .build(),
            allocation
        );
        assertThat(RoutingNodesHelper.shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(8));

        final ImmutableOpenMap.Builder<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpacesBuilder = ImmutableOpenMap
            .builder();
        final int reservedSpaceNode1 = between(0, 10);
        reservedSpacesBuilder.put(
            new ClusterInfo.NodeAndPath("node1", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode1).build()
        );
        final int reservedSpaceNode2 = between(0, 10);
        reservedSpacesBuilder.put(
            new ClusterInfo.NodeAndPath("node2", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode2).build()
        );
        ImmutableOpenMap<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpaces = reservedSpacesBuilder.build();

        currentClusterState.set(clusterState);

        final DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            currentClusterState::get,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(currentClusterState.get());
            }
        ) {
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
        monitor.onNewInfo(clusterInfo(builder.build(), reservedSpaces));
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // Reserved space is ignored when applying block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(5, 90)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(5, 90)));
        monitor.onNewInfo(clusterInfo(builder.build(), reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // Change cluster state so that "test_2" index is blocked (read only)
        IndexMetadata indexMetadata = IndexMetadata.builder(clusterState.metadata().index("test_2"))
            .settings(
                Settings.builder()
                    .put(clusterState.metadata().index("test_2").getSettings())
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)
            )
            .build();

        final String sourceNode;
        final String targetNode;
        if (randomBoolean()) {
            sourceNode = "node1";
            targetNode = "my-node2";
        } else {
            sourceNode = "node2";
            targetNode = "my-node1";
        }

        final ClusterState clusterStateWithBlocks = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(indexMetadata, true)
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Collections.singletonMap(
                                sourceNode,
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(sourceNode)
                                    .setReason("testing")
                                    .setType(SingleNodeShutdownMetadata.Type.REPLACE)
                                    .setTargetNodeName(targetNode)
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .build()
                            )
                        )
                    )
                    .build()
            )
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata).build())
            .build();

        assertTrue(clusterStateWithBlocks.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test_2"));

        currentClusterState.set(clusterStateWithBlocks);

        // When free disk on any of node1 or node2 goes below 5% flood watermark, then apply index block on indices not having the block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 100)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(0, 4)));
        monitor.onNewInfo(clusterInfo(builder.build(), reservedSpaces));
        assertThat(indicesToMarkReadOnly.get(), contains("test_1"));
        assertNull(indicesToRelease.get());

        // While the REPLACE is ongoing the lock will not be removed from the index
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(10, 100)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(10, 100)));
        monitor.onNewInfo(clusterInfo(builder.build(), reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        final ClusterState clusterStateNoShutdown = ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadata, true).removeCustom(NodesShutdownMetadata.TYPE).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata).build())
            .build();

        assertTrue(clusterStateNoShutdown.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test_2"));

        currentClusterState.set(clusterStateNoShutdown);

        // Now that the REPLACE is gone, auto-releasing can occur for the index
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(10, 100)));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(10, 100)));
        monitor.onNewInfo(clusterInfo(builder.build(), reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertThat(indicesToRelease.get(), contains("test_2"));
    }

    @TestLogging(value = "org.elasticsearch.cluster.routing.allocation.DiskThresholdMonitor:INFO", reason = "testing INFO/WARN logging")
    public void testDiskMonitorLogging() throws IllegalAccessException {
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(newNormalNode("node1")).add(newFrozenOnlyNode("frozen")))
            .build();
        final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(clusterState);
        final AtomicBoolean advanceTime = new AtomicBoolean(true);

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

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            clusterStateRef::get,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            timeSupplier,
            (reason, priority, listener) -> listener.onResponse(clusterStateRef.get())
        ) {
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
        if (randomBoolean()) {
            allDisksOkBuilder.put("frozen", new DiskUsage("frozen", "frozen", "/foo/bar", 100, between(15, 100)));
        } else {
            allDisksOkBuilder.put(
                "frozen",
                new DiskUsage(
                    "frozen",
                    "frozen",
                    "/foo/bar",
                    ByteSizeValue.ofGb(1000).getBytes(),
                    (randomBoolean() ? ByteSizeValue.ofGb(between(20, 1000)) : ByteSizeValue.ofGb(between(20, 50))).getBytes()
                )
            );
        }
        final ImmutableOpenMap<String, DiskUsage> allDisksOk = allDisksOkBuilder.build();

        final ImmutableOpenMap.Builder<String, DiskUsage> aboveLowWatermarkBuilder = ImmutableOpenMap.builder();
        aboveLowWatermarkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(10, 14)));
        aboveLowWatermarkBuilder.put("frozen", new DiskUsage("frozen", "frozen", "/foo/bar", 100, between(10, 14)));
        final ImmutableOpenMap<String, DiskUsage> aboveLowWatermark = aboveLowWatermarkBuilder.build();

        final ImmutableOpenMap.Builder<String, DiskUsage> aboveHighWatermarkBuilder = ImmutableOpenMap.builder();
        aboveHighWatermarkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(5, 9)));
        aboveHighWatermarkBuilder.put("frozen", new DiskUsage("frozen", "frozen", "/foo/bar", 100, between(5, 9)));
        final ImmutableOpenMap<String, DiskUsage> aboveHighWatermark = aboveHighWatermarkBuilder.build();

        final ImmutableOpenMap.Builder<String, DiskUsage> aboveFloodStageWatermarkBuilder = ImmutableOpenMap.builder();
        aboveFloodStageWatermarkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 4)));
        // frozen is below flood stage, so no logging from it.
        aboveFloodStageWatermarkBuilder.put("frozen", new DiskUsage("frozen", "frozen", "/foo/bar", 100, between(5, 9)));
        final ImmutableOpenMap<String, DiskUsage> aboveFloodStageWatermark = aboveFloodStageWatermarkBuilder.build();

        final ImmutableOpenMap.Builder<String, DiskUsage> frozenAboveFloodStageWatermarkBuilder = ImmutableOpenMap.builder();
        // node1 is below low watermark, so no logging from it.
        frozenAboveFloodStageWatermarkBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(15, 100)));
        frozenAboveFloodStageWatermarkBuilder.put("frozen", new DiskUsage("frozen", "frozen", "/foo/bar", 100, between(0, 4)));
        final ImmutableOpenMap<String, DiskUsage> frozenAboveFloodStageWatermark = frozenAboveFloodStageWatermarkBuilder.build();

        final ImmutableOpenMap.Builder<String, DiskUsage> frozenAboveFloodStageMaxHeadroomBuilder = ImmutableOpenMap.builder();
        // node1 is below low watermark, so no logging from it.
        frozenAboveFloodStageMaxHeadroomBuilder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(15, 100)));
        frozenAboveFloodStageMaxHeadroomBuilder.put(
            "frozen",
            new DiskUsage(
                "frozen",
                "frozen",
                "/foo/bar",
                ByteSizeValue.ofGb(1000).getBytes(),
                ByteSizeValue.ofGb(between(0, 19)).getBytes()
            )
        );
        final ImmutableOpenMap<String, DiskUsage> frozenAboveFloodStageMaxHeadroom = frozenAboveFloodStageMaxHeadroomBuilder.build();

        advanceTime.set(true); // first check sees new nodes and triggers a reroute
        assertNoLogging(monitor, allDisksOk);
        advanceTime.set(randomBoolean()); // no new nodes so no reroute delay needed
        assertNoLogging(monitor, allDisksOk);

        assertSingleInfoMessage(
            monitor,
            aboveLowWatermark,
            "low disk watermark [85%] exceeded on *node1* replicas will not be assigned to this node"
        );

        advanceTime.set(false); // will do one reroute and emit warnings, but subsequent reroutes and associated messages are delayed
        assertSingleWarningMessage(
            monitor,
            aboveHighWatermark,
            "high disk watermark [90%] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(true);
        assertRepeatedWarningMessages(
            monitor,
            aboveHighWatermark,
            "high disk watermark [90%] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(randomBoolean());
        assertRepeatedWarningMessages(
            monitor,
            aboveFloodStageWatermark,
            "flood stage disk watermark [95%] exceeded on *node1* all indices on this node will be marked read-only"
        );

        relocatingShardSizeRef.set(-5L);
        advanceTime.set(true);
        assertSingleInfoMessage(
            monitor,
            aboveHighWatermark,
            "high disk watermark [90%] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to be below the high disk watermark when these relocations are complete"
        );

        relocatingShardSizeRef.set(0L);
        timeSupplier.getAsLong(); // advance time long enough to do another reroute
        advanceTime.set(false); // will do one reroute and emit warnings, but subsequent reroutes and associated messages are delayed
        assertSingleWarningMessage(
            monitor,
            aboveHighWatermark,
            "high disk watermark [90%] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(true);
        assertRepeatedWarningMessages(
            monitor,
            aboveHighWatermark,
            "high disk watermark [90%] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(randomBoolean());
        assertSingleInfoMessage(
            monitor,
            aboveLowWatermark,
            "high disk watermark [90%] no longer exceeded on *node1* but low disk watermark [85%] is still exceeded"
        );

        advanceTime.set(true); // only log about dropping below the low disk watermark on a reroute
        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [85%] no longer exceeded on *node1*");

        advanceTime.set(randomBoolean());
        assertRepeatedWarningMessages(
            monitor,
            aboveFloodStageWatermark,
            "flood stage disk watermark [95%] exceeded on *node1* all indices on this node will be marked read-only"
        );

        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [85%] no longer exceeded on *node1*");

        advanceTime.set(true);
        assertRepeatedWarningMessages(
            monitor,
            aboveHighWatermark,
            "high disk watermark [90%] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [85%] no longer exceeded on *node1*");

        assertRepeatedWarningMessages(
            monitor,
            aboveFloodStageWatermark,
            "flood stage disk watermark [95%] exceeded on *node1* all indices on this node will be marked read-only"
        );

        assertSingleInfoMessage(
            monitor,
            aboveLowWatermark,
            "high disk watermark [90%] no longer exceeded on *node1* but low disk watermark [85%] is still exceeded"
        );

        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [85%] no longer exceeded on *node1*");

        assertRepeatedWarningMessages(monitor, frozenAboveFloodStageWatermark, "flood stage disk watermark [95%] exceeded on *frozen*");

        assertRepeatedWarningMessages(
            monitor,
            frozenAboveFloodStageMaxHeadroom,
            "flood stage disk watermark [max_headroom=20gb] exceeded on *frozen*"
        );

        assertNoLogging(monitor, allDisksOk);
    }

    private void assertNoLogging(DiskThresholdMonitor monitor, ImmutableOpenMap<String, DiskUsage> diskUsages)
        throws IllegalAccessException {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation("any INFO message", DiskThresholdMonitor.class.getCanonicalName(), Level.INFO, "*")
        );
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation("any WARN message", DiskThresholdMonitor.class.getCanonicalName(), Level.WARN, "*")
        );

        Logger diskThresholdMonitorLogger = LogManager.getLogger(DiskThresholdMonitor.class);
        Loggers.addAppender(diskThresholdMonitorLogger, mockAppender);

        for (int i = between(1, 3); i >= 0; i--) {
            monitor.onNewInfo(clusterInfo(diskUsages));
        }

        mockAppender.assertAllExpectationsMatched();
        Loggers.removeAppender(diskThresholdMonitorLogger, mockAppender);
        mockAppender.stop();
    }

    private void assertRepeatedWarningMessages(DiskThresholdMonitor monitor, ImmutableOpenMap<String, DiskUsage> diskUsages, String message)
        throws IllegalAccessException {
        for (int i = between(1, 3); i >= 0; i--) {
            assertLogging(monitor, diskUsages, Level.WARN, message);
        }
    }

    private void assertSingleWarningMessage(DiskThresholdMonitor monitor, ImmutableOpenMap<String, DiskUsage> diskUsages, String message)
        throws IllegalAccessException {
        assertLogging(monitor, diskUsages, Level.WARN, message);
        assertNoLogging(monitor, diskUsages);
    }

    private void assertSingleInfoMessage(DiskThresholdMonitor monitor, ImmutableOpenMap<String, DiskUsage> diskUsages, String message)
        throws IllegalAccessException {
        assertLogging(monitor, diskUsages, Level.INFO, message);
        assertNoLogging(monitor, diskUsages);
    }

    private void assertLogging(DiskThresholdMonitor monitor, ImmutableOpenMap<String, DiskUsage> diskUsages, Level level, String message)
        throws IllegalAccessException {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation("expected message", DiskThresholdMonitor.class.getCanonicalName(), level, message)
        );
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "any message of another level",
                DiskThresholdMonitor.class.getCanonicalName(),
                level == Level.INFO ? Level.WARN : Level.INFO,
                "*"
            )
        );

        Logger diskThresholdMonitorLogger = LogManager.getLogger(DiskThresholdMonitor.class);
        Loggers.addAppender(diskThresholdMonitorLogger, mockAppender);

        monitor.onNewInfo(clusterInfo(diskUsages));

        mockAppender.assertAllExpectationsMatched();
        Loggers.removeAppender(diskThresholdMonitorLogger, mockAppender);
        mockAppender.stop();
    }

    private static ClusterInfo clusterInfo(ImmutableOpenMap<String, DiskUsage> diskUsages) {
        return clusterInfo(diskUsages, ImmutableOpenMap.of());
    }

    private static ClusterInfo clusterInfo(
        ImmutableOpenMap<String, DiskUsage> diskUsages,
        ImmutableOpenMap<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace
    ) {
        return new ClusterInfo(diskUsages, null, null, null, null, reservedSpace);
    }

    private static DiscoveryNode newFrozenOnlyNode(String nodeId) {
        Set<DiscoveryNodeRole> irrelevantRoles = new HashSet<>(
            randomSubsetOf(
                DiscoveryNodeRole.roles().stream().filter(Predicate.not(DiscoveryNodeRole::canContainData)).collect(Collectors.toSet())
            )
        );
        return newNode(nodeId, Sets.union(Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE), irrelevantRoles));
    }

    private static DiscoveryNode newNormalNode(String nodeId, String nodeName) {
        Set<DiscoveryNodeRole> randomRoles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles()));
        Set<DiscoveryNodeRole> roles = Sets.union(
            randomRoles,
            Set.of(
                randomFrom(
                    DiscoveryNodeRole.DATA_ROLE,
                    DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE,
                    DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
                    DiscoveryNodeRole.DATA_WARM_NODE_ROLE,
                    DiscoveryNodeRole.DATA_COLD_NODE_ROLE
                )
            )
        );
        return newNode(nodeName, nodeId, roles);
    }

    private static DiscoveryNode newNormalNode(String nodeId) {
        return newNormalNode(nodeId, "");
    }
}
