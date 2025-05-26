/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class DiskThresholdMonitorTests extends ESAllocationTestCase {

    private void doTestMarkFloodStageIndicesReadOnly(boolean testMaxHeadroom) {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put("index.routing.allocation.require._id", "node2"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("test_1")
                    .settings(settings(IndexVersion.current()).put("index.routing.allocation.require._id", "node1"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("test_2")
                    .settings(settings(IndexVersion.current()).put("index.routing.allocation.require._id", "node1"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("frozen")
                    .settings(settings(IndexVersion.current()).put("index.routing.allocation.require._id", "frozen"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(projectMetadata.index("test"))
            .addAsNew(projectMetadata.index("test_1"))
            .addAsNew(projectMetadata.index("test_2"))
            .addAsNew(projectMetadata.index("frozen"))
            .build();

        final Index test1Index = routingTable.index("test_1").getIndex();
        final Index test2Index = routingTable.index("test_2").getIndex();

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().put(projectMetadata))
                .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
                .nodes(DiscoveryNodes.builder().add(newNormalNode("node1")).add(newNormalNode("node2")).add(newFrozenOnlyNode("frozen")))
                .build(),
            allocation
        );
        AtomicBoolean reroute = new AtomicBoolean(false);
        AtomicReference<Set<Index>> indices = new AtomicReference<>();
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
            },
            TestProjectResolvers.singleProject(projectId)
        ) {

            @Override
            protected void updateIndicesReadOnly(
                ClusterState state,
                Set<Index> indicesToMarkReadOnly,
                Releasable onCompletion,
                boolean readOnly
            ) {
                assertTrue(indices.compareAndSet(null, indicesToMarkReadOnly));
                assertTrue(readOnly);
                onCompletion.close();
            }
        };

        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage("node1", "node1", "/foo/bar", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(99).getBytes() : 4)
        );
        builder.put(
            "node2",
            new DiskUsage("node2", "node2", "/foo/bar", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(250).getBytes() : 30)
        );
        builder.put(
            "frozen",
            new DiskUsage(
                "frozen",
                "frozen",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 10000)).getBytes() : between(0, 100)
            )
        );
        final ClusterInfo initialClusterInfo = clusterInfo(builder);
        monitor.onNewInfo(initialClusterInfo);
        assertTrue(reroute.get()); // reroute on new nodes
        assertThat(indices.get(), containsInAnyOrder(test1Index, test2Index));

        indices.set(null);
        reroute.set(false);
        monitor.onNewInfo(initialClusterInfo);
        assertFalse(reroute.get()); // no reroute if no change

        indices.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage("node1", "node1", "/foo/bar", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(99).getBytes() : 4)
        );
        builder.put(
            "node2",
            new DiskUsage("node2", "node2", "/foo/bar", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(100).getBytes() : 5)
        );
        builder.put(
            "frozen",
            new DiskUsage(
                "frozen",
                "frozen",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 19)).getBytes() : between(0, 4)
            )
        );
        currentTime.addAndGet(randomLongBetween(60000, 120000));
        monitor.onNewInfo(clusterInfo(builder));
        assertTrue(reroute.get());
        assertThat(indices.get(), containsInAnyOrder(test1Index, test2Index));
        IndexMetadata indexMetadata = IndexMetadata.builder(clusterState.metadata().getProject(projectId).index("test_2"))
            .settings(
                Settings.builder()
                    .put(clusterState.metadata().getProject(projectId).index("test_2").getSettings())
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)
            )
            .build();

        // now we mark one index as read-only and assert that we don't mark it as such again
        final ClusterState anotherFinalClusterState = ClusterState.builder(clusterState)
            .putProjectMetadata(
                ProjectMetadata.builder(clusterState.metadata().getProject(projectId))
                    .put(clusterState.metadata().getProject(projectId).index("test"), false)
                    .put(clusterState.metadata().getProject(projectId).index("test_1"), false)
                    .put(indexMetadata, true)
                    .build()
            )
            .blocks(ClusterBlocks.builder().addBlocks(projectId, indexMetadata).build())
            .build();
        assertTrue(anotherFinalClusterState.blocks().indexBlocked(projectId, ClusterBlockLevel.WRITE, "test_2"));

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
            },
            TestProjectResolvers.singleProject(projectId)
        ) {
            @Override
            protected void updateIndicesReadOnly(
                ClusterState state,
                Set<Index> indicesToMarkReadOnly,
                Releasable onCompletion,
                boolean readOnly
            ) {
                assertTrue(indices.compareAndSet(null, indicesToMarkReadOnly));
                assertTrue(readOnly);
                onCompletion.close();
            }
        };

        indices.set(null);
        reroute.set(false);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage("node1", "node1", "/foo/bar", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(99).getBytes() : 4)
        );
        builder.put(
            "node2",
            new DiskUsage("node2", "node2", "/foo/bar", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(100).getBytes() : 5)
        );
        builder.put(
            "frozen",
            new DiskUsage(
                "frozen",
                "frozen",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 19)).getBytes() : between(0, 4)
            )
        );
        monitor.onNewInfo(clusterInfo(builder));
        assertTrue(reroute.get());
        assertThat(indices.get(), containsInAnyOrder(test1Index));
    }

    public void testMarkFloodStageIndicesReadOnlyWithPercentages() {
        doTestMarkFloodStageIndicesReadOnly(false);
    }

    public void testMarkFloodStageIndicesReadOnlyWithMaxHeadroom() {
        doTestMarkFloodStageIndicesReadOnly(true);
    }

    private void doTestDoesNotSubmitRerouteTaskTooFrequently(boolean testMaxHeadroom) {
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNormalNode("node1")).add(newNormalNode("node2")))
            .build();
        AtomicLong currentTime = new AtomicLong();
        AtomicReference<ActionListener<Void>> listenerReference = new AtomicReference<>();
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
            },
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        ) {
            @Override
            protected void updateIndicesReadOnly(
                ClusterState state,
                Set<Index> indicesToMarkReadOnly,
                Releasable onCompletion,
                boolean readOnly
            ) {
                throw new AssertionError("unexpected");
            }
        };

        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;
        Map<String, DiskUsage> allDisksOk = new HashMap<>();
        allDisksOk.put(
            "node1",
            new DiskUsage("node1", "node1", "/foo/bar", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(500).getBytes() : 50)
        );
        allDisksOk.put(
            "node2",
            new DiskUsage("node2", "node2", "/foo/bar", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(500).getBytes() : 50)
        );

        Map<String, DiskUsage> oneDiskAboveWatermark = new HashMap<>();
        oneDiskAboveWatermark.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(101, 149)).getBytes() : between(5, 9)
            )
        );
        oneDiskAboveWatermark.put(
            "node2",
            new DiskUsage("node2", "node2", "/foo/bar", totalBytes, testMaxHeadroom ? ByteSizeValue.ofGb(500).getBytes() : 50)
        );

        // should reroute when receiving info about previously-unknown nodes
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNotNull(listenerReference.get());
        listenerReference.getAndSet(null).onResponse(null);

        // should not reroute when all disks are ok and no new info received
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNull(listenerReference.get());

        // might or might not reroute when a node crosses a watermark, depending on whether the reroute interval has elapsed or not
        if (randomBoolean()) {
            currentTime.addAndGet(randomLongBetween(0, 120000));
            monitor.onNewInfo(clusterInfo(oneDiskAboveWatermark));
            Optional.ofNullable(listenerReference.getAndSet(null)).ifPresent(l -> l.onResponse(null));
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
        listenerReference.getAndSet(null).onResponse(null);

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
        final ActionListener<Void> rerouteListener1 = listenerReference.getAndSet(null);

        // should not re-route again before reroute has completed
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(clusterInfo(allDisksOk));
        assertNull(listenerReference.get());

        // complete reroute
        rerouteListener1.onResponse(null);

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
        Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpaces = Map.of(
            new ClusterInfo.NodeAndPath("node1", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(
                new ShardId("baz", "quux", 0),
                testMaxHeadroom ? ByteSizeValue.ofGb(between(401, 10000)).getBytes() : between(41, 100)
            ).build()
        );

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

    public void testDoesNotSubmitRerouteTaskTooFrequentlyWithPercentages() {
        doTestDoesNotSubmitRerouteTaskTooFrequently(false);
    }

    public void testDoesNotSubmitRerouteTaskTooFrequentlyWithMaxHeadroom() {
        doTestDoesNotSubmitRerouteTaskTooFrequently(true);
    }

    private void doTestAutoReleaseIndices(boolean testMaxHeadroom) {
        AtomicReference<Set<Index>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<Index>> indicesToRelease = new AtomicReference<>();
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        final ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata project = ProjectMetadata.builder(projectId)
            .put(IndexMetadata.builder("test_1").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(1))
            .put(IndexMetadata.builder("test_2").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(project.index("test_1"))
            .addAsNew(project.index("test_2"))
            .build();
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().put(project))
                .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
                .nodes(DiscoveryNodes.builder().add(newNormalNode("node1")).add(newNormalNode("node2")))
                .build(),
            allocation
        );
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(8));

        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;

        Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpaces = new HashMap<>();
        final long reservedSpaceNode1 = testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 150)).getBytes() : between(0, 10);
        reservedSpaces.put(
            new ClusterInfo.NodeAndPath("node1", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode1).build()
        );
        final long reservedSpaceNode2 = testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 150)).getBytes() : between(0, 10);
        reservedSpaces.put(
            new ClusterInfo.NodeAndPath("node2", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode2).build()
        );

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            TestProjectResolvers.singleProject(projectId)
        ) {
            @Override
            protected void updateIndicesReadOnly(
                ClusterState state,
                Set<Index> indicesToUpdate,
                Releasable onCompletion,
                boolean readOnly
            ) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToRelease.compareAndSet(null, indicesToUpdate));
                }
                onCompletion.close();
            }
        };
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 99)).getBytes() : between(0, 4)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 99)).getBytes() : between(0, 4)
            )
        );
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces));
        final Index test1Index = routingTable.index("test_1").getIndex();
        final Index test2Index = routingTable.index("test_2").getIndex();
        assertEquals(new HashSet<>(Arrays.asList(test1Index, test2Index)), indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // Reserved space is ignored when applying block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(100, 9850)).getBytes() : between(5, 90)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(100, 9850)).getBytes() : between(5, 90)
            )
        );
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // Change cluster state so that "test_2" index is blocked (read only)
        IndexMetadata indexMetadata = IndexMetadata.builder(clusterState.metadata().getProject(projectId).index("test_2"))
            .settings(
                Settings.builder()
                    .put(clusterState.metadata().getProject(projectId).index("test_2").getSettings())
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)
            )
            .build();

        ClusterState clusterStateWithBlocks = ClusterState.builder(clusterState)
            .putProjectMetadata(ProjectMetadata.builder(clusterState.metadata().getProject(projectId)).put(indexMetadata, true))
            .blocks(ClusterBlocks.builder().addBlocks(projectId, indexMetadata).build())
            .build();

        assertTrue(clusterStateWithBlocks.blocks().indexBlocked(projectId, ClusterBlockLevel.WRITE, "test_2"));
        monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterStateWithBlocks,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            () -> 0L,
            (reason, priority, listener) -> {
                assertNotNull(listener);
                assertThat(priority, equalTo(Priority.HIGH));
                listener.onResponse(null);
            },
            TestProjectResolvers.singleProject(projectId)
        ) {
            @Override
            protected void updateIndicesReadOnly(
                ClusterState state,
                Set<Index> indicesToUpdate,
                Releasable onCompletion,
                boolean readOnly
            ) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToRelease.compareAndSet(null, indicesToUpdate));
                }
                onCompletion.close();
            }
        };
        // When free disk on any of node1 or node2 goes below the flood watermark, then apply index block on indices not having the block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 10000)).getBytes() : between(0, 100)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 99)).getBytes() : between(0, 4)
            )
        );
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces));
        assertThat(indicesToMarkReadOnly.get(), contains(test1Index));
        assertNull(indicesToRelease.get());

        // When free disk on node1 and node2 goes above the high watermark then release index block, ignoring reserved space
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(150, 10000)).getBytes() : between(10, 100)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(150, 10000)).getBytes() : between(10, 100)
            )
        );
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertThat(indicesToRelease.get(), contains(test2Index));

        // When no usage information is present for node2, we don't release the block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 99)).getBytes() : between(0, 4)
            )
        );
        monitor.onNewInfo(clusterInfo(builder));
        assertThat(indicesToMarkReadOnly.get(), contains(test1Index));
        assertNull(indicesToRelease.get());

        // When disk usage on one node is between the high and flood-stage watermarks, nothing changes
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(100, 149)).getBytes() : between(5, 9)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(100, 10000)).getBytes() : between(5, 100)
            )
        );
        if (randomBoolean()) {
            builder.put(
                "node3",
                new DiskUsage(
                    "node3",
                    "node3",
                    "/foo/bar",
                    totalBytes,
                    testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 10000)).getBytes() : between(0, 100)
                )
            );
        }
        monitor.onNewInfo(clusterInfo(builder));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // When disk usage on one node is missing and the other is below the high watermark, nothing changes
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(100, 10000)).getBytes() : between(5, 100)
            )
        );
        if (randomBoolean()) {
            builder.put(
                "node3",
                new DiskUsage(
                    "node3",
                    "node3",
                    "/foo/bar",
                    totalBytes,
                    testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 10000)).getBytes() : between(0, 100)
                )
            );
        }
        monitor.onNewInfo(clusterInfo(builder));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // When disk usage on one node is missing and the other is above the flood-stage watermark, affected indices are blocked
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 99)).getBytes() : between(0, 4)
            )
        );
        if (randomBoolean()) {
            builder.put(
                "node3",
                new DiskUsage(
                    "node3",
                    "node3",
                    "/foo/bar",
                    totalBytes,
                    testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 10000)).getBytes() : between(0, 100)
                )
            );
        }
        monitor.onNewInfo(clusterInfo(builder));
        assertThat(indicesToMarkReadOnly.get(), contains(test1Index));
        assertNull(indicesToRelease.get());
    }

    public void testAutoReleaseIndicesWithPercentages() {
        doTestAutoReleaseIndices(false);
    }

    public void testAutoReleaseIndicesWithMaxHeadroom() {
        doTestAutoReleaseIndices(true);
    }

    private void doTestNoAutoReleaseOfIndicesOnReplacementNodes(boolean testMaxHeadroom) {
        AtomicReference<Set<Index>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<Index>> indicesToRelease = new AtomicReference<>();
        AtomicReference<ClusterState> currentClusterState = new AtomicReference<>();
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );
        final var projectId = randomProjectIdOrDefault();
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId)
            .put(IndexMetadata.builder("test_1").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(1))
            .put(IndexMetadata.builder("test_2").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(1))
            .build();
        final RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(projectMetadata.index("test_1"))
            .addAsNew(projectMetadata.index("test_2"))
            .build();

        final Index test1Index = routingTable.index("test_1").getIndex();
        final Index test2Index = routingTable.index("test_2").getIndex();

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().put(projectMetadata).build())
                .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
                .nodes(DiscoveryNodes.builder().add(newNormalNode("node1", "my-node1")).add(newNormalNode("node2", "my-node2")))
                .build(),
            allocation
        );
        assertThat(RoutingNodesHelper.shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(8));

        final long totalBytes = testMaxHeadroom ? ByteSizeValue.ofGb(10000).getBytes() : 100;

        Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpaces = new HashMap<>();
        final long reservedSpaceNode1 = testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 150)).getBytes() : between(0, 10);
        reservedSpaces.put(
            new ClusterInfo.NodeAndPath("node1", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode1).build()
        );
        final long reservedSpaceNode2 = testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 150)).getBytes() : between(0, 10);
        reservedSpaces.put(
            new ClusterInfo.NodeAndPath("node2", "/foo/bar"),
            new ClusterInfo.ReservedSpace.Builder().add(new ShardId("", "", 0), reservedSpaceNode2).build()
        );

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
                listener.onResponse(null);
            },
            TestProjectResolvers.singleProject(projectId)
        ) {
            @Override
            protected void updateIndicesReadOnly(
                ClusterState state,
                Set<Index> indicesToUpdate,
                Releasable onCompletion,
                boolean readOnly
            ) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToRelease.compareAndSet(null, indicesToUpdate));
                }
                onCompletion.close();
            }
        };
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        Map<String, DiskUsage> builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 99)).getBytes() : between(0, 4)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 99)).getBytes() : between(0, 4)
            )
        );
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces));
        assertThat(indicesToMarkReadOnly.get(), containsInAnyOrder(test1Index, test2Index));
        assertNull(indicesToRelease.get());

        // Reserved space is ignored when applying block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(100, 9850)).getBytes() : between(5, 90)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(100, 9850)).getBytes() : between(5, 90)
            )
        );
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // Change cluster state so that "test_2" index is blocked (read only)
        IndexMetadata indexMetadata = IndexMetadata.builder(clusterState.metadata().getProject(projectId).index("test_2"))
            .settings(
                Settings.builder()
                    .put(clusterState.metadata().getProject(projectId).index("test_2").getSettings())
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
                    .put(ProjectMetadata.builder(clusterState.metadata().getProject(projectId)).put(indexMetadata, true))
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Collections.singletonMap(
                                sourceNode,
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(sourceNode)
                                    .setNodeEphemeralId(sourceNode)
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
            .blocks(ClusterBlocks.builder().addBlocks(projectId, indexMetadata).build())
            .build();

        assertTrue(clusterStateWithBlocks.blocks().indexBlocked(projectId, ClusterBlockLevel.WRITE, "test_2"));

        currentClusterState.set(clusterStateWithBlocks);

        // When free disk on any of node1 or node2 goes below the flood watermark, then apply index block on indices not having the block
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 10000)).getBytes() : between(0, 100)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(0, 99)).getBytes() : between(0, 4)
            )
        );
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces));
        assertThat(indicesToMarkReadOnly.get(), contains(test1Index));
        assertNull(indicesToRelease.get());

        // While the REPLACE is ongoing the lock will not be removed from the index
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(150, 10000)).getBytes() : between(10, 100)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(150, 10000)).getBytes() : between(10, 100)
            )
        );
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        final ClusterState clusterStateNoShutdown = ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadata, true).removeCustom(NodesShutdownMetadata.TYPE).build())
            .blocks(ClusterBlocks.builder().addBlocks(projectId, indexMetadata).build())
            .build();

        assertTrue(clusterStateNoShutdown.blocks().indexBlocked(projectId, ClusterBlockLevel.WRITE, "test_2"));

        currentClusterState.set(clusterStateNoShutdown);

        // Now that the REPLACE is gone, auto-releasing can occur for the index
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = new HashMap<>();
        builder.put(
            "node1",
            new DiskUsage(
                "node1",
                "node1",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(150, 10000)).getBytes() : between(10, 100)
            )
        );
        builder.put(
            "node2",
            new DiskUsage(
                "node2",
                "node2",
                "/foo/bar",
                totalBytes,
                testMaxHeadroom ? ByteSizeValue.ofGb(between(150, 10000)).getBytes() : between(10, 100)
            )
        );
        monitor.onNewInfo(clusterInfo(builder, reservedSpaces));
        assertNull(indicesToMarkReadOnly.get());
        assertThat(indicesToRelease.get(), contains(test2Index));
    }

    public void testNoAutoReleaseOfIndicesOnReplacementNodesWithPercentages() {
        doTestNoAutoReleaseOfIndicesOnReplacementNodes(false);
    }

    public void testNoAutoReleaseOfIndicesOnReplacementNodesWithMaxHeadroom() {
        doTestNoAutoReleaseOfIndicesOnReplacementNodes(true);
    }

    private void doTestDiskMonitorLogging(boolean testHeadroom) throws IllegalAccessException {
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
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
            (reason, priority, listener) -> listener.onResponse(null),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        ) {
            @Override
            protected void updateIndicesReadOnly(
                ClusterState state,
                Set<Index> indicesToMarkReadOnly,
                Releasable onCompletion,
                boolean readOnly
            ) {
                onCompletion.close();
            }

            @Override
            long sizeOfRelocatingShards(RoutingNode routingNode, DiskUsage diskUsage, ClusterInfo info, ClusterState reroutedClusterState) {
                return relocatingShardSizeRef.get();
            }
        };

        long thousandTb = ByteSizeValue.ofTb(1000).getBytes();
        long total = testHeadroom ? thousandTb : 100;

        Map<String, DiskUsage> allDisksOk = new HashMap<>();
        allDisksOk.put("node1", new DiskUsage("node1", "node1", "/foo/bar", total, testHeadroom ? betweenGb(200, 1000) : between(15, 100)));
        allDisksOk.put(
            "frozen",
            new DiskUsage(
                "frozen",
                "frozen",
                "/foo/bar",
                total,
                testHeadroom ? (randomBoolean() ? betweenGb(20, 1000) : betweenGb(20, 50)) : between(15, 100)
            )
        );

        Map<String, DiskUsage> aboveLowWatermark = new HashMap<>();
        aboveLowWatermark.put(
            "node1",
            new DiskUsage("node1", "node1", "/foo/bar", total, testHeadroom ? betweenGb(150, 199) : between(10, 14))
        );
        aboveLowWatermark.put(
            "frozen",
            new DiskUsage("frozen", "frozen", "/foo/bar", total, testHeadroom ? betweenGb(150, 199) : between(10, 14))
        );

        Map<String, DiskUsage> aboveHighWatermark = new HashMap<>();
        aboveHighWatermark.put(
            "node1",
            new DiskUsage("node1", "node1", "/foo/bar", total, testHeadroom ? betweenGb(100, 149) : between(5, 9))
        );
        aboveHighWatermark.put(
            "frozen",
            new DiskUsage("frozen", "frozen", "/foo/bar", total, testHeadroom ? betweenGb(20, 99) : between(5, 9))
        );

        Map<String, DiskUsage> aboveFloodStageWatermark = new HashMap<>();
        aboveFloodStageWatermark.put(
            "node1",
            new DiskUsage("node1", "node1", "/foo/bar", total, testHeadroom ? betweenGb(0, 99) : between(0, 4))
        );
        // frozen is below flood stage, so no logging from it.
        aboveFloodStageWatermark.put(
            "frozen",
            new DiskUsage("frozen", "frozen", "/foo/bar", total, testHeadroom ? betweenGb(20, 99) : between(5, 9))
        );

        Map<String, DiskUsage> frozenAboveFloodStageWatermark = new HashMap<>();
        // node1 is below low watermark, so no logging from it.
        frozenAboveFloodStageWatermark.put(
            "node1",
            new DiskUsage("node1", "node1", "/foo/bar", total, testHeadroom ? betweenGb(200, 1000) : between(15, 100))
        );
        frozenAboveFloodStageWatermark.put(
            "frozen",
            new DiskUsage("frozen", "frozen", "/foo/bar", total, testHeadroom ? betweenGb(0, 19) : between(0, 4))
        );

        advanceTime.set(true); // first check sees new nodes and triggers a reroute
        assertNoLogging(monitor, allDisksOk);
        advanceTime.set(randomBoolean()); // no new nodes so no reroute delay needed
        assertNoLogging(monitor, allDisksOk);

        String lowWatermarkString = testHeadroom ? "max_headroom=200gb" : "85%";
        String highWatermarkString = testHeadroom ? "max_headroom=150gb" : "90%";
        String floodWatermarkString = testHeadroom ? "max_headroom=100gb" : "95%";
        String frozenFloodWatermarkString = testHeadroom ? "max_headroom=20gb" : "95%";

        assertSingleInfoMessage(
            monitor,
            aboveLowWatermark,
            "low disk watermark [" + lowWatermarkString + "] exceeded on *node1* replicas will not be assigned to this node"
        );

        advanceTime.set(false); // will do one reroute and emit warnings, but subsequent reroutes and associated messages are delayed
        assertSingleWarningMessage(
            monitor,
            aboveHighWatermark,
            "high disk watermark ["
                + highWatermarkString
                + "] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(true);
        assertRepeatedWarningMessages(
            monitor,
            aboveHighWatermark,
            "high disk watermark ["
                + highWatermarkString
                + "] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(randomBoolean());
        assertRepeatedWarningMessages(
            monitor,
            aboveFloodStageWatermark,
            "flood stage disk watermark ["
                + floodWatermarkString
                + "] exceeded on *node1* all indices on this node will be marked read-only"
        );

        relocatingShardSizeRef.set(testHeadroom ? (-1L) * ByteSizeValue.ofGb(100).getBytes() : -5L);
        advanceTime.set(true);
        assertSingleInfoMessage(
            monitor,
            aboveHighWatermark,
            "high disk watermark ["
                + highWatermarkString
                + "] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to be below the high disk watermark when these relocations are complete"
        );

        relocatingShardSizeRef.set(0L);
        timeSupplier.getAsLong(); // advance time long enough to do another reroute
        advanceTime.set(false); // will do one reroute and emit warnings, but subsequent reroutes and associated messages are delayed
        assertSingleWarningMessage(
            monitor,
            aboveHighWatermark,
            "high disk watermark ["
                + highWatermarkString
                + "] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(true);
        assertRepeatedWarningMessages(
            monitor,
            aboveHighWatermark,
            "high disk watermark ["
                + highWatermarkString
                + "] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        advanceTime.set(randomBoolean());
        assertSingleInfoMessage(
            monitor,
            aboveLowWatermark,
            "high disk watermark ["
                + highWatermarkString
                + "] no longer exceeded on *node1* but low disk watermark ["
                + lowWatermarkString
                + "] is still exceeded"
        );

        advanceTime.set(true); // only log about dropping below the low disk watermark on a reroute
        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [" + lowWatermarkString + "] no longer exceeded on *node1*");

        advanceTime.set(randomBoolean());
        assertRepeatedWarningMessages(
            monitor,
            aboveFloodStageWatermark,
            "flood stage disk watermark ["
                + floodWatermarkString
                + "] exceeded on *node1* all indices on this node will be marked read-only"
        );

        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [" + lowWatermarkString + "] no longer exceeded on *node1*");

        advanceTime.set(true);
        assertRepeatedWarningMessages(
            monitor,
            aboveHighWatermark,
            "high disk watermark ["
                + highWatermarkString
                + "] exceeded on *node1* shards will be relocated away from this node* "
                + "the node is expected to continue to exceed the high disk watermark when these relocations are complete"
        );

        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [" + lowWatermarkString + "] no longer exceeded on *node1*");

        assertRepeatedWarningMessages(
            monitor,
            aboveFloodStageWatermark,
            "flood stage disk watermark ["
                + floodWatermarkString
                + "] exceeded on *node1* all indices on this node will be marked read-only"
        );

        assertSingleInfoMessage(
            monitor,
            aboveLowWatermark,
            "high disk watermark ["
                + highWatermarkString
                + "] no longer exceeded on *node1* but low disk watermark ["
                + lowWatermarkString
                + "] is still exceeded"
        );

        assertSingleInfoMessage(monitor, allDisksOk, "low disk watermark [" + lowWatermarkString + "] no longer exceeded on *node1*");

        assertRepeatedWarningMessages(
            monitor,
            frozenAboveFloodStageWatermark,
            "flood stage disk watermark [" + frozenFloodWatermarkString + "] exceeded on *frozen*"
        );

        assertNoLogging(monitor, allDisksOk);
    }

    @TestLogging(value = "org.elasticsearch.cluster.routing.allocation.DiskThresholdMonitor:INFO", reason = "testing INFO/WARN logging")
    public void testDiskMonitorLoggingWithPercentages() throws IllegalAccessException {
        doTestDiskMonitorLogging(false);
    }

    @TestLogging(value = "org.elasticsearch.cluster.routing.allocation.DiskThresholdMonitor:INFO", reason = "testing INFO/WARN logging")
    public void testDiskMonitorLoggingWithMaxHeadrooms() throws IllegalAccessException {
        doTestDiskMonitorLogging(true);
    }

    public void testSkipDiskThresholdMonitorWhenStateNotRecovered() {
        final var projectId = randomProjectIdOrDefault();
        boolean shutdownMetadataInState = randomBoolean();
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
                    .build()
            );
        if (shutdownMetadataInState) {
            metadataBuilder.putCustom(
                NodesShutdownMetadata.TYPE,
                new NodesShutdownMetadata(
                    Collections.singletonMap(
                        "node1",
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId("node1")
                            .setNodeEphemeralId("node1")
                            .setReason("testing")
                            .setType(SingleNodeShutdownMetadata.Type.REPLACE)
                            .setTargetNodeName("node3")
                            .setStartedAtMillis(randomNonNegativeLong())
                            .build()
                    )
                )
            );
        }
        final Metadata metadata = metadataBuilder.build();
        final RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject(projectId).index("test"))
            .build();
        DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder()
            .add(newNormalNode("node1", "node1"))
            .add(newNormalNode("node2", "node2"));
        // node3 which is to replace node1 may or may not be in the cluster
        if (shutdownMetadataInState && randomBoolean()) {
            discoveryNodes.add(newNormalNode("node3", "node3"));
        }
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata)
                .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
                .nodes(discoveryNodes)
                .build(),
            createAllocationService(Settings.EMPTY)
        );
        final Index testIndex = routingTable.index("test").getIndex();

        Map<String, DiskUsage> diskUsages = new HashMap<>();
        diskUsages.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 4)));
        diskUsages.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(0, 4)));
        final ClusterInfo clusterInfo = clusterInfo(diskUsages);
        var result = runDiskThresholdMonitor(clusterState, clusterInfo);
        assertTrue(result.v1()); // reroute on new nodes
        assertThat(result.v2(), contains(testIndex));

        final ClusterState blockedClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(discoveryNodes)
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        var result2 = runDiskThresholdMonitor(blockedClusterState, clusterInfo);
        assertFalse(result2.v1());
        assertNull(result2.v2());
    }

    private void doTestSkipNodesNotInRoutingTable(boolean sourceNodeInTable, boolean targetNodeInTable) {
        final var projectId = randomProjectIdOrDefault();
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
                    .build()
            );

        metadataBuilder.putCustom(
            NodesShutdownMetadata.TYPE,
            new NodesShutdownMetadata(
                Collections.singletonMap(
                    "node1",
                    SingleNodeShutdownMetadata.builder()
                        .setNodeId("node1")
                        .setNodeEphemeralId("node1")
                        .setReason("testing")
                        .setType(SingleNodeShutdownMetadata.Type.REPLACE)
                        .setTargetNodeName("node3")
                        .setStartedAtMillis(randomNonNegativeLong())
                        .build()
                )
            )
        );

        final Metadata metadata = metadataBuilder.build();
        final RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject(projectId).index("test"))
            .build();
        DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder().add(newNormalNode("node2", "node2"));
        // node1 which is replaced by node3 may or may not be in the cluster
        if (sourceNodeInTable) {
            discoveryNodes.add(newNormalNode("node1", "node1"));
        }
        // node3 which is to replace node1 may or may not be in the cluster
        if (targetNodeInTable) {
            discoveryNodes.add(newNormalNode("node3", "node3"));
        }
        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata)
                .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
                .nodes(discoveryNodes)
                .build(),
            createAllocationService(Settings.EMPTY)
        );
        final Index testIndex = routingTable.index("test").getIndex();

        Map<String, DiskUsage> diskUsages = new HashMap<>();
        diskUsages.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, between(0, 4)));
        diskUsages.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, between(0, 4)));
        final ClusterInfo clusterInfo = clusterInfo(diskUsages);
        Tuple<Boolean, Set<Index>> result = runDiskThresholdMonitor(clusterState, clusterInfo);
        assertTrue(result.v1()); // reroute on new nodes
        assertThat(result.v2(), contains(testIndex));
    }

    public void testSkipReplaceSourceNodeNotInRoutingTable() {
        doTestSkipNodesNotInRoutingTable(false, true);
    }

    public void testSkipReplaceTargetNodeNotInRoutingTable() {
        doTestSkipNodesNotInRoutingTable(true, false);
    }

    public void testSkipReplaceSourceAndTargetNodesNotInRoutingTable() {
        doTestSkipNodesNotInRoutingTable(false, false);
    }

    // Runs a disk threshold monitor with a given cluster state and cluster info and returns whether a reroute should
    // happen and any indices that should be marked as read-only.
    private Tuple<Boolean, Set<Index>> runDiskThresholdMonitor(ClusterState clusterState, ClusterInfo clusterInfo) {
        AtomicBoolean reroute = new AtomicBoolean(false);
        AtomicReference<Set<Index>> indices = new AtomicReference<>();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(
            Settings.EMPTY,
            () -> clusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            System::currentTimeMillis,
            (reason, priority, listener) -> {
                reroute.set(true);
                listener.onResponse(null);
            },
            TestProjectResolvers.allProjects()
        ) {

            @Override
            protected void updateIndicesReadOnly(
                ClusterState state,
                Set<Index> indicesToMarkReadOnly,
                Releasable onCompletion,
                boolean readOnly
            ) {
                assertTrue(readOnly);
                indices.set(indicesToMarkReadOnly);
                onCompletion.close();
            }
        };
        monitor.onNewInfo(clusterInfo);
        return Tuple.tuple(reroute.get(), indices.get());
    }

    private void assertNoLogging(DiskThresholdMonitor monitor, Map<String, DiskUsage> diskUsages) throws IllegalAccessException {
        try (var mockLog = MockLog.capture(DiskThresholdMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("any INFO message", DiskThresholdMonitor.class.getCanonicalName(), Level.INFO, "*")
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("any WARN message", DiskThresholdMonitor.class.getCanonicalName(), Level.WARN, "*")
            );

            for (int i = between(1, 3); i >= 0; i--) {
                monitor.onNewInfo(clusterInfo(diskUsages));
            }

            mockLog.assertAllExpectationsMatched();
        }
    }

    private void assertRepeatedWarningMessages(DiskThresholdMonitor monitor, Map<String, DiskUsage> diskUsages, String message)
        throws IllegalAccessException {
        for (int i = between(1, 3); i >= 0; i--) {
            assertLogging(monitor, diskUsages, Level.WARN, message);
        }
    }

    private void assertSingleWarningMessage(DiskThresholdMonitor monitor, Map<String, DiskUsage> diskUsages, String message)
        throws IllegalAccessException {
        assertLogging(monitor, diskUsages, Level.WARN, message);
        assertNoLogging(monitor, diskUsages);
    }

    private void assertSingleInfoMessage(DiskThresholdMonitor monitor, Map<String, DiskUsage> diskUsages, String message)
        throws IllegalAccessException {
        assertLogging(monitor, diskUsages, Level.INFO, message);
        assertNoLogging(monitor, diskUsages);
    }

    private void assertLogging(DiskThresholdMonitor monitor, Map<String, DiskUsage> diskUsages, Level level, String message) {
        try (var mockLog = MockLog.capture(DiskThresholdMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation("expected message", DiskThresholdMonitor.class.getCanonicalName(), level, message)
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "any message of another level",
                    DiskThresholdMonitor.class.getCanonicalName(),
                    level == Level.INFO ? Level.WARN : Level.INFO,
                    "*"
                )
            );

            monitor.onNewInfo(clusterInfo(diskUsages));
            mockLog.assertAllExpectationsMatched();
        }
    }

    private static long betweenGb(int min, int max) {
        return ByteSizeValue.ofGb(between(min, max)).getBytes();
    }

    private static ClusterInfo clusterInfo(Map<String, DiskUsage> diskUsages) {
        return clusterInfo(diskUsages, Map.of());
    }

    private static ClusterInfo clusterInfo(
        Map<String, DiskUsage> diskUsages,
        Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> reservedSpace
    ) {
        return new ClusterInfo(diskUsages, Map.of(), Map.of(), Map.of(), Map.of(), reservedSpace);
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
