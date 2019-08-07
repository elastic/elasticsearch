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
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
        AtomicReference<ActionListener<Void>> listenerReference = new AtomicReference<>();
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
        listenerReference.getAndSet(null).onResponse(null);

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
        final ActionListener<Void> rerouteListener1 = listenerReference.getAndSet(null);

        // should not re-route again before reroute has completed
        currentTime.addAndGet(randomLongBetween(0, 120000));
        monitor.onNewInfo(new ClusterInfo(allDisksOk, null, null, null));
        assertNull(listenerReference.get());

        // complete reroute
        rerouteListener1.onResponse(null);

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
                listener.onResponse(null);
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
                listener.onResponse(null);
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
}
