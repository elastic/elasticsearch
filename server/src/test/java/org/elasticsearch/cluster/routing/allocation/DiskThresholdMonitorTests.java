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
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;

public class DiskThresholdMonitorTests extends ESAllocationTestCase {


    public void testMarkFloodStageIndicesReadOnly() {
        Settings settings = Settings.EMPTY;
        ClusterState clusterState = bootstrapCluster();
        ClusterState finalState = clusterState;
        AtomicBoolean reroute = new AtomicBoolean(false);
        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<String>> indicesToRelease = new AtomicReference<>();
        DiskThresholdMonitor monitor = new DiskThresholdMonitor(settings, () -> finalState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null) {
            @Override
            protected void reroute() {
                assertTrue(reroute.compareAndSet(false, true));
            }

            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, boolean readOnly) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToRelease.compareAndSet(null, indicesToUpdate));
                }
            }
        };
        ImmutableOpenMap.Builder<String, DiskUsage> builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1","node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2","node2", "/foo/bar", 100, 30));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertFalse(reroute.get());
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        indicesToMarkReadOnly.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1","node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2","node2", "/foo/bar", 100, 5));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertTrue(reroute.get());
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

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

        monitor = new DiskThresholdMonitor(settings, () -> anotherFinalClusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null) {
            @Override
            protected void reroute() {
                assertTrue(reroute.compareAndSet(false, true));
            }

            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, boolean readOnly) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToRelease.compareAndSet(null, indicesToUpdate));
                }
            }
        };

        indicesToMarkReadOnly.set(null);
        reroute.set(false);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1","node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2","node2", "/foo/bar", 100, 5));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertTrue(reroute.get());
        assertEquals(new HashSet<>(Arrays.asList("test_1")), indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());
    }

    public void testAutoReleaseIndices() {
        Settings settings = Settings.EMPTY;
        ClusterState clusterState = bootstrapCluster();

        AtomicReference<Set<String>> indicesToMarkReadOnly = new AtomicReference<>();
        AtomicReference<Set<String>> indicesToRelease = new AtomicReference<>();

        // Change cluster state so that "test" index is blocked (read only)
        IndexMetaData indexMetaData = IndexMetaData.builder(clusterState.metaData().index("test")).settings(Settings.builder()
            .put(clusterState.metaData()
                .index("test").getSettings())
            .put(IndexMetaData.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true)).build();

        final ClusterState anotherFinalClusterState = ClusterState.builder(clusterState).metaData(MetaData.builder(clusterState.metaData())
            .put(clusterState.metaData().index("test_1"), false)
            .put(clusterState.metaData().index("test_2"), false)
            .put(indexMetaData, true).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetaData).build()).build();

        assertTrue(anotherFinalClusterState.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test"));

        DiskThresholdMonitor monitor = new DiskThresholdMonitor(settings, () -> anotherFinalClusterState,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null) {
            @Override
            protected void updateIndicesReadOnly(Set<String> indicesToUpdate, boolean readOnly) {
                if (readOnly) {
                    assertTrue(indicesToMarkReadOnly.compareAndSet(null, indicesToUpdate));
                } else {
                    assertTrue(indicesToRelease.compareAndSet(null, indicesToUpdate));
                }
            }
        };

        // When free disk on node2 goes above threshold (10% high watermark in this test case), index on node2 marked for
        // auto release
        ImmutableOpenMap.Builder<String, DiskUsage> builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, 11));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indicesToMarkReadOnly.get());
        assertEquals(new HashSet<>(Arrays.asList("test")), indicesToRelease.get());

        // When free disk on node1 and node2 goes below threshold (10% high watermark in this test case).
        // index with read only block isn't marked again to be read only and no indices to be auto-released
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, 4));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, 4));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertEquals(new HashSet<>(Arrays.asList("test_1", "test_2")), indicesToMarkReadOnly.get());
        assertNull(indicesToRelease.get());

        // When free disk on node1 and node2 goes above threshold (10% high watermark in this test case).
        // index with read only block to be auto-released
        indicesToMarkReadOnly.set(null);
        indicesToRelease.set(null);
        builder = ImmutableOpenMap.builder();
        builder.put("node1", new DiskUsage("node1", "node1", "/foo/bar", 100, 14));
        builder.put("node2", new DiskUsage("node2", "node2", "/foo/bar", 100, 14));
        monitor.onNewInfo(new ClusterInfo(builder.build(), null, null, null));
        assertNull(indicesToMarkReadOnly.get());
        assertEquals(new HashSet<>(Arrays.asList("test")), indicesToRelease.get());
    }

    private ClusterState bootstrapCluster() {
        AllocationService allocation = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());
        Settings settings = Settings.EMPTY;
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
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(metaData).routingTable(routingTable).build();
        logger.info("adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))
            .add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");
        logger.info("start primary shard");
        return allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
    }
}
