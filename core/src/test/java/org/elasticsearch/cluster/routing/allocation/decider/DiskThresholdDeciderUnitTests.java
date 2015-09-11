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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Unit tests for the DiskThresholdDecider
 */
public class DiskThresholdDeciderUnitTests extends ESTestCase {

    @Test
    public void testDynamicSettings() {
        NodeSettingsService nss = new NodeSettingsService(Settings.EMPTY);

        ClusterInfoService cis = EmptyClusterInfoService.INSTANCE;
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss, cis, null);

        assertThat(decider.getFreeBytesThresholdHigh(), equalTo(ByteSizeValue.parseBytesSizeValue("0b", "test")));
        assertThat(decider.getFreeDiskThresholdHigh(), equalTo(10.0d));
        assertThat(decider.getFreeBytesThresholdLow(), equalTo(ByteSizeValue.parseBytesSizeValue("0b", "test")));
        assertThat(decider.getFreeDiskThresholdLow(), equalTo(15.0d));
        assertThat(decider.getUsedDiskThresholdLow(), equalTo(85.0d));
        assertThat(decider.getRerouteInterval().seconds(), equalTo(60L));
        assertTrue(decider.isEnabled());
        assertTrue(decider.isIncludeRelocations());

        DiskThresholdDecider.ApplySettings applySettings = decider.newApplySettings();

        Settings newSettings = Settings.builder()
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, false)
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS, false)
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "70%")
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "500mb")
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL, "30s")
                .build();

        applySettings.onRefreshSettings(newSettings);

        assertThat("high threshold bytes should be unset",
                decider.getFreeBytesThresholdHigh(), equalTo(ByteSizeValue.parseBytesSizeValue("0b", "test")));
        assertThat("high threshold percentage should be changed",
                decider.getFreeDiskThresholdHigh(), equalTo(30.0d));
        assertThat("low threshold bytes should be set to 500mb",
                decider.getFreeBytesThresholdLow(), equalTo(ByteSizeValue.parseBytesSizeValue("500mb", "test")));
        assertThat("low threshold bytes should be unset",
                decider.getFreeDiskThresholdLow(), equalTo(0.0d));
        assertThat("reroute interval should be changed to 30 seconds",
                decider.getRerouteInterval().seconds(), equalTo(30L));
        assertFalse("disk threshold decider should now be disabled", decider.isEnabled());
        assertFalse("relocations should now be disabled", decider.isIncludeRelocations());
    }

    public void testCanAllocateUsesMaxAvailableSpace() {
        NodeSettingsService nss = new NodeSettingsService(Settings.EMPTY);
        ClusterInfoService cis = EmptyClusterInfoService.INSTANCE;
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss, cis, null);

        ShardRouting test_0 = ShardRouting.newUnassigned("test", 0, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        DiscoveryNode node_0 = new DiscoveryNode("node_0", DummyTransportAddress.INSTANCE, Version.CURRENT);
        DiscoveryNode node_1 = new DiscoveryNode("node_1", DummyTransportAddress.INSTANCE, Version.CURRENT);

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                        .put(node_0)
                        .put(node_1)
        ).build();

        // actual test -- after all that bloat :)
        Map<String, DiskUsage> leastAvailableUsages = new HashMap<>();
        leastAvailableUsages.put("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, 0)); // all full
        leastAvailableUsages.put("node_1", new DiskUsage("node_1", "node_1", "_na_", 100, 0)); // all full

        Map<String, DiskUsage> mostAvailableUsage = new HashMap<>();
        mostAvailableUsage.put("node_0", new DiskUsage("node_0", "node_0", "_na_", 100, randomIntBetween(20, 100))); // 20 - 99 percent since after allocation there must be at least 10% left and shard is 10byte
        mostAvailableUsage.put("node_1", new DiskUsage("node_1", "node_1", "_na_", 100, randomIntBetween(0, 10))); // this is weird and smells like a bug! it should be up to 20%?

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        final ClusterInfo clusterInfo = new ClusterInfo(Collections.unmodifiableMap(leastAvailableUsages), Collections.unmodifiableMap(mostAvailableUsage), Collections.unmodifiableMap(shardSizes), Collections.EMPTY_MAP);
        RoutingAllocation allocation = new RoutingAllocation(new AllocationDeciders(Settings.EMPTY, new AllocationDecider[]{decider}), clusterState.getRoutingNodes(), clusterState.nodes(), clusterInfo);
        assertEquals(mostAvailableUsage.toString(), Decision.YES, decider.canAllocate(test_0, new RoutingNode("node_0", node_0), allocation));
        assertEquals(mostAvailableUsage.toString(), Decision.NO, decider.canAllocate(test_0, new RoutingNode("node_1", node_1), allocation));
    }

    public void testCanRemainUsesLeastAvailableSpace() {
        NodeSettingsService nss = new NodeSettingsService(Settings.EMPTY);
        ClusterInfoService cis = EmptyClusterInfoService.INSTANCE;
        DiskThresholdDecider decider = new DiskThresholdDecider(Settings.EMPTY, nss, cis, null);
        Map<ShardRouting, String> shardRoutingMap = new HashMap<>();

        DiscoveryNode node_0 = new DiscoveryNode("node_0", DummyTransportAddress.INSTANCE, Version.CURRENT);
        DiscoveryNode node_1 = new DiscoveryNode("node_1", DummyTransportAddress.INSTANCE, Version.CURRENT);

        ShardRouting test_0 = ShardRouting.newUnassigned("test", 0, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(test_0, node_0.getId());
        ShardRoutingHelper.moveToStarted(test_0);
        shardRoutingMap.put(test_0, "/node0/least");

        ShardRouting test_1 = ShardRouting.newUnassigned("test", 1, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(test_1, node_1.getId());
        ShardRoutingHelper.moveToStarted(test_1);
        shardRoutingMap.put(test_1, "/node1/least");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                        .put(node_0)
                        .put(node_1)
        ).build();

        // actual test -- after all that bloat :)
        Map<String, DiskUsage> leastAvailableUsages = new HashMap<>();
        leastAvailableUsages.put("node_0", new DiskUsage("node_0", "node_0", "/node0/least", 100, 10)); // 90% used
        leastAvailableUsages.put("node_1", new DiskUsage("node_1", "node_1", "/node1/least", 100, 9)); // 91% used

        Map<String, DiskUsage> mostAvailableUsage = new HashMap<>();
        mostAvailableUsage.put("node_0", new DiskUsage("node_0", "node_0", "/node0/most", 100, 90)); // 10% used
        mostAvailableUsage.put("node_1", new DiskUsage("node_1", "node_1", "/node1/most", 100, 90)); // 10% used

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 10L); // 10 bytes
        shardSizes.put("[test][1][p]", 10L);
        shardSizes.put("[test][2][p]", 10L);

        final ClusterInfo clusterInfo = new ClusterInfo(Collections.unmodifiableMap(leastAvailableUsages), Collections.unmodifiableMap(mostAvailableUsage), Collections.unmodifiableMap(shardSizes), shardRoutingMap);
        RoutingAllocation allocation = new RoutingAllocation(new AllocationDeciders(Settings.EMPTY, new AllocationDecider[]{decider}), clusterState.getRoutingNodes(), clusterState.nodes(), clusterInfo);
        assertEquals(Decision.YES, decider.canRemain(test_0, new RoutingNode("node_0", node_0), allocation));
        assertEquals(Decision.NO, decider.canRemain(test_1, new RoutingNode("node_1", node_1), allocation));
        try {
            decider.canRemain(test_0, new RoutingNode("node_1", node_1), allocation);
            fail("not allocated on this node");
        } catch (IllegalArgumentException ex) {
            // not allocated on that node
        }
        try {
            decider.canRemain(test_1, new RoutingNode("node_0", node_0), allocation);
            fail("not allocated on this node");
        } catch (IllegalArgumentException ex) {
            // not allocated on that node
        }

        ShardRouting test_2 = ShardRouting.newUnassigned("test", 2, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(test_2, node_1.getId());
        ShardRoutingHelper.moveToStarted(test_2);
        shardRoutingMap.put(test_2, "/node1/most");
        assertEquals("can stay since allocated on a different path with enough space", Decision.YES, decider.canRemain(test_2, new RoutingNode("node_1", node_1), allocation));

        ShardRouting test_3 = ShardRouting.newUnassigned("test", 3, null, true, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(test_3, node_1.getId());
        ShardRoutingHelper.moveToStarted(test_3);
        assertEquals("can stay since we don't have information about this shard", Decision.YES, decider.canRemain(test_2, new RoutingNode("node_1", node_1), allocation));
    }


    public void testShardSizeAndRelocatingSize() {
        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][r]", 10L);
        shardSizes.put("[test][1][r]", 100L);
        shardSizes.put("[test][2][r]", 1000L);
        shardSizes.put("[other][0][p]", 10000L);
        ClusterInfo info = new ClusterInfo(Collections.EMPTY_MAP, Collections.EMPTY_MAP, shardSizes, MockInternalClusterInfoService.DEV_NULL_MAP);
        ShardRouting test_0 = ShardRouting.newUnassigned("test", 0, null, false, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(test_0, "node1");
        ShardRoutingHelper.moveToStarted(test_0);
        ShardRoutingHelper.relocate(test_0, "node2");

        ShardRouting test_1 = ShardRouting.newUnassigned("test", 1, null, false, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(test_1, "node2");
        ShardRoutingHelper.moveToStarted(test_1);
        ShardRoutingHelper.relocate(test_1, "node1");

        ShardRouting test_2 = ShardRouting.newUnassigned("test", 2, null, false, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(test_2, "node1");
        ShardRoutingHelper.moveToStarted(test_2);

        assertEquals(1000l, DiskThresholdDecider.getShardSize(test_2, info));
        assertEquals(100l, DiskThresholdDecider.getShardSize(test_1, info));
        assertEquals(10l, DiskThresholdDecider.getShardSize(test_0, info));

        RoutingNode node = new RoutingNode("node1", new DiscoveryNode("node1", LocalTransportAddress.PROTO, Version.CURRENT), Arrays.asList(test_0, test_1.buildTargetRelocatingShard(), test_2));
        assertEquals(100l, DiskThresholdDecider.sizeOfRelocatingShards(node, info, false, "/dev/null"));
        assertEquals(90l, DiskThresholdDecider.sizeOfRelocatingShards(node, info, true, "/dev/null"));
        assertEquals(0l, DiskThresholdDecider.sizeOfRelocatingShards(node, info, true, "/dev/some/other/dev"));
        assertEquals(0l, DiskThresholdDecider.sizeOfRelocatingShards(node, info, true, "/dev/some/other/dev"));

        ShardRouting test_3 = ShardRouting.newUnassigned("test", 3, null, false, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(test_3, "node1");
        ShardRoutingHelper.moveToStarted(test_3);
        assertEquals(0l, DiskThresholdDecider.getShardSize(test_3, info));


        ShardRouting other_0 = ShardRouting.newUnassigned("other", 0, null, randomBoolean(), new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        ShardRoutingHelper.initialize(other_0, "node2");
        ShardRoutingHelper.moveToStarted(other_0);
        ShardRoutingHelper.relocate(other_0, "node1");


        node = new RoutingNode("node1", new DiscoveryNode("node1", LocalTransportAddress.PROTO, Version.CURRENT), Arrays.asList(test_0, test_1.buildTargetRelocatingShard(), test_2, other_0.buildTargetRelocatingShard()));
        if (other_0.primary()) {
            assertEquals(10100l, DiskThresholdDecider.sizeOfRelocatingShards(node, info, false, "/dev/null"));
            assertEquals(10090l, DiskThresholdDecider.sizeOfRelocatingShards(node, info, true, "/dev/null"));
        } else {
            assertEquals(100l, DiskThresholdDecider.sizeOfRelocatingShards(node, info, false, "/dev/null"));
            assertEquals(90l, DiskThresholdDecider.sizeOfRelocatingShards(node, info, true, "/dev/null"));
        }

    }

}
