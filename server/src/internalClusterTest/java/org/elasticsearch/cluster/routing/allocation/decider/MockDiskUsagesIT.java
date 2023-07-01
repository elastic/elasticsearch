/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MockDiskUsagesIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockInternalClusterInfoService.TestPlugin.class);
    }

    @Override
    public Settings indexSettings() {
        // ensure that indices do not use custom data paths
        return Settings.builder().put(super.indexSettings()).putNull(IndexMetadata.SETTING_DATA_PATH).build();
    }

    private static FsInfo.Path setDiskUsage(FsInfo.Path original, long totalBytes, long freeBytes) {
        return new FsInfo.Path(original.getPath(), original.getMount(), totalBytes, freeBytes, freeBytes);
    }

    public void testRerouteOccursOnDiskPassingHighWatermark() throws Exception {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), createTempDir()));
        }

        final List<String> nodeIds = clusterAdmin().prepareState()
            .get()
            .getState()
            .getRoutingNodes()
            .stream()
            .map(RoutingNode::nodeId)
            .toList();

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setUpdateFrequency(TimeValue.timeValueMillis(200));

        // prevent any effects from in-flight recoveries, since we are only simulating a 100-byte disk
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 0L);

        // start with all nodes below the watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 100, between(10, 100)));

        final boolean watermarkBytes = randomBoolean(); // we have to consistently use bytes or percentage for the disk watermark settings
        Settings.Builder settings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), watermarkBytes ? "10b" : "90%")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), watermarkBytes ? "10b" : "90%")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), watermarkBytes ? "0b" : "100%")
            .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms");
        if (watermarkBytes == false && randomBoolean()) {
            String headroom = randomIntBetween(10, 100) + "b";
            settings = settings.put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), headroom)
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), headroom)
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), headroom);
        }
        updateClusterSettings(settings);
        // Create an index with 10 shards so we can check allocation for it
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", 10).put("number_of_replicas", 0)));
        ensureGreen("test");

        assertBusy(() -> {
            final Map<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(0)), greaterThanOrEqualTo(3));
            assertThat("node1 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(1)), greaterThanOrEqualTo(3));
            assertThat("node2 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(2)), greaterThanOrEqualTo(3));
        });

        // move node2 above high watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(
                fsInfoPath,
                100,
                discoveryNode.getId().equals(nodeIds.get(2)) ? between(0, 9) : between(10, 100)
            )
        );

        logger.info("--> waiting for shards to relocate off node [{}]", nodeIds.get(2));

        assertBusy(() -> {
            final Map<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has 5 shards", shardCountByNodeId.get(nodeIds.get(0)), equalTo(5));
            assertThat("node1 has 5 shards", shardCountByNodeId.get(nodeIds.get(1)), equalTo(5));
            assertThat("node2 has 0 shards", shardCountByNodeId.get(nodeIds.get(2)), equalTo(0));
        });

        // move all nodes below watermark again
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 100, between(10, 100)));

        logger.info("--> waiting for shards to rebalance back onto node [{}]", nodeIds.get(2));

        assertBusy(() -> {
            final Map<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(0)), greaterThanOrEqualTo(3));
            assertThat("node1 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(1)), greaterThanOrEqualTo(3));
            assertThat("node2 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(2)), greaterThanOrEqualTo(3));
        });
    }

    public void testAutomaticReleaseOfIndexBlock() throws Exception {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), createTempDir()));
        }

        final List<String> nodeIds = clusterAdmin().prepareState()
            .get()
            .getState()
            .getRoutingNodes()
            .stream()
            .map(RoutingNode::nodeId)
            .toList();

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setUpdateFrequency(TimeValue.timeValueMillis(200));

        // prevent any effects from in-flight recoveries, since we are only simulating a 100-byte disk
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 0L);

        // start with all nodes below the low watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 100, between(15, 100)));

        final boolean watermarkBytes = randomBoolean(); // we have to consistently use bytes or percentage for the disk watermark settings
        Settings.Builder builder = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), watermarkBytes ? "10b" : "90%")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), watermarkBytes ? "10b" : "90%")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), watermarkBytes ? "5b" : "95%")
            .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "150ms");
        if (watermarkBytes == false) {
            builder = builder.put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "10b")
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "10b")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "5b");
        }
        updateClusterSettings(builder);

        // Create an index with 6 shards so we can check allocation for it
        prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)).get();
        ensureGreen("test");

        {
            final Map<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has 2 shards", shardCountByNodeId.get(nodeIds.get(0)), equalTo(2));
            assertThat("node1 has 2 shards", shardCountByNodeId.get(nodeIds.get(1)), equalTo(2));
            assertThat("node2 has 2 shards", shardCountByNodeId.get(nodeIds.get(2)), equalTo(2));
        }

        client().prepareIndex("test").setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertSearchHits(client().prepareSearch("test").get(), "1");

        // Move all nodes above the low watermark so no shard movement can occur, and at least one node above the flood stage watermark so
        // the index is blocked
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(
                fsInfoPath,
                100,
                discoveryNode.getId().equals(nodeIds.get(2)) ? between(0, 4) : between(0, 9)
            )
        );

        assertBusy(
            () -> assertBlocked(
                client().prepareIndex().setIndex("test").setId("1").setSource("foo", "bar"),
                IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK
            )
        );

        assertFalse(clusterAdmin().prepareHealth("test").setWaitForEvents(Priority.LANGUID).get().isTimedOut());

        // Cannot add further documents
        assertBlocked(
            client().prepareIndex().setIndex("test").setId("2").setSource("foo", "bar"),
            IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK
        );
        assertSearchHits(client().prepareSearch("test").get(), "1");

        logger.info("--> index is confirmed read-only, releasing disk space");

        // Move all nodes below the high watermark so that the index is unblocked
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 100, between(10, 100)));

        // Attempt to create a new document until DiskUsageMonitor unblocks the index
        assertBusy(() -> {
            try {
                client().prepareIndex("test")
                    .setId("3")
                    .setSource("foo", "bar")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get();
            } catch (ClusterBlockException e) {
                throw new AssertionError("retrying", e);
            }
        });
        assertSearchHits(client().prepareSearch("test").get(), "1", "3");
    }

    public void testOnlyMovesEnoughShardsToDropBelowHighWatermark() throws Exception {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), createTempDir()));
        }

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();

        final AtomicReference<ClusterState> masterAppliedClusterState = new AtomicReference<>();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            masterAppliedClusterState.set(event.state());
            ClusterInfoServiceUtils.refresh(clusterInfoService); // so that subsequent reroutes see disk usage according to current state
        });

        // shards are 1 byte large
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 1L);

        // start with all nodes below the watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 1000L, 1000L));

        updateClusterSettings(
            Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "90%")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "100%")
                .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
        );

        final List<String> nodeIds = clusterAdmin().prepareState()
            .get()
            .getState()
            .getRoutingNodes()
            .stream()
            .map(RoutingNode::nodeId)
            .toList();

        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));

        ensureGreen("test");

        assertBusy(() -> {
            final Map<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has 2 shards", shardCountByNodeId.get(nodeIds.get(0)), equalTo(2));
            assertThat("node1 has 2 shards", shardCountByNodeId.get(nodeIds.get(1)), equalTo(2));
            assertThat("node2 has 2 shards", shardCountByNodeId.get(nodeIds.get(2)), equalTo(2));
        });

        // disable rebalancing, or else we might move too many shards away and then rebalance them back again
        updateClusterSettings(
            Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        );

        // node2 suddenly has 99 bytes free, less than 10%, but moving one shard is enough to bring it up to 100 bytes free:
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(
                fsInfoPath,
                1000L,
                discoveryNode.getId().equals(nodeIds.get(2))
                    ? 101L - masterAppliedClusterState.get().getRoutingNodes().node(nodeIds.get(2)).numberOfOwningShards()
                    : 1000L
            )
        );

        ClusterInfoServiceUtils.refresh(clusterInfoService);

        logger.info("--> waiting for shards to relocate off node [{}]", nodeIds.get(2));

        // must wait for relocation to start
        assertBusy(() -> assertThat("node2 has 1 shard", getShardCountByNodeId().get(nodeIds.get(2)), equalTo(1)));

        // ensure that relocations finished without moving any more shards
        ensureGreen("test");
        assertThat("node2 has 1 shard", getShardCountByNodeId().get(nodeIds.get(2)), equalTo(1));
    }

    public void testDoesNotExceedLowWatermarkWhenRebalancing() throws Exception {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), createTempDir()));
        }

        final AtomicReference<ClusterState> masterAppliedClusterState = new AtomicReference<>();

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();

        final List<String> nodeIds = clusterAdmin().prepareState()
            .get()
            .getState()
            .getRoutingNodes()
            .stream()
            .map(RoutingNode::nodeId)
            .toList();

        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            assertThat(event.state().getRoutingNodes().node(nodeIds.get(2)).size(), lessThanOrEqualTo(1));
            masterAppliedClusterState.set(event.state());
            ClusterInfoServiceUtils.refresh(clusterInfoService); // so that subsequent reroutes see disk usage according to current state
        });

        updateClusterSettings(
            Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "85%")
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "100%")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "100%")
        );

        // shards are 1 byte large
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 1L);

        // node 2 only has space for one shard
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(
                fsInfoPath,
                1000L,
                discoveryNode.getId().equals(nodeIds.get(2))
                    ? 150L - masterAppliedClusterState.get().getRoutingNodes().node(nodeIds.get(2)).numberOfOwningShards()
                    : 1000L
            )
        );

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put("number_of_shards", 6)
                    .put("number_of_replicas", 0)
                    .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_id").getKey(), nodeIds.get(2))
            )
        );
        ensureGreen("test");

        assertBusy(() -> {
            final Map<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has 3 shards", shardCountByNodeId.get(nodeIds.get(0)), equalTo(3));
            assertThat("node1 has 3 shards", shardCountByNodeId.get(nodeIds.get(1)), equalTo(3));
            assertThat("node2 has 0 shards", shardCountByNodeId.get(nodeIds.get(2)), equalTo(0));
        });

        updateIndexSettings(
            Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_id").getKey()),
            "test"
        );

        logger.info("--> waiting for shards to relocate onto node [{}]", nodeIds.get(2));

        ensureGreen("test");
        assertThat("node2 has 1 shard", getShardCountByNodeId().get(nodeIds.get(2)), equalTo(1));
    }

    public void testMovesShardsOffSpecificDataPathAboveWatermark() throws Exception {

        // start one node with two data paths
        final Path pathOverWatermark = createTempDir();
        final Settings.Builder twoPathSettings = Settings.builder();
        if (randomBoolean()) {
            twoPathSettings.putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString(), pathOverWatermark.toString());
        } else {
            twoPathSettings.putList(Environment.PATH_DATA_SETTING.getKey(), pathOverWatermark.toString(), createTempDir().toString());
        }
        internalCluster().startNode(twoPathSettings);
        final String nodeWithTwoPaths = clusterAdmin().prepareNodesInfo().get().getNodes().get(0).getNode().getId();

        // other two nodes have one data path each
        internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), createTempDir()));
        internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), createTempDir()));

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();

        // prevent any effects from in-flight recoveries, since we are only simulating a 100-byte disk
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 0L);

        // start with all paths below the watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 100, between(10, 100)));

        updateClusterSettings(
            Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "90%")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "100%")
                .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
        );

        final List<String> nodeIds = clusterAdmin().prepareState()
            .get()
            .getState()
            .getRoutingNodes()
            .stream()
            .map(RoutingNode::nodeId)
            .toList();

        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));

        ensureGreen("test");

        {
            final Map<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has 2 shards", shardCountByNodeId.get(nodeIds.get(0)), equalTo(2));
            assertThat("node1 has 2 shards", shardCountByNodeId.get(nodeIds.get(1)), equalTo(2));
            assertThat("node2 has 2 shards", shardCountByNodeId.get(nodeIds.get(2)), equalTo(2));
        }

        final long shardsOnGoodPath = Arrays.stream(indicesAdmin().prepareStats("test").get().getShards())
            .filter(
                shardStats -> shardStats.getShardRouting().currentNodeId().equals(nodeWithTwoPaths)
                    && shardStats.getDataPath().startsWith(pathOverWatermark.toString()) == false
            )
            .count();
        logger.info("--> shards on good path: [{}]", shardsOnGoodPath);

        // disable rebalancing, or else we might move shards back onto the over-full path since we're not faking that
        updateClusterSettings(
            Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        );

        // one of the paths on node0 suddenly exceeds the high watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(
                fsInfoPath,
                100L,
                fsInfoPath.getPath().startsWith(pathOverWatermark.toString()) ? between(0, 9) : between(10, 100)
            )
        );

        logger.info("--> waiting for shards to relocate off path [{}]", pathOverWatermark);

        assertBusy(() -> {
            for (final ShardStats shardStats : indicesAdmin().prepareStats("test").get().getShards()) {
                assertThat(shardStats.getDataPath(), not(startsWith(pathOverWatermark.toString())));
            }
        });

        ensureGreen("test");

        for (final ShardStats shardStats : indicesAdmin().prepareStats("test").get().getShards()) {
            assertThat(shardStats.getDataPath(), not(startsWith(pathOverWatermark.toString())));
        }

        assertThat(
            "should not have moved any shards off of the path that wasn't too full",
            Arrays.stream(indicesAdmin().prepareStats("test").get().getShards())
                .filter(
                    shardStats -> shardStats.getShardRouting().currentNodeId().equals(nodeWithTwoPaths)
                        && shardStats.getDataPath().startsWith(pathOverWatermark.toString()) == false
                )
                .count(),
            equalTo(shardsOnGoodPath)
        );
    }

    private Map<String, Integer> getShardCountByNodeId() {
        final Map<String, Integer> shardCountByNodeId = new HashMap<>();
        final ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        for (final RoutingNode node : clusterState.getRoutingNodes()) {
            logger.info(
                "----> node {} has {} shards",
                node.nodeId(),
                clusterState.getRoutingNodes().node(node.nodeId()).numberOfOwningShards()
            );
            shardCountByNodeId.put(node.nodeId(), clusterState.getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
        }
        return shardCountByNodeId;
    }

    private MockInternalClusterInfoService getMockInternalClusterInfoService() {
        return (MockInternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
    }

}
