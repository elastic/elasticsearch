/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN;
import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class HealthMetadataServiceIT extends ESIntegTestCase {

    private volatile boolean percentageMode;

    @Before
    public void setup() {
        percentageMode = randomBoolean();
    }

    public void testEachMasterPublishesTheirThresholds() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            int numberOfNodes = 3;
            Map<String, String> watermarkByNode = new HashMap<>();
            Map<String, ByteSizeValue> maxHeadroomByNode = new HashMap<>();
            Map<String, HealthMetadata.ShardLimits> shardLimitsPerNode = new HashMap<>();
            for (int i = 0; i < numberOfNodes; i++) {
                ByteSizeValue randomBytes = ByteSizeValue.ofBytes(randomLongBetween(6, 19));
                String customWatermark = percentageMode ? randomIntBetween(86, 94) + "%" : randomBytes.toString();
                var customShardLimits = new HealthMetadata.ShardLimits(randomIntBetween(1, 1000), randomIntBetween(1001, 2000));
                String nodeName = startNode(internalCluster, customWatermark, randomBytes.toString(), customShardLimits);
                watermarkByNode.put(nodeName, customWatermark);
                maxHeadroomByNode.put(nodeName, randomBytes);
                shardLimitsPerNode.put(nodeName, customShardLimits);
            }
            ensureStableCluster(numberOfNodes);

            String electedMaster = internalCluster.getMasterName();
            {
                var healthMetadata = HealthMetadata.getFromClusterState(internalCluster.clusterService().state());
                var diskMetadata = healthMetadata.getDiskMetadata();
                assertThat(diskMetadata.describeHighWatermark(), equalTo(watermarkByNode.get(electedMaster)));
                // The value of the setting `cluster.routing.allocation.disk.watermark.high.max_headroom` depends upon the existence of
                // `cluster.routing.allocation.disk.watermark.high`. Check {@link CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING}
                assertThat(
                    diskMetadata.highMaxHeadroom(),
                    equalTo(percentageMode ? maxHeadroomByNode.get(electedMaster) : ByteSizeValue.ofGb(150))
                );

                var shardLimitsMetadata = healthMetadata.getShardLimitsMetadata();
                assertEquals(shardLimitsMetadata, shardLimitsPerNode.get(electedMaster));
            }

            // Stop the master to ensure another node will become master with a different watermark
            internalCluster.stopNode(electedMaster);
            ensureStableCluster(numberOfNodes - 1);
            electedMaster = internalCluster.getMasterName();
            {
                var healthMetadata = HealthMetadata.getFromClusterState(internalCluster.clusterService().state());
                var diskMetadata = healthMetadata.getDiskMetadata();
                assertThat(diskMetadata.describeHighWatermark(), equalTo(watermarkByNode.get(electedMaster)));

                // The value of the setting `cluster.routing.allocation.disk.watermark.high.max_headroom` depends upon the existence of
                // `cluster.routing.allocation.disk.watermark.high`.
                // Check {@link DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING}
                assertThat(
                    diskMetadata.highMaxHeadroom(),
                    equalTo(percentageMode ? maxHeadroomByNode.get(electedMaster) : ByteSizeValue.ofGb(150))
                );

                var shardLimitsMetadata = healthMetadata.getShardLimitsMetadata();
                assertEquals(shardLimitsMetadata, shardLimitsPerNode.get(electedMaster));
            }
        }
    }

    public void testWatermarkSettingUpdate() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            int numberOfNodes = 3;
            ByteSizeValue randomBytes = ByteSizeValue.ofBytes(randomLongBetween(6, 19));
            String initialWatermark = percentageMode ? randomIntBetween(86, 94) + "%" : randomBytes.toString();
            ByteSizeValue initialMaxHeadroom = randomBytes;
            HealthMetadata.ShardLimits initialShardLimits = new HealthMetadata.ShardLimits(
                randomIntBetween(1, 1000),
                randomIntBetween(1001, 2000)
            );
            for (int i = 0; i < numberOfNodes; i++) {
                startNode(internalCluster, initialWatermark, initialMaxHeadroom.toString(), initialShardLimits);
            }

            randomBytes = ByteSizeValue.ofBytes(randomLongBetween(101, 200));
            String updatedLowWatermark = percentageMode ? randomIntBetween(40, 59) + "%" : randomBytes.toString();
            ByteSizeValue updatedLowMaxHeadroom = percentageMode ? randomBytes : ByteSizeValue.MINUS_ONE;
            randomBytes = ByteSizeValue.ofBytes(randomLongBetween(50, 100));
            String updatedHighWatermark = percentageMode ? randomIntBetween(60, 90) + "%" : randomBytes.toString();
            ByteSizeValue updatedHighMaxHeadroom = randomBytes;
            randomBytes = ByteSizeValue.ofBytes(randomLongBetween(5, 10));
            String updatedFloodStageWatermark = percentageMode ? randomIntBetween(91, 95) + "%" : randomBytes.toString();
            ByteSizeValue updatedFloodStageMaxHeadroom = percentageMode ? randomBytes : ByteSizeValue.MINUS_ONE;
            HealthMetadata.ShardLimits updatedShardLimits = new HealthMetadata.ShardLimits(
                randomIntBetween(3000, 4000),
                randomIntBetween(4001, 5000)
            );

            ensureStableCluster(numberOfNodes);
            {
                var healthMetadata = HealthMetadata.getFromClusterState(internalCluster.clusterService().state());
                var diskMetadata = healthMetadata.getDiskMetadata();
                assertThat(diskMetadata.describeHighWatermark(), equalTo(initialWatermark));

                // The value of the setting `cluster.routing.allocation.disk.watermark.high.max_headroom` depends upon the existence of
                // `cluster.routing.allocation.disk.watermark.high`.
                // Check {@link DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING}
                assertThat(diskMetadata.highMaxHeadroom(), equalTo(percentageMode ? initialMaxHeadroom : ByteSizeValue.ofGb(150)));

                var shardLimitsMetadata = healthMetadata.getShardLimitsMetadata();
                assertEquals(shardLimitsMetadata, initialShardLimits);
            }
            Settings.Builder builder = Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), updatedLowWatermark)
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), updatedHighWatermark)
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), updatedFloodStageWatermark)
                .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), updatedShardLimits.maxShardsPerNode())
                .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey(), updatedShardLimits.maxShardsPerNodeFrozen());
            if (percentageMode) {
                builder = builder.put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), updatedLowMaxHeadroom)
                    .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), updatedHighMaxHeadroom)
                    .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), updatedFloodStageMaxHeadroom);
            }
            internalCluster.client()
                .admin()
                .cluster()
                .updateSettings(new ClusterUpdateSettingsRequest().persistentSettings(builder))
                .actionGet();

            assertBusy(() -> {
                var healthMetadata = HealthMetadata.getFromClusterState(internalCluster.clusterService().state());
                var diskMetadata = healthMetadata.getDiskMetadata();
                assertThat(diskMetadata.describeHighWatermark(), equalTo(updatedHighWatermark));

                // The value of the setting `cluster.routing.allocation.disk.watermark.high.max_headroom` depends upon the existence of
                // `cluster.routing.allocation.disk.watermark.high`.
                // Check {@link DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING}
                assertThat(diskMetadata.highMaxHeadroom(), equalTo(percentageMode ? updatedHighMaxHeadroom : ByteSizeValue.ofGb(150)));
                assertThat(diskMetadata.describeFloodStageWatermark(), equalTo(updatedFloodStageWatermark));

                // The value of the setting `cluster.routing.allocation.disk.watermark.flood_stage.max_headroom` depends upon the existence
                // of `cluster.routing.allocation.disk.watermark.flood_stage`.
                // Check{@link DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING}
                assertThat(
                    diskMetadata.floodStageMaxHeadroom(),
                    equalTo(percentageMode ? updatedFloodStageMaxHeadroom : ByteSizeValue.ofGb(100))
                );

                var shardLimitsMetadata = healthMetadata.getShardLimitsMetadata();
                assertEquals(shardLimitsMetadata, updatedShardLimits);
            });
        }
    }

    private String startNode(
        InternalTestCluster internalCluster,
        String customWatermark,
        String customMaxHeadroom,
        HealthMetadata.ShardLimits customShardLimits
    ) {
        return internalCluster.startNode(
            Settings.builder()
                .put(onlyRoles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)))
                .put(createWatermarkSettings(customWatermark, customMaxHeadroom))
                .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), customShardLimits.maxShardsPerNode())
                .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey(), customShardLimits.maxShardsPerNodeFrozen())
                .build()
        );
    }

    private Settings createWatermarkSettings(String highWatermark, String highMaxHeadroom) {
        // We define both thresholds to avoid inconsistencies over the type of the thresholds
        Settings.Builder settings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), percentageMode ? "85%" : "20b")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), highWatermark)
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), percentageMode ? "95%" : "1b")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(), percentageMode ? "95%" : "5b");

        if (percentageMode) {
            settings = settings.put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "20b")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "1b")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey(), "5b");
            if (highMaxHeadroom.equals("-1") == false) {
                settings = settings.put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), highMaxHeadroom);
            }
        }
        return settings.build();
    }
}
