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
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
            for (int i = 0; i < numberOfNodes; i++) {
                String customWatermark = percentageMode
                    ? randomIntBetween(86, 94) + "%"
                    : new ByteSizeValue(randomIntBetween(6, 19)).toString();
                String nodeName = startNode(internalCluster, customWatermark);
                watermarkByNode.put(nodeName, customWatermark);
            }
            ensureStableCluster(numberOfNodes);

            String electedMaster = internalCluster.getMasterName();
            {
                HealthMetadata.Disk diskMetadata = HealthMetadata.getFromClusterState(internalCluster.clusterService().state())
                    .getDiskMetadata();
                assertThat(diskMetadata.describeHighWatermark(), equalTo(watermarkByNode.get(electedMaster)));
            }

            // Stop the master to ensure another node will become master with a different watermark
            internalCluster.stopNode(electedMaster);
            ensureStableCluster(numberOfNodes - 1);
            electedMaster = internalCluster.getMasterName();
            {
                HealthMetadata.Disk diskMetadata = HealthMetadata.getFromClusterState(internalCluster.clusterService().state())
                    .getDiskMetadata();
                assertThat(diskMetadata.describeHighWatermark(), equalTo(watermarkByNode.get(electedMaster)));
            }
        }
    }

    public void testWatermarkSettingUpdate() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            int numberOfNodes = 3;
            String initialWatermark = percentageMode
                ? randomIntBetween(86, 94) + "%"
                : new ByteSizeValue(randomIntBetween(6, 19)).toString();
            for (int i = 0; i < numberOfNodes; i++) {
                startNode(internalCluster, initialWatermark);
            }

            String updatedLowWatermark = percentageMode
                ? randomIntBetween(40, 59) + "%"
                : new ByteSizeValue(randomIntBetween(101, 200)).toString();
            String updatedHighWatermark = percentageMode
                ? randomIntBetween(60, 90) + "%"
                : new ByteSizeValue(randomIntBetween(50, 100)).toString();
            String updatedFloodStageWatermark = percentageMode
                ? randomIntBetween(91, 95) + "%"
                : new ByteSizeValue(randomIntBetween(5, 10)).toString();

            ensureStableCluster(numberOfNodes);
            {
                HealthMetadata.Disk diskMetadata = HealthMetadata.getFromClusterState(internalCluster.clusterService().state())
                    .getDiskMetadata();
                assertThat(diskMetadata.describeHighWatermark(), equalTo(initialWatermark));
            }
            internalCluster.client()
                .admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder()
                            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), updatedLowWatermark)
                            .put(
                                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                                updatedHighWatermark
                            )
                            .put(
                                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                                updatedFloodStageWatermark
                            )
                    )
                )
                .actionGet();
            assertBusy(() -> {
                HealthMetadata.Disk diskMetadata = HealthMetadata.getFromClusterState(internalCluster.clusterService().state())
                    .getDiskMetadata();
                assertThat(diskMetadata.describeHighWatermark(), equalTo(updatedHighWatermark));
                assertThat(diskMetadata.describeFloodStageWatermark(), equalTo(updatedFloodStageWatermark));
            });
        }
    }

    private String startNode(InternalTestCluster internalCluster, String customWatermark) {
        return internalCluster.startNode(
            Settings.builder()
                .put(onlyRoles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)))
                .put(createWatermarkSettings(customWatermark))
                .build()
        );
    }

    private Settings createWatermarkSettings(String highWatermark) {
        // We define both thresholds to avoid inconsistencies over the type of the thresholds
        return Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), percentageMode ? "85%" : "20b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), highWatermark)
            .put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                percentageMode ? "95%" : "1b"
            )
            .put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                percentageMode ? "95%" : "5b"
            )
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey(), "5b")
            .build();
    }
}
