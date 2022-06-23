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
                    ? randomIntBetween(60, 80) + "%"
                    : new ByteSizeValue(randomIntBetween(10, 100)).toString();
                String nodeName = startNode(internalCluster, customWatermark);
                watermarkByNode.put(nodeName, customWatermark);
            }
            ensureStableCluster(numberOfNodes);

            String electedMaster = internalCluster.getMasterName();
            {
                HealthMetadata.DiskHealthThresholds.Threshold lowWatermark = HealthMetadata.getHealthCustomMetadata(
                    internalCluster.clusterService().state()
                ).getDiskThresholds().lowWatermark();
                assertThat(describeThreshold(lowWatermark), equalTo(watermarkByNode.get(electedMaster)));
            }

            // Stop the master to ensure another node will be come master with a different watermark
            internalCluster.stopNode(electedMaster);
            ensureStableCluster(numberOfNodes - 1);
            electedMaster = internalCluster.getMasterName();
            {
                HealthMetadata.DiskHealthThresholds.Threshold lowWatermark = HealthMetadata.getHealthCustomMetadata(
                    internalCluster.clusterService().state()
                ).getDiskThresholds().lowWatermark();
                assertThat(describeThreshold(lowWatermark), equalTo(watermarkByNode.get(electedMaster)));
            }
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

    public void testThresholdSettingUpdate() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            int numberOfNodes = 3;
            String initialWatermark = percentageMode
                ? randomIntBetween(60, 80) + "%"
                : new ByteSizeValue(randomIntBetween(10, 100)).toString();
            for (int i = 0; i < numberOfNodes; i++) {
                startNode(internalCluster, initialWatermark);
            }

            String updatedWatermark = percentageMode
                ? randomIntBetween(40, 59) + "%"
                : new ByteSizeValue(randomIntBetween(101, 200)).toString();

            ensureStableCluster(numberOfNodes);
            {
                HealthMetadata.DiskHealthThresholds.Threshold lowWatermark = HealthMetadata.getHealthCustomMetadata(
                    internalCluster.clusterService().state()
                ).getDiskThresholds().lowWatermark();
                assertThat(describeThreshold(lowWatermark), equalTo(initialWatermark));
            }
            internalCluster.client()
                .admin()
                .cluster()
                .updateSettings(new ClusterUpdateSettingsRequest().persistentSettings(createWatermarkSettings(updatedWatermark)))
                .actionGet();
            assertBusy(() -> {
                HealthMetadata.DiskHealthThresholds.Threshold lowWatermark = HealthMetadata.getHealthCustomMetadata(
                    internalCluster.clusterService().state()
                ).getDiskThresholds().lowWatermark();
                assertThat(describeThreshold(lowWatermark), equalTo(updatedWatermark));
            });
        }
    }

    private Settings createWatermarkSettings(String lowWatermark) {
        return Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), lowWatermark)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), percentageMode ? "90%" : "5b")
            .put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                percentageMode ? "95%" : "1b"
            )
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_SETTING.getKey(), percentageMode ? "90%" : "5b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey(), "5b")
            .build();
    }

    private String describeThreshold(HealthMetadata.DiskHealthThresholds.Threshold threshold) {
        return percentageMode ? String.format("%.0f%%", threshold.maxPercentageUsed()) : threshold.minFreeBytes().toString();
    }
}
