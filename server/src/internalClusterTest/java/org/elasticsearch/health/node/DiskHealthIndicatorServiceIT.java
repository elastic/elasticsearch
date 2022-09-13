/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskHealthIndicatorServiceIT extends ESIntegTestCase {

    public void testGreen() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            internalCluster.startMasterOnlyNode();
            internalCluster.startDataOnlyNode();
            ensureStableCluster(internalCluster.getNodeNames().length);
            waitForAllNodesToReportHealth();
            for (String node : internalCluster.getNodeNames()) {
                HealthService healthService = internalCluster.getInstance(HealthService.class, node);
                List<HealthIndicatorResult> resultList = healthService.getHealth(
                    // internalCluster.client(node), //TODO: add this line
                    DiskHealthIndicatorService.NAME,
                    true
                );
                assertNotNull(resultList);
                assertThat(resultList.size(), equalTo(1));
                HealthIndicatorResult testIndicatorResult = resultList.get(0);
                assertThat(testIndicatorResult.status(), equalTo(HealthStatus.GREEN));
                assertThat(testIndicatorResult.symptom(), equalTo("Disk usage is within configured thresholds"));
            }
        }
    }

    public void testRed() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            internalCluster.startMasterOnlyNode(getVeryLowWatermarksSettings());
            internalCluster.startDataOnlyNode(getVeryLowWatermarksSettings());
            ensureStableCluster(internalCluster.getNodeNames().length);
            waitForAllNodesToReportHealth();
            for (String node : internalCluster.getNodeNames()) {
                HealthService healthService = internalCluster.getInstance(HealthService.class, node);
                List<HealthIndicatorResult> resultList = healthService.getHealth(
                    // internalCluster.client(node), //TODO: add this line
                    DiskHealthIndicatorService.NAME,
                    true
                );
                assertNotNull(resultList);
                assertThat(resultList.size(), equalTo(1));
                HealthIndicatorResult testIndicatorResult = resultList.get(0);
                assertThat(testIndicatorResult.status(), equalTo(HealthStatus.RED));
                assertThat(testIndicatorResult.symptom(), equalTo("2 nodes are out of disk space."));
            }
        }
    }

    private Settings getVeryLowWatermarksSettings() {
        return Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "0.5%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "0.5%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0.5%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .build();
    }

    private void waitForAllNodesToReportHealth() throws Exception {
        assertBusy(() -> {
            ClusterState state = internalCluster().client()
                .admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetadata(true)
                .setNodes(true)
                .get()
                .getState();
            DiscoveryNode healthNode = HealthNode.findHealthNode(state);
            assertNotNull(healthNode);
            Map<String, DiskHealthInfo> healthInfoCache = internalCluster().getInstance(HealthInfoCache.class, healthNode.getName())
                .getHealthInfo()
                .diskInfoByNode();
            assertThat(healthInfoCache.size(), equalTo(state.getNodes().getNodes().keySet().size()));
        });
    }
}
