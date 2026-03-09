/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeShutdownTestUtils;

import java.util.EnumSet;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests verifying that the health node task is always assigned and that health API
 * requests succeed during rolling node shutdowns.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class HealthNodeAssignmentIT extends ESIntegTestCase {

    private static final DiskHealthInfo GREEN_DISK_HEALTH = new DiskHealthInfo(HealthStatus.GREEN, null);

    public void testHealthNodeReassignedOnShutdown() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String node1 = internalCluster().startDataOnlyNode();
        final String node2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        decreasePollingInterval();

        final DiscoveryNode initialHealthNode = waitAndGetHealthNode(internalCluster());
        assertNotNull("health task must be assigned after cluster starts", initialHealthNode);

        // Alternate shutdowns: each pass marks the current health node for shutdown and verifies
        // the task migrates to the other node, then clears the shutdown before the next pass.
        String currentHealthNodeName = initialHealthNode.getName();
        final var passes = randomIntBetween(1,4);
        for (int pass = 0; pass < passes; pass++) {
            final String shutdownNodeName = currentHealthNodeName;
            final String expectedNodeName = shutdownNodeName.equals(node1) ? node2 : node1;
            final String expectedNodeId = nodeId(expectedNodeName);

            logger.info("Pass {}: marking [{}] for shutdown, expecting health task on [{}]", pass, shutdownNodeName, expectedNodeName);

            NodeShutdownTestUtils.putShutdownMetadata(
                shutdownNodeName,
                internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
                EnumSet.of(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.SIGTERM, SingleNodeShutdownMetadata.Type.RESTART)
            );
            try {
                awaitClusterState(state -> {
                    final DiscoveryNode healthNode = HealthNode.findHealthNode(state);
                    assertNotNull("health task must never be unassigned", healthNode);
                    return healthNode.getId().equals(expectedNodeId);
                });

                final DiscoveryNode newHealthNode = HealthNode.findHealthNode(internalCluster().clusterService().state());
                assertNotNull("health task must never be unassigned", newHealthNode);
                assertThat(
                    "health task must be on [" + expectedNodeName + "] after [" + shutdownNodeName + "] is marked for shutdown",
                    newHealthNode.getId(),
                    equalTo(expectedNodeId)
                );

                NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
                waitForAllNodesToReportHealthy(newHealthNode.getName());
                currentHealthNodeName = newHealthNode.getName();
            } finally {
                NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
            }
        }
    }

    public void testHealthNodeStoppedOnDisableAndRestartedOnReEnable() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        final DiscoveryNode initialHealthNode = waitAndGetHealthNode(internalCluster());
        assertNotNull("health task must be assigned on startup", initialHealthNode);
        decreasePollingInterval();
        waitForAllNodesToReportHealthy(initialHealthNode.getName());

        // Disable on all nodes.
        updateClusterSettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), false));
        try {
            awaitClusterState(state -> HealthNode.findTask(state) == null);

            // Re-enable.
            updateClusterSettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), true));

            final DiscoveryNode reEnabledHealthNode = waitAndGetHealthNode(internalCluster());
            assertNotNull("health task must be reassigned after re-enabling", reEnabledHealthNode);
            waitForAllNodesToReportHealthy(reEnabledHealthNode.getName());
        } finally {
            updateClusterSettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), (String) null));
        }
    }

    private void decreasePollingInterval() {
        updateClusterSettings(
            Settings.builder().put(LocalHealthMonitor.POLL_INTERVAL_SETTING.getKey(), LocalHealthMonitor.MIN_POLL_INTERVAL)
        );
    }

    private String nodeId(String nodeName) {
        final ClusterState state = internalCluster().clusterService().state();
        final DiscoveryNode node = state.nodes().resolveNode(nodeName);
        assertNotNull("could not resolve node [" + nodeName + "] in cluster state", node);
        return node.getId();
    }

    /**
     * Blocks until every node in the cluster has reported health to the health node cache.
     */
    private void waitForAllNodesToReportHealthy(String healthNodeName) throws Exception {
        // todo(ines): can I get rid of the assertBusy here?
        assertBusy(() -> {
            final var healthResponse = internalCluster().client(healthNodeName)
                .execute(FetchHealthInfoCacheAction.INSTANCE, new FetchHealthInfoCacheAction.Request())
                .get();
            final Map<String, DiskHealthInfo> diskInfo = healthResponse.getHealthInfo().diskInfoByNode();
            final var state = internalCluster().clusterService().state();
            assertThat(
                "all cluster nodes must have reported health to node [" + healthNodeName + "]",
                diskInfo.size(),
                equalTo(state.nodes().size())
            );
            for (final String nodeId : state.nodes().getNodes().keySet()) {
                assertThat(
                        "node [" + nodeId + "] must have GREEN disk health in cache on [" + healthNodeName + "]",
                        diskInfo.get(nodeId),
                        equalTo(GREEN_DISK_HEALTH)
                );
            }
        });
    }
}
