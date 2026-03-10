/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for health node task assignment during rolling shutdowns and
 * feature enable/disable cycles.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class HealthNodeTaskAssignmentIT extends ESIntegTestCase {

    private static final DiskHealthInfo GREEN_DISK_HEALTH = new DiskHealthInfo(HealthStatus.GREEN, null);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LocalHealthMonitor.POLL_INTERVAL_SETTING.getKey(), LocalHealthMonitor.MIN_POLL_INTERVAL)
            .build();
    }

    /**
     * Verifies that the health node task is always assigned and succeeds as expected during rolling node shutdowns.
     */
    public void testHealthNodeReassignedOnShutdown() throws Exception {
        final var initialHealthNode = waitAndGetHealthNode(internalCluster());
        assertNotNull("health task must be assigned after cluster starts", initialHealthNode);
        waitForAllNodesToReportHealthy(initialHealthNode.getName());

        // Alternate shutdowns: each pass marks the current health node for shutdown,
        // verifies the task migrates to the other data node atomically.
        String currentHealthNodeName = initialHealthNode.getName();
        final int passes = randomIntBetween(2, 4);
        for (int pass = 0; pass < passes; pass++) {
            final String shutdownNodeId = resolveNodeId(currentHealthNodeName);
            logger.info("Pass {}: marking [{}] for shutdown, expecting health task to be reassigned", pass, currentHealthNodeName);

            NodeShutdownTestUtils.putShutdownMetadata(
                currentHealthNodeName,
                internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
                EnumSet.of(
                    SingleNodeShutdownMetadata.Type.REMOVE,
                    SingleNodeShutdownMetadata.Type.SIGTERM,
                    SingleNodeShutdownMetadata.Type.RESTART
                )
            );
            final var unassigned = new AtomicBoolean();
            try {
                awaitClusterState(state -> {
                    final var healthNode = HealthNode.findHealthNode(state);
                    if (healthNode == null) {
                        unassigned.set(true);
                        return true;
                    }
                    return healthNode.getId().equals(shutdownNodeId) == false;
                });
                assertFalse("health task must never be unassigned", unassigned.get());

                final var newHealthNode = HealthNode.findHealthNode(internalCluster().clusterService().state());
                assertNotNull("health task must not be unassigned", newHealthNode);
                assertNotEquals(
                    "health task must be reassigned after [" + currentHealthNodeName + "] is marked for shutdown",
                    newHealthNode.getId(),
                    shutdownNodeId
                );

                NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
                currentHealthNodeName = newHealthNode.getName();
            } finally {
                NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
            }
        }
        final var finalHealthNode = waitAndGetHealthNode(internalCluster());
        waitForAllNodesToReportHealthy(finalHealthNode.getName());
    }

    /**
     * Verifies that disabling the health node feature removes the task from the cluster
     * and that re-enabling it recreates and reassigns the task
     */
    public void testHealthNodeTaskEnabledAndDisabled() throws Exception {
        final var initialHealthNode = waitAndGetHealthNode(internalCluster());
        assertNotNull("health task must be assigned on startup", initialHealthNode);
        waitForAllNodesToReportHealthy(initialHealthNode.getName());

        try {
            // Disable the task
            updateClusterSettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), false));
            awaitClusterState(state -> HealthNode.findTask(state) == null && HealthNode.findHealthNode(state) == null);

            // Re-enable the task
            updateClusterSettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), true));

            final var reEnabledHealthNode = waitAndGetHealthNode(internalCluster());
            assertNotNull("health task must be reassigned after re-enabling", reEnabledHealthNode);
            waitForAllNodesToReportHealthy(reEnabledHealthNode.getName());
        } finally {
            updateClusterSettings(Settings.builder().putNull(HealthNodeTaskExecutor.ENABLED_SETTING.getKey()));
        }
    }

    private String resolveNodeId(String nodeName) {
        final DiscoveryNode node = internalCluster().clusterService().state().nodes().resolveNode(nodeName);
        assertNotNull("could not resolve node [" + nodeName + "] in cluster state", node);
        return node.getId();
    }

    /**
     * Blocks until every node in the cluster has reported health to the health node.
     * Retries up to 3 times, sleeping one full poll interval between attempts.
     */
    private void waitForAllNodesToReportHealthy(String healthNodeName) throws Exception {
        AssertionError lastFailure = null;
        final var maxAttempts = 3;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (attempt > 0) {
                safeSleep(LocalHealthMonitor.MIN_POLL_INTERVAL);
            }
            try {
                assertAllNodesReportedHealthy(healthNodeName);
                return;
            } catch (AssertionError e) {
                lastFailure = e;
            }
        }
        throw lastFailure;
    }

    private void assertAllNodesReportedHealthy(String healthNodeName) throws Exception {
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
    }
}
