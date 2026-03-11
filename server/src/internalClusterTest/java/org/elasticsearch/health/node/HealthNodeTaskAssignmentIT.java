/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NodeShutdownTestUtils;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

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
        final var dataNodeNames = getDataNodeNames();
        assertThat("exactly 2 data nodes", dataNodeNames.length, equalTo(2));

        try {
            // Wait for health task to be initially assigned
            awaitClusterState(state -> HealthNode.findTask(state) != null && HealthNode.findHealthNode(state) != null);
            final var initialHealthNode = HealthNode.findHealthNode(internalCluster().clusterService().state());
            assertNotNull("health node should now be assigned", initialHealthNode);

            final int passes = randomIntBetween(1, 4);
            var currentHealthNodeName = initialHealthNode.getName();
            for (int pass = 0; pass < passes; pass++) {
                final var otherNodeName = dataNodeNames[0].equals(currentHealthNodeName) ? dataNodeNames[1] : dataNodeNames[0];
                final var shutdownNodeId = resolveNodeId(currentHealthNodeName);
                logger.info(
                    "Pass {}: marking [{}] for shutdown, expecting reassignment to [{}]",
                    pass,
                    currentHealthNodeName,
                    otherNodeName
                );

                final var healthUpdateLatch = new CountDownLatch(internalCluster().size() - 1);
                setUpHealthUpdatesHandler(otherNodeName, healthUpdateLatch);

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
                awaitClusterState(state -> {
                    final var healthNode = HealthNode.findHealthNode(state);
                    if (healthNode == null) {
                        unassigned.set(true);
                        return true;
                    }
                    return healthNode.getId().equals(shutdownNodeId) == false;
                });
                assertFalse("health task must never be unassigned", unassigned.get());

                safeAwait(healthUpdateLatch);
                clearHealthUpdateHandler(otherNodeName);

                final var newHealthNode = HealthNode.findHealthNode(internalCluster().clusterService().state());
                assertNotNull("health task must be assigned to [" + otherNodeName + "]", newHealthNode);
                assertThat(newHealthNode.getName(), equalTo(otherNodeName));
                assertAllNodesReportedHealthy(otherNodeName);

                NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
                currentHealthNodeName = otherNodeName;
            }
        } finally {
            NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
            clearHealthUpdateHandlers();
        }
    }

    /**
     * Verifies that disabling the health node feature removes the task from the cluster
     * and that re-enabling it recreates and reassigns the task.
     */
    public void testHealthNodeTaskEnabledAndDisabled() throws Exception {
        String[] dataNodeNames = getDataNodeNames();
        assertThat("exactly 2 data nodes", dataNodeNames.length, equalTo(2));

        try {
            // Wait for health task to be initially assigned
            awaitClusterState(state -> HealthNode.findTask(state) != null && HealthNode.findHealthNode(state) != null);

            updateClusterSettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), false));
            awaitClusterState(state -> HealthNode.findTask(state) == null && HealthNode.findHealthNode(state) == null);

            // Deterministically assign health node to dataNodeNames[1]
            NodeShutdownTestUtils.putShutdownMetadata(
                dataNodeNames[0],
                internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
                EnumSet.of(SingleNodeShutdownMetadata.Type.RESTART)
            );

            final var healthUpdateLatch = new CountDownLatch(internalCluster().size() - 1);
            setUpHealthUpdatesHandler(dataNodeNames[1], healthUpdateLatch);

            updateClusterSettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), true));

            safeAwait(healthUpdateLatch);

            final var reEnabledHealthNode = HealthNode.findHealthNode(internalCluster().clusterService().state());
            assertNotNull("health task must be reassigned after re-enabling", reEnabledHealthNode);
            assertThat(reEnabledHealthNode.getName(), equalTo(dataNodeNames[1]));
            assertAllNodesReportedHealthy(dataNodeNames[1]);

        } finally {
            clearHealthUpdateHandlers();
            NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
            updateClusterSettings(Settings.builder().putNull(HealthNodeTaskExecutor.ENABLED_SETTING.getKey()));
        }
    }

    private String resolveNodeId(String nodeName) {
        final DiscoveryNode node = internalCluster().clusterService().state().nodes().resolveNode(nodeName);
        assertNotNull("could not resolve node [" + nodeName + "] in cluster state", node);
        return node.getId();
    }

    private String[] getDataNodeNames() {
        return internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .toArray(String[]::new);
    }

    /**
     * Adds a mock transport handler on the target node that holds incoming {@link UpdateHealthInfoCacheAction}
     * requests until that node recognizes itself as the health node, then forwards them and counts down the latch.
     */
    private void setUpHealthUpdatesHandler(String nodeName, CountDownLatch latch) {
        var transportService = MockTransportService.getInstance(nodeName);
        var nodeClusterService = internalCluster().clusterService(nodeName);
        transportService.addRequestHandlingBehavior(UpdateHealthInfoCacheAction.NAME, (handler, request, channel, task) -> {
            ClusterServiceUtils.addTemporaryStateListener(nodeClusterService, state -> {
                final var healthNode = HealthNode.findHealthNode(state);
                return healthNode != null && HealthNode.findTask(state) != null && nodeName.equals(healthNode.getName());
            }).addListener(ActionListener.wrap(v -> {
                handler.messageReceived(request, channel, task);
                latch.countDown();
            }, ESTestCase::fail));
        });
    }

    private void clearHealthUpdateHandler(String nodeName) {
        MockTransportService.getInstance(nodeName).clearAllRules();
    }

    private void clearHealthUpdateHandlers() {
        for (String nodeName : internalCluster().getNodeNames()) {
            clearHealthUpdateHandler(nodeName);
        }
    }

    private void assertAllNodesReportedHealthy(String healthNodeName) throws Exception {
        // Brief delay in case the health node is slow at reporting its own health status.
        // In practice, we expect the first check to succeed.
        assertBusy(() -> {
            final var healthInfo = internalCluster().client(healthNodeName)
                .execute(FetchHealthInfoCacheAction.INSTANCE, new FetchHealthInfoCacheAction.Request())
                .actionGet()
                .getHealthInfo();
            final Map<String, DiskHealthInfo> diskInfo = healthInfo.diskInfoByNode();
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
        }, 3, TimeUnit.SECONDS);
    }
}
