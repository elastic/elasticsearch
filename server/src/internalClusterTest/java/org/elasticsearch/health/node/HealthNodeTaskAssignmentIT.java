/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.NodeShutdownTestUtils;

import java.util.EnumSet;

import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for health node task assignment during rolling shutdowns and
 * feature enable/disable cycles.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class HealthNodeTaskAssignmentIT extends ESIntegTestCase {

    /**
     * Verifies that the health node task is always assigned and succeeds as expected during rolling node shutdowns.
     */
    public void testHealthNodeReassignedOnShutdown() {
        final var dataNodeNames = internalCluster().clusterService().state().nodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        assertThat("exactly 2 data nodes", dataNodeNames.length, equalTo(2));

        try {
            // Wait for health task to be initially assigned
            awaitClusterState(state -> HealthNode.findHealthNode(state) != null);
            final var initialHealthNode = HealthNode.findHealthNode(internalCluster().clusterService().state());
            assertNotNull("health node should now be assigned", initialHealthNode);

            final int passes = randomIntBetween(1, 4);
            var currentHealthNodeName = initialHealthNode.getName();
            for (int pass = 0; pass < passes; pass++) {
                final var otherNode = dataNodeNames[0].getName().equals(currentHealthNodeName) ? dataNodeNames[1] : dataNodeNames[0];
                logger.info(
                    "Pass {}: marking [{}] for shutdown, expecting reassignment to [{}]",
                    pass,
                    currentHealthNodeName,
                    otherNode.getName()
                );

                try (var mockLog = MockLog.capture(HealthNodeTaskExecutor.class)) {
                    mockLog.addExpectation(
                        new MockLog.SeenEventExpectation(
                            "health node selected on [" + otherNode.getName() + "]",
                            HealthNodeTaskExecutor.class.getCanonicalName(),
                            Level.INFO,
                            "Node [" + otherNode.getShortNodeDescription() + "] is selected as the current health node*"
                        )
                    );
                    NodeShutdownTestUtils.putShutdownMetadata(
                        currentHealthNodeName,
                        internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
                        EnumSet.of(
                            SingleNodeShutdownMetadata.Type.REMOVE,
                            SingleNodeShutdownMetadata.Type.SIGTERM,
                            SingleNodeShutdownMetadata.Type.RESTART
                        )
                    );
                    awaitClusterState(state -> {
                        final var healthNode = HealthNode.findHealthNode(state);
                        assertNotNull("health task must never be unassigned", healthNode);
                        return healthNode.getName().equals(otherNode.getName());
                    });
                    mockLog.awaitAllExpectationsMatched();
                }

                final var newHealthNode = HealthNode.findHealthNode(internalCluster().clusterService().state());
                assertNotNull("health task must be assigned to [" + otherNode.getName() + "]", newHealthNode);
                assertThat(newHealthNode.getName(), equalTo(otherNode.getName()));

                NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
                currentHealthNodeName = otherNode.getName();
            }
        } finally {
            NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
        }
    }

    /**
     * Verifies that disabling the health node feature removes the task from the cluster
     * and that re-enabling it recreates and reassigns the task.
     */
    public void testHealthNodeTaskEnabledAndDisabled() {
        try {
            // Wait for health task to be initially assigned
            awaitClusterState(state -> HealthNode.findHealthNode(state) != null);

            updateClusterSettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), false));
            awaitClusterState(state -> HealthNode.findHealthNode(state) == null);

            try (var mockLog = MockLog.capture(HealthNodeTaskExecutor.class)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "health node selected",
                        HealthNodeTaskExecutor.class.getCanonicalName(),
                        Level.INFO,
                        "*is selected as the current health node*"
                    )
                );

                updateClusterSettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), true));
                awaitClusterState(state -> HealthNode.findHealthNode(state) != null);
                mockLog.awaitAllExpectationsMatched();
            }
            assertNotNull(
                "health task must be reassigned after re-enabling",
                HealthNode.findHealthNode(internalCluster().clusterService().state())
            );

        } finally {
            updateClusterSettings(Settings.builder().putNull(HealthNodeTaskExecutor.ENABLED_SETTING.getKey()));
        }
    }
}
