/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for {@link IndexBalanceMetricsTaskExecutor} verifying task lifecycle and metric publication
 * in a multi-node cluster with index and search roles.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexBalanceMetricsIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        var settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndexBalanceMetricsTaskExecutor.INDEX_BALANCE_METRICS_ENABLED_SETTING.getKey(), true)
            .put(IndexBalanceMetricsTaskExecutor.INDEX_BALANCE_METRICS_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200))
            .build();
        return NodeRoles.onlyRoles(
            settings,
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE)
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(TestTelemetryPlugin.class)).toList();
    }

    public void testIndexBalanceMetricsTaskStartedAndAssigned() {
        internalCluster().startNode();
        ensureGreen();
        awaitClusterState(state -> {
            PersistentTasksCustomMetadata.PersistentTask<?> task = IndexBalanceMetricsTaskExecutor.Task.findTask(state);
            return task != null && task.isAssigned();
        });
    }

    public void testTaskReassignedWhenNodeShutsDown() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();
        ensureGreen();
        awaitClusterState(state -> {
            var task = IndexBalanceMetricsTaskExecutor.Task.findTask(state);
            return task != null && task.isAssigned();
        });
        final var clusterService = internalCluster().getInstance(ClusterService.class);
        final var task = IndexBalanceMetricsTaskExecutor.Task.findTask(clusterService.state());
        final var executorNodeId = task.getAssignment().getExecutorNode();
        final var executorNodeName = clusterService.state().nodes().get(executorNodeId).getName();
        internalCluster().stopNode(executorNodeName);
        ensureGreen();
        awaitClusterState(state -> {
            var reassignedTask = IndexBalanceMetricsTaskExecutor.Task.findTask(state);
            return reassignedTask != null
                && reassignedTask.isAssigned()
                && executorNodeId.equals(reassignedTask.getAssignment().getExecutorNode()) == false;
        });
    }

    public void testDynamicEnableDisable() throws Exception {
        final int numNodes = between(1, 5);
        final int numReplicas = between(0, numNodes - 1);
        internalCluster().startNodes(numNodes);
        ensureGreen();

        // Create some indices so the metrics have values to report.
        final int numIndices = between(1, 3);
        for (int i = 0; i < numIndices; i++) {
            prepareCreate(randomIndexName()).setSettings(indexSettings(1, numReplicas)).get();
        }
        ensureGreen();

        // The task should already be running (enabled=true from nodeSettings).
        awaitClusterState(state -> {
            var t = IndexBalanceMetricsTaskExecutor.Task.findTask(state);
            return t != null && t.isAssigned();
        });
        final var clusterService = internalCluster().getInstance(ClusterService.class);
        final var task = IndexBalanceMetricsTaskExecutor.Task.findTask(clusterService.state());
        final var executorNodeId = task.getAssignment().getExecutorNode();
        final var executorNodeName = clusterService.state().nodes().get(executorNodeId).getName();
        final var telemetryPlugin = getTelemetryPlugin(executorNodeName);

        // While enabled, the executor node should publish metrics for all indices.
        assertBusy(() -> {
            telemetryPlugin.resetMeter();
            telemetryPlugin.collect();
            assertImbalanceMetrics(telemetryPlugin, "primary", numIndices);
            assertImbalanceMetrics(telemetryPlugin, "replica", numIndices);
        });

        // Disable the feature dynamically and verify the persistent task is removed.
        updateClusterSettings(
            Settings.builder().put(IndexBalanceMetricsTaskExecutor.INDEX_BALANCE_METRICS_ENABLED_SETTING.getKey(), false)
        );
        awaitClusterState(state -> IndexBalanceMetricsTaskExecutor.Task.findTask(state) == null);

        // With executorNodeTask cleared, gauges should now return no measurements.
        assertBusy(() -> {
            telemetryPlugin.resetMeter();
            telemetryPlugin.collect();
            for (var tier : new String[] { "primary", "replica" }) {
                for (var name : IndexBalanceMetricsComputer.metricNames(tier)) {
                    assertThat(
                        name + " should publish no measurements when disabled",
                        telemetryPlugin.getLongGaugeMeasurement(name),
                        empty()
                    );
                }
            }
        });

        // Re-enable and verify the task is created and metrics are published again.
        updateClusterSettings(Settings.builder().put(IndexBalanceMetricsTaskExecutor.INDEX_BALANCE_METRICS_ENABLED_SETTING.getKey(), true));
        awaitClusterState(state -> {
            var t = IndexBalanceMetricsTaskExecutor.Task.findTask(state);
            return t != null && t.isAssigned();
        });
        final var reEnabledTask = IndexBalanceMetricsTaskExecutor.Task.findTask(clusterService.state());
        final var reEnabledNodeId = reEnabledTask.getAssignment().getExecutorNode();
        final var reEnabledNodeName = clusterService.state().nodes().get(reEnabledNodeId).getName();
        final var reEnabledTelemetryPlugin = getTelemetryPlugin(reEnabledNodeName);
        assertBusy(() -> {
            reEnabledTelemetryPlugin.resetMeter();
            reEnabledTelemetryPlugin.collect();
            assertImbalanceMetrics(reEnabledTelemetryPlugin, "primary", numIndices);
            assertImbalanceMetrics(reEnabledTelemetryPlugin, "replica", numIndices);
        });
    }

    public void testImbalanceMetricsPublished() throws Exception {
        final int numNodes = between(2, 5);
        final int numIndices = between(1, 5);
        final int numPrimaries = between(1, 3);
        final int numReplicas = between(1, numNodes - 1);
        final var allNodeNames = internalCluster().startNodes(numNodes);
        ensureGreen();

        for (int i = 0; i < numIndices; i++) {
            prepareCreate(randomIndexName()).setSettings(indexSettings(numPrimaries, numReplicas)).get();
        }
        ensureGreen();

        awaitClusterState(state -> {
            var task = IndexBalanceMetricsTaskExecutor.Task.findTask(state);
            return task != null && task.isAssigned();
        });
        final var clusterService = internalCluster().getInstance(ClusterService.class);
        final var task = IndexBalanceMetricsTaskExecutor.Task.findTask(clusterService.state());
        final var executorNodeId = task.getAssignment().getExecutorNode();
        final var executorNodeName = clusterService.state().nodes().get(executorNodeId).getName();
        final var telemetryPlugin = getTelemetryPlugin(executorNodeName);

        assertBusy(() -> {
            telemetryPlugin.resetMeter();
            telemetryPlugin.collect();
            assertImbalanceMetrics(telemetryPlugin, "primary", numIndices);
            assertImbalanceMetrics(telemetryPlugin, "replica", numIndices);
        });

        // Only the executor node should be publishing metrics
        for (String otherNodeName : allNodeNames) {
            if (otherNodeName.equals(executorNodeName)) {
                continue;
            }
            final var otherNodeTelemetryPlugin = getTelemetryPlugin(otherNodeName);
            otherNodeTelemetryPlugin.collect();
            for (var tier : new String[] { "primary", "replica" }) {
                for (var name : IndexBalanceMetricsComputer.metricNames(tier)) {
                    assertThat(otherNodeTelemetryPlugin.getLongGaugeMeasurement(name), empty());
                }
            }
        }
    }

    private static void assertImbalanceMetrics(TestTelemetryPlugin plugin, String tier, int expectedTotal) {
        long sum = 0;
        for (var name : IndexBalanceMetricsComputer.metricNames(tier)) {
            var measurements = plugin.getLongGaugeMeasurement(name);
            assertThat(name + " should have exactly one measurement", measurements, hasSize(1));
            sum += measurements.get(0).getLong();
        }
        assertThat("bucket values should sum to number of indices", sum, equalTo((long) expectedTotal));
    }

    private static TestTelemetryPlugin getTelemetryPlugin(String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
    }
}
