/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for {@link IndexBalanceMetricsTask} verifying task lifecycle and metric publication
 * in a multi-node cluster with index and search roles.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexBalanceMetricsIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndexBalanceMetricsTask.INDEX_BALANCE_METRICS_ENABLED_SETTING.getKey(), true)
            .put(IndexBalanceMetricsTask.INDEX_BALANCE_METRIC_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200))
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master", "index", "search")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(TestTelemetryPlugin.class)).toList();
    }

    public void testIndexBalanceMetricsTaskStartedAndAssigned() throws Exception {
        internalCluster().startNode();
        ensureGreen();
        awaitClusterState(state -> IndexBalanceMetricsTask.Task.findTask(state) != null);
    }

    public void testTaskReassignedWhenNodeShutsDown() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();
        ensureGreen();
        awaitClusterState(state -> {
            var task = IndexBalanceMetricsTask.Task.findTask(state);
            return task != null && task.isAssigned();
        });
        final var clusterService = internalCluster().getInstance(ClusterService.class);
        final var task = IndexBalanceMetricsTask.Task.findTask(clusterService.state());
        final var executorNodeId = task.getAssignment().getExecutorNode();
        final var executorNodeName = clusterService.state().nodes().get(executorNodeId).getName();
        internalCluster().stopNode(executorNodeName);
        ensureGreen();
        awaitClusterState(state -> {
            var reassignedTask = IndexBalanceMetricsTask.Task.findTask(state);
            return reassignedTask != null
                && reassignedTask.isAssigned()
                && executorNodeId.equals(reassignedTask.getAssignment().getExecutorNode()) == false;
        });
    }

    public void testImbalanceMetricsPublished() throws Exception {
        final int numNodes = between(2, 5);
        final int numIndices = between(1, 5);
        final int numPrimaries = between(1, 3);
        final int numReplicas = between(1, numNodes - 1);
        internalCluster().startNodes(numNodes);
        ensureGreen();

        for (int i = 0; i < numIndices; i++) {
            prepareCreate(randomIndexName()).setSettings(indexSettings(numPrimaries, numReplicas)).get();
        }
        ensureGreen();

        awaitClusterState(state -> {
            var task = IndexBalanceMetricsTask.Task.findTask(state);
            return task != null && task.isAssigned();
        });
        final var clusterService = internalCluster().getInstance(ClusterService.class);
        final var task = IndexBalanceMetricsTask.Task.findTask(clusterService.state());
        final var executorNodeId = task.getAssignment().getExecutorNode();
        final var executorNodeName = clusterService.state().nodes().get(executorNodeId).getName();
        final var telemetryPlugin = getTelemetryPlugin(executorNodeName);

        assertBusy(() -> {
            telemetryPlugin.resetMeter();
            telemetryPlugin.collect();
            assertImbalanceMetrics(telemetryPlugin, IndexBalanceMetricsTask.PRIMARY_METRIC_NAMES, numIndices);
            assertImbalanceMetrics(telemetryPlugin, IndexBalanceMetricsTask.REPLICA_METRIC_NAMES, numIndices);
        });
    }

    private static void assertImbalanceMetrics(TestTelemetryPlugin plugin, String[] metricNames, int expectedTotal) {
        long sum = 0;
        for (String name : metricNames) {
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
