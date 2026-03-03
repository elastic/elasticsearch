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
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

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
        assertBusy(() -> {
            assertThat(
                "index balance metrics task should be present in cluster state",
                IndexBalanceMetricsTask.Task.findTask(internalCluster().getInstance(ClusterService.class).state()),
                notNullValue()
            );
        });
    }

    public void testTaskReassignedWhenNodeShutsDown() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();
        ensureGreen();
        final var clusterService = internalCluster().getInstance(ClusterService.class);
        assertBusy(() -> {
            assertThat(
                "index balance metrics task should be present in cluster state",
                IndexBalanceMetricsTask.Task.findTask(clusterService.state()),
                notNullValue()
            );
        });
        final var task = IndexBalanceMetricsTask.Task.findTask(clusterService.state());
        assertThat("task should be assigned", task.isAssigned(), equalTo(true));
        final var executorNodeId = task.getAssignment().getExecutorNode();
        final var executorNodeName = clusterService.state().nodes().get(executorNodeId).getName();
        internalCluster().stopNode(executorNodeName);
        ensureGreen();
        assertBusy(() -> {
            final var remainingNodeClusterService = internalCluster().getInstance(ClusterService.class);
            final var reassignedTask = IndexBalanceMetricsTask.Task.findTask(remainingNodeClusterService.state());
            assertThat("task should still be present after node shutdown", reassignedTask, notNullValue());
            assertThat("task should be reassigned", reassignedTask.isAssigned(), equalTo(true));
            assertThat("task should run on the remaining node", reassignedTask.getAssignment().getExecutorNode(), notNullValue());
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

        final var clusterService = internalCluster().getInstance(ClusterService.class);
        assertBusy(() -> assertThat(IndexBalanceMetricsTask.Task.findTask(clusterService.state()), notNullValue()));

        final var task = IndexBalanceMetricsTask.Task.findTask(clusterService.state());
        final var executorNodeId = task.getAssignment().getExecutorNode();
        final var executorNodeName = clusterService.state().nodes().get(executorNodeId).getName();
        final var telemetryPlugin = getTelemetryPlugin(executorNodeName);

        assertBusy(() -> {
            telemetryPlugin.resetMeter();
            telemetryPlugin.collect();
            assertImbalanceHistogram(
                telemetryPlugin.getLongGaugeMeasurement(IndexBalanceMetricsTask.PRIMARY_IMBALANCE_METRIC_NAME),
                numIndices
            );
            assertImbalanceHistogram(
                telemetryPlugin.getLongGaugeMeasurement(IndexBalanceMetricsTask.REPLICA_IMBALANCE_METRIC_NAME),
                numIndices
            );
        });
    }

    private static void assertImbalanceHistogram(List<Measurement> measurements, int expectedIndexCount) {
        assertThat(measurements, hasSize(IndexBalanceMetrics.BUCKET_COUNT));
        assertThat(measurements.stream().mapToLong(Measurement::getLong).sum(), equalTo((long) expectedIndexCount));
        for (int i = 0; i < IndexBalanceMetrics.BUCKET_COUNT; i++) {
            assertThat(measurements.get(i).attributes().get("indexImbalanceBucket"), equalTo(IndexBalanceMetrics.BUCKET_LABELS[i]));
        }
    }

    private static TestTelemetryPlugin getTelemetryPlugin(String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
    }
}
