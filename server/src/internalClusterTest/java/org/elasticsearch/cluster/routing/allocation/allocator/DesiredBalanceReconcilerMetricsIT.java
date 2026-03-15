/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.snapshots.SnapshotTestUtils.clearShutdownMetadata;
import static org.elasticsearch.snapshots.SnapshotTestUtils.putShutdownForRemovalMetadata;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DesiredBalanceReconcilerMetricsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    public void testDesiredBalanceGaugeMetricsAreOnlyPublishedByCurrentMaster() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(indexSettings(2, 1)).get();
        indexRandom(randomBoolean(), "test", between(50, 100));
        ensureGreen();

        assertOnlyMasterIsPublishingMetrics();

        // fail over and check again
        int numFailOvers = randomIntBetween(1, 3);
        for (int i = 0; i < numFailOvers; i++) {
            internalCluster().restartNode(internalCluster().getMasterName());
            ensureGreen();

            assertOnlyMasterIsPublishingMetrics();
        }
    }

    public void testDesiredBalanceMetrics() {
        internalCluster().startNodes(2);
        prepareCreate("test").setSettings(indexSettings(2, 1)).get();
        ensureGreen();

        indexRandom(randomBoolean(), "test", between(50, 100));
        flush("test");
        // Make sure new cluster info is available
        final var infoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.setUpdateFrequency(infoService, TimeValue.timeValueMillis(200));
        assertNotNull("info should not be null", ClusterInfoServiceUtils.refresh(infoService));
        ClusterRerouteUtils.reroute(client()); // ensure we leverage the latest cluster info

        final var telemetryPlugin = getTelemetryPlugin(internalCluster().getMasterName());
        telemetryPlugin.collect();
        assertThat(telemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.UNASSIGNED_SHARDS_METRIC_NAME), not(empty()));
        assertThat(telemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.TOTAL_SHARDS_METRIC_NAME), not(empty()));
        assertThat(telemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.UNDESIRED_ALLOCATION_COUNT_METRIC_NAME), not(empty()));
        assertThat(telemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.UNDESIRED_ALLOCATION_RATIO_METRIC_NAME), not(empty()));

        var nodeIds = internalCluster().clusterService().state().nodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        var nodeNames = internalCluster().clusterService().state().nodes().stream().map(DiscoveryNode::getName).collect(Collectors.toSet());

        final var desiredBalanceNodeWeightsMetrics = telemetryPlugin.getDoubleGaugeMeasurement(
            DesiredBalanceMetrics.DESIRED_BALANCE_NODE_WEIGHT_METRIC_NAME
        );
        assertThat(desiredBalanceNodeWeightsMetrics.size(), equalTo(2));
        for (var nodeStat : desiredBalanceNodeWeightsMetrics) {
            assertTrue(nodeStat.isDouble());
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        final var desiredBalanceNodeShardCountMetrics = telemetryPlugin.getLongGaugeMeasurement(
            DesiredBalanceMetrics.DESIRED_BALANCE_NODE_SHARD_COUNT_METRIC_NAME
        );
        assertThat(desiredBalanceNodeShardCountMetrics.size(), equalTo(2));
        for (var nodeStat : desiredBalanceNodeShardCountMetrics) {
            assertThat(nodeStat.value().longValue(), equalTo(2L));
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        final var desiredBalanceNodeWriteLoadMetrics = telemetryPlugin.getDoubleGaugeMeasurement(
            DesiredBalanceMetrics.DESIRED_BALANCE_NODE_WRITE_LOAD_METRIC_NAME
        );
        assertThat(desiredBalanceNodeWriteLoadMetrics.size(), equalTo(2));
        for (var nodeStat : desiredBalanceNodeWriteLoadMetrics) {
            assertThat(nodeStat.value().doubleValue(), greaterThanOrEqualTo(0.0));
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        final var desiredBalanceNodeDiskUsageMetrics = telemetryPlugin.getDoubleGaugeMeasurement(
            DesiredBalanceMetrics.DESIRED_BALANCE_NODE_DISK_USAGE_METRIC_NAME
        );
        assertThat(desiredBalanceNodeDiskUsageMetrics.size(), equalTo(2));
        for (var nodeStat : desiredBalanceNodeDiskUsageMetrics) {
            assertThat(nodeStat.value().doubleValue(), greaterThanOrEqualTo(0.0));
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        final var currentNodeWeightsMetrics = telemetryPlugin.getDoubleGaugeMeasurement(
            DesiredBalanceMetrics.CURRENT_NODE_WEIGHT_METRIC_NAME
        );
        assertThat(currentNodeWeightsMetrics.size(), equalTo(2));
        for (var nodeStat : currentNodeWeightsMetrics) {
            assertTrue(nodeStat.isDouble());
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        final var currentNodeShardCountMetrics = telemetryPlugin.getLongGaugeMeasurement(
            DesiredBalanceMetrics.CURRENT_NODE_SHARD_COUNT_METRIC_NAME
        );
        assertThat(currentNodeShardCountMetrics.size(), equalTo(2));
        for (var nodeStat : currentNodeShardCountMetrics) {
            assertThat(nodeStat.value().longValue(), equalTo(2L));
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        final var currentNodeWriteLoadMetrics = telemetryPlugin.getDoubleGaugeMeasurement(
            DesiredBalanceMetrics.CURRENT_NODE_WRITE_LOAD_METRIC_NAME
        );
        assertThat(currentNodeWriteLoadMetrics.size(), equalTo(2));
        for (var nodeStat : currentNodeWriteLoadMetrics) {
            assertThat(nodeStat.value().doubleValue(), greaterThanOrEqualTo(0.0));
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        final var currentNodeDiskUsageMetrics = telemetryPlugin.getLongGaugeMeasurement(
            DesiredBalanceMetrics.CURRENT_NODE_DISK_USAGE_METRIC_NAME
        );
        assertThat(currentNodeDiskUsageMetrics.size(), equalTo(2));
        for (var nodeStat : currentNodeDiskUsageMetrics) {
            assertThat(nodeStat.value().longValue(), greaterThanOrEqualTo(0L));
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        assertTrue(currentNodeDiskUsageMetrics.stream().anyMatch(m -> m.getLong() > 0L));
        final var currentNodeUndesiredShardCountMetrics = telemetryPlugin.getLongGaugeMeasurement(
            DesiredBalanceMetrics.CURRENT_NODE_UNDESIRED_SHARD_COUNT_METRIC_NAME
        );
        assertThat(currentNodeUndesiredShardCountMetrics.size(), equalTo(2));
        for (var nodeStat : currentNodeUndesiredShardCountMetrics) {
            assertThat(nodeStat.value().longValue(), greaterThanOrEqualTo(0L));
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        final var currentNodeForecastedDiskUsageMetrics = telemetryPlugin.getLongGaugeMeasurement(
            DesiredBalanceMetrics.CURRENT_NODE_FORECASTED_DISK_USAGE_METRIC_NAME
        );
        assertThat(currentNodeForecastedDiskUsageMetrics.size(), equalTo(2));
        for (var nodeStat : currentNodeForecastedDiskUsageMetrics) {
            assertThat(nodeStat.value().longValue(), greaterThanOrEqualTo(0L));
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
        }
        assertTrue(currentNodeForecastedDiskUsageMetrics.stream().anyMatch(m -> m.getLong() > 0L));
    }

    public void testReconcilerShardMovementMetrics() {
        internalCluster().startNodes(2);
        final TestTelemetryPlugin telemetryPlugin = getTelemetryPlugin(internalCluster().getMasterName());

        // Create an index, should result in 'unassigned' reconciler shard movement
        final var indexName = randomIdentifier();
        prepareCreate(indexName).setSettings(indexSettings(6, 1)).get();
        ensureGreen(indexName);

        telemetryPlugin.collect();
        List<Measurement> measurements = telemetryPlugin.getLongCounterMeasurement(
            DesiredBalanceMetrics.RECONCILER_SHARD_MOVEMENTS_METRIC_NAME
        );
        assertTrue(
            "expected at least one 'unassigned' shard movement when assigning the new index",
            measurements.stream()
                .anyMatch(
                    m -> DesiredBalanceMetrics.ShardMovementReason.UNASSIGNED.getAttributeValue()
                        .equals(m.attributes().get(DesiredBalanceMetrics.SHARD_MOVEMENT_REASON_ATTRIBUTE))
                )
        );

        // Trigger rebalance by adding a third node
        internalCluster().startNode();
        ClusterRerouteUtils.reroute(client());

        telemetryPlugin.collect();
        measurements = telemetryPlugin.getLongCounterMeasurement(DesiredBalanceMetrics.RECONCILER_SHARD_MOVEMENTS_METRIC_NAME);
        assertTrue(
            "expected at least one 'rebalance' shard movement when adding a node",
            measurements.stream()
                .anyMatch(
                    m -> DesiredBalanceMetrics.ShardMovementReason.REBALANCE.getAttributeValue()
                        .equals(m.attributes().get(DesiredBalanceMetrics.SHARD_MOVEMENT_REASON_ATTRIBUTE))
                )
        );

        // Trigger a 'cannot_remain' movement by marking one node for shutdown
        ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var state = clusterService.state();
        final var randomNodeId = randomFrom(state.routingTable().index(indexName).allActivePrimaries().toList()).currentNodeId();
        final var nodeNameToShutdown = state.nodes().get(randomNodeId).getName();
        putShutdownForRemovalMetadata(nodeNameToShutdown, clusterService);
        ClusterRerouteUtils.reroute(client());

        try {
            telemetryPlugin.collect();
            measurements = telemetryPlugin.getLongCounterMeasurement(DesiredBalanceMetrics.RECONCILER_SHARD_MOVEMENTS_METRIC_NAME);
            assertTrue(
                "expected at least one 'cannot_remain' shard movement when a node is shutting down",
                measurements.stream()
                    .anyMatch(
                        m -> DesiredBalanceMetrics.ShardMovementReason.CANNOT_REMAIN.getAttributeValue()
                            .equals(m.attributes().get(DesiredBalanceMetrics.SHARD_MOVEMENT_REASON_ATTRIBUTE))
                    )
            );
        } finally {
            clearShutdownMetadata(clusterService);
        }
    }

    private static void assertOnlyMasterIsPublishingMetrics() {
        String masterNodeName = internalCluster().getMasterName();
        String[] nodeNames = internalCluster().getNodeNames();
        for (String nodeName : nodeNames) {
            assertMetricsAreBeingPublished(nodeName, nodeName.equals(masterNodeName));
        }
    }

    private static void assertMetricsAreBeingPublished(String nodeName, boolean shouldBePublishing) {
        final TestTelemetryPlugin testTelemetryPlugin = getTelemetryPlugin(nodeName);
        testTelemetryPlugin.resetMeter();
        testTelemetryPlugin.collect();
        Matcher<Collection<?>> matcher = shouldBePublishing ? not(empty()) : empty();
        assertThat(testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.UNASSIGNED_SHARDS_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.TOTAL_SHARDS_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.UNDESIRED_ALLOCATION_COUNT_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.UNDESIRED_ALLOCATION_RATIO_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.DESIRED_BALANCE_NODE_WEIGHT_METRIC_NAME), matcher);
        assertThat(
            testTelemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.DESIRED_BALANCE_NODE_WRITE_LOAD_METRIC_NAME),
            matcher
        );
        assertThat(
            testTelemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.DESIRED_BALANCE_NODE_DISK_USAGE_METRIC_NAME),
            matcher
        );
        assertThat(
            testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.DESIRED_BALANCE_NODE_SHARD_COUNT_METRIC_NAME),
            matcher
        );
        assertThat(testTelemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.CURRENT_NODE_WEIGHT_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.CURRENT_NODE_WRITE_LOAD_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.CURRENT_NODE_DISK_USAGE_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.CURRENT_NODE_SHARD_COUNT_METRIC_NAME), matcher);
        assertThat(
            testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.CURRENT_NODE_FORECASTED_DISK_USAGE_METRIC_NAME),
            matcher
        );
        assertThat(
            testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.CURRENT_NODE_UNDESIRED_SHARD_COUNT_METRIC_NAME),
            matcher
        );
    }

    private static TestTelemetryPlugin getTelemetryPlugin(String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
    }
}
