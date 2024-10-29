/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DesiredBalanceReconcilerMetricsIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceReconcilerMetricsIT.class);

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

    public void testDesiredBalanceNodeWeightMetrics() {
        internalCluster().startNodes(2);
        prepareCreate("test").setSettings(indexSettings(2, 1)).get();
        indexRandom(randomBoolean(), "test", between(50, 100));
        ensureGreen();
        final var telemetryPlugin = getTelemetryPlugin(internalCluster().getMasterName());
        telemetryPlugin.collect();
        assertThat(telemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.UNASSIGNED_SHARDS_METRIC_NAME), not(empty()));
        assertThat(telemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.TOTAL_SHARDS_METRIC_NAME), not(empty()));
        assertThat(telemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.UNDESIRED_ALLOCATION_COUNT_METRIC_NAME), not(empty()));
        assertThat(telemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.UNDESIRED_ALLOCATION_RATIO_METRIC_NAME), not(empty()));
        final var weightsMetrics = telemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.NODE_BALANCE_WEIGHT_STATS_METRIC_NAME);
        assertThat(weightsMetrics.size(), equalTo(2));
        var nodeIds = internalCluster().clusterService().state().nodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        var nodeNames = internalCluster().clusterService().state().nodes().stream().map(DiscoveryNode::getName).collect(Collectors.toSet());
        for (var nodeStat : weightsMetrics) {
            assertThat(nodeStat.value().doubleValue(), greaterThanOrEqualTo(0.0));
            assertThat(nodeStat.attributes().get("shard_count"), equalTo(2));
            assertThat((Double) nodeStat.attributes().get("disk_usage_in_bytes"), greaterThanOrEqualTo(0.0));
            assertThat((Double) nodeStat.attributes().get("write_load"), greaterThanOrEqualTo(0.0));
            assertThat((String) nodeStat.attributes().get("node_id"), is(in(nodeIds)));
            assertThat((String) nodeStat.attributes().get("node_name"), is(in(nodeNames)));
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
        assertThat(testTelemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.NODE_BALANCE_WEIGHT_STATS_METRIC_NAME), matcher);
    }

    private static TestTelemetryPlugin getTelemetryPlugin(String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
    }
}
