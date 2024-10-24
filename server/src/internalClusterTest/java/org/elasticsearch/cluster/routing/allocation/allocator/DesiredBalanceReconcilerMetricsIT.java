/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Collection;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class DesiredBalanceReconcilerMetricsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    public void testDesiredBalanceGaugeMetricsAreOnlyPublishedByCurrentMaster() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(indexSettings(2, 1)).get();
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

    private static void assertOnlyMasterIsPublishingMetrics() {
        String masterNodeName = internalCluster().getMasterName();
        String[] nodeNames = internalCluster().getNodeNames();
        for (String nodeName : nodeNames) {
            assertMetricsAreBeingPublished(nodeName, nodeName.equals(masterNodeName));
        }
    }

    private static void assertMetricsAreBeingPublished(String nodeName, boolean shouldBePublishing) {
        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        testTelemetryPlugin.resetMeter();
        testTelemetryPlugin.collect();
        Matcher<Collection<?>> matcher = shouldBePublishing ? not(empty()) : empty();
        assertThat(testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.UNASSIGNED_SHARDS_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.TOTAL_SHARDS_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.UNDESIRED_ALLOCATION_COUNT_METRIC_NAME), matcher);
        assertThat(testTelemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.UNDESIRED_ALLOCATION_RATIO_METRIC_NAME), matcher);
    }
}
