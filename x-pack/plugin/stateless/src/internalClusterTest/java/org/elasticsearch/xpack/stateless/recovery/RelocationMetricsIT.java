/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.recovery.metering.RecoveryMetricsCollector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class RelocationMetricsIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    public void testRelocationPhaseMetricsRecorded() {
        final String sourceNode = startMasterAndIndexNode();

        final String indexName = "test-relocation-metrics";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // Flush multiple times so the BCC chain is non-trivial.
        int numBCCs = randomIntBetween(5, 10);
        for (int i = 0; i < numBCCs; i++) {
            indexDocs(indexName, 100);
            flush(indexName);
        }

        // Start a second index node to be the relocation target.
        final String targetNode = startIndexNode();
        ensureStableCluster(2);

        // Reset measurements on both nodes so we observe only the relocation event.
        getTelemetryPlugin(sourceNode).resetMeter();
        getTelemetryPlugin(targetNode).resetMeter();

        // Trigger relocation: exclude the source, wait for green on target.
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", sourceNode), indexName);
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(sourceNode)));

        // Source records all four source-side phase histograms.
        final TestTelemetryPlugin sourceTelemetry = getTelemetryPlugin(sourceNode);
        sourceTelemetry.collect();
        assertHistogramRecorded(sourceTelemetry, RecoveryMetricsCollector.RELOCATION_INITIAL_FLUSH_TIME_METRIC_IN_SECONDS);
        assertHistogramRecorded(sourceTelemetry, RecoveryMetricsCollector.RELOCATION_ACQUIRE_PERMITS_TIME_METRIC_IN_SECONDS);
        assertHistogramRecorded(sourceTelemetry, RecoveryMetricsCollector.RELOCATION_SECOND_FLUSH_TIME_METRIC_IN_SECONDS);
        assertHistogramRecorded(sourceTelemetry, RecoveryMetricsCollector.RELOCATION_HANDOFF_TIME_METRIC_IN_SECONDS);

        // Target records each handoff sub-phase histogram.
        final TestTelemetryPlugin targetTelemetry = getTelemetryPlugin(targetNode);
        targetTelemetry.collect();
        assertHistogramRecorded(targetTelemetry, RecoveryMetricsCollector.RELOCATION_TARGET_PRE_RECOVERY_TIME_METRIC_IN_SECONDS);
        assertHistogramRecorded(
            targetTelemetry,
            RecoveryMetricsCollector.RELOCATION_TARGET_READ_INDEXING_SHARD_STATE_TIME_METRIC_IN_SECONDS
        );
        assertHistogramRecorded(targetTelemetry, RecoveryMetricsCollector.RELOCATION_TARGET_OPEN_ENGINE_TIME_METRIC_IN_SECONDS);
    }

    private static void assertHistogramRecorded(TestTelemetryPlugin telemetry, String metricName) {
        List<Measurement> measurements = telemetry.getDoubleHistogramMeasurement(metricName);
        assertThat(metricName + " should have at least one sample", measurements.size(), greaterThanOrEqualTo(1));
        for (Measurement m : measurements) {
            // We use ThreadPool.relativeTimeInMillis in production code to measure elapsed time, and that caches the value,
            // so it's quite likely that the times are 0 since the recovery is quite fast in the test.
            assertThat(metricName + " values must be non-negative", m.getDouble(), greaterThanOrEqualTo(0.0));
        }
    }
}
