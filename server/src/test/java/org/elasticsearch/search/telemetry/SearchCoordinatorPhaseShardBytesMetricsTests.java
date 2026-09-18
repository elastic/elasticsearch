/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.telemetry;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.action.search.SearchResponseMetrics.DFS_QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.DFS_QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.RANK_FEATURE_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.RANK_FEATURE_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.telemetry.InstrumentType.LONG_HISTOGRAM;

/**
 * Unit tests for the coordinator-level shard request and shard result bytes histograms in
 * {@link SearchResponseMetrics}.
 */
public class SearchCoordinatorPhaseShardBytesMetricsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class);
    }

    /**
     * {@link SearchResponseMetrics#recordSearchPhaseShardResultBytes} records to the correct
     * histogram for each tracked phase.
     */
    public void testRecordSearchPhaseShardResponseBytesDispatch() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        SearchResponseMetrics metrics = new SearchResponseMetrics(registry);

        Map<String, Object> attrs = Map.of();
        metrics.recordSearchPhaseShardResultBytes("query", 100, attrs);
        metrics.recordSearchPhaseShardResultBytes("fetch", 200, attrs);
        metrics.recordSearchPhaseShardResultBytes("dfs", 50, attrs);
        metrics.recordSearchPhaseShardResultBytes("dfs_query", 75, attrs);
        metrics.recordSearchPhaseShardResultBytes("rank-feature", 30, attrs);

        assertRecordedValue(registry, QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME, 100);
        assertRecordedValue(registry, FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME, 200);
        assertRecordedValue(registry, DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME, 50);
        assertRecordedValue(registry, DFS_QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME, 75);
        assertRecordedValue(registry, RANK_FEATURE_SHARD_RESULT_BYTES_HISTOGRAM_NAME, 30);
    }

    /**
     * {@link SearchResponseMetrics#recordSearchPhaseShardRequestBytes} records to the correct
     * histogram for each tracked phase.
     */
    public void testRecordSearchPhaseShardRequestBytesDispatch() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        SearchResponseMetrics metrics = new SearchResponseMetrics(registry);

        Map<String, Object> attrs = Map.of();
        metrics.recordSearchPhaseShardRequestBytes("query", 100, attrs);
        metrics.recordSearchPhaseShardRequestBytes("fetch", 200, attrs);
        metrics.recordSearchPhaseShardRequestBytes("dfs", 50, attrs);
        metrics.recordSearchPhaseShardRequestBytes("dfs_query", 75, attrs);
        metrics.recordSearchPhaseShardRequestBytes("rank-feature", 30, attrs);

        assertRecordedValue(registry, QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME, 100);
        assertRecordedValue(registry, FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME, 200);
        assertRecordedValue(registry, DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME, 50);
        assertRecordedValue(registry, DFS_QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME, 75);
        assertRecordedValue(registry, RANK_FEATURE_SHARD_REQUEST_BYTES_HISTOGRAM_NAME, 30);
    }

    private static void assertRecordedValue(RecordingMeterRegistry registry, String metricName, long expectedValue) {
        List<Measurement> measurements = registry.getRecorder().getMeasurements(LONG_HISTOGRAM, metricName);
        assertEquals("expected 1 measurement for " + metricName, 1, measurements.size());
        assertEquals("expected value " + expectedValue + " for " + metricName, expectedValue, measurements.get(0).getLong());
    }
}
