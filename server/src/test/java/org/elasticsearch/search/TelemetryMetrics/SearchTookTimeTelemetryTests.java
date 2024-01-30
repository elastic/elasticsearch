/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.MatcherAssert;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.TOOK_DURATION_TOTAL_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.TOOK_DURATION_TOTAL_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.TOOK_MEASUREMENT_NUM_COUNT_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.greaterThan;

public class SearchTookTimeTelemetryTests extends ESSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(
            TestTelemetryPlugin.class
        );
    }

    public void testSearchTransportMetricsDfsQueryThenFetch() {
        var indexName = "test1";
        var num_primaries = randomIntBetween(1, 4);
        createIndex(indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(2, 7))
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).
                build()
        );

        prepareIndex("index").setId("1").setSource("body", "foo").setRefreshPolicy(IMMEDIATE).get();

        assertSearchHitsWithoutFailures(
            client().prepareSearch("index").setQuery(simpleQueryStringQuery("foo")),
            "1"
        );
        MatcherAssert.assertThat(getNumberOfLongCounterMeasurements(TOOK_MEASUREMENT_NUM_COUNT_NAME), greaterThan(0));
        MatcherAssert.assertThat(getNumberOfLongCounterMeasurements(TOOK_DURATION_TOTAL_NAME), greaterThan(0));
        MatcherAssert.assertThat(getNumberOfLongHistogramMeasurements(TOOK_DURATION_TOTAL_HISTOGRAM_NAME), greaterThan(0));
        resetMeter();
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    private int getNumberOfLongHistogramMeasurements(String instrumentName) {
        final List<Measurement> measurements = getTestTelemetryPlugin()
            .getLongHistogramMeasurement(instrumentName);
        return measurements.size();
    }

    private int getNumberOfLongCounterMeasurements(String instrumentName) {
        final List<Measurement> measurements = getTestTelemetryPlugin()
            .getLongCounterMeasurement(instrumentName);
        return measurements.size();
    }
}
