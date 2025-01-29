/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.TOOK_DURATION_TOTAL_HISTOGRAM_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.greaterThan;

public class SearchTookTimeTelemetryTests extends ESSingleNodeTestCase {
    private static final String indexName = "test_search_metrics2";

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Before
    public void setUpIndex() throws Exception {
        var num_primaries = randomIntBetween(1, 4);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource("body", "foo").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("2").setSource("body", "foo").setRefreshPolicy(IMMEDIATE).get();
    }

    @After
    private void afterTest() {
        resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class);
    }

    public void testSimpleQuery() {
        assertSearchHitsWithoutFailures(client().prepareSearch(indexName).setQuery(simpleQueryStringQuery("foo")), "1", "2");
        assertMetricsRecorded();
    }

    public void testScroll() {
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            client().prepareSearch(indexName).setSize(1).setQuery(simpleQueryStringQuery("foo")),
            2,
            (respNum, response) -> {
                if (respNum <= 2) {
                    assertMetricsRecorded();
                }
                resetMeter();
            }
        );
    }

    private void assertMetricsRecorded() {
        MatcherAssert.assertThat(getNumberOfLongHistogramMeasurements(TOOK_DURATION_TOTAL_HISTOGRAM_NAME), greaterThan(0));
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private int getNumberOfLongHistogramMeasurements(String instrumentName) {
        final List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(instrumentName);
        return measurements.size();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }
}
