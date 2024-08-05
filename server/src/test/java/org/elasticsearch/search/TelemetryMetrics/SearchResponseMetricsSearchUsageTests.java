/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.QUERY_USAGE_SUFFIX;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.QUERY_USAGE_TAG_PREFIX;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.RESCORER_USAGE_SUFFIX;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.RESPONSE_COUNT_TOTAL_COUNTER_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.SECTION_USAGE_SUFFIX;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.TOOK_DURATION_TOTAL_HISTOGRAM_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SearchResponseMetricsSearchUsageTests extends ESSingleNodeTestCase {

    private static final String INDEX_NAME = "test_search_response_count_metrics";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class, SearchResponseCountTelemetryTests.TestQueryBuilderPlugin.class);
    }

    @After
    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Before
    public void setUpIndex() throws Exception {
        var numPrimaries = randomIntBetween(3, 5);
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numPrimaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(INDEX_NAME);
    }

    public void testResponseCountSearchUsage() throws Exception {
        checkMeasurementsForSearchRequest(() -> getTestTelemetryPlugin().getLongCounterMeasurement(RESPONSE_COUNT_TOTAL_COUNTER_NAME));
    }

    public void testTookDurationSearchUsage() throws Exception {
        checkMeasurementsForSearchRequest(() -> getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME));
    }

    private void checkMeasurementsForSearchRequest(Provider<List<Measurement>> measurementsProvider) throws Exception {
        SearchUsage searchUsage = new SearchUsage();
        Set<String> queries = randomSet(0, 10, () -> randomAlphaOfLength(10));
        queries.forEach(searchUsage::trackQueryUsage);
        Set<String> sections = randomSet(0, 10, () -> randomAlphaOfLength(10));
        sections.forEach(searchUsage::trackSectionUsage);
        Set<String> rescorers = randomSet(0, 10, () -> randomAlphaOfLength(10));
        rescorers.forEach(searchUsage::trackRescorerUsage);

        SearchRequestBuilder requestBuilder = client().prepareSearch(INDEX_NAME)
            .setSearchUsage(searchUsage)
            .setQuery(simpleQueryStringQuery("green"));
        assertNoFailures(requestBuilder);
        assertBusy(() -> {
            List<Measurement> measurements = measurementsProvider.get();
            assertThat(measurements.size(), equalTo(1));
            Measurement measurement = measurements.get(0);
            int searchUsageAttrs = queries.size() + sections.size() + rescorers.size();
            assertThat(measurement.attributes().size(), greaterThanOrEqualTo(searchUsageAttrs));
            for (int i = 0; i < searchUsageAttrs; i++) {
                String usageAttr = (String) measurement.attributes().get(QUERY_USAGE_TAG_PREFIX + i);
                assertNotNull(usageAttr);
                int suffixIndex = usageAttr.lastIndexOf("_");
                String usage = usageAttr.substring(0, suffixIndex);
                String usageType = usageAttr.substring(suffixIndex);
                if (usageType.equals(QUERY_USAGE_SUFFIX)) {
                    assertTrue(queries.contains(usage));
                } else if (usageType.equals(SECTION_USAGE_SUFFIX)) {
                    assertTrue(sections.contains(usage));
                } else if (usageType.equals(RESCORER_USAGE_SUFFIX)) {
                    assertTrue(rescorers.contains(usage));
                } else {
                    fail("unexpected usage type: " + usageType);
                }
            }
        });
    }
}
