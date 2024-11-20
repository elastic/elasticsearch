/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics.FETCH_SEARCH_PHASE_METRIC;
import static org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics.QUERY_SEARCH_PHASE_METRIC;
import static org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics.SYSTEM_THREAD_ATTRIBUTE_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;

public class ShardSearchPhaseAPMMetricsTests extends ESSingleNodeTestCase {

    private static final String indexName = "test_search_metrics2";
    private final int num_primaries = randomIntBetween(2, 7);

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Before
    private void setUpIndex() throws Exception {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource("body", "doc1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("2").setSource("body", "doc2").setRefreshPolicy(IMMEDIATE).get();

        prepareIndex(TestSystemIndexPlugin.INDEX_NAME).setId("1").setSource("body", "doc1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(TestSystemIndexPlugin.INDEX_NAME).setId("2").setSource("body", "doc2").setRefreshPolicy(IMMEDIATE).get();
    }

    @After
    private void afterTest() {
        resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class, TestSystemIndexPlugin.class);
    }

    public void testMetricsDfsQueryThenFetch() throws InterruptedException {
        checkMetricsDfsQueryThenFetch(indexName, false);
    }

    public void testMetricsDfsQueryThenFetchSystem() throws InterruptedException {
        checkMetricsDfsQueryThenFetch(TestSystemIndexPlugin.INDEX_NAME, true);
    }

    private void checkMetricsDfsQueryThenFetch(String indexName, boolean isSystemIndex) throws InterruptedException {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        checkNumberOfMeasurementsForPhase(QUERY_SEARCH_PHASE_METRIC, isSystemIndex);
        assertNotEquals(0, getNumberOfMeasurementsForPhase(FETCH_SEARCH_PHASE_METRIC));
        checkMetricsAttributes(isSystemIndex);
    }

    public void testSearchTransportMetricsQueryThenFetch() throws InterruptedException {
        checkSearchTransportMetricsQueryThenFetch(indexName, false);
    }

    public void testSearchTransportMetricsQueryThenFetchSystem() throws InterruptedException {
        checkSearchTransportMetricsQueryThenFetch(TestSystemIndexPlugin.INDEX_NAME, true);
    }

    private void checkSearchTransportMetricsQueryThenFetch(String indexName, boolean isSystemIndex) throws InterruptedException {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        checkNumberOfMeasurementsForPhase(QUERY_SEARCH_PHASE_METRIC, isSystemIndex);
        assertNotEquals(0, getNumberOfMeasurementsForPhase(FETCH_SEARCH_PHASE_METRIC));
        checkMetricsAttributes(isSystemIndex);
    }

    public void testSearchTransportMetricsScroll() throws InterruptedException {
        checkSearchTransportMetricsScroll(indexName, false);
    }

    public void testSearchTransportMetricsScrollSystem() throws InterruptedException {
        checkSearchTransportMetricsScroll(TestSystemIndexPlugin.INDEX_NAME, true);
    }

    private void checkSearchTransportMetricsScroll(String indexName, boolean isSystemIndex) throws InterruptedException {
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            client().prepareSearch(indexName)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(1)
                .setQuery(simpleQueryStringQuery("doc1 doc2")),
            2,
            (respNum, response) -> {
                // No hits, no fetching done
                assertEquals(isSystemIndex ? 1 : num_primaries, getNumberOfMeasurementsForPhase(QUERY_SEARCH_PHASE_METRIC));
                if (response.getHits().getHits().length > 0) {
                    assertNotEquals(0, getNumberOfMeasurementsForPhase(FETCH_SEARCH_PHASE_METRIC));
                } else {
                    assertEquals(isSystemIndex ? 1 : 0, getNumberOfMeasurementsForPhase(FETCH_SEARCH_PHASE_METRIC));
                }
                checkMetricsAttributes(isSystemIndex);
                resetMeter();
            }
        );

    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    private void checkNumberOfMeasurementsForPhase(String phase, boolean isSystemIndex) {
        int numMeasurements = getNumberOfMeasurementsForPhase(phase);
        assertEquals(isSystemIndex ? 1 : num_primaries, numMeasurements);
    }

    private int getNumberOfMeasurementsForPhase(String phase) {
        final List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(phase);
        return measurements.size();
    }

    private void checkMetricsAttributes(boolean isSystem) {
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        assertTrue(
            Stream.concat(queryMeasurements.stream(), fetchMeasurements.stream()).allMatch(m -> checkMeasurementAttributes(m, isSystem))
        );
    }

    private boolean checkMeasurementAttributes(Measurement m, boolean isSystem) {
        return ((boolean) m.attributes().get(SYSTEM_THREAD_ATTRIBUTE_NAME)) == isSystem;
    }

    public static class TestSystemIndexPlugin extends Plugin implements SystemIndexPlugin {

        static final String INDEX_NAME = ".test-system-index";

        public TestSystemIndexPlugin() {}

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(INDEX_NAME + "*")
                    .setPrimaryIndex(INDEX_NAME)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .build()
                    )
                    .setMappings("""
                          {
                            "_meta": {
                              "version": "8.0.0",
                              "managed_index_mappings_version": 3
                            },
                            "properties": {
                              "body": { "type": "keyword" }
                            }
                          }
                        """)
                    .setThreadPools(ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS)
                    .setOrigin(ShardSearchPhaseAPMMetricsTests.class.getSimpleName())
                    .build()
            );
        }

        @Override
        public String getFeatureName() {
            return ShardSearchPhaseAPMMetricsTests.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "test plugin";
        }
    }
}
