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

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.search.stats.CanMatchPhaseAPMMetrics.CAN_MATCH_SEARCH_PHASE_COORDINATING_NODE_METRIC;
import static org.elasticsearch.index.search.stats.CanMatchPhaseAPMMetrics.CAN_MATCH_SEARCH_PHASE_PER_SHARD_METRIC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;

public class CanMatchPhaseAPMMetricsTests extends ESSingleNodeTestCase {

    private static final String indexName = "test_can_match_metrics2";
    private final int num_primaries = randomIntBetween(128, 133);

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

    public void testSearchTransportMetricsQueryThenFetch() throws InterruptedException {
        checkSearchTransportMetricsQueryThenFetch(indexName, false);
    }

    private void checkSearchTransportMetricsQueryThenFetch(String indexName, boolean isSystemIndex) throws InterruptedException {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        checkNumberOfMeasurementsForMeter(CAN_MATCH_SEARCH_PHASE_PER_SHARD_METRIC, isSystemIndex);
        assertNotEquals(0, getNumberOfMeasurementsForMeter(CAN_MATCH_SEARCH_PHASE_COORDINATING_NODE_METRIC));
        // checkMetricsAttributes(isSystemIndex);
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    private void checkNumberOfMeasurementsForMeter(String meter, boolean isSystemIndex) {
        int numMeasurements = getNumberOfMeasurementsForMeter(meter);
        assertEquals(isSystemIndex ? 1 : num_primaries, numMeasurements);
    }

    private int getNumberOfMeasurementsForMeter(String meter) {
        final List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(meter);
        return measurements.size();
    }

    /*
    private void checkMetricsAttributes(boolean isSystem) {
        final List<Measurement> shardMeasurements =
            getTestTelemetryPlugin().getLongHistogramMeasurement(CAN_MATCH_SEARCH_PHASE_PER_SHARD_METRIC);
        final List<Measurement> coordinatorMeasurements =
            getTestTelemetryPlugin().getLongHistogramMeasurement(CAN_MATCH_SEARCH_PHASE_COORDINATING_NODE_METRIC);
        assertTrue(
            Stream.concat(shardMeasurements.stream(), coordinatorMeasurements.stream()).allMatch(m -> checkMeasurementAttributes(m, isSystem))
        );
    }

    private boolean checkMeasurementAttributes(Measurement m, boolean isSystem) {
        return ((boolean) m.attributes().get(SYSTEM_THREAD_ATTRIBUTE_NAME)) == isSystem;
    }
    */
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
                    .setOrigin(CanMatchPhaseAPMMetricsTests.class.getSimpleName())
                    .build()
            );
        }

        @Override
        public String getFeatureName() {
            return CanMatchPhaseAPMMetricsTests.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "test plugin";
        }
    }
}
