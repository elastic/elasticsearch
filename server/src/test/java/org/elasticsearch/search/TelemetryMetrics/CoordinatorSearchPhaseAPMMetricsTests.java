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
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CoordinatorSearchPhaseAPMMetricsTests extends ESSingleNodeTestCase {
    private static final String indexName = "test_coordinator_search_phase_metrics";
    private final int num_primaries = randomIntBetween(128, 133);

    private final Set<String> expectedMetrics = Set.of(
        "es.search.coordinator.phases.fetch_lookup_fields.duration.histogram",
        "es.search.coordinator.phases.fetch.duration.histogram",
        "es.search.coordinator.phases.expand.duration.histogram",
        "es.search.coordinator.phases.query.duration.histogram"
    );

    private final Set<String> expectedMetricsWithDfs = Set.of(
        "es.search.coordinator.phases.fetch_lookup_fields.duration.histogram",
        "es.search.coordinator.phases.fetch.duration.histogram",
        "es.search.coordinator.phases.expand.duration.histogram",
        "es.search.coordinator.phases.dfs_query.duration.histogram",
        "es.search.coordinator.phases.dfs.duration.histogram"
    );

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

        prepareIndex(CoordinatorSearchPhaseAPMMetricsTests.TestSystemIndexPlugin.INDEX_NAME).setId("1").setSource("body", "doc1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(CoordinatorSearchPhaseAPMMetricsTests.TestSystemIndexPlugin.INDEX_NAME).setId("2").setSource("body", "doc2").setRefreshPolicy(IMMEDIATE).get();
    }

    @After
    private void afterTest() {
        resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class, CoordinatorSearchPhaseAPMMetricsTests.TestSystemIndexPlugin.class);
    }

    public void testMetricsDfsQueryThenFetch() throws InterruptedException {
        checkMetricsDfsQueryThenFetch(indexName);
    }

    private void checkMetricsDfsQueryThenFetch(String indexName) throws InterruptedException {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );

        var coordinatorMetrics = filterForCoordinatorMetrics(
            getTestTelemetryPlugin().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM));
        assertThat(coordinatorMetrics, hasSize(5));
        assertThat(coordinatorMetrics, equalTo(expectedMetricsWithDfs));
    }

    public void testSearchTransportMetricsQueryThenFetch() throws InterruptedException {
        checkSearchTransportMetricsQueryThenFetch(indexName);
    }

    private void checkSearchTransportMetricsQueryThenFetch(String indexName) throws InterruptedException {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );

        var coordinatorMetrics = filterForCoordinatorMetrics(
            getTestTelemetryPlugin().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM));
        assertThat(coordinatorMetrics, hasSize(4));
        assertThat(coordinatorMetrics, equalTo(expectedMetrics));
    }


    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    private Set<String> filterForCoordinatorMetrics(List<String> registeredMetrics) {
        return registeredMetrics.stream()
            .filter(m -> m.startsWith("es.search.coordinator"))
            .collect(Collectors.toSet());
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
