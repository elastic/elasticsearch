/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.action.search.SearchRequestAttributesExtractor;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.retriever.RescorerRetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics.CAN_MATCH_SEARCH_PHASE_METRIC;
import static org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics.DFS_SEARCH_PHASE_METRIC;
import static org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics.FETCH_SEARCH_PHASE_METRIC;
import static org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics.QUERY_SEARCH_PHASE_METRIC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.hasSize;

public class ShardSearchPhaseAPMMetricsTests extends ESSingleNodeTestCase {

    private static final String indexName = "test_search_metrics2";
    private final int num_primaries = randomIntBetween(2, 7);

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Before
    public void setUpIndex() {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource("body", "doc1", "@timestamp", "2024-11-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("2").setSource("body", "doc2", "@timestamp", "2024-12-01").setRefreshPolicy(IMMEDIATE).get();

        prepareIndex(TestSystemIndexPlugin.INDEX_NAME).setId("1")
            .setSource("body", "doc1", "@timestamp", "2024-11-01")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(TestSystemIndexPlugin.INDEX_NAME).setId("2")
            .setSource("body", "doc2", "@timestamp", "2024-12-01")
            .setRefreshPolicy(IMMEDIATE)
            .get();
    }

    @After
    public void afterTest() {
        resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class, TestSystemIndexPlugin.class);
    }

    public void testMetricsDfsQueryThenFetch() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        final List<Measurement> dfsMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(DFS_SEARCH_PHASE_METRIC);
        assertEquals(num_primaries, dfsMeasurements.size());
        assertAttributes(dfsMeasurements, false, false);
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        assertEquals(num_primaries, queryMeasurements.size());
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
        assertEquals(1, fetchMeasurements.size());
        assertAttributes(fetchMeasurements, false, false);
    }

    public void testMetricsDfsQueryThenFetchSystem() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(TestSystemIndexPlugin.INDEX_NAME)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        final List<Measurement> dfsMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(DFS_SEARCH_PHASE_METRIC);
        assertEquals(0, dfsMeasurements.size()); // DFS phase not done for index with single shard
        assertAttributes(dfsMeasurements, true, false);
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        assertEquals(1, queryMeasurements.size());
        assertAttributes(queryMeasurements, true, false);
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
        assertEquals(1, fetchMeasurements.size());
        assertAttributes(fetchMeasurements, true, false);
    }

    public void testSearchTransportMetricsQueryThenFetch() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        assertEquals(num_primaries, queryMeasurements.size());
        assertAttributes(queryMeasurements, false, false);
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
        assertEquals(1, fetchMeasurements.size());
        assertAttributes(fetchMeasurements, false, false);
    }

    public void testSearchTransportMetricsQueryThenFetchSystem() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(TestSystemIndexPlugin.INDEX_NAME)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        assertEquals(1, queryMeasurements.size());
        assertAttributes(queryMeasurements, true, false);
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
        assertEquals(1, fetchMeasurements.size());
        assertAttributes(fetchMeasurements, true, false);
    }

    public void testSearchMultipleIndices() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName, TestSystemIndexPlugin.INDEX_NAME)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(simpleQueryStringQuery("doc1")),
            "1",
            "1"
        );
        {
            final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
            assertEquals(num_primaries + 1, queryMeasurements.size());
            int userTarget = 0;
            int systemTarget = 0;
            for (Measurement measurement : queryMeasurements) {
                Map<String, Object> attributes = measurement.attributes();
                assertEquals(4, attributes.size());

                String target = attributes.get("target").toString();
                if (target.equals("user")) {
                    userTarget++;
                } else {
                    systemTarget++;
                    assertEquals(".others", target);
                    assertEquals(true, measurement.attributes().get(SearchRequestAttributesExtractor.SYSTEM_THREAD_ATTRIBUTE_NAME));
                }
                assertEquals("hits_only", attributes.get("query_type"));
                assertEquals("_score", attributes.get("sort"));
            }
            assertEquals(num_primaries, userTarget);
            assertEquals(1, systemTarget);
        }
        {
            final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
            assertEquals(2, fetchMeasurements.size());
            int userTarget = 0;
            int systemTarget = 0;
            for (Measurement measurement : fetchMeasurements) {
                Map<String, Object> attributes = measurement.attributes();
                assertEquals(4, attributes.size());

                String target = attributes.get("target").toString();
                if (target.equals("user")) {
                    userTarget++;
                } else {
                    systemTarget++;
                    assertEquals(".others", target);
                    assertEquals(true, measurement.attributes().get(SearchRequestAttributesExtractor.SYSTEM_THREAD_ATTRIBUTE_NAME));
                }
                assertEquals("hits_only", attributes.get("query_type"));
                assertEquals("_score", attributes.get("sort"));
            }
            assertEquals(1, userTarget);
            assertEquals(1, systemTarget);
        }
    }

    public void testSearchTransportMetricsScroll() {
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            client().prepareSearch(indexName).setSize(1).setQuery(simpleQueryStringQuery("doc1 doc2")),
            2,
            (respNum, response) -> {
                final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
                assertEquals(num_primaries, queryMeasurements.size());
                assertAttributes(queryMeasurements, false, true);
                // No hits, no fetching done
                if (response.getHits().getHits().length > 0) {
                    final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(
                        FETCH_SEARCH_PHASE_METRIC
                    );
                    assertThat(fetchMeasurements.size(), Matchers.greaterThan(0));
                    int numFetchShards = Math.min(2, num_primaries);
                    assertThat(fetchMeasurements.size(), Matchers.lessThanOrEqualTo(numFetchShards));
                    assertAttributes(fetchMeasurements, false, true);
                } else {
                    final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(
                        FETCH_SEARCH_PHASE_METRIC
                    );
                    assertEquals(0, fetchMeasurements.size());
                }
                resetMeter();
            }
        );
    }

    public void testSearchTransportMetricsScrollSystem() {
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            client().prepareSearch(TestSystemIndexPlugin.INDEX_NAME).setSize(1).setQuery(simpleQueryStringQuery("doc1 doc2")),
            2,
            (respNum, response) -> {
                final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
                assertEquals(1, queryMeasurements.size());
                assertAttributes(queryMeasurements, true, true);
                final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
                assertEquals(1, fetchMeasurements.size());
                assertAttributes(fetchMeasurements, true, true);
                resetMeter();
            }
        );
    }

    public void testCanMatchSearch() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPreFilterShardSize(1)
                .setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );

        final List<Measurement> canMatchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(CAN_MATCH_SEARCH_PHASE_METRIC);
        assertEquals(num_primaries, canMatchMeasurements.size());
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        assertEquals(num_primaries, queryMeasurements.size());
        assertAttributes(queryMeasurements, false, false);
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
        assertEquals(1, fetchMeasurements.size());
        assertAttributes(fetchMeasurements, false, false);
    }

    private static void assertAttributes(List<Measurement> measurements, boolean isSystem, boolean isScroll) {
        for (Measurement measurement : measurements) {
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(isScroll ? 5 : 4, attributes.size());
            if (isSystem) {
                assertEquals(".others", attributes.get("target"));
            } else {
                assertEquals("user", attributes.get("target"));
            }
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
            if (isScroll) {
                assertEquals("scroll", attributes.get("pit_scroll"));
            }
            assertEquals(isSystem, attributes.get(SearchRequestAttributesExtractor.SYSTEM_THREAD_ATTRIBUTE_NAME));
        }
    }

    public void testTimeRangeFilterOneResult() {
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("@timestamp").from("2024-12-01");
        // target the system index because it has one shard, that simplifies testing. Otherwise, only when the two docs end up indexed
        // on the same shard do you get the time range as attribute.
        assertSearchHitsWithoutFailures(client().prepareSearch(TestSystemIndexPlugin.INDEX_NAME).setQuery(rangeQueryBuilder), "2");
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        assertEquals(1, queryMeasurements.size());
        assertTimeRangeAttributes(queryMeasurements, ".others", true, false);
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
        assertEquals(1, fetchMeasurements.size());
        assertTimeRangeAttributes(fetchMeasurements, ".others", true, false);
    }

    public void testTimeRangeFilterRetrieverOneResult() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.retriever(new StandardRetrieverBuilder(new RangeQueryBuilder("@timestamp").from("2024-12-01")));
        // target the system index because it has one shard, that simplifies testing. Otherwise, only when the two docs end up indexed
        // on the same shard do you get the time range as attribute.
        assertSearchHitsWithoutFailures(client().prepareSearch(TestSystemIndexPlugin.INDEX_NAME).setSource(searchSourceBuilder), "2");
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        assertEquals(1, queryMeasurements.size());
        assertTimeRangeAttributes(queryMeasurements, ".others", true, false);
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
        assertEquals(1, fetchMeasurements.size());
        assertTimeRangeAttributes(fetchMeasurements, ".others", true, false);
    }

    public void testTimeRangeFilterCompoundRetrieverOneResult() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.retriever(
            new RescorerRetrieverBuilder(
                new StandardRetrieverBuilder(new RangeQueryBuilder("@timestamp").from("2024-12-01")),
                List.of(new QueryRescorerBuilder(new MatchAllQueryBuilder()))
            )
        );
        // target the system index because it has one shard, that simplifies testing. Otherwise, only when the two docs end up indexed
        // on the same shard do you get the time range as attribute.
        assertSearchHitsWithoutFailures(client().prepareSearch(TestSystemIndexPlugin.INDEX_NAME).setSource(searchSourceBuilder), "2");
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        // compound retriever does its own search as an async action, whose metrics are recorded separately
        assertEquals(2, queryMeasurements.size());
        assertTimeRangeAttributes(queryMeasurements, ".others", true, true);
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
        assertEquals(2, fetchMeasurements.size());
        assertTimeRangeAttributes(fetchMeasurements, ".others", true, true);
    }

    private static void assertTimeRangeAttributes(List<Measurement> measurements, String target, boolean isSystem, boolean isPit) {
        for (Measurement measurement : measurements) {
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(isPit ? 7 : 6, attributes.size());
            assertEquals(target, attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
            assertEquals("@timestamp", attributes.get("time_range_filter_field"));
            assertEquals(isSystem, attributes.get(SearchRequestAttributesExtractor.SYSTEM_THREAD_ATTRIBUTE_NAME));
            assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
            if (isPit) {
                assertEquals("pit", attributes.get("pit_scroll"));
            }
        }
    }

    public void testTimeRangeFilterAllResults() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2024-10-01"));
        // enable can match: empty shards get filtered out by the can match round
        assertResponse(client().prepareSearch(indexName).setPreFilterShardSize(1).setQuery(boolQueryBuilder), searchResponse -> {
            assertNoFailures(searchResponse);
            assertHitCount(searchResponse, 2);
            assertSearchHits(searchResponse, "1", "2");
            assertThat(searchResponse.getSkippedShards(), Matchers.greaterThanOrEqualTo(num_primaries - 2));
        });
        final List<Measurement> canMatchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(CAN_MATCH_SEARCH_PHASE_METRIC);
        assertEquals(num_primaries, canMatchMeasurements.size());
        for (Measurement measurement : canMatchMeasurements) {
            Map<String, Object> attributes = measurement.attributes();
            assertThat(attributes.size(), Matchers.greaterThanOrEqualTo(4));
            assertEquals("user", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
            assertEquals(false, attributes.get(SearchRequestAttributesExtractor.SYSTEM_THREAD_ATTRIBUTE_NAME));
            // the shards that get short-cut filtered because they are empty won't have time range attributes in their metric attributes
            if (attributes.containsKey("time_range_filter_from")) {
                assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
            }
        }
        final List<Measurement> queryMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(QUERY_SEARCH_PHASE_METRIC);
        // the two docs are at most spread across two shards, other shards are empty and get filtered out
        assertThat(queryMeasurements.size(), Matchers.lessThanOrEqualTo(2));
        for (Measurement measurement : queryMeasurements) {
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(5, attributes.size());
            assertEquals("user", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
            assertEquals(false, attributes.get(SearchRequestAttributesExtractor.SYSTEM_THREAD_ATTRIBUTE_NAME));
            // the range query was rewritten to one without bounds: we do track the time range filter from value but we don't set
            // the time range filter field because no range query is executed at the shard level.
            assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
        }
        final List<Measurement> fetchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(FETCH_SEARCH_PHASE_METRIC);
        // in this case, each shard queried has results to be fetched
        assertEquals(queryMeasurements.size(), fetchMeasurements.size());
        // no range info stored because we had no bounds after rewrite, basically a match_all
        for (Measurement measurement : fetchMeasurements) {
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(4, attributes.size());
            assertEquals("user", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
            assertEquals(false, attributes.get(SearchRequestAttributesExtractor.SYSTEM_THREAD_ATTRIBUTE_NAME));
            // no time range filter bucketing on the fetch phase, because the query was rewritten to one without bounds
        }
    }

    public void testUniformCanMatchMetricAttributesWhenPlentyOfDocumentsInIndex() {
        // create an index with a large number of documents so no shard should be empty and no shard will be short-circuited in can match
        String indexName = "every_shard_has_documents";
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        LocalDate baseDate = LocalDate.of(2024, 11, 1);
        for (int i = 1; i <= num_primaries * 2; i++) {
            LocalDate docDate = baseDate.plusMonths(i - 1);
            prepareIndex(indexName).setId(Integer.toString(i))
                .setSource("body", "doc" + i, "@timestamp", docDate.format(DateTimeFormatter.ISO_LOCAL_DATE))
                .setRefreshPolicy(IMMEDIATE)
                .get();

        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2024-10-01"));
        assertResponse(client().prepareSearch(indexName).setPreFilterShardSize(1).setQuery(boolQueryBuilder), searchResponse -> {
            assertNoFailures(searchResponse);
            assertHitCount(searchResponse, num_primaries * 2L);
        });
        final List<Measurement> canMatchMeasurements = getTestTelemetryPlugin().getLongHistogramMeasurement(CAN_MATCH_SEARCH_PHASE_METRIC);
        assertEquals(num_primaries, canMatchMeasurements.size());
        for (Measurement measurement : canMatchMeasurements) {
            Map<String, Object> attributes = measurement.attributes();
            assertThat(attributes.entrySet(), hasSize(5));
            assertEquals("user", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
            assertEquals(false, attributes.get(SearchRequestAttributesExtractor.SYSTEM_THREAD_ATTRIBUTE_NAME));
            assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
        }
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
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
