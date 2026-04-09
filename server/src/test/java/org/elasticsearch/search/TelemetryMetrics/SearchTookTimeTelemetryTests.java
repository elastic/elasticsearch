/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.retriever.RescorerRetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.TOOK_DURATION_TOTAL_HISTOGRAM_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class SearchTookTimeTelemetryTests extends ESSingleNodeTestCase {
    private static final String indexName = "test_search_metrics2";
    private static final String indexNameNanoPrecision = "nano_search_metrics2";
    private static final String singleShardIndexName = "single_shard_test_search_metric";
    private static final LocalDateTime NOW = LocalDateTime.now(ZoneOffset.UTC);
    private static final DateTimeFormatter FORMATTER_MILLIS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ROOT);
    private static final DateTimeFormatter FORMATTER_NANOS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn", Locale.ROOT);

    @Before
    public void setUpIndex() {
        var num_primaries = randomIntBetween(2, 4);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);
        prepareIndex(indexName).setId("1")
            .setSource("body", "foo", "@timestamp", "2024-11-01", "event.ingested", "2024-11-01")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(indexName).setId("2")
            .setSource("body", "foo", "@timestamp", "2024-12-01", "event.ingested", "2024-12-01")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // we use a single shard index to test the case where query and fetch execute in the same round-trip
        createIndex(
            singleShardIndexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(singleShardIndexName);
        prepareIndex(singleShardIndexName).setId("1")
            .setSource("body", "foo", "@timestamp", NOW.minusMinutes(5).withSecond(randomIntBetween(0, 59)).format(FORMATTER_MILLIS))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(singleShardIndexName).setId("2")
            .setSource("body", "foo", "@timestamp", NOW.minusMinutes(30).withSecond(randomIntBetween(0, 59)).format(FORMATTER_MILLIS))
            .setRefreshPolicy(IMMEDIATE)
            .get();

        createIndex(
            indexNameNanoPrecision,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build(),
            "@timestamp",
            "type=date_nanos"
        );
        ensureGreen(indexNameNanoPrecision);
        prepareIndex(indexNameNanoPrecision).setId("10")
            .setSource(
                "body",
                "foo",
                "@timestamp",
                NOW.minusMinutes(2).withNano(randomIntBetween(0, 1_000_000_000)).format(FORMATTER_NANOS)
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(indexNameNanoPrecision).setId("11")
            .setSource(
                "body",
                "foo",
                "@timestamp",
                NOW.minusMinutes(3).withNano(randomIntBetween(0, 1_000_000_000)).format(FORMATTER_NANOS)
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(indexNameNanoPrecision).setId("12")
            .setSource(
                "body",
                "foo",
                "@timestamp",
                NOW.minusMinutes(4).withNano(randomIntBetween(0, 1_000_000_000)).format(FORMATTER_NANOS)
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(indexNameNanoPrecision).setId("13")
            .setSource(
                "body",
                "foo",
                "@timestamp",
                NOW.minusMinutes(5).withNano(randomIntBetween(0, 1_000_000_000)).format(FORMATTER_NANOS)
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(indexNameNanoPrecision).setId("14")
            .setSource(
                "body",
                "foo",
                "@timestamp",
                NOW.minusMinutes(6).withNano(randomIntBetween(0, 1_000_000_000)).format(FORMATTER_NANOS)
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(indexNameNanoPrecision).setId("15")
            .setSource(
                "body",
                "foo",
                "@timestamp",
                NOW.minusMinutes(75).withNano(randomIntBetween(0, 1_000_000_000)).format(FORMATTER_NANOS)
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
    }

    @After
    public void afterTest() {
        resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class);
    }

    public void testOthersDottedIndexName() {
        createIndex(".whatever");
        createIndex(".kibana");
        {
            SearchResponse searchResponse = client().prepareSearch(".whatever").setQuery(simpleQueryStringQuery("foo")).get();
            try {
                assertNoFailures(searchResponse);
                assertSearchHits(searchResponse);
            } finally {
                searchResponse.decRef();
            }
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
            assertEquals(1, measurements.size());
            Measurement measurement = measurements.getFirst();
            assertEquals(searchResponse.getTook().millis(), measurement.getLong());
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(3, attributes.size());
            assertEquals(".others", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
        }
        {
            SearchResponse searchResponse = client().prepareSearch(".kibana*").setQuery(simpleQueryStringQuery("foo")).get();
            try {
                assertNoFailures(searchResponse);
                assertSearchHits(searchResponse);
            } finally {
                searchResponse.decRef();
            }
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
            assertEquals(2, measurements.size());
            Measurement measurement = measurements.getLast();
            assertEquals(searchResponse.getTook().millis(), measurement.getLong());
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(3, attributes.size());
            assertEquals(".kibana", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
        }
        {
            SearchResponse searchResponse = client().prepareSearch(".*").setQuery(simpleQueryStringQuery("foo")).get();
            try {
                assertNoFailures(searchResponse);
                assertSearchHits(searchResponse);
            } finally {
                searchResponse.decRef();
            }
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
            assertEquals(3, measurements.size());
            Measurement measurement = measurements.getLast();
            assertEquals(searchResponse.getTook().millis(), measurement.getLong());
            // two dotted indices: categorized as "user"
            assertSimpleQueryAttributes(measurement.attributes());
        }
        {
            SearchResponse searchResponse = client().prepareSearch(".kibana", ".whatever").setQuery(simpleQueryStringQuery("foo")).get();
            try {
                assertNoFailures(searchResponse);
                assertSearchHits(searchResponse);
            } finally {
                searchResponse.decRef();
            }
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
            assertEquals(4, measurements.size());
            Measurement measurement = measurements.getLast();
            assertEquals(searchResponse.getTook().millis(), measurement.getLong());
            // two dotted indices: categorized as "user"
            assertSimpleQueryAttributes(measurement.attributes());
        }
        {
            SearchResponse searchResponse = client().prepareSearch(".kibana", ".does_not_exist")
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .setQuery(simpleQueryStringQuery("foo"))
                .get();
            try {
                assertNoFailures(searchResponse);
                assertSearchHits(searchResponse);
            } finally {
                searchResponse.decRef();
            }
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
            assertEquals(5, measurements.size());
            Measurement measurement = measurements.getLast();
            assertEquals(searchResponse.getTook().millis(), measurement.getLong());
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(3, attributes.size());
            // because the second index does not exist, yet the search goes through, the remaining index is categorized correctly
            assertEquals(".kibana", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
        }
        {
            SearchResponse searchResponse = client().prepareSearch("_all").setQuery(simpleQueryStringQuery("foo")).get();
            try {
                assertNoFailures(searchResponse);
                assertSearchHits(searchResponse, "1", "2", "1", "2", "10", "11", "12", "13", "14", "15");
            } finally {
                searchResponse.decRef();
            }
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
            assertEquals(6, measurements.size());
            Measurement measurement = measurements.getLast();
            assertEquals(searchResponse.getTook().millis(), measurement.getLong());
            assertSimpleQueryAttributes(measurement.attributes());
        }
    }

    public void testIndexNameMustExist() {
        SearchResponse searchResponse = client().prepareSearch(".must_exist")
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setQuery(simpleQueryStringQuery("foo"))
            .get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse);
        } finally {
            searchResponse.decRef();
        }
        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        // edge case rather than under .others (as it's a dotted index name), the index is categorized under "user" because no existing
        // indices are targeted.
        assertSimpleQueryAttributes(measurement.attributes());
    }

    public void testSimpleQuery() {
        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(simpleQueryStringQuery("foo")).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        assertSimpleQueryAttributes(measurement.attributes());
    }

    public void testSimpleQueryAgainstWildcardExpression() {
        SearchResponse searchResponse = client().prepareSearch("test*").setQuery(simpleQueryStringQuery("foo")).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        assertSimpleQueryAttributes(measurement.attributes());
    }

    public void testSimpleQueryAgainstAlias() {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest(
            RestUtils.REST_MASTER_TIMEOUT_DEFAULT,
            new TimeValue(30, TimeUnit.SECONDS)
        );
        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().indices(indexName).alias(".alias"));
        IndicesAliasesResponse indicesAliasesResponse = client().admin().indices().aliases(indicesAliasesRequest).actionGet();
        assertFalse(indicesAliasesResponse.hasErrors());
        SearchResponse searchResponse = client().prepareSearch(".alias").setQuery(simpleQueryStringQuery("foo")).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        assertSimpleQueryAttributes(measurement.attributes());
    }

    private static void assertSimpleQueryAttributes(Map<String, Object> attributes) {
        assertEquals(3, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("_score", attributes.get("sort"));
    }

    public void testCompoundRetriever() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.retriever(
            new RescorerRetrieverBuilder(
                new StandardRetrieverBuilder(new MatchAllQueryBuilder()),
                List.of(new QueryRescorerBuilder(new MatchAllQueryBuilder()))
            )
        );
        SearchResponse searchResponse = client().prepareSearch(indexName).setSource(searchSourceBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        // compound retriever does its own search as an async action, whose took time is recorded separately
        assertEquals(2, measurements.size());
        assertThat(measurements.getFirst().getLong(), Matchers.lessThanOrEqualTo(searchResponse.getTook().millis()));
        assertEquals(searchResponse.getTook().millis(), measurements.getLast().getLong());
        for (Measurement measurement : measurements) {
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(4, attributes.size());
            assertEquals("user", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
            assertEquals("pit", attributes.get("pit_scroll"));
        }
    }

    public void testMultiSearch() {
        MultiSearchRequestBuilder multiSearchRequestBuilder = client().prepareMultiSearch();
        int numSearchRequests = randomIntBetween(3, 10);
        for (int i = 0; i < numSearchRequests; i++) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().query(simpleQueryStringQuery("foo")));
            multiSearchRequestBuilder.add(searchRequest);
        }
        List<Long> tookTimes;
        MultiSearchResponse multiSearchResponse = null;
        try {
            multiSearchResponse = multiSearchRequestBuilder.get();
            tookTimes = Arrays.stream(multiSearchResponse.getResponses())
                .map(item -> item.getResponse().getTook().millis())
                .sorted()
                .toList();
        } finally {
            if (multiSearchResponse != null) {
                multiSearchResponse.decRef();
            }
        }
        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(numSearchRequests, measurements.size());
        measurements.sort(Comparator.comparing(Measurement::getLong));

        int i = 0;
        for (Measurement measurement : measurements) {
            assertEquals(tookTimes.get(i++).longValue(), measurement.getLong());
            assertSimpleQueryAttributes(measurement.attributes());
        }
    }

    public void testScroll() {
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            client().prepareSearch(indexName).setSize(1).setQuery(simpleQueryStringQuery("foo")),
            2,
            (respNum, response) -> {
                if (respNum == 1) {
                    List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(
                        TOOK_DURATION_TOTAL_HISTOGRAM_NAME
                    );
                    assertEquals(1, measurements.size());
                    Measurement measurement = measurements.getFirst();
                    Map<String, Object> attributes = measurement.attributes();
                    assertEquals(4, attributes.size());
                    assertEquals("user", attributes.get("target"));
                    assertEquals("hits_only", attributes.get("query_type"));
                    assertEquals("scroll", attributes.get("pit_scroll"));
                    assertEquals("_score", attributes.get("sort"));
                } else {
                    List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(
                        TOOK_DURATION_TOTAL_HISTOGRAM_NAME
                    );
                    assertEquals(1, measurements.size());
                    Measurement measurement = measurements.getFirst();
                    Map<String, Object> attributes = measurement.attributes();
                    assertEquals(1, attributes.size());
                    assertEquals("scroll", attributes.get("query_type"));
                }
                resetMeter();
            }
        );
    }

    /**
     * Make sure that despite can match and query rewrite, we see the time range filter and record its corresponding attribute
     */
    public void testTimeRangeFilterNoResults() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2025-01-01"));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(indexName).setPreFilterShardSize(1).setQuery(boolQueryBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse);
            // can match kicked in, query got rewritten to match_none, yet we extracted the time range before rewrite
            assertEquals(searchResponse.getSkippedShards(), searchResponse.getTotalShards());
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        Map<String, Object> attributes = measurement.attributes();
        assertEquals(4, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("_score", attributes.get("sort"));
        assertEquals("@timestamp", attributes.get("time_range_filter_field"));
        // there were no results, and no shards queried, hence no range filter extracted from the query either
    }

    /**
     * Make sure that despite can match and query rewrite, we see the time range filter and record its corresponding attribute
     */
    public void testTimeRangeFilterAllResults() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2024-10-01"));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(indexName).setPreFilterShardSize(1).setQuery(boolQueryBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        // in this case the range query gets rewritten to a range query with open bounds on the shards. Here we test that query rewrite
        // is able to grab the parsed range filter and propagate it all the way to the search response
        assertTimeRangeAttributes(measurement.attributes());
    }

    public void testTimeRangeFilterOneResult() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2024-12-01"));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(indexName).setPreFilterShardSize(1).setQuery(boolQueryBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        assertTimeRangeAttributes(measurement.attributes());
    }

    private static void assertTimeRangeAttributes(Map<String, Object> attributes) {
        assertEquals(5, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("_score", attributes.get("sort"));
        assertEquals("@timestamp", attributes.get("time_range_filter_field"));
        assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
    }

    public void testTimeRangeFilterAllResultsFilterOnEventIngested() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("event.ingested").from("2024-10-01"));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(indexName).setPreFilterShardSize(1).setQuery(boolQueryBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        Map<String, Object> attributes = measurement.attributes();
        assertEquals(5, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("_score", attributes.get("sort"));
        assertEquals("event.ingested", attributes.get("time_range_filter_field"));
        assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
    }

    public void testTimeRangeFilterAllResultsFilterOnEventIngestedAndTimestamp() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("event.ingested").from("2024-10-01"));
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2024-10-01"));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(indexName).setPreFilterShardSize(1).setQuery(boolQueryBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        Map<String, Object> attributes = measurement.attributes();
        assertEquals(5, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("_score", attributes.get("sort"));
        assertEquals("@timestamp_AND_event.ingested", attributes.get("time_range_filter_field"));
        assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
    }

    public void testTimeRangeFilterOneResultQueryAndFetchRecentTimestamps() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from(FORMATTER_MILLIS.format(NOW.minusMinutes(10))));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(singleShardIndexName)
            .setQuery(boolQueryBuilder)
            .addSort(new FieldSortBuilder("@timestamp"))
            .get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        Map<String, Object> attributes = measurement.attributes();
        assertEquals(5, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("@timestamp", attributes.get("sort"));
        assertEquals("@timestamp", attributes.get("time_range_filter_field"));
        assertEquals("15_minutes", attributes.get("time_range_filter_from"));
    }

    public void testMultipleTimeRangeFiltersQueryAndFetchRecentTimestamps() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        // we take the lowest of the two bounds
        boolQueryBuilder.must(new RangeQueryBuilder("@timestamp").from(FORMATTER_MILLIS.format(NOW.minusMinutes(20))));
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from(FORMATTER_MILLIS.format(NOW.minusMinutes(10))));
        // should and must_not get ignored
        boolQueryBuilder.should(new RangeQueryBuilder("@timestamp").from(FORMATTER_MILLIS.format(NOW.minusMinutes(2))));
        boolQueryBuilder.mustNot(new RangeQueryBuilder("@timestamp").from(FORMATTER_MILLIS.format(NOW.minusMinutes(1))));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(singleShardIndexName)
            .setQuery(boolQueryBuilder)
            .addSort(new FieldSortBuilder("@timestamp"))
            .get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        Map<String, Object> attributes = measurement.attributes();
        assertEquals(5, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("@timestamp", attributes.get("sort"));
        assertEquals("@timestamp", attributes.get("time_range_filter_field"));
        assertEquals("1_hour", attributes.get("time_range_filter_from"));
    }

    public void testTimeRangeFilterAllResultsShouldClause() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(new RangeQueryBuilder("@timestamp").from("2024-10-01"));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(boolQueryBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        assertSimpleQueryAttributes(measurement.attributes());
    }

    public void testTimeRangeFilterOneResultMustNotClause() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.mustNot(new RangeQueryBuilder("@timestamp").from("2024-12-01"));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(boolQueryBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        assertSimpleQueryAttributes(measurement.attributes());
    }

    public void testTimeRangeFilterAllResultsNanoPrecision() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from(FORMATTER_NANOS.format(NOW.minusMinutes(20))));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(indexNameNanoPrecision).setQuery(boolQueryBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "10", "11", "12", "13", "14");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        Map<String, Object> attributes = measurement.attributes();
        assertEquals(5, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("_score", attributes.get("sort"));
        assertEquals("@timestamp", attributes.get("time_range_filter_field"));
        assertEquals("1_hour", attributes.get("time_range_filter_from"));
    }

    public void testTimeRangeFilterAllResultsMixedPrecision() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from(FORMATTER_NANOS.format(NOW.minusMinutes(20))));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SearchResponse searchResponse = client().prepareSearch(singleShardIndexName, indexNameNanoPrecision)
            .setQuery(boolQueryBuilder)
            .get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "10", "11", "12", "13", "14");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(searchResponse.getTook().millis(), measurement.getLong());
        Map<String, Object> attributes = measurement.attributes();
        assertEquals(5, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("_score", attributes.get("sort"));
        assertEquals("@timestamp", attributes.get("time_range_filter_field"));
        assertEquals("1_hour", attributes.get("time_range_filter_from"));
    }

    public void testStandardRetrieverWithTimeRangeQuery() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.retriever(new StandardRetrieverBuilder(new RangeQueryBuilder("event.ingested").from("2024-12-01")));
        SearchResponse searchResponse = client().prepareSearch(indexName).setSource(searchSourceBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        assertThat(measurements.getFirst().getLong(), Matchers.lessThanOrEqualTo(searchResponse.getTook().millis()));
        assertEquals(searchResponse.getTook().millis(), measurements.getLast().getLong());
        for (Measurement measurement : measurements) {
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(5, attributes.size());
            assertEquals("user", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
            assertEquals("event.ingested", attributes.get("time_range_filter_field"));
            assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
        }
    }

    public void testCompoundRetrieverWithTimeRangeQuery() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.retriever(
            new RescorerRetrieverBuilder(
                new StandardRetrieverBuilder(new RangeQueryBuilder("@timestamp").from("2024-12-01")),
                List.of(new QueryRescorerBuilder(new MatchAllQueryBuilder()))
            )
        );
        SearchResponse searchResponse = client().prepareSearch(indexName).setSource(searchSourceBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        // compound retriever does its own search as an async action, whose took time is recorded separately
        assertEquals(2, measurements.size());
        assertThat(measurements.getFirst().getLong(), Matchers.lessThan(searchResponse.getTook().millis()));
        assertEquals(searchResponse.getTook().millis(), measurements.getLast().getLong());
        for (Measurement measurement : measurements) {
            Map<String, Object> attributes = measurement.attributes();
            assertEquals(6, attributes.size());
            assertEquals("user", attributes.get("target"));
            assertEquals("hits_only", attributes.get("query_type"));
            assertEquals("_score", attributes.get("sort"));
            assertEquals("pit", attributes.get("pit_scroll"));
            assertEquals("@timestamp", attributes.get("time_range_filter_field"));
            assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
        }
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().getFirst();
    }
}
