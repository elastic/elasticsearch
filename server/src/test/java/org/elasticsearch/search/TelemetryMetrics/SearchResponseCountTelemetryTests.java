/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.RESPONSE_COUNT_TOTAL_COUNTER_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SearchResponseCountTelemetryTests extends ESSingleNodeTestCase {

    private static final String indexName = "test_search_response_count_metrics";

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    @After
    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Before
    public void setUpIndex() throws Exception {
        var numPrimaries = randomIntBetween(3, 5);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numPrimaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource("body", "red").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("2").setSource("body", "green").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("3").setSource("body", "blue").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("4").setSource("body", "blue").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("5").setSource("body", "pink").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("6").setSource("body", "brown").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("7").setSource("body", "red").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("8").setSource("body", "purple").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("9").setSource("body", "black").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("10").setSource("body", "green").setRefreshPolicy(IMMEDIATE).get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class, TestQueryBuilderPlugin.class);
    }

    public static class TestQueryBuilderPlugin extends Plugin implements SearchPlugin {
        public TestQueryBuilderPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            QuerySpec<ThrowingQueryBuilder> throwingSpec = new QuerySpec<>(ThrowingQueryBuilder.NAME, ThrowingQueryBuilder::new, p -> {
                throw new IllegalStateException("not implemented");
            });

            return List.of(throwingSpec);
        }
    }

    public void testSimpleQuery() throws Exception {
        assertSearchHitsWithoutFailures(client().prepareSearch(indexName).setQuery(simpleQueryStringQuery("green")), "2", "10");
        assertBusy(() -> {
            List<Measurement> measurements = getTestTelemetryPlugin().getLongCounterMeasurement(RESPONSE_COUNT_TOTAL_COUNTER_NAME);
            assertThat(measurements.size(), equalTo(1));
            assertThat(measurements.get(0).getLong(), equalTo(1L));
            assertThat(
                measurements.get(0).attributes().get(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME),
                equalTo(SearchResponseMetrics.ResponseCountTotalStatus.SUCCESS.getDisplayName())
            );
        });
    }

    public void testSearchWithSingleShardFailure() throws Exception {
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("something bad"), 0);
        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(queryBuilder).get();
        try {
            assertThat(searchResponse.getFailedShards(), equalTo(1));
            assertBusy(() -> {
                List<Measurement> measurements = getTestTelemetryPlugin().getLongCounterMeasurement(RESPONSE_COUNT_TOTAL_COUNTER_NAME);
                assertThat(measurements.size(), equalTo(1));
                assertThat(measurements.get(0).getLong(), equalTo(1L));
                assertThat(
                    measurements.get(0).attributes().get(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME),
                    equalTo(SearchResponseMetrics.ResponseCountTotalStatus.PARTIAL_FAILURE.getDisplayName())
                );
            });
        } finally {
            searchResponse.decRef();
        }
    }

    public void testSearchWithAllShardsFail() throws Exception {
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("something bad"), indexName);
        SearchPhaseExecutionException exception = expectThrows(
            SearchPhaseExecutionException.class,
            client().prepareSearch(indexName).setQuery(queryBuilder)
        );
        assertThat(exception.getCause().getMessage(), containsString("something bad"));
        assertBusy(() -> {
            List<Measurement> measurements = getTestTelemetryPlugin().getLongCounterMeasurement(RESPONSE_COUNT_TOTAL_COUNTER_NAME);
            assertThat(measurements.size(), equalTo(1));
            assertThat(measurements.get(0).getLong(), equalTo(1L));
            assertThat(
                measurements.get(0).attributes().get(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME),
                equalTo(SearchResponseMetrics.ResponseCountTotalStatus.FAILURE.getDisplayName())
            );
        });
    }

    public void testScroll() {
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            client().prepareSearch(indexName).setSize(1).setQuery(simpleQueryStringQuery("green")),
            2,
            (respNum, response) -> {
                if (respNum <= 2) {
                    try {
                        assertBusy(() -> {
                            List<Measurement> measurements = getTestTelemetryPlugin().getLongCounterMeasurement(
                                RESPONSE_COUNT_TOTAL_COUNTER_NAME
                            );
                            assertThat(measurements.size(), equalTo(1));
                            assertThat(measurements.get(0).getLong(), equalTo(1L));
                            assertThat(
                                measurements.get(0).attributes().get(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME),
                                equalTo(SearchResponseMetrics.ResponseCountTotalStatus.SUCCESS.getDisplayName())
                            );
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                resetMeter();
            }
        );
    }

    public void testScrollWithSingleShardFailure() throws Exception {
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("something bad"), 0);
        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(indexName).setSize(1).setQuery(queryBuilder);
        TimeValue keepAlive = TimeValue.timeValueSeconds(60);
        searchRequestBuilder.setScroll(keepAlive);
        List<SearchResponse> responses = new ArrayList<>();
        var scrollResponse = searchRequestBuilder.get();
        responses.add(scrollResponse);
        try {
            assertBusy(() -> {
                List<Measurement> measurements = getTestTelemetryPlugin().getLongCounterMeasurement(RESPONSE_COUNT_TOTAL_COUNTER_NAME);
                assertThat(measurements.size(), equalTo(1));
                assertThat(measurements.get(0).getLong(), equalTo(1L));
                assertThat(
                    measurements.get(0).attributes().get(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME),
                    equalTo(SearchResponseMetrics.ResponseCountTotalStatus.PARTIAL_FAILURE.getDisplayName())
                );
            });
            int numResponses = 1;
            while (scrollResponse.getHits().getHits().length > 0) {
                scrollResponse = client().prepareSearchScroll(scrollResponse.getScrollId()).setScroll(keepAlive).get();
                int expectedNumMeasurements = ++numResponses;
                responses.add(scrollResponse);
                assertBusy(() -> {
                    List<Measurement> measurements = getTestTelemetryPlugin().getLongCounterMeasurement(RESPONSE_COUNT_TOTAL_COUNTER_NAME);
                    // verify that one additional measurement recorded (in TransportScrollSearchAction)
                    assertThat(measurements.size(), equalTo(expectedNumMeasurements));
                    // verify that zero shards failed in secondary scroll search rounds
                    assertThat(measurements.get(expectedNumMeasurements - 1).getLong(), equalTo(1L));
                    assertThat(
                        measurements.get(expectedNumMeasurements - 1).attributes().get(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME),
                        equalTo(SearchResponseMetrics.ResponseCountTotalStatus.SUCCESS.getDisplayName())
                    );
                });
            }
        } finally {
            ClearScrollResponse clear = client().prepareClearScroll().setScrollIds(Arrays.asList(scrollResponse.getScrollId())).get();
            responses.forEach(SearchResponse::decRef);
            assertThat(clear.isSucceeded(), equalTo(true));
        }
    }

    public void testScrollWithAllShardsFail() throws Exception {
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("something bad"), indexName);
        SearchPhaseExecutionException exception = expectThrows(
            SearchPhaseExecutionException.class,
            client().prepareSearch(indexName).setSize(1).setQuery(queryBuilder).setScroll(TimeValue.timeValueSeconds(60))
        );
        assertThat(exception.getCause().getMessage(), containsString("something bad"));
        assertBusy(() -> {
            List<Measurement> measurements = getTestTelemetryPlugin().getLongCounterMeasurement(RESPONSE_COUNT_TOTAL_COUNTER_NAME);
            assertThat(measurements.size(), equalTo(1));
            assertThat(measurements.get(0).getLong(), equalTo(1L));
            assertThat(
                measurements.get(0).attributes().get(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME),
                equalTo(SearchResponseMetrics.ResponseCountTotalStatus.FAILURE.getDisplayName())
            );
        });
    }
}
